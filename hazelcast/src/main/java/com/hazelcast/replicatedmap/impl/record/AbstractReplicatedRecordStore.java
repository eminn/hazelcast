/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.messages.ReplicationMessage;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * This is the base class for all {@link ReplicatedRecordStore} implementations
 *
 * @param <V> value type
 */
public abstract class AbstractReplicatedRecordStore<V> extends AbstractBaseReplicatedRecordStore<V> {
    static final String CLEAR_REPLICATION_MAGIC_KEY = ReplicatedMapService.SERVICE_NAME + "$CLEAR$MESSAGE$";

    // entries are not removed on replicatedMap.remove() as it would reset a vector clock and we wouldn't be able to
    // order subsequent events related to the entry. a tombstone is created instead. this constant says how long we
    // keep the tombstone alive. if there is no event in this period then the tombstone is removed.
    static final int TOMBSTONE_REMOVAL_PERIOD_MS = 5 * 60 * 1000;

    public AbstractReplicatedRecordStore(String name, NodeEngine nodeEngine, ReplicatedMapService replicatedMapService) {

        super(name, nodeEngine, replicatedMapService);
    }

    @Override
    public Object unmarshall(Object object) {
        return object == null ? null : nodeEngine.toObject(object);
    }

    @Override
    public Data marshall(Object object) {
        return object == null ? null : nodeEngine.toData(object);
    }


    @Override
    public void removeTombstone(Data key) {
        isNotNull(key, "key");
        storage.checkState();
        ReplicatedRecord<V> current = storage.get(key);
        if (current == null || current.getValueInternal() != null) {
            return;
        }
        storage.remove(key, current);
    }

    @Override
    public Object remove(Data key) {
        isNotNull(key, "key");
        long time = Clock.currentTimeMillis();
        storage.checkState();
        V oldValue;
        final ReplicatedRecord current = storage.get(key);
        if (current == null) {
            oldValue = null;
        } else {
            oldValue = (V) current.getValueInternal();
            if (oldValue != null) {
                current.setValue(null, localMemberHash, TOMBSTONE_REMOVAL_PERIOD_MS);
                scheduleTtlEntry(TOMBSTONE_REMOVAL_PERIOD_MS, key, null);
                ReplicationMessage message = buildReplicationMessage(key, null, TOMBSTONE_REMOVAL_PERIOD_MS);
                replicationPublisher.publishReplicatedMessage(message);
            }
        }
        Object unmarshalledOldValue = unmarshall(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, null);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementRemoves(Clock.currentTimeMillis() - time);
        }
        return oldValue;
    }

    @Override
    public void evict(Data key) {
        isNotNull(key, "key");
        long time = Clock.currentTimeMillis();
        storage.checkState();
        V oldValue;
        final ReplicatedRecord current = storage.get(key);
        if (current == null) {
            oldValue = null;
        } else {
            oldValue = (V) current.getValueInternal();
            if (oldValue != null) {
                current.setValueInternal(null, localMemberHash, TOMBSTONE_REMOVAL_PERIOD_MS);
                scheduleTtlEntry(TOMBSTONE_REMOVAL_PERIOD_MS, key, null);
            }
        }
        Object unmarshalledOldValue = unmarshall(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, null, EntryEventType.EVICTED);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementRemoves(Clock.currentTimeMillis() - time);
        }
    }

    @Override
    public Object get(Data key) {
        isNotNull(key, "key");
        long time = Clock.currentTimeMillis();
        storage.checkState();
        ReplicatedRecord replicatedRecord = storage.get(key);

        // Force return null on ttl expiration (but before cleanup thread run)
        long ttlMillis = replicatedRecord == null ? 0 : replicatedRecord.getTtlMillis();
        if (ttlMillis > 0 && Clock.currentTimeMillis() - replicatedRecord.getUpdateTime() >= ttlMillis) {
            replicatedRecord = null;
        }

        Object value = replicatedRecord == null ? null : unmarshall(replicatedRecord.getValue());
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementGets(Clock.currentTimeMillis() - time);
        }
        return value;
    }

    @Override
    public Object put(Data key, Object value) {
        isNotNull(key, "key");
        isNotNull(value, "value");
        storage.checkState();
        return put(key, value, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public Object put(Data key, Object value, long ttl, TimeUnit timeUnit) {
        isNotNull(key, "key");
        isNotNull(value, "value");
        isNotNull(timeUnit, "timeUnit");
        if (ttl < 0) {
            throw new IllegalArgumentException("ttl must be a positive integer");
        }
        long time = Clock.currentTimeMillis();
        storage.checkState();
        V oldValue = null;
        final long ttlMillis = ttl == 0 ? 0 : timeUnit.toMillis(ttl);
        final ReplicatedRecord<V> old = storage.get(key);
        ReplicatedRecord<V> record = old;
        if (old == null) {
            record = buildReplicatedRecord(key, value, ttlMillis);
            storage.put(key, record);
        } else {
            oldValue = old.getValueInternal();
            storage.get(key).setValue((V) value, localMemberHash, ttlMillis);
        }
        if (ttlMillis > 0) {
            scheduleTtlEntry(ttlMillis, key, value);
        } else {
            cancelTtlEntry(key);
        }

        ReplicationMessage message = buildReplicationMessage(key, value, ttlMillis);
        replicationPublisher.publishReplicatedMessage(message);
        fireEntryListenerEvent(key, oldValue, value);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementPuts(Clock.currentTimeMillis() - time);
        }
        Object unmarshalledOldValue = unmarshall(oldValue);
        return unmarshalledOldValue;
    }

    @Override
    public boolean containsKey(Data key) {
        isNotNull(key, "key");
        storage.checkState();
        mapStats.incrementOtherOperations();

        return containsKeyAndValue(key);
    }

    // IMPORTANT >> Increments hit counter
    private boolean containsKeyAndValue(Data key) {
        ReplicatedRecord replicatedRecord = storage.get(key);
        return replicatedRecord != null && replicatedRecord.getValue() != null;
    }

    @Override
    public boolean containsValue(Object value) {
        isNotNull(value, "value");
        storage.checkState();
        mapStats.incrementOtherOperations();
        for (Map.Entry<Data, ReplicatedRecord<V>> entry : storage.entrySet()) {
            V entryValue = entry.getValue().getValue();
            if (compare(value, entryValue)) {
                return true;
            }
        }
        return false;
    }


    @Override
    public Set keySet() {
        storage.checkState();
        mapStats.incrementOtherOperations();

        // Lazy evaluation to prevent to much copying
        return new LazySet<Data, V, Data>(new KeySetIteratorFactory<Data, V>(this), storage);
    }

    @Override
    public Collection values() {
        storage.checkState();
        mapStats.incrementOtherOperations();

        // Lazy evaluation to prevent to much copying
        return new LazyCollection<Data, V>(new ValuesIteratorFactory<Data, V>(this), storage);
    }

    @Override
    public Collection values(Comparator comparator) {
        storage.checkState();
        List values = new ArrayList(storage.size());
        for (ReplicatedRecord record : storage.values()) {
            values.add(unmarshall(record.getValue()));
        }
        Collections.sort(values, comparator);
        mapStats.incrementOtherOperations();
        return values;
    }

    @Override
    public Set entrySet() {
        storage.checkState();
        mapStats.incrementOtherOperations();

        // Lazy evaluation to prevent to much copying
        return new LazySet<Data, V, Map.Entry<Data, V>>(new EntrySetIteratorFactory<Data, V>(this), storage);
    }

    @Override
    public ReplicatedRecord getReplicatedRecord(Data key) {
        isNotNull(key, "key");
        storage.checkState();
        return storage.get(key);
    }

    @Override
    public boolean isEmpty() {
        mapStats.incrementOtherOperations();
        return storage.isEmpty();
    }

    @Override
    public int size() {
        mapStats.incrementOtherOperations();
        return storage.size();
    }

    @Override
    public void clear(boolean distribute, boolean emptyReplicationQueue) {
        storage.checkState();
        if (emptyReplicationQueue) {
            replicationPublisher.emptyReplicationQueue();
        }
        storage.clear();
        if (distribute) {
            replicationPublisher.distributeClear(emptyReplicationQueue);
        }
        mapStats.incrementOtherOperations();
    }

    @Override
    public String addEntryListener(EntryListener listener, Data key) {
        isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedEntryEventFilter(key);
        mapStats.incrementOtherOperations();
        return replicatedMapService.addEventListener(listener, eventFilter, getName());
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate predicate, Data key) {
        isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedQueryEventFilter(key, predicate);
        mapStats.incrementOtherOperations();
        return replicatedMapService.addEventListener(listener, eventFilter, getName());
    }

    @Override
    public boolean removeEntryListenerInternal(String id) {
        isNotNull(id, "id");
        mapStats.incrementOtherOperations();
        return replicatedMapService.removeEventListener(getName(), id);
    }

    private ReplicationMessage buildReplicationMessage(Data key, Object value, long ttlMillis) {
        return new ReplicationMessage(getName(), key, marshall(value), localMember, nodeEngine.getPartitionService().getPartitionId(key), ttlMillis);
    }

    abstract ReplicatedRecord buildReplicatedRecord(Data key, Object value, long ttlMillis);

    abstract boolean isEquals(Object value1, Object value2);

    public boolean compare(Object value1, Object value2) {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null) {
            return false;
        }
        if (value2 == null) {
            return false;
        }
        return isEquals(value1, value2);
    }


}
