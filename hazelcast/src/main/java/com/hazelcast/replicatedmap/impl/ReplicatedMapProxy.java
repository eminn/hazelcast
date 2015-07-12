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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * The internal {@link com.hazelcast.core.ReplicatedMap} implementation proxying the requests to the underlying
 * {@code ReplicatedRecordStore}
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReplicatedMapProxy<K, V>
        extends AbstractDistributedObject
        implements ReplicatedMap<K, V>, InitializingObject {

    private final AbstractReplicatedRecordStore<V> replicatedRecordStore;
    private final OperationService operationService;
    private final InternalPartitionService partitionService;
    private SerializationService serializationService;

    ReplicatedMapProxy(NodeEngine nodeEngine, AbstractReplicatedRecordStore<V> replicatedRecordStore) {
        super(nodeEngine, replicatedRecordStore.getReplicatedMapService());
        this.replicatedRecordStore = replicatedRecordStore;
        this.operationService = getNodeEngine().getOperationService();
        this.serializationService = getNodeEngine().getSerializationService();
        partitionService = getNodeEngine().getPartitionService();
    }

    @Override
    public String getName() {
        return replicatedRecordStore.getName();
    }

    @Override
    public String getPartitionKey() {
        return getName();
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public int size() {
        return replicatedRecordStore.size();
    }

    @Override
    public boolean isEmpty() {
        return replicatedRecordStore.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        Data dataKey = serializationService.toData(key);
        return replicatedRecordStore.containsKey(dataKey);
    }

    @Override
    public boolean containsValue(Object value) {
        return replicatedRecordStore.containsValue(value);
    }

    @Override
    public V get(Object key) {
        Data dataKey = replicatedRecordStore.marshall(key);
        return (V) replicatedRecordStore.get(dataKey);
    }

    @Override
    public V put(K key, V value) {
        Data dataKey = serializationService.toData(key);
        Data dataValue = serializationService.toData(value);
        PutOperation op = new PutOperation(getName(), dataKey, dataValue);
        int partitionId = partitionService.getPartitionId(dataKey);
        InternalCompletableFuture<Object> future = operationService.invokeOnPartition(ReplicatedMapService.SERVICE_NAME, op, partitionId);
        return (V) serializationService.toObject(future.getSafely());
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        Data dataKey = serializationService.toData(key);
        Data dataValue = serializationService.toData(value);
        PutOperation op = new PutOperation(getName(), dataKey, dataValue);
        int partitionId = partitionService.getPartitionId(dataKey);
        InternalCompletableFuture<Object> future = operationService.invokeOnPartition(ReplicatedMapService.SERVICE_NAME, op, partitionId);
        return (V) serializationService.toObject(future.getSafely());
    }

    @Override
    public V remove(Object key) {
        Data dataKey = replicatedRecordStore.marshall(key);
        return (V) replicatedRecordStore.remove(dataKey);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        checkNotNull(m, "m cannot be null");
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        replicatedRecordStore.clear(true, true);
    }

    @Override
    public boolean removeEntryListener(String id) {
        return replicatedRecordStore.removeEntryListenerInternal(id);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        return replicatedRecordStore.addEntryListener(listener, null);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        Data dataKey = replicatedRecordStore.marshall(key);
        return replicatedRecordStore.addEntryListener(listener, dataKey);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        return replicatedRecordStore.addEntryListener(listener, predicate, null);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        Data dataKey = replicatedRecordStore.marshall(key);
        return replicatedRecordStore.addEntryListener(listener, predicate, dataKey);
    }

    @Override
    public Set<K> keySet() {
        return replicatedRecordStore.keySet();
    }

    @Override
    public Collection<V> values() {
        return replicatedRecordStore.values();
    }

    @Override
    public Collection<V> values(Comparator<V> comparator) {
        return replicatedRecordStore.values(comparator);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return replicatedRecordStore.entrySet();
    }


    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + replicatedRecordStore.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " -> " + replicatedRecordStore.getName();
    }

    @Override
    public void initialize() {
        replicatedRecordStore.initialize();
    }

    public LocalReplicatedMapStats getReplicatedMapStats() {
        return replicatedRecordStore.createReplicatedMapStats();
    }

}
