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

import com.hazelcast.core.EntryListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This interface describes a common record store for replicated maps and their actual records
 */
public interface ReplicatedRecordStore {

    String getName();

    Object remove(Data key);

    void evict(Data key);

    void removeTombstone(Data key);

    Object get(Data key);

    Object put(Data key, Object value);

    Object put(Data key, Object value, long ttl, TimeUnit timeUnit);

    boolean containsKey(Data key);

    boolean containsValue(Object value);

    ReplicatedRecord getReplicatedRecord(Data key);

    Set keySet();

    Collection values();

    Collection values(Comparator comparator);

    Set entrySet();

    int size();

    void clear(boolean distribute, boolean emptyReplicationQueue);

    boolean isEmpty();

    String addEntryListener(EntryListener listener, Data key);

    String addEntryListener(EntryListener listener, Predicate predicate, Data key);

    Data marshall(Object object);

    Object unmarshall(Object object);

    boolean removeEntryListenerInternal(String id);

    ReplicationPublisher getReplicationPublisher();

    void destroy();

}
