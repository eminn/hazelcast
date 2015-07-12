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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.NodeEngine;

/**
 * This is a {@link com.hazelcast.nio.serialization.Data} based {@link ReplicatedRecordStore}
 * implementation
 */
public class DataReplicatedRecordStore
        extends AbstractReplicatedRecordStore<Data> {

    private final NodeEngine nodeEngine;

    public DataReplicatedRecordStore(String name, NodeEngine nodeEngine,
                                     ReplicatedMapService replicatedMapService) {
        super(name, nodeEngine, replicatedMapService);
        this.nodeEngine = nodeEngine;
    }

    ReplicatedRecord buildReplicatedRecord(Data key, Object value, long ttlMillis) {
        Data dataValue = nodeEngine.toData(value);
        return new ReplicatedRecord(key, dataValue, localMemberHash, ttlMillis);
    }

    @Override
    public boolean isEquals(Object value1, Object value2) {
        return nodeEngine.toData(value1).equals(nodeEngine.toData(value2));
    }


}
