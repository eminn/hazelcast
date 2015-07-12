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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import java.io.IOException;

public class PutOperation extends AbstractOperation implements PartitionAwareOperation {

    private String name;
    private Data dataKey;
    private Data dataValue;
    protected transient Data dataOldValue;

    public PutOperation() {
    }

    public PutOperation(String name, Data dataKey, Data dataValue) {
        this.name = name;
        this.dataKey = dataKey;
        this.dataValue = dataValue;

    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true);
        dataOldValue = getNodeEngine().getSerializationService().toData(store.put(dataKey, dataValue));
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeData(dataKey);
        out.writeData(dataValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        dataKey = in.readData();
        dataValue = in.readData();
    }
}
