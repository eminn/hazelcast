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

package com.hazelcast.map.impl.client;

import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public class MapRemoveRequest extends MapKeyBasedClientRequest implements Portable, SecureRequest {

    protected Data key;
    protected long threadId;
    protected boolean async;
    protected transient long startTime;

    public MapRemoveRequest() {
    }

    public MapRemoveRequest(String name, Data key, long threadId) {
        super(name);
        this.key = key;
        this.threadId = threadId;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.REMOVE;
    }

    public Object getKey() {
        return key;
    }

    @Override
    protected void beforeProcess() {
        startTime = System.currentTimeMillis();
    }

    @Override
    protected void beforeResponse() {
        final long latency = System.currentTimeMillis() - startTime;
        final MapService mapService = getService();
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(name);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            mapService.getMapServiceContext().getLocalMapStatsProvider().getLocalMapStatsImpl(name)
                    .incrementRemoves(latency);
        }
    }

    protected Operation prepareOperation() {
        MapOperation op = getOperationProvider().createRemoveOperation(name, key, false);
        op.setThreadId(threadId);
        return op;
    }

    public void setAsAsync() {
        this.async = true;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeLong("t", threadId);
        writer.writeBoolean("a", async);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        threadId = reader.readLong("t");
        async = reader.readBoolean("a");
        final ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "remove";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key};
    }
}
