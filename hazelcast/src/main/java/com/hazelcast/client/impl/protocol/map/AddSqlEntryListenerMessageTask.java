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

package com.hazelcast.client.impl.protocol.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.MapAddSqlEntryListenerParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;

public class AddSqlEntryListenerMessageTask
        extends AbstractAddEntryListenerMessageTask<MapAddSqlEntryListenerParameters> {

    public AddSqlEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Data getKey() {
        return parameters.key;
    }

    @Override
    protected boolean getIncludeValue() {
        return parameters.includeValue;
    }

    @Override
    protected MapAddSqlEntryListenerParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddSqlEntryListenerParameters.decode(clientMessage);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    protected Predicate getPredicate() {
        return new SqlPredicate(parameters.sqlPredicate);
    }
}
