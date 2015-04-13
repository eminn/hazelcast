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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.map.AddEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.map.AddInterceptorMessageTask;
import com.hazelcast.client.impl.protocol.map.AddNearCacheEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.map.AddSqlEntryListenerMessageTask;
import com.hazelcast.client.impl.protocol.map.ContainsKeyMessageTask;
import com.hazelcast.client.impl.protocol.map.ContainsValueMessageTask;
import com.hazelcast.client.impl.protocol.map.DeleteMessageTask;
import com.hazelcast.client.impl.protocol.map.FlushMessageTask;
import com.hazelcast.client.impl.protocol.map.GetAsyncMessageTask;
import com.hazelcast.client.impl.protocol.map.GetMessageTask;
import com.hazelcast.client.impl.protocol.map.IsLockedMessageTask;
import com.hazelcast.client.impl.protocol.map.LockMessageTask;
import com.hazelcast.client.impl.protocol.map.PutAsyncMessageTask;
import com.hazelcast.client.impl.protocol.map.PutIfAbsentMessageTask;
import com.hazelcast.client.impl.protocol.map.PutMessageTask;
import com.hazelcast.client.impl.protocol.map.PutTransientMessageTask;
import com.hazelcast.client.impl.protocol.map.RemoveAsyncMessageTask;
import com.hazelcast.client.impl.protocol.map.RemoveIfSameMessageTask;
import com.hazelcast.client.impl.protocol.map.RemoveMessageTask;
import com.hazelcast.client.impl.protocol.map.ReplaceIfSameMessageTask;
import com.hazelcast.client.impl.protocol.map.ReplaceMessageTask;
import com.hazelcast.client.impl.protocol.map.SetMessageTask;
import com.hazelcast.client.impl.protocol.map.TryPutMessageTask;
import com.hazelcast.client.impl.protocol.map.TryRemoveMessageTask;
import com.hazelcast.client.impl.protocol.map.UnlockMessageTask;
import com.hazelcast.client.impl.protocol.parameters.MapMessageType;
import com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask;
import com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask;
import com.hazelcast.client.impl.protocol.task.CreateProxyMessageTask;
import com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.task.NoSuchMessageTask;
import com.hazelcast.client.impl.protocol.task.RegisterMembershipListenerMessageTask;
import com.hazelcast.client.impl.protocol.util.Int2ObjectHashMap;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;

/**
 * Message task factory
 */
public class MessageTaskFactoryImpl implements MessageTaskFactory {

    private final Int2ObjectHashMap<MessageTaskFactory> tasks = new Int2ObjectHashMap<MessageTaskFactory>();

    private final Node node;

    public MessageTaskFactoryImpl(Node node) {
        this.node = node;
        initFactories();
    }

    public void initFactories() {
        tasks.put(ClientMessageType.AUTHENTICATION_DEFAULT_REQUEST.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AuthenticationMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.AUTHENTICATION_CUSTOM_REQUEST.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AuthenticationCustomCredentialsMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(ClientMessageType.REGISTER_MEMBERSHIP_LISTENER_REQUEST.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new RegisterMembershipListenerMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.CREATE_PROXY_REQUEST.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CreateProxyMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.GET_PARTITIONS_REQUEST.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetPartitionsMessageTask(clientMessage, node, connection);
            }
        });


        //================================  MAP ===============================//

        tasks.put(MapMessageType.MAP_CONTAINSKEY.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ContainsKeyMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_CONTAINSVALUE.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ContainsValueMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_GET.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_GETASYNC.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetAsyncMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_PUT.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new PutMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_PUTASYNC.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new PutAsyncMessageTask(clientMessage, node, connection);
            }
        });


        tasks.put(MapMessageType.MAP_REMOVE.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new RemoveMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_REMOVEIFSAME.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new RemoveIfSameMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_REMOVEASYNC.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new RemoveAsyncMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_DELETE.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DeleteMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_FLUSH.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new FlushMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_TRYREMOVE.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new TryRemoveMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_TRYPUT.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new TryPutMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_PUTTRANSIENT.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new PutTransientMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_PUTIFABSENT.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new PutIfAbsentMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_REPLACE.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ReplaceMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_REPLACEIFSAME.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new ReplaceIfSameMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_SET.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new SetMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_LOCK.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LockMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_ISLOCKED.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new IsLockedMessageTask(clientMessage, node, connection);
            }
        });


        tasks.put(MapMessageType.MAP_UNLOCK.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new UnlockMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_ADDINTERCEPTOR.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddInterceptorMessageTask(clientMessage, node, connection);
            }
        });


        tasks.put(MapMessageType.MAP_ADDENTRYLISTENER.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddEntryListenerMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_ADDNEARCACHEENTRYLISTENER.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddNearCacheEntryListenerMessageTask(clientMessage, node, connection);
            }
        });

        tasks.put(MapMessageType.MAP_ADDSQLENTRYLISTENER.id(), new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddSqlEntryListenerMessageTask(clientMessage, node, connection);
            }
        });

        //TODO more factories to come here
    }

    @Override
    public MessageTask create(ClientMessage clientMessage, Connection connection) {
        final MessageTaskFactory factory = tasks.get(clientMessage.getMessageType());
        if (factory != null) {
            return factory.create(clientMessage, connection);
        }
        return new NoSuchMessageTask(clientMessage, node, connection);
    }

}
