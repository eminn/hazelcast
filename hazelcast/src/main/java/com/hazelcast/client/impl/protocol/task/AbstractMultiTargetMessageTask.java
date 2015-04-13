package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.synchronizedSet;

public abstract class AbstractMultiTargetMessageTask<P> extends AbstractMessageTask<P> {

    private static final int TRY_COUNT = 100;

    protected AbstractMultiTargetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        ClientEndpoint endpoint = getEndpoint();
        OperationFactory operationFactory = createOperationFactory();
        Collection<Address> targets = getTargets();
        final int correlationId = clientMessage.getCorrelationId();
        if (targets.isEmpty()) {
            endpoint.sendResponse(reduce(new HashMap<Address, Object>()), correlationId);
            return;
        }
        final InternalOperationService operationService = nodeEngine.getOperationService();

        MultiTargetCallback callback = new MultiTargetCallback(targets);
        for (Address target : targets) {
            Operation op = operationFactory.createOperation();
            op.setCallerUuid(endpoint.getUuid());
            InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, target)
                    .setTryCount(TRY_COUNT)
                    .setResultDeserialized(false)
                    .setCallback(new SingleTargetCallback(target, callback));
            builder.invoke();
        }
    }

    protected abstract OperationFactory createOperationFactory();

    protected abstract Object reduce(Map<Address, Object> map);

    public abstract Collection<Address> getTargets();

    private final class MultiTargetCallback {

        final Collection<Address> targets;
        final ConcurrentMap<Address, Object> results;

        private MultiTargetCallback(Collection<Address> targets) {
            this.targets = synchronizedSet(new HashSet<Address>(targets));
            this.results = new ConcurrentHashMap<Address, Object>(targets.size());
        }

        public void notify(Address target, Object result) {
            if (targets.remove(target)) {
                results.put(target, result);
            } else {
                if (results.containsKey(target)) {
                    throw new IllegalArgumentException("Duplicate response from -> " + target);
                }
                throw new IllegalArgumentException("Unknown target! -> " + target);
            }
            if (targets.isEmpty()) {
                Object response = reduce(results);
                endpoint.sendResponse(response, clientMessage.getCorrelationId());
            }
        }
    }

    private final class SingleTargetCallback implements Callback<Object> {

        final Address target;
        final MultiTargetCallback parent;

        private SingleTargetCallback(Address target, MultiTargetCallback parent) {
            this.target = target;
            this.parent = parent;
        }

        @Override
        public void notify(Object object) {
            parent.notify(target, object);
        }
    }
}
