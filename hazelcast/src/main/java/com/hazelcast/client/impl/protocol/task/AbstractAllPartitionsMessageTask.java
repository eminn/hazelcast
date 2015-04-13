package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.operations.OperationFactoryWrapper;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Map;

public abstract class AbstractAllPartitionsMessageTask<P> extends AbstractMessageTask<P> {

    public AbstractAllPartitionsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        ClientEndpoint endpoint = getEndpoint();
        OperationFactory operationFactory = new OperationFactoryWrapper(createOperationFactory(), endpoint.getUuid());
        final InternalOperationService operationService = nodeEngine.getOperationService();
        try {
            Map<Integer, Object> map = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
            Object result = reduce(map);
            endpoint.sendResponse(result, clientMessage.getCorrelationId());
        } catch (Exception e) {
            clientEngine.getLogger(getClass()).warning(e);
            final ClientMessage exceptionMessage = createExceptionMessage(e);
            sendClientMessage(exceptionMessage);
        }
    }

    protected abstract OperationFactory createOperationFactory();

    protected abstract Object reduce(Map<Integer, Object> map);
}
