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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.quorum.impl.QuorumServiceImpl;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.ResponseNotSentException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.counters.Counter;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.nio.IOUtil.extractOperationCallId;
import static com.hazelcast.spi.Operation.CALL_ID_LOCAL_SKIPPED;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setConnection;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.spi.impl.operationutil.Operations.isJoinOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isMigrationOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isWanReplicationOperation;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

/**
 * Responsible for processing an Operation.
 */
class OperationRunnerImpl extends OperationRunner {

    static final int AD_HOC_PARTITION_ID = -2;

    private final ILogger logger;
    private final OperationServiceImpl operationService;
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final AtomicLong executedOperationsCount;

    @Probe(level = DEBUG)
    private final Counter count;

    // This field doesn't need additional synchronization, since a partition-specific OperationRunner
    // will never be called concurrently.
    private InternalPartition internalPartition;

    private final OperationResponseHandler remoteResponseHandler;

    // When partitionId >= 0, it is a partition specific
    // when partitionId = -1, it is generic
    // when partitionId = -2, it is ad hoc
    // an ad-hoc OperationRunner can only process generic operations, but it can be shared between threads
    // and therefor the {@link OperationRunner#currentTask()} always returns null
    public OperationRunnerImpl(OperationServiceImpl operationService, int partitionId) {
        super(partitionId);
        this.operationService = operationService;
        this.logger = operationService.logger;
        this.node = operationService.node;
        this.nodeEngine = operationService.nodeEngine;
        this.remoteResponseHandler = new RemoteInvocationResponseHandler(operationService);
        this.executedOperationsCount = operationService.completedOperationsCount;

        if (partitionId >= 0) {
            this.count = newSwCounter();
            nodeEngine.getMetricsRegistry().scanAndRegister(this, "operation.partition[" + partitionId + "]");
        } else {
            this.count = null;
        }
    }

    @Override
    public void run(Runnable task) {
        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = task;
        }

        try {
            task.run();
        } finally {
            if (publishCurrentTask) {
                currentTask = null;
            }
        }
    }

    private boolean publishCurrentTask() {
        boolean isClientRunnable = currentTask instanceof MessageTask
                || currentTask instanceof ClientEngineImpl.ClientPacketProcessor;
        return (getPartitionId() != AD_HOC_PARTITION_ID && (currentTask == null || isClientRunnable));
    }

    @Override
    public void run(Operation op) {
        if (count != null) {
            count.inc();
        }

        executedOperationsCount.incrementAndGet();

        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = op;
        }

        try {
            checkNodeState(op);

            if (timeout(op)) {
                return;
            }

            ensureNoPartitionProblems(op);

            ensureQuorumPresent(op);

            op.beforeRun();

            if (waitingNeeded(op)) {
                return;
            }

            op.run();
            handleResponse(op);
            afterRun(op);
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            if (publishCurrentTask) {
                currentTask = null;
            }
        }
    }

    private void checkNodeState(Operation op) {
        if (node.getState() == NodeState.ACTIVE) {
            return;
        }

        final NodeState state = node.getState();
        if (state == NodeState.SHUT_DOWN) {
            throw new HazelcastInstanceNotActiveException("This node is shut down! Operation: " + op);
        }

        if (op instanceof AllowedDuringPassiveState) {
            return;
        }

        // Cluster is in passive state. There is no need to retry.
        if (nodeEngine.getClusterService().getClusterState() == ClusterState.PASSIVE) {
            throw new IllegalStateException("Cluster is in " + ClusterState.PASSIVE + " state! Operation: " + op);
        }

        // Operation has no partition id. So it is sent to this node in purpose.
        // Operation will fail since node is shutting down or cluster is passive.
        if (op.getPartitionId() < 0) {
            throw new HazelcastInstanceNotActiveException("This node is currently passive! Operation: " + op);
        }

        // Custer is not passive but this node is shutting down.
        // Since operation has a partition id, it must be retried on another node.
        throw new RetryableHazelcastException("This node is currently shutting down! Operation: " + op);
    }

    private void ensureQuorumPresent(Operation op) {
        QuorumServiceImpl quorumService = operationService.nodeEngine.getQuorumService();
        quorumService.ensureQuorumPresent(op);
    }

    private boolean waitingNeeded(Operation op) {
        if (!(op instanceof WaitSupport)) {
            return false;
        }

        WaitSupport waitSupport = (WaitSupport) op;
        if (waitSupport.shouldWait()) {
            nodeEngine.getWaitNotifyService().await(waitSupport);
            return true;
        }
        return false;
    }

    private boolean timeout(Operation op) {
        if (!operationService.isCallTimedOut(op)) {
            return false;
        }

        CallTimeoutResponse callTimeoutResponse = new CallTimeoutResponse(op.getCallId(), op.isUrgent());
        OperationResponseHandler responseHandler = op.getOperationResponseHandler();
        responseHandler.sendResponse(op, callTimeoutResponse);
        return true;
    }

    private void handleResponse(Operation op) throws Exception {
        boolean returnsResponse = op.returnsResponse();
        int syncBackupCount = sendBackup(op);

        if (!returnsResponse) {
            return;
        }

        sendResponse(op, syncBackupCount);
    }

    private int sendBackup(Operation op) throws Exception {
        if (!(op instanceof BackupAwareOperation)) {
            return 0;
        }

        int syncBackupCount = 0;
        BackupAwareOperation backupAwareOp = (BackupAwareOperation) op;
        if (backupAwareOp.shouldBackup()) {
            syncBackupCount = operationService.operationBackupHandler.backup(backupAwareOp);
        }
        return syncBackupCount;
    }

    private void sendResponse(Operation op, int syncBackupCount) {
        OperationResponseHandler responseHandler = op.getOperationResponseHandler();
        if (responseHandler == null) {
            throw new IllegalStateException("ResponseHandler should not be null! " + op);
        }

        try {
            Object response = op.getResponse();
            if (syncBackupCount > 0) {
                response = new NormalResponse(response, op.getCallId(), syncBackupCount, op.isUrgent());
            }
            responseHandler.sendResponse(op, response);
        } catch (ResponseAlreadySentException e) {
            logOperationError(op, e);
        }
    }

    private void afterRun(Operation op) {
        try {
            op.afterRun();
            if (op instanceof Notifier) {
                final Notifier notifier = (Notifier) op;
                if (notifier.shouldNotify()) {
                    operationService.nodeEngine.getWaitNotifyService().notify(notifier);
                }
            }
        } catch (Throwable e) {
            // passed the response phase
            // `afterRun` and `notifier` errors cannot be sent to the caller anymore
            // just log the error
            logOperationError(op, e);
        }
    }

    protected void ensureNoPartitionProblems(Operation op) {
        int partitionId = op.getPartitionId();

        if (partitionId < 0) {
            return;
        }

        if (partitionId != getPartitionId()) {
            throw new IllegalStateException("wrong partition, expected: " + getPartitionId() + " but found:" + partitionId);
        }

        if (internalPartition == null) {
            internalPartition = nodeEngine.getPartitionService().getPartition(partitionId);
        }

        if (retryDuringMigration(op) && internalPartition.isMigrating()) {
            throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                    op.getClass().getName(), op.getServiceName());
        }

        Address owner = internalPartition.getReplicaAddress(op.getReplicaIndex());
        if (op.validatesTarget() && !node.getThisAddress().equals(owner)) {
            throw new WrongTargetException(node.getThisAddress(), owner, partitionId, op.getReplicaIndex(),
                    op.getClass().getName(), op.getServiceName());
        }
    }

    private boolean retryDuringMigration(Operation op) {
        return !(op instanceof ReadonlyOperation || isMigrationOperation(op));
    }

    private void handleOperationError(Operation operation, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        try {
            operation.onExecutionFailure(e);
        } catch (Throwable t) {
            logger.warning("While calling 'operation.onFailure(e)'... op: " + operation + ", error: " + e, t);
        }

        operation.logError(e);

        if (e instanceof ResponseNotSentException || !operation.returnsResponse()) {
            return;
        }

        OperationResponseHandler responseHandler = operation.getOperationResponseHandler();
        try {
            if (nodeEngine.isRunning()) {
                responseHandler.sendResponse(operation, e);
            } else if (responseHandler.isLocal()) {
                responseHandler.sendResponse(operation, new HazelcastInstanceNotActiveException());
            }
        } catch (Throwable t) {
            logger.warning("While sending op error... op: " + operation + ", error: " + e, t);
        }
    }

    private void logOperationError(Operation op, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        op.logError(e);
    }

    @Override
    public void run(Packet packet) throws Exception {
        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = packet;
        }

        Connection connection = packet.getConn();
        Address caller = connection.getEndPoint();
        try {
            Object object = nodeEngine.toObject(packet);
            Operation op = (Operation) object;
            op.setNodeEngine(nodeEngine);
            setCallerAddress(op, caller);
            setConnection(op, connection);
            setCallerUuidIfNotSet(caller, op);
            setOperationResponseHandler(op);

            if (!ensureValidMember(op)) {
                return;
            }

            if (publishCurrentTask) {
                currentTask = null;
            }
            run(op);
        } catch (Throwable throwable) {
            // If exception happens we need to extract the callId from the bytes directly!
            long callId = extractOperationCallId(packet, node.getSerializationService());
            operationService.send(new ErrorResponse(throwable, callId, packet.isUrgent()), caller);
            logOperationDeserializationException(throwable, callId);
            throw ExceptionUtil.rethrow(throwable);
        } finally {
            if (publishCurrentTask) {
                currentTask = null;
            }
        }
    }

    private void setOperationResponseHandler(Operation op) {
        OperationResponseHandler handler = remoteResponseHandler;
        if (op.getCallId() == 0 || op.getCallId() == CALL_ID_LOCAL_SKIPPED) {
            if (op.returnsResponse()) {
                throw new HazelcastException(
                        "Op: " + op + " can not return response without call-id!");
            }
            handler = createEmptyResponseHandler();
        }
        op.setOperationResponseHandler(handler);
    }

    private boolean ensureValidMember(Operation op) {
        if (node.clusterService.getMember(op.getCallerAddress()) != null
                || isJoinOperation(op)
                || isWanReplicationOperation(op)) {
            return true;
        }

        Exception error = new CallerNotMemberException(node.getThisAddress(),
                op.getCallerAddress(), op.getPartitionId(),
                op.getClass().getName(), op.getServiceName());
        handleOperationError(op, error);
        return false;
    }

    private void setCallerUuidIfNotSet(Address caller, Operation op) {
        if (op.getCallerUuid() != null) {
            return;

        }
        MemberImpl callerMember = node.clusterService.getMember(caller);
        if (callerMember != null) {
            op.setCallerUuid(callerMember.getUuid());
        }
    }

    public void logOperationDeserializationException(Throwable t, long callId) {
        boolean returnsResponse = callId != 0;

        if (t instanceof RetryableException) {
            final Level level = returnsResponse ? FINEST : WARNING;
            if (logger.isLoggable(level)) {
                logger.log(level, t.getClass().getName() + ": " + t.getMessage());
            }
        } else if (t instanceof OutOfMemoryError) {
            try {
                logger.log(SEVERE, t.getMessage(), t);
            } catch (Throwable ignored) {
                logger.log(SEVERE, ignored.getMessage(), t);
            }
        } else {
            final Level level = nodeEngine.isRunning() ? SEVERE : FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, t.getMessage(), t);
            }
        }
    }
}
