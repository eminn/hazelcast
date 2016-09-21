package com.hazelcast.internal.diagnostics;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.DummyOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;

import static com.hazelcast.internal.diagnostics.OperationDescriptors.toOperationDesc;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * idea: in the future we could check the operation 'names'. E.g. it would be helpful to figure out of a Get operation from
 * employees map would be slow. Currently you would see just 'Get operation'
 */
public class OperationDescriptorsTest {

    @Test
    public void testNormalOperation() {
        assertEquals(DummyOperation.class.getName(), toOperationDesc(new DummyOperation()));
    }

    @Test
    public void testBackupOperation() throws UnknownHostException {
        Backup backup = new Backup(new DummyBackupOperation(), new Address("127.0.0.1", 5701), new long[]{}, false);
        String result = toOperationDesc(backup);
        assertEquals(format("Backup(%s)", DummyBackupOperation.class.getName()), result);
    }

    @Test
    public void testPartitionIteratingOperation() throws UnknownHostException {
        PartitionIteratingOperation op = new PartitionIteratingOperation(new DummyOperationFactory(), new LinkedList<Integer>());
        String result = toOperationDesc(op);
        assertEquals(format("PartitionIteratingOperation(%s)", DummyOperationFactory.class.getName()), result);
    }

    static class DummyOperationFactory implements OperationFactory{
        @Override
        public Operation createOperation() {
            return new DummyOperation();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {

        }
    }

    static class DummyBackupOperation extends Operation implements BackupOperation {
        @Override
        public void run() throws Exception {
        }
    }
}