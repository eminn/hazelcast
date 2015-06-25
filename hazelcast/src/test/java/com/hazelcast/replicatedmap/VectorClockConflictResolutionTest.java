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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.VectorClockTimestamp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class VectorClockConflictResolutionTest extends ReplicatedMapBaseTest {

    @Test
    public void testVectorClocksAreSameAfterConflictResolutionBinaryDelay0() throws Exception {
        Config config = buildConfig(InMemoryFormat.BINARY, 0);
        testVectorClocksAreSameAfterConflictResolution(config);
    }

    @Test
    public void testVectorClocksAreSameAfterConflictResolutionBinaryDelayDefault() throws Exception {
        Config config = buildConfig(InMemoryFormat.BINARY, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
        testVectorClocksAreSameAfterConflictResolution(config);
    }

    @Repeat(10)
    @Test
    public void testVectorClocksAreSameAfterConflictResolutionObjectDelay0() throws Exception {
        Config config = buildConfig(InMemoryFormat.OBJECT, 0);
        testVectorClocksAreSameAfterConflictResolution(config);
    }

    @Test
    public void testVectorClocksAreSameAfterConflictResolutionObjectDelayDefault() throws Exception {
        Config config = buildConfig(InMemoryFormat.OBJECT, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
        testVectorClocksAreSameAfterConflictResolution(config);
    }

    private void testVectorClocksAreSameAfterConflictResolution(Config config) throws InterruptedException {
        int nodeCount = 5;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(nodeCount);
        HazelcastInstance[] instances = nodeFactory.newInstances(config);
        String replicatedMapName = randomMapName();
        final List<ReplicatedMap> maps = createMaps(instances, replicatedMapName);
        int operations = 1000;
        int keyCount = 10;
        ArrayList<Integer> keys = generateRandomIntegerList(keyCount);
        Thread[] threads = createThreads(nodeCount, maps, keys, operations);
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        for (int i = 0; i < keyCount; i++) {
            final String key = "foo-" + keys.get(i);
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    VectorClockTimestamp[] clocks = getVectorClocksForKey(maps, key);
                    System.out.println("---------------------");
                    System.out.println("key = " + key);
                    printValues();
                    printClocks(clocks);
                    assertValuesAreEqual();
                    assertClocksAreEqual(clocks);
                }

                private void printClocks(VectorClockTimestamp[] clocks) {
                    for (int j = 0; j < clocks.length; j++) {
                        System.out.println("clock[" + j + "] = " + clocks[j]);
                    }
                }

                private void printValues() {
                    for (int j = 0; j < maps.size(); j++) {
                        ReplicatedMap map = maps.get(j);
                        System.out.println("value[" + j + "] = " + map.get(key));
                    }
                }

                private void assertValuesAreEqual() {
                    for (int i = 0; i < maps.size() - 1; i++) {
                        ReplicatedMap map1 = maps.get(i);
                        ReplicatedMap map2 = maps.get(i + 1);
                        assertEquals(map1.get(key), map2.get(key));
                    }
                }

                private void assertClocksAreEqual(VectorClockTimestamp[] clocks) {
                    for (int j = 0; j < clocks.length - 1; j++) {
                        assertEquals(clocks[j], clocks[j + 1]);
                    }
                }
            });
        }
    }

    private VectorClockTimestamp[] getVectorClocksForKey(List<ReplicatedMap> maps, String key) throws Exception {
        VectorClockTimestamp[] clocks = new VectorClockTimestamp[maps.size()];
        for (int i = 0; i < maps.size(); i++) {
            ReplicatedMap map = maps.get(i);
            clocks[i] = getVectorClockForKey(map, key);
        }
        return clocks;
    }

    private Thread[] createThreads(int count, List<ReplicatedMap> maps, ArrayList<Integer> keys, int operations) {
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = createPutOperationThread(maps.get(i), keys, operations);
        }
        return threads;
    }

    private List<ReplicatedMap> createMaps(HazelcastInstance[] instances, String replicatedMapName) {
        ArrayList<ReplicatedMap> maps = new ArrayList<ReplicatedMap>();
        for (int i = 0; i < instances.length; i++) {
            ReplicatedMap<Object, Object> replicatedMap = instances[i].getReplicatedMap(replicatedMapName);
            maps.add(replicatedMap);
        }
        return maps;
    }

    private ArrayList<Integer> generateRandomIntegerList(int count) {
        final ArrayList<Integer> keys = new ArrayList<Integer>();
        final Random random = new Random();
        for (int i = 0; i < count; i++) {
            keys.add(random.nextInt());
        }
        return keys;
    }

    private Thread createPutOperationThread(final ReplicatedMap<String, String> map, final ArrayList<Integer> keys,
                                            final int operations) {
        return new Thread(new Runnable() {
            @Override
            public void run() {
                int size = keys.size();
                for (int i = 0; i < operations; i++) {
                    int index = i % size;
                    map.put("foo-" + keys.get(index), randomString());
                }
            }
        });
    }

    private VectorClockTimestamp getVectorClockForKey(ReplicatedMap map, Object key) throws Exception {
        ReplicatedRecord foo = getReplicatedRecord(map, key);
        return foo.getVectorClockTimestamp();
    }

}
