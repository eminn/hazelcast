package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LocalMapStatsTest extends HazelcastTestSupport {

    private final String name = "fooMap";

    @Test
    public void testLastAccessTime() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        HazelcastInstance h1 = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        IMap<String, String> map1 = h1.getMap(name);

        String key = "key";
        map1.put(key, "value");

        long lastUpdateTime = map1.getLocalMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime >= startTime);

        Thread.sleep(5);
        map1.put(key, "value2");
        long lastUpdateTime2 = map1.getLocalMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime2 > lastUpdateTime);
    }

    @Test
    public void testMisses() throws Exception {
        HazelcastInstance h1 = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        final String mapName = "testMisses";

        IMap<String, String> map1 = h1.getMap(mapName);
        map1.get("test");
        map1.get("test");
        map1.get("test");
        long misses = map1.getLocalMapStats().getMisses();
        assertEquals(3, misses);

        HashSet set = new HashSet();
        set.add("a");
        set.add("b");
        set.add("c");
        set.add("d");
        map1.getAll(set);
        misses = map1.getLocalMapStats().getMisses();
        assertEquals(7, misses);

        map1.put("1", "1");

        map1.get("1");  // hit
        map1.get("2");  // miss
        misses = map1.getLocalMapStats().getMisses();
        assertEquals(8, misses);

        final Future<String> async = map1.getAsync("3");
        async.get();
        misses = map1.getLocalMapStats().getMisses();
        assertEquals(9, misses);

        map1.executeOnKey("1", new TempData.LoggingEntryProcessor());  // hit
        misses = map1.getLocalMapStats().getMisses();
        assertEquals(9, misses);

        map1.executeOnKey("test",new TempData.LoggingEntryProcessor());  // miss
        misses = map1.getLocalMapStats().getMisses();
        assertEquals(10, misses);

        map1.replace("1","newValue"); // hit
        misses = map1.getLocalMapStats().getMisses();
        assertEquals(10, misses);

        map1.replace("test","newValue"); // miss
        misses = map1.getLocalMapStats().getMisses();
        assertEquals(11, misses);

        map1.remove("1"); // hit
        misses = map1.getLocalMapStats().getMisses();
        assertEquals(11, misses);

        map1.remove("1","22"); // miss
        misses = map1.getLocalMapStats().getMisses();
        assertEquals(12, misses);

    }
}
