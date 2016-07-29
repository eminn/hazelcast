package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.util.FilteringClassLoader;
import java.lang.reflect.Method;
import java.util.Arrays;

public class IsolatedNodeSupport extends HazelcastTestSupport {

    protected Object isolatedNode;

    protected HazelcastInstance startNode() {
        Config config = getConfig();
        return Hazelcast.newHazelcastInstance(config);
    }

    protected void startIsolatedNode() {
        if (isolatedNode != null) {
            throw new IllegalStateException("There is already an isolated node running!");
        }
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            FilteringClassLoader cl = new FilteringClassLoader(Arrays.asList("com.hazelcast.executor.ExecutorServiceClassLoaderTest"), "com.hazelcast");
            thread.setContextClassLoader(cl);

            Class<?> configClazz = cl.loadClass("com.hazelcast.config.Config");
            Object config = configClazz.newInstance();
            Method setClassLoader = configClazz.getDeclaredMethod("setClassLoader", ClassLoader.class);
            setClassLoader.invoke(config, cl);

            Class<?> hazelcastClazz = cl.loadClass("com.hazelcast.core.Hazelcast");
            Method newHazelcastInstance = hazelcastClazz.getDeclaredMethod("newHazelcastInstance", configClazz);
            isolatedNode = newHazelcastInstance.invoke(hazelcastClazz, config);
        } catch (Exception e) {
            throw new RuntimeException("Could not start isolated Hazelcast instance", e);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    protected void shutdownIsolatedNode() {
        if (isolatedNode == null) {
            return;
        }
        try {
            Class<?> instanceClass = isolatedNode.getClass();
            Method method = instanceClass.getMethod("shutdown");
            method.invoke(isolatedNode);
            isolatedNode = null;
        } catch (Exception e) {
            throw new RuntimeException("Could not start shutdown Hazelcast instance", e);
        }
    }

}
