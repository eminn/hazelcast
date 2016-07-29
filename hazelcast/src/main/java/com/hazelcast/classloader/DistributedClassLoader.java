package com.hazelcast.classloader;

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.NodeEngine;

/**
 * date: 7/26/16
 * author: emindemirci
 */
public class DistributedClassLoader extends ClassLoader {

    private static final String CLASS_MAP_NAME = "hz:internal:classes";
    private static final ILogger LOGGER = Logger.getLogger(DistributedClassLoader.class);
    private NodeEngine nodeEngine;

    public DistributedClassLoader(ClassLoader parent) {
        super(parent);
        LOGGER.severe("DistributedClassLoader.DistributedClassLoader");
        LOGGER.severe("parent = " + parent);
    }


    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        LOGGER.severe("DistributedClassLoader.loadClass");
        LOGGER.severe("name = [" + name + "]");
        return super.loadClass(name, resolve);
    }



    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        LOGGER.severe("DistributedClassLoader.findClass");
        LOGGER.severe("name = [" + name + "]");
        try {
            return super.findClass(name);
        } catch (ClassNotFoundException e) {
            ReplicatedMap<String, byte[]> classesMap = nodeEngine.getHazelcastInstance().getReplicatedMap(CLASS_MAP_NAME);
            byte[] clazz = classesMap.get(name);
            if (clazz == null) {

                // initiate request from caller for that class
                throw new ClassNotFoundException(name);
            }
            return defineClass(name, clazz, 0, clazz.length);
        }
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        LOGGER.severe("DistributedClassLoader.loadClass");
        LOGGER.severe("name = [" + name + "]");
        try {
            return super.loadClass(name);
        } catch (ClassNotFoundException e) {
            ReplicatedMap<String, byte[]> classesMap = nodeEngine.getHazelcastInstance().getReplicatedMap(CLASS_MAP_NAME);
            byte[] clazz = classesMap.get(name);
            if (clazz == null) {
                throw new ClassNotFoundException(name);
            }
            return defineClass(name, clazz, 0, clazz.length);
        }
    }

    public void setNodeEngine(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public static boolean isClassNotFoundException(Throwable e) {
        if (e instanceof ClassNotFoundException) {
            return true;
        } else if (e instanceof HazelcastSerializationException) {
            HazelcastSerializationException exception = (HazelcastSerializationException) e;
            if (exception.getCause() instanceof ClassNotFoundException) {
                return true;
            }
        }
        return false;
    }

}
