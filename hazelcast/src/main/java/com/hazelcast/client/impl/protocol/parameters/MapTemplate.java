package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;
import com.hazelcast.nio.serialization.Data;

@GenerateParameters(id = 1, name = "Map", ns = "Hazelcast.Client.Protocol.Map")
public interface MapTemplate {

    @EncodeMethod(id = 1)
    void put(String name, Data key, Data value, long threadId, long ttl);

    @EncodeMethod(id = 2)
    void get(String name, Data key, long threadId);

    @EncodeMethod(id = 3)
    void remove(String name, Data key, long threadId);

    @EncodeMethod(id = 4)
    void replace(String name, Data key, Data value, long threadId);

    @EncodeMethod(id = 5)
    void replaceIfSame(String name, Data key, Data testValue, Data value, long threadId);

    @EncodeMethod(id = 6)
    void addEntryListener(String name, Data key, Data predicate, boolean includeValue);

    @EncodeMethod(id = 7)
    void addSqlEntryListener(String name, Data key, String sqlPredicate, boolean includeValue);

    @EncodeMethod(id = 8)
    void addNearCacheEntryListener(String name, boolean includeValue);

    @EncodeMethod(id = 9)
    void putAsync(String name, Data key, Data value, long threadId, long ttl);

    @EncodeMethod(id = 10)
    void getAsync(String name, Data key, long threadId);

    @EncodeMethod(id = 11)
    void removeAsync(String name, Data key, long threadId);

    @EncodeMethod(id = 12)
    void containsKey(String name, Data key, long threadId);

    @EncodeMethod(id = 13)
    void containsValue(String name, Data value);

    @EncodeMethod(id = 14)
    void removeIfSame(String name, Data key, Data value, long threadId);

    @EncodeMethod(id = 15)
    void delete(String name, Data key, long threadId);

    @EncodeMethod(id = 16)
    void flush(String name);

    @EncodeMethod(id = 17)
    void tryRemove(String name, Data key, long threadId, long timeout);

    @EncodeMethod(id = 18)
    void tryPut(String name, Data key, Data value, long threadId, long timeout);

    @EncodeMethod(id = 19)
    void putTransient(String name, Data key, Data value, long threadId, long ttl);

    @EncodeMethod(id = 20)
    void putIfAbsent(String name, Data key, Data value, long threadId, long ttl);

    @EncodeMethod(id = 21)
    void set(String name, Data key, Data value, long threadId, long ttl);

    @EncodeMethod(id = 22)
    void lock(String name, Data key, long threadId, long ttl, long timeout);

    @EncodeMethod(id = 23)
    void isLocked(String name, Data key, long threadId);

    @EncodeMethod(id = 24)
    void unlock(String name, Data key, long threadId, boolean force);

    @EncodeMethod(id = 25)
    void addInterceptor(String name, Data interceptor);

}
