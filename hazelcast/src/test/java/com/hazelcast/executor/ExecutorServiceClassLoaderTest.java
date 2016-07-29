package com.hazelcast.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.IsolatedNodeSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExecutorServiceClassLoaderTest extends IsolatedNodeSupport {

    @After
    public void tearDown() throws Exception {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testExecutorLoadsClassFromAnotherNode() throws Exception {

        HazelcastInstance instance = startNode();
        startIsolatedNode();

        IExecutorService executorService = instance.getExecutorService(randomName());
        Set<Member> members = new HashSet<Member>(instance.getCluster().getMembers());
        members.remove(instance.getCluster().getLocalMember());
        executorService.submitToMember(new PrintClassLoaderCallable(), members.iterator().next());
    }


    static class PrintClassLoaderCallable implements Callable, Serializable {

        @Override
        public Object call() throws Exception {
            User user = new User("emin");
            System.out.println("user = " + user);
            System.out.println("Thread.currentThread().getContextClassLoader() = " + Thread.currentThread().getContextClassLoader());
            return null;
        }
    }

    static class User {
        String name;

        public User(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
