package com.hazelcast.test;

import com.hazelcast.core.*;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 702161900
 */
public class DistributedGroupListenersTest extends HazelcastTestSupport {
    private HazelcastInstance[] instances;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instances = factory.newInstances();
    }

    @Test
    public void itemEventsShouldBeDispatchedToGroupListeners() throws InterruptedException {
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        final CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        String r1 = q1.addItemListener(new GroupItemListener(latch, addCount, removeCount, "g1"), false);
        System.out.println("reg1 = " + r1);
        String r2 = q2.addItemListener(new GroupItemListener(latch, addCount, removeCount, "g1"), false);
        System.out.println("reg2 = " + r2);


        q1.put("a");
        q2.remove("a");

        boolean result = latch.await(3, TimeUnit.SECONDS);

        Thread.sleep(1000);

        System.out.println("result = " + result);
        System.out.println("add = " + addCount.get());
        System.out.println("remove = " + removeCount.get());
    }

    @Test
    public void topicEventsShouldBeDispatchedToGroupListeners() throws InterruptedException {
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
        final ITopic t1 = h1.getTopic("default");
        final ITopic t2 = h2.getTopic("default");
        final CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        String r1 = t1.addMessageListener(new GroupItemListener(latch, addCount, removeCount, "g1"));
        System.out.println("reg1 = " + r1);
        String r2 = t2.addMessageListener(new GroupItemListener(latch, addCount, removeCount, "g1"));
        System.out.println("reg2 = " + r2);


        t1.publish("a");

        boolean result = latch.await(3, TimeUnit.SECONDS);

        Thread.sleep(1000);

        System.out.println("result = " + result);
        System.out.println("add = " + addCount.get());
        System.out.println("remove = " + removeCount.get());
    }

    private class GroupItemListener implements ItemListener<String>, MessageListener<String>, GroupListener {

        private final String name;
        private final CountDownLatch latch;
        private final AtomicInteger removeCount;
        private final AtomicInteger addCount;

        public GroupItemListener(CountDownLatch latch, AtomicInteger addCount, AtomicInteger removeCount, String name) {
            this.name = name;
            this.latch = latch;
            this.removeCount = removeCount;
            this.addCount = addCount;
        }

        @Override
        public String getGroupName() {
            return name;
        }

        @Override
        public void onMessage(Message<String> message) {

            latch.countDown();
            addCount.incrementAndGet();
        }

        @Override
        public void itemAdded(ItemEvent<String> item) {
            latch.countDown();
            addCount.incrementAndGet();
        }

        @Override
        public void itemRemoved(ItemEvent<String> item) {
            latch.countDown();
            removeCount.incrementAndGet();
        }
    }
}
