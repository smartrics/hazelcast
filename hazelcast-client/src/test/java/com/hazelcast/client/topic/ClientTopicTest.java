/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.topic;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientTopicTest {

    static HazelcastInstance client;
    static HazelcastInstance server;

    abstract class MessageListenerWithGroup implements MessageListener, GroupListener {

    }

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient(null);
    }

    @AfterClass
    public static void stop(){
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testListener() throws InterruptedException{
        ITopic topic = client.getTopic(randomString());

        final CountDownLatch latch = new CountDownLatch(10);
        MessageListener listener = new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
            }
        };
        topic.addMessageListener(listener);

        for (int i=0; i<10; i++){
            topic.publish(i);
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void testRemoveListener() {
        ITopic topic = client.getTopic(randomString());

        MessageListener listener = new MessageListener() {
            public void onMessage(Message message) {
            }
        };
        String id = topic.addMessageListener(listener);

        assertTrue(topic.removeMessageListener(id));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalTopicStats() throws Exception {
        ITopic topic = client.getTopic(randomString());

        topic.getLocalTopicStats();
    }

    @Test
    public void testListenerInSameGroups() throws Exception {
        ITopic topic = client.getTopic(randomString());

        final CountDownLatch latch = new CountDownLatch(11);
        final AtomicInteger count = new AtomicInteger(0);
        MessageListener listener1 = makeListenerInGroup(count, latch, "group1");
        MessageListener listener2 = makeListenerInGroup(count, latch, "group1");
        topic.addMessageListener(listener1);
        topic.addMessageListener(listener2);

        for (int i=0; i<10; i++){
            topic.publish("naber"+i);
        }
        assertFalse("latch didn't timeout as expected as more than 10 onMessage invoked", latch.await(1, TimeUnit.SECONDS));
        assertThat(count.get(), is(10));
    }

    @Test
    public void testListenerInNullGroupHasSameBehaviourToNoGroup() throws Exception {
        ITopic topic = client.getTopic(randomString());

        final CountDownLatch latch = new CountDownLatch(20);
        final AtomicInteger count = new AtomicInteger(0);
        MessageListener listener1 = makeListener(count, latch);
        MessageListener listener2 = makeListenerInGroup(count, latch, null);
        topic.addMessageListener(listener1);
        topic.addMessageListener(listener2);

        for (int i=0; i<10; i++){
            topic.publish("naber"+i);
        }
        assertTrue("latch didn't timeout as expected as more than 10 onMessage invoked", latch.await(3, TimeUnit.SECONDS));
        assertThat(count.get(), is(20));
    }

    @Test
    public void testListenerInDifferentGroups() throws Exception {
        ITopic topic = client.getTopic(randomString());

        final CountDownLatch latch = new CountDownLatch(30);
        final AtomicInteger count = new AtomicInteger(0);
        MessageListener listener = makeListener(count, latch);
        MessageListener listener1 = makeListenerInGroup(count, latch, null);
        MessageListener listener2 = makeListenerInGroup(count, latch, "group2");
        topic.addMessageListener(listener);
        topic.addMessageListener(listener1);
        topic.addMessageListener(listener2);

        for (int i=0; i<10; i++){
            topic.publish("naber"+i);
        }
        assertTrue("latch did timeout as not all messages received", latch.await(3, TimeUnit.SECONDS));
        assertThat(count.get(), is(30));
    }



    private MessageListener makeListener(final AtomicInteger count, final CountDownLatch latch) {
        return new MessageListener() {
            public void onMessage(Message message) {
                count.incrementAndGet();
                latch.countDown();
            }
        };
    }


    private MessageListener makeListenerInGroup(final AtomicInteger count, final CountDownLatch latch, final String group1) {
        return new MessageListenerWithGroup() {
            public void onMessage(Message message) {
                count.incrementAndGet();
                latch.countDown();
            }

            @Override
            public String getGroupName() {
                return group1;
            }
        };
    }
}
