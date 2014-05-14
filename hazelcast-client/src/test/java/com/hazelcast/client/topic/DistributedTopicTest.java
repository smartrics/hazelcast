package com.hazelcast.client.topic;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class DistributedTopicTest {

    abstract class MessageListenerWithGroup implements MessageListener, GroupListener {

    }

    final static int MESSAGE_WAIT_TO = 100;
    final static int NUM = 10;
    final static String name = "test1";

    @Test // run on its own to start an instance and setup a listener of messages published by client.
    public void startInstance() throws Exception {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        ITopic t = server.getTopic(name);
        CountDownLatch latch = new CountDownLatch(NUM);
        t.addMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                System.out.println("message on server listener: " + message.getMessageObject());
            }
        });
        latch.await(MESSAGE_WAIT_TO, TimeUnit.SECONDS);
    }

    @Test
    public void startsClientWithListenersInGroup1() throws Exception {
        clientStartsWithListenersIn("group1", "group1");
    }

    @Test
    public void startsClientWithListenersInGroup2() throws Exception {
        clientStartsWithListenersIn("group2", "group2");
    }

    @Test
    public void startsClientWithListenersInGroups3and4() throws Exception {
        clientStartsWithListenersIn("group3", "group4");
    }

    @Test
    public void clientStartsAndSendsMessages() throws Exception {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(null);
        ITopic t = hz.getTopic(name);
        for (int i=0; i<NUM; i++){
            t.publish("naber"+i);
        }
    }

    private MessageListener makeListenerInGroup(final CountDownLatch latch, final String g) {
        return new MessageListenerWithGroup() {
            public void onMessage(Message message) {
                System.out.println("message on listener " + this.hashCode() + ", group=" + g + ", message=" + message.getMessageObject());
                latch.countDown();
            }

            @Override
            public String getGroupName() {
                return g;
            }
        };
    }

    private void clientStartsWithListenersIn(String group1, String group2) throws Exception {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(null);
        ITopic t = hz.getTopic(name);
        int messages = group1.equals(group2) ? NUM : 2 * NUM;
        final CountDownLatch latch = new CountDownLatch(messages + 1);
        MessageListener listener1 = makeListenerInGroup(latch, group1);
        MessageListener listener2 = makeListenerInGroup(latch, group2);
        t.addMessageListener(listener1);
        t.addMessageListener(listener2);

        assertFalse("latch didn't timeout as expected as more than 10 onMessage invoked", latch.await(MESSAGE_WAIT_TO, TimeUnit.SECONDS));
        // latch should be 1 as initialised to 11 and 10 messages received.
        assertThat("the latch timed out but less than 10 messages were dispatched", latch.getCount(), is(equalTo(1L)));
    }

    @Test
    public void clientStartsWithAListenerAndFilter() throws Exception {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(null);
        ITopic t = hz.getTopic(name);
        final CountDownLatch latch = new CountDownLatch(1 + NUM / 2);
        MessageListener listener1 = makeListenerInGroup(latch, "GROUP");
        t.addMessageListener(listener1, new MessageEventFilter() {
            @Override
            public boolean shouldPublish(Object messageData) {
                int len = messageData.toString().length();
                // allow only messages ending with an even number
                boolean shouldPublish = Integer.parseInt(messageData.toString().substring(len - 1)) % 2 == 0;
                return shouldPublish;
            }
        });

        assertFalse("latch didn't timeout as expected as more than 5 onMessage invoked", latch.await(MESSAGE_WAIT_TO, TimeUnit.SECONDS));
        // latch should be 1 as initialised to 11 and 10 messages received.
        assertThat("the latch timed out but less than 5 messages were dispatched", latch.getCount(), is(equalTo(1L)));
    }

}
