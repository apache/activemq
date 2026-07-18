/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

import org.apache.activemq.broker.SharedTopicBrokerService;
import org.apache.activemq.SharedTopicConnectionFactory;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.activemq.test.annotations.ParallelTest;

/**
 * End-to-end JMS integration tests for shared topic subscriptions.
 * Uses {@link SharedTopicConnectionFactory} (client) and
 * {@link SharedTopicBrokerService} (broker) to exercise the full stack.
 */
@Category(ParallelTest.class)
public class SharedSubscriptionJmsTest {

    private static final int WAIT_TIMEOUT = 10_000;
    private static final int WAIT_INTERVAL = 10;

    private SharedTopicBrokerService broker;
    private SharedTopicConnectionFactory factory;
    private Connection connection1;
    private Connection connection2;

    @Before
    public void setUp() throws Exception {
        broker = new SharedTopicBrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setBrokerName("shared-jms-test");
        broker.addConnector("vm://shared-jms-test");
        broker.start();
        broker.waitUntilStarted();

        factory = new SharedTopicConnectionFactory("vm://shared-jms-test");
    }

    @After
    public void tearDown() throws Exception {
        closeQuietly(connection1);
        closeQuietly(connection2);
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testSharedDurableCreateAndReceive() throws Exception {
        connection1 = factory.createConnection();
        connection1.setClientID("client-1");
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.durable");
        MessageConsumer consumer = session.createSharedDurableConsumer(topic, "durSub1");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Durable subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("hello-durable"));

        Message msg = consumer.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive message", msg);
        assertEquals("hello-durable", ((TextMessage) msg).getText());
    }

    @Test
    public void testSharedDurableMultipleConsumersRoundRobin() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        connection2 = factory.createConnection();
        connection2.start();

        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session1.createTopic("test.shared.roundrobin");

        int messageCount = 20;
        CountDownLatch latch = new CountDownLatch(messageCount);
        ConcurrentLinkedQueue<String> consumer1Msgs = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> consumer2Msgs = new ConcurrentLinkedQueue<>();

        MessageConsumer c1 = session1.createSharedDurableConsumer(topic, "rrSub");
        c1.setMessageListener(msg -> {
            try {
                consumer1Msgs.add(((TextMessage) msg).getText());
                latch.countDown();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });

        MessageConsumer c2 = session2.createSharedDurableConsumer(topic, "rrSub");
        c2.setMessageListener(msg -> {
            try {
                consumer2Msgs.add(((TextMessage) msg).getText());
                latch.countDown();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });

        BrokerView adminView = broker.getAdminView();
        assertTrue("Durable subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        Session producerSession = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(topic);
        for (int i = 0; i < messageCount; i++) {
            producer.send(producerSession.createTextMessage("msg-" + i));
        }

        assertTrue("All messages should be received",
                latch.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS));

        assertEquals("Total messages should match",
                messageCount, consumer1Msgs.size() + consumer2Msgs.size());
        assertFalse("Consumer 1 should receive some messages", consumer1Msgs.isEmpty());
        assertFalse("Consumer 2 should receive some messages", consumer2Msgs.isEmpty());

        Set<String> allReceived = new HashSet<>(consumer1Msgs);
        allReceived.addAll(consumer2Msgs);
        assertEquals("No duplicates — each message to exactly one consumer",
                messageCount, allReceived.size());
    }

    @Test
    public void testSharedDurableReconnectReceivesBuffered() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session1.createTopic("test.shared.reconnect");
        MessageConsumer consumer = session1.createSharedDurableConsumer(topic, "reconnSub");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        consumer.close();
        assertTrue("Subscriber should become inactive",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length == 0
                                && adminView.getInactiveDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageProducer producer = session1.createProducer(topic);
        producer.send(session1.createTextMessage("buffered-msg"));

        connection2 = factory.createConnection();
        connection2.start();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic2 = session2.createTopic("test.shared.reconnect");
        MessageConsumer consumer2 = session2.createSharedDurableConsumer(topic2, "reconnSub");

        assertTrue("Subscriber should reactivate",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        Message msg = consumer2.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive buffered message after reconnect", msg);
        assertEquals("buffered-msg", ((TextMessage) msg).getText());
    }

    @Test
    public void testSharedDurableWithSelector() throws Exception {
        connection1 = factory.createConnection();
        connection1.setClientID("client-sel");
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.selector");
        MessageConsumer consumer = session.createSharedDurableConsumer(
                topic, "selSub", "color = 'red'");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageProducer producer = session.createProducer(topic);

        TextMessage red = session.createTextMessage("red-msg");
        red.setStringProperty("color", "red");
        producer.send(red);

        TextMessage blue = session.createTextMessage("blue-msg");
        blue.setStringProperty("color", "blue");
        producer.send(blue);

        Message msg = consumer.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive red message", msg);
        assertEquals("red-msg", ((TextMessage) msg).getText());

        Message noMsg = consumer.receiveNoWait();
        assertNull("Should NOT receive blue message", noMsg);
    }

    @Test
    public void testSharedNonDurableCreateAndReceive() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.nondurable");
        MessageConsumer consumer = session.createSharedConsumer(topic, "nonDurSub1");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Topic subscriber should register",
                Wait.waitFor(() -> adminView.getTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("hello-nondurable"));

        Message msg = consumer.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive message", msg);
        assertEquals("hello-nondurable", ((TextMessage) msg).getText());
    }

    @Test
    public void testSharedNonDurableCleanupOnDisconnect() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        BrokerView adminView = broker.getAdminView();
        final int baselineCount = adminView.getTopicSubscribers().length;

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.cleanup");
        MessageConsumer consumer = session.createSharedConsumer(topic, "cleanupSub");

        assertTrue("Topic subscriber should register",
                Wait.waitFor(() -> adminView.getTopicSubscribers().length > baselineCount,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        consumer.close();
        assertTrue("Topic subscriber should be removed after close",
                Wait.waitFor(() -> adminView.getTopicSubscribers().length <= baselineCount,
                        WAIT_TIMEOUT, WAIT_INTERVAL));
    }

    @Test
    public void testSharedNonDurableMultipleConsumersRoundRobin() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        connection2 = factory.createConnection();
        connection2.start();

        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session1.createTopic("test.shared.nondurable.rr");

        int messageCount = 20;
        CountDownLatch latch = new CountDownLatch(messageCount);
        ConcurrentLinkedQueue<String> consumer1Msgs = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> consumer2Msgs = new ConcurrentLinkedQueue<>();

        MessageConsumer c1 = session1.createSharedConsumer(topic, "ndRRSub");
        c1.setMessageListener(msg -> {
            try {
                consumer1Msgs.add(((TextMessage) msg).getText());
                latch.countDown();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });

        MessageConsumer c2 = session2.createSharedConsumer(topic, "ndRRSub");
        c2.setMessageListener(msg -> {
            try {
                consumer2Msgs.add(((TextMessage) msg).getText());
                latch.countDown();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });

        BrokerView adminView = broker.getAdminView();
        assertTrue("Topic subscriber should register",
                Wait.waitFor(() -> adminView.getTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        Session producerSession = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(topic);
        for (int i = 0; i < messageCount; i++) {
            producer.send(producerSession.createTextMessage("nd-msg-" + i));
        }

        assertTrue("All messages should be received",
                latch.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS));

        assertEquals("Total messages should match",
                messageCount, consumer1Msgs.size() + consumer2Msgs.size());
        assertFalse("Consumer 1 should receive some messages", consumer1Msgs.isEmpty());
        assertFalse("Consumer 2 should receive some messages", consumer2Msgs.isEmpty());

        Set<String> allReceived = new HashSet<>(consumer1Msgs);
        allReceived.addAll(consumer2Msgs);
        assertEquals("No duplicates — each message to exactly one consumer",
                messageCount, allReceived.size());
    }

    @Test
    public void testSharedNonDurableWithSelector() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.nondurable.sel");
        MessageConsumer consumer = session.createSharedConsumer(
                topic, "ndSelSub", "color = 'green'");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Topic subscriber should register",
                Wait.waitFor(() -> adminView.getTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageProducer producer = session.createProducer(topic);

        TextMessage green = session.createTextMessage("green-msg");
        green.setStringProperty("color", "green");
        producer.send(green);

        TextMessage yellow = session.createTextMessage("yellow-msg");
        yellow.setStringProperty("color", "yellow");
        producer.send(yellow);

        Message msg = consumer.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive green message", msg);
        assertEquals("green-msg", ((TextMessage) msg).getText());

        Message noMsg = consumer.receiveNoWait();
        assertNull("Should NOT receive yellow message", noMsg);
    }

    @Test
    public void testSharedNonDurableConsumerLeaveRebalance() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        connection2 = factory.createConnection();
        connection2.start();

        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session1.createTopic("test.shared.nondurable.rebalance");

        MessageConsumer c1 = session1.createSharedConsumer(topic, "rebalSub");
        MessageConsumer c2 = session2.createSharedConsumer(topic, "rebalSub");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Topic subscriber should register",
                Wait.waitFor(() -> adminView.getTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        c2.close();

        MessageProducer producer = session1.createProducer(topic);
        for (int i = 0; i < 5; i++) {
            producer.send(session1.createTextMessage("solo-" + i));
        }

        for (int i = 0; i < 5; i++) {
            Message msg = c1.receive(WAIT_TIMEOUT);
            assertNotNull("Remaining consumer should receive all messages, msg " + i, msg);
            assertEquals("solo-" + i, ((TextMessage) msg).getText());
        }
    }

    @Test(expected = JMSException.class)
    public void testSelectorMismatchOnJoinThrows() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        connection2 = factory.createConnection();
        connection2.start();

        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session1.createTopic("test.shared.selmismatch");
        session1.createSharedDurableConsumer(topic, "selMisSub", "color = 'red'");

        BrokerView adminView = broker.getAdminView();
        assertTrue("First subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic2 = session2.createTopic("test.shared.selmismatch");
        session2.createSharedDurableConsumer(topic2, "selMisSub", "color = 'blue'");
    }

    @Test(expected = JMSException.class)
    public void testSharedToUnsharedTypeConflictThrows() throws Exception {
        connection1 = factory.createConnection();
        connection1.setClientID("client-conflict");
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.conflict");
        session.createSharedDurableConsumer(topic, "conflictSub");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Shared subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        session.createDurableSubscriber(topic, "conflictSub");
    }

    @Test(expected = JMSException.class)
    public void testUnsharedToSharedTypeConflictThrows() throws Exception {
        connection1 = factory.createConnection();
        connection1.setClientID("client-conflict2");
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.conflict2");
        session.createDurableSubscriber(topic, "conflictSub2");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Unshared subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        session.createSharedDurableConsumer(topic, "conflictSub2");
    }

    @Test
    public void testSharedDurableWithoutClientId() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.nocid");
        MessageConsumer consumer = session.createSharedDurableConsumer(topic, "noCidSub");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Subscriber should register without clientId",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("no-cid-msg"));

        Message msg = consumer.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive message without clientId", msg);
        assertEquals("no-cid-msg", ((TextMessage) msg).getText());
    }

    @Test
    public void testSharedDurableUnsubscribeWhenInactive() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.unsub");
        MessageConsumer consumer = session.createSharedDurableConsumer(topic, "unsubSub");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        consumer.close();
        assertTrue("Subscriber should become inactive",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length == 0
                                && adminView.getInactiveDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        session.unsubscribe("unsubSub");
        assertTrue("Inactive subscriber should be removed after unsubscribe",
                Wait.waitFor(() -> adminView.getInactiveDurableTopicSubscribers().length == 0,
                        WAIT_TIMEOUT, WAIT_INTERVAL));
    }

    @Test(expected = JMSException.class)
    public void testSharedDurableUnsubscribeWithActiveConsumerThrows() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.unsub.active");
        session.createSharedDurableConsumer(topic, "unsubActiveSub");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        session.unsubscribe("unsubActiveSub");
    }

    @Test
    public void testSharedDurableClientAcknowledge() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.clientack");
        MessageConsumer consumer = session.createSharedDurableConsumer(topic, "clientAckSub");

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("ack-me"));

        Message msg = consumer.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive message with CLIENT_ACKNOWLEDGE", msg);
        assertEquals("ack-me", ((TextMessage) msg).getText());
        msg.acknowledge();
    }

    @Test
    public void testSharedDurableDupsOkAcknowledge() throws Exception {
        connection1 = factory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.dupsok");
        MessageConsumer consumer = session.createSharedDurableConsumer(topic, "dupsOkSub");

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("dup-ok-msg"));

        Message msg = consumer.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive message with DUPS_OK_ACKNOWLEDGE", msg);
        assertEquals("dup-ok-msg", ((TextMessage) msg).getText());
    }

    @Test
    public void testConversionUnsharedToSharedWhenEnabled() throws Exception {
        broker.stop();
        broker.waitUntilStopped();

        broker = new SharedTopicBrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setBrokerName("shared-jms-conv1");
        broker.setTopicSubscriptionConversionEnabled(true);
        broker.addConnector("vm://shared-jms-conv1");
        broker.start();
        broker.waitUntilStarted();

        factory = new SharedTopicConnectionFactory("vm://shared-jms-conv1");

        connection1 = factory.createConnection();
        connection1.setClientID("client-conv1");
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.conv1");

        MessageConsumer unshared = session.createDurableSubscriber(topic, "convSub1");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Unshared subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        unshared.close();
        assertTrue("Subscriber should become inactive",
                Wait.waitFor(() -> adminView.getInactiveDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageConsumer shared = session.createSharedDurableConsumer(topic, "convSub1");

        assertTrue("Shared subscriber should register after conversion",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("converted-msg"));

        Message msg = shared.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive message after unshared-to-shared conversion", msg);
        assertEquals("converted-msg", ((TextMessage) msg).getText());
    }

    @Test
    public void testConversionSharedToUnsharedWhenEnabled() throws Exception {
        broker.stop();
        broker.waitUntilStopped();

        broker = new SharedTopicBrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setBrokerName("shared-jms-conv2");
        broker.setTopicSubscriptionConversionEnabled(true);
        broker.addConnector("vm://shared-jms-conv2");
        broker.start();
        broker.waitUntilStarted();

        factory = new SharedTopicConnectionFactory("vm://shared-jms-conv2");

        connection1 = factory.createConnection();
        connection1.setClientID("client-conv2");
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.conv2");

        MessageConsumer shared = session.createSharedDurableConsumer(topic, "convSub2");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Shared subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        shared.close();
        assertTrue("Subscriber should become inactive",
                Wait.waitFor(() -> adminView.getInactiveDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageConsumer unshared = session.createDurableSubscriber(topic, "convSub2");

        assertTrue("Unshared subscriber should register after conversion",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("converted-msg-2"));

        Message msg = unshared.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive message after shared-to-unshared conversion", msg);
        assertEquals("converted-msg-2", ((TextMessage) msg).getText());
    }

    @Test
    public void testSharedDurablePersistsAcrossRestart() throws Exception {
        broker.stop();
        broker.waitUntilStopped();

        String brokerName = "shared-persist-test";
        broker = createPersistentBroker(brokerName, true);
        factory = new SharedTopicConnectionFactory("vm://" + brokerName);

        connection1 = factory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.persist");
        MessageConsumer consumer = session.createSharedDurableConsumer(topic, "persistSub");

        BrokerView adminView = broker.getAdminView();
        assertTrue("Subscriber should register",
                Wait.waitFor(() -> adminView.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("before-restart"));
        Message msg1 = consumer.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive message before restart", msg1);

        consumer.close();
        assertTrue("Subscriber should become inactive",
                Wait.waitFor(() -> adminView.getInactiveDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        producer.send(session.createTextMessage("buffered-across-restart"));

        connection1.close();
        connection1 = null;
        broker.stop();
        broker.waitUntilStopped();

        // Restart broker — preserve data
        broker = createPersistentBroker(brokerName, false);
        factory = new SharedTopicConnectionFactory("vm://" + brokerName);

        BrokerView adminView2 = broker.getAdminView();
        assertTrue("Subscription should be restored as inactive after restart",
                Wait.waitFor(() -> adminView2.getInactiveDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        connection1 = factory.createConnection();
        connection1.start();
        Session session2 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic2 = session2.createTopic("test.shared.persist");
        MessageConsumer consumer2 = session2.createSharedDurableConsumer(topic2, "persistSub");

        assertTrue("Subscriber should reactivate after restart",
                Wait.waitFor(() -> adminView2.getDurableTopicSubscribers().length >= 1,
                        WAIT_TIMEOUT, WAIT_INTERVAL));

        Message buffered = consumer2.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive buffered message after restart", buffered);
        assertEquals("buffered-across-restart", ((TextMessage) buffered).getText());
    }

    @Test
    public void testSharedNonDurableDoesNotPersistAcrossRestart() throws Exception {
        broker.stop();
        broker.waitUntilStopped();

        String brokerName = "shared-nondurable-persist";
        broker = createPersistentBroker(brokerName, true);
        factory = new SharedTopicConnectionFactory("vm://" + brokerName);

        connection1 = factory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.shared.nondurable.persist");
        MessageConsumer consumer = session.createSharedConsumer(topic, "nonDurPersistSub");

        MessageProducer producer = session.createProducer(topic);
        producer.send(session.createTextMessage("ephemeral-msg"));
        Message msg = consumer.receive(WAIT_TIMEOUT);
        assertNotNull("Should receive message before restart", msg);

        connection1.close();
        connection1 = null;
        broker.stop();
        broker.waitUntilStopped();

        // Restart broker — preserve data
        broker = createPersistentBroker(brokerName, false);

        BrokerView adminView = broker.getAdminView();
        assertEquals("No durable subscriptions should exist after restart",
                0, adminView.getDurableTopicSubscribers().length);
        assertEquals("No inactive durable subscriptions should exist after restart",
                0, adminView.getInactiveDurableTopicSubscribers().length);
    }

    private SharedTopicBrokerService createPersistentBroker(String name, boolean deleteOnStartup)
            throws Exception {
        SharedTopicBrokerService b = new SharedTopicBrokerService();
        b.setPersistent(true);
        b.setUseJmx(true);
        b.setBrokerName(name);
        b.setDataDirectory("target/test-data/" + name);
        b.setDeleteAllMessagesOnStartup(deleteOnStartup);
        b.addConnector("vm://" + name);
        b.start();
        b.waitUntilStarted();
        return b;
    }

    private static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ignored) {
            }
        }
    }
}
