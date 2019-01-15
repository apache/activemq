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
package org.apache.activemq.store.kahadb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class KahaDBDurableMessageRecoveryTest {

    @Parameters(name = "recoverIndex={0},enableSubscriptionStats={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { false, false }, { false, true }, { true, false }, { true, true } });
    }

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));
    private BrokerService broker;
    private URI brokerConnectURI;

    private boolean recoverIndex;
    private boolean enableSubscriptionStats;

    @Before
    public void setUpBroker() throws Exception {
        startBroker(false);
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    /**
     * @param deleteIndex
     */
    public KahaDBDurableMessageRecoveryTest(boolean recoverIndex, boolean enableSubscriptionStats) {
        super();
        this.recoverIndex = recoverIndex;
        this.enableSubscriptionStats = enableSubscriptionStats;
    }

    protected void startBroker(boolean recoverIndex) throws Exception {
        broker = new BrokerService();
        broker.setPersistent(true);
        broker.setDataDirectoryFile(dataFileDir.getRoot());

        TransportConnector connector = broker.addConnector(new TransportConnector());
        connector.setUri(new URI("tcp://0.0.0.0:0"));
        connector.setName("tcp");
        configurePersistence(broker, recoverIndex);

        broker.start();
        broker.waitUntilStarted();
        brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();
    }

    protected void configurePersistence(BrokerService brokerService, boolean forceRecoverIndex) throws Exception {
        KahaDBPersistenceAdapter adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        adapter.setForceRecoverIndex(forceRecoverIndex);
        adapter.setEnableSubscriptionStatistics(enableSubscriptionStats);

        // set smaller size for test
        adapter.setJournalMaxFileLength(1024 * 20);
    }

    protected void restartBroker(boolean deleteIndex) throws Exception {
        stopBroker();
        startBroker(deleteIndex);
    }

    protected Session getSession(int ackMode) throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId1");
        connection.start();
        Session session = connection.createSession(false, ackMode);

        return session;
    }

    /**
     * Test that on broker restart a durable topic subscription will recover all
     * messages before the "last ack" in KahaDB which could happen if using
     * individual acknowledge mode and skipping messages
     */
    @Test
    public void durableRecoveryIndividualAcknowledge() throws Exception {
        String testTopic = "test.topic";

        Session session = getSession(ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        ActiveMQTopic topic = (ActiveMQTopic) session.createTopic(testTopic);
        MessageProducer producer = session.createProducer(topic);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, "sub1");

        for (int i = 1; i <= 10; i++) {
            producer.send(session.createTextMessage("msg: " + i));
        }
        producer.close();
        assertTrue(Wait.waitFor(() -> 10 == getPendingMessageCount(topic, "clientId1", "sub1"), 3000, 500));

        // Receive only the 5th message using individual ack mode
        for (int i = 1; i <= 10; i++) {
            TextMessage received = (TextMessage) subscriber.receive(1000);
            assertNotNull(received);
            if (i == 5) {
                received.acknowledge();
            }
        }

        // Verify there are 9 messages left still and restart broker
        assertTrue(Wait.waitFor(() -> 9 == getPendingMessageCount(topic, "clientId1", "sub1"), 3000, 500));
        subscriber.close();
        restartBroker(recoverIndex);

        // Verify 9 messages exist in store on startup
        assertTrue(Wait.waitFor(() -> 9 == getPendingMessageCount(topic, "clientId1", "sub1"), 3000, 500));

        // Recreate subscriber and try and receive the other 9 messages
        session = getSession(ActiveMQSession.AUTO_ACKNOWLEDGE);
        subscriber = session.createDurableSubscriber(topic, "sub1");

        for (int i = 1; i <= 4; i++) {
            TextMessage received = (TextMessage) subscriber.receive(1000);
            assertNotNull(received);
            assertEquals("msg: " + i, received.getText());
        }
        for (int i = 6; i <= 10; i++) {
            TextMessage received = (TextMessage) subscriber.receive(1000);
            assertNotNull(received);
            assertEquals("msg: " + i, received.getText());
        }

        subscriber.close();
        assertTrue(Wait.waitFor(() -> 0 == getPendingMessageCount(topic, "clientId1", "sub1"), 3000, 500));
    }

    @Test
    public void multipleDurableRecoveryIndividualAcknowledge() throws Exception {
        String testTopic = "test.topic";

        Session session = getSession(ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        ActiveMQTopic topic = (ActiveMQTopic) session.createTopic(testTopic);
        MessageProducer producer = session.createProducer(topic);
        TopicSubscriber subscriber1 = session.createDurableSubscriber(topic, "sub1");
        TopicSubscriber subscriber2 = session.createDurableSubscriber(topic, "sub2");

        for (int i = 1; i <= 10; i++) {
            producer.send(session.createTextMessage("msg: " + i));
        }
        producer.close();
        assertTrue(Wait.waitFor(() -> 10 == getPendingMessageCount(topic, "clientId1", "sub1"), 3000, 500));
        assertTrue(Wait.waitFor(() -> 10 == getPendingMessageCount(topic, "clientId1", "sub2"), 3000, 500));

        // Receive 2 messages using individual ack mode only on first sub
        for (int i = 1; i <= 10; i++) {
            TextMessage received = (TextMessage) subscriber1.receive(1000);
            assertNotNull(received);
            if (i == 3 || i == 7) {
                received.acknowledge();
            }
        }

        // Verify there are 8 messages left still and restart broker
        assertTrue(Wait.waitFor(() -> 8 == getPendingMessageCount(topic, "clientId1", "sub1"), 3000, 500));
        assertTrue(Wait.waitFor(() -> 10 == getPendingMessageCount(topic, "clientId1", "sub2"), 3000, 500));

        // Verify the pending size is less for sub1
        final long sub1PendingSizeBeforeRestart = getPendingMessageSize(topic, "clientId1", "sub1");
        final long sub2PendingSizeBeforeRestart = getPendingMessageSize(topic, "clientId1", "sub2");
        assertTrue(sub1PendingSizeBeforeRestart > 0);
        assertTrue(sub2PendingSizeBeforeRestart > 0);
        assertTrue(sub1PendingSizeBeforeRestart < sub2PendingSizeBeforeRestart);

        subscriber1.close();
        subscriber2.close();
        restartBroker(recoverIndex);

        // Verify 8 messages exist in store on startup on sub 1 and 10 on sub 2
        assertTrue(Wait.waitFor(() -> 8 == getPendingMessageCount(topic, "clientId1", "sub1"), 3000, 500));
        assertTrue(Wait.waitFor(() -> 10 == getPendingMessageCount(topic, "clientId1", "sub2"), 3000, 500));

        // Verify the pending size is less for sub1
        assertEquals(sub1PendingSizeBeforeRestart, getPendingMessageSize(topic, "clientId1", "sub1"));
        assertEquals(sub2PendingSizeBeforeRestart, getPendingMessageSize(topic, "clientId1", "sub2"));

        // Recreate subscriber and try and receive the other 8 messages
        session = getSession(ActiveMQSession.AUTO_ACKNOWLEDGE);
        subscriber1 = session.createDurableSubscriber(topic, "sub1");
        subscriber2 = session.createDurableSubscriber(topic, "sub2");

        for (int i = 1; i <= 2; i++) {
            TextMessage received = (TextMessage) subscriber1.receive(1000);
            assertNotNull(received);
            assertEquals("msg: " + i, received.getText());
        }
        for (int i = 4; i <= 6; i++) {
            TextMessage received = (TextMessage) subscriber1.receive(1000);
            assertNotNull(received);
            assertEquals("msg: " + i, received.getText());
        }
        for (int i = 8; i <= 10; i++) {
            TextMessage received = (TextMessage) subscriber1.receive(1000);
            assertNotNull(received);
            assertEquals("msg: " + i, received.getText());
        }

        // Make sure sub 2 gets all 10
        for (int i = 1; i <= 10; i++) {
            TextMessage received = (TextMessage) subscriber2.receive(1000);
            assertNotNull(received);
            assertEquals("msg: " + i, received.getText());
        }

        subscriber1.close();
        subscriber2.close();
        assertTrue(Wait.waitFor(() -> 0 == getPendingMessageCount(topic, "clientId1", "sub1"), 3000, 500));
        assertTrue(Wait.waitFor(() -> 0 == getPendingMessageCount(topic, "clientId1", "sub2"), 3000, 500));
    }

    @Test
    public void multipleDurableTestRecoverSubscription() throws Exception {
        String testTopic = "test.topic";

        Session session = getSession(ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        ActiveMQTopic topic = (ActiveMQTopic) session.createTopic(testTopic);
        MessageProducer producer = session.createProducer(topic);
        TopicSubscriber subscriber1 = session.createDurableSubscriber(topic, "sub1");
        TopicSubscriber subscriber2 = session.createDurableSubscriber(topic, "sub2");

        for (int i = 1; i <= 10; i++) {
            producer.send(session.createTextMessage("msg: " + i));
        }
        producer.close();

        // Receive 2 messages using individual ack mode only on first sub
        for (int i = 1; i <= 10; i++) {
            TextMessage received = (TextMessage) subscriber1.receive(1000);
            assertNotNull(received);
            if (i == 3 || i == 7) {
                received.acknowledge();
            }
        }

        // Verify there are 8 messages left on sub 1 and 10 on sub2 and restart
        assertTrue(Wait.waitFor(() -> 8 == getPendingMessageCount(topic, "clientId1", "sub1"), 3000, 500));
        assertTrue(Wait.waitFor(() -> 10 == getPendingMessageCount(topic, "clientId1", "sub2"), 3000, 500));
        subscriber1.close();
        subscriber2.close();
        restartBroker(recoverIndex);

        // Manually recover subscription and verify proper messages are loaded
        final Topic brokerTopic = (Topic) broker.getDestination(topic);
        final TopicMessageStore store = (TopicMessageStore) brokerTopic.getMessageStore();
        final AtomicInteger sub1Recovered = new AtomicInteger();
        final AtomicInteger sub2Recovered = new AtomicInteger();
        store.recoverSubscription("clientId1", "sub1", new MessageRecoveryListener() {
            @Override
            public boolean recoverMessageReference(MessageId ref) throws Exception {
                return false;
            }

            @Override
            public boolean recoverMessage(Message message) throws Exception {
                TextMessage textMessage = (TextMessage) message;
                if (textMessage.getText().equals("msg: " + 3) || textMessage.getText().equals("msg: " + 7)) {
                    throw new IllegalStateException("Got wrong message: " + textMessage.getText());
                }
                sub1Recovered.incrementAndGet();
                return true;
            }

            @Override
            public boolean isDuplicate(MessageId ref) {
                return false;
            }

            @Override
            public boolean hasSpace() {
                return true;
            }
        });

        store.recoverSubscription("clientId1", "sub2", new MessageRecoveryListener() {
            @Override
            public boolean recoverMessageReference(MessageId ref) throws Exception {
                return false;
            }

            @Override
            public boolean recoverMessage(Message message) throws Exception {
                sub2Recovered.incrementAndGet();
                return true;
            }

            @Override
            public boolean isDuplicate(MessageId ref) {
                return false;
            }

            @Override
            public boolean hasSpace() {
                return true;
            }
        });

        // Verify proper number of messages are recovered
        assertEquals(8, sub1Recovered.get());
        assertEquals(10, sub2Recovered.get());
    }

    protected long getPendingMessageCount(ActiveMQTopic topic, String clientId, String subId) throws Exception {
        final Topic brokerTopic = (Topic) broker.getDestination(topic);
        final TopicMessageStore store = (TopicMessageStore) brokerTopic.getMessageStore();
        return store.getMessageCount(clientId, subId);
    }

    protected long getPendingMessageSize(ActiveMQTopic topic, String clientId, String subId) throws Exception {
        final Topic brokerTopic = (Topic) broker.getDestination(topic);
        final TopicMessageStore store = (TopicMessageStore) brokerTopic.getMessageStore();
        return store.getMessageSize(clientId, subId);
    }
}
