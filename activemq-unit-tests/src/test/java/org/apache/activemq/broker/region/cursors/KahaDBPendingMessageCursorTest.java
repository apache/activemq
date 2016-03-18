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
package org.apache.activemq.broker.region.cursors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.MessageStoreSubscriptionStatistics;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks that pending message metrics work properly with KahaDB
 *
 * AMQ-5923, AMQ-6375
 *
 */
@RunWith(Parameterized.class)
public class KahaDBPendingMessageCursorTest extends
        AbstractPendingMessageCursorTest {

    protected static final Logger LOG = LoggerFactory
            .getLogger(KahaDBPendingMessageCursorTest.class);

    @Parameters(name = "prioritizedMessages={0},enableSubscriptionStatistics={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // use priority messages
                { true, true },
                { true, false },
                // don't use priority messages
                { false, true },
                { false, false }
        });
    }

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    /**
     * @param prioritizedMessages
     */
    public KahaDBPendingMessageCursorTest(final boolean prioritizedMessages,
            final boolean enableSubscriptionStatistics) {
        super(prioritizedMessages);
        this.enableSubscriptionStatistics = enableSubscriptionStatistics;
    }

    @Override
    protected void setUpBroker(boolean clearDataDir) throws Exception {
        if (clearDataDir && dataFileDir.getRoot().exists())
            FileUtils.cleanDirectory(dataFileDir.getRoot());
        super.setUpBroker(clearDataDir);
    }

    @Override
    protected void initPersistence(BrokerService brokerService)
            throws IOException {
        broker.setPersistent(true);
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(dataFileDir.getRoot());
        persistenceAdapter.setEnableSubscriptionStatistics(enableSubscriptionStatistics);
        broker.setPersistenceAdapter(persistenceAdapter);
    }

    /**
     * Test that the the counter restores size and works after restart and more
     * messages are published
     *
     * @throws Exception
     */
    @Test
    public void testDurableMessageSizeAfterRestartAndPublish() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();
        Topic topic =  publishTestMessagesDurable(connection, new String[] {"sub1"}, 200,
                publishedMessageSize, DeliveryMode.PERSISTENT);

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");

        // verify the count and size
        verifyPendingStats(topic, subKey, 200, publishedMessageSize.get());
        verifyStoreStats(topic, 200, publishedMessageSize.get());

        //should be equal in this case
        long beforeRestartSize = topic.getDurableTopicSubs().get(subKey).getPendingMessageSize();
        assertEquals(beforeRestartSize,
                topic.getMessageStore().getMessageStoreStatistics().getMessageSize().getTotalSize());

        // stop, restart broker and publish more messages
        stopBroker();
        this.setUpBroker(false);

        //verify that after restart the size is the same as before restart on recovery
        topic = (Topic) getBroker().getDestination(new ActiveMQTopic(defaultTopicName));
        assertEquals(beforeRestartSize, topic.getDurableTopicSubs().get(subKey).getPendingMessageSize());

        connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        topic = publishTestMessagesDurable(connection, new String[] {"sub1"}, 200,
                publishedMessageSize, DeliveryMode.PERSISTENT);

        // verify the count and size
        verifyPendingStats(topic, subKey, 400, publishedMessageSize.get());
        verifyStoreStats(topic, 400, publishedMessageSize.get());

    }

    @Test
    public void testMessageSizeTwoDurablesPartialConsumption() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        SubscriptionKey subKey2 = new SubscriptionKey("clientId", "sub2");
        org.apache.activemq.broker.region.Topic dest = publishTestMessagesDurable(
                connection, new String[] {"sub1", "sub2"}, 200, publishedMessageSize, DeliveryMode.PERSISTENT);

        //verify the count and size - durable is offline so all 200 should be pending since none are in prefetch
        verifyPendingStats(dest, subKey, 200, publishedMessageSize.get());
        verifyStoreStats(dest, 200, publishedMessageSize.get());

        //consume all messages
        consumeDurableTestMessages(connection, "sub1", 50, publishedMessageSize);

        //150 should be left
        verifyPendingStats(dest, subKey, 150, publishedMessageSize.get());

        //200 should be left
        verifyPendingStats(dest, subKey2, 200, publishedMessageSize.get());
        verifyStoreStats(dest, 200, publishedMessageSize.get());

        connection.close();
    }

    /**
     * Test that the the counter restores size and works after restart and more
     * messages are published
     *
     * @throws Exception
     */
    @Test
    public void testNonPersistentDurableMessageSize() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();
        Topic topic =  publishTestMessagesDurable(connection, new String[] {"sub1"}, 200,
                publishedMessageSize, DeliveryMode.NON_PERSISTENT);

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");

        // verify the count and size
        verifyPendingStats(topic, subKey, 200, publishedMessageSize.get());
        verifyStoreStats(topic, 0, 0);
    }

    /**
     * Test that the subscription counters are properly set when enabled
     * and not set when disabled
     *
     * @throws Exception
     */
    @Test
    public void testEnabledSubscriptionStatistics() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        SubscriptionKey subKey2 = new SubscriptionKey("clientId", "sub2");
        org.apache.activemq.broker.region.Topic dest = publishTestMessagesDurable(
                connection, new String[] {"sub1", "sub2"}, 200, publishedMessageSize, DeliveryMode.PERSISTENT);

        TopicMessageStore store = (TopicMessageStore) dest.getMessageStore();
        MessageStoreSubscriptionStatistics stats = store.getMessageStoreSubStatistics();
        if (enableSubscriptionStatistics) {
            assertTrue(stats.getMessageCount(subKey.toString()).getCount() == 200);
            assertTrue(stats.getMessageSize(subKey.toString()).getTotalSize() > 0);
            assertTrue(stats.getMessageCount(subKey2.toString()).getCount() == 200);
            assertTrue(stats.getMessageSize(subKey2.toString()).getTotalSize() > 0);
            assertEquals(stats.getMessageCount().getCount(),
                    stats.getMessageCount(subKey.toString()).getCount() +
                    stats.getMessageSize(subKey.toString()).getCount());
            assertEquals(stats.getMessageSize().getTotalSize(),
                    stats.getMessageSize(subKey.toString()).getTotalSize() +
                    stats.getMessageSize(subKey2.toString()).getTotalSize());

            //Delete second subscription and verify stats are updated accordingly
            store.deleteSubscription(subKey2.getClientId(), subKey2.getSubscriptionName());
            assertEquals(stats.getMessageCount().getCount(), stats.getMessageCount(subKey.toString()).getCount());
            assertEquals(stats.getMessageSize().getTotalSize(), stats.getMessageSize(subKey.toString()).getTotalSize());
            assertTrue(stats.getMessageCount(subKey2.toString()).getCount() == 0);
            assertTrue(stats.getMessageSize(subKey2.toString()).getTotalSize() == 0);

        } else {
            assertTrue(stats.getMessageCount(subKey.toString()).getCount() == 0);
            assertTrue(stats.getMessageSize(subKey.toString()).getTotalSize() == 0);
            assertTrue(stats.getMessageCount(subKey2.toString()).getCount() == 0);
            assertTrue(stats.getMessageSize(subKey2.toString()).getTotalSize() == 0);
            assertEquals(0, stats.getMessageCount().getCount());
            assertEquals(0, stats.getMessageSize().getTotalSize());
        }

    }

    @Test
    public void testUpdateMessageSubSize() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();
        Session session = connection.createSession(false, TopicSession.AUTO_ACKNOWLEDGE);
        javax.jms.Topic dest = session.createTopic(defaultTopicName);
        session.createDurableSubscriber(dest, "sub1");
        session.createDurableSubscriber(dest, "sub2");
        MessageProducer prod = session.createProducer(dest);

        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("SmallMessage");
        prod.send(message);

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        SubscriptionKey subKey2 = new SubscriptionKey("clientId", "sub1");

        final Topic topic =  (Topic) getBroker().getDestination(new ActiveMQTopic(defaultTopicName));
        final DurableTopicSubscription sub = topic.getDurableTopicSubs().get(subKey);
        final DurableTopicSubscription sub2 = topic.getDurableTopicSubs().get(subKey2);
        long sizeBeforeUpdate = sub.getPendingMessageSize();

        message = (ActiveMQTextMessage) topic.getMessageStore().getMessage(message.getMessageId());
        message.setText("LargerMessageLargerMessage");

        //update the message
        topic.getMessageStore().updateMessage(message);

        //should be at least 10 bytes bigger and match the store size
        assertTrue(sub.getPendingMessageSize() > sizeBeforeUpdate + 10);
        assertEquals(sub.getPendingMessageSize(), topic.getMessageStore().getMessageSize());
        assertEquals(sub.getPendingMessageSize(), sub2.getPendingMessageSize());
    }

    @Test
    public void testUpdateMessageSubSizeAfterConsume() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();
        Session session = connection.createSession(false, TopicSession.AUTO_ACKNOWLEDGE);
        javax.jms.Topic dest = session.createTopic(defaultTopicName);
        session.createDurableSubscriber(dest, "sub1");
        TopicSubscriber subscriber2 = session.createDurableSubscriber(dest, "sub2");
        MessageProducer prod = session.createProducer(dest);

        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("SmallMessage");
        ActiveMQTextMessage message2 = new ActiveMQTextMessage();
        message2.setText("SmallMessage2");
        prod.send(message);
        prod.send(message2);

        //Receive first message for sub 2 and wait for stats to update
        subscriber2.receive();

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        SubscriptionKey subKey2 = new SubscriptionKey("clientId", "sub2");
        final Topic topic =  (Topic) getBroker().getDestination(new ActiveMQTopic(defaultTopicName));
        final DurableTopicSubscription sub = topic.getDurableTopicSubs().get(subKey);
        final DurableTopicSubscription sub2 = topic.getDurableTopicSubs().get(subKey2);

        Wait.waitFor(new Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return sub.getPendingMessageSize() > sub2.getPendingMessageSize();
            }
        });

        long sizeBeforeUpdate = sub.getPendingMessageSize();
        long sizeBeforeUpdate2 = sub2.getPendingMessageSize();

        //update message 2
        message = (ActiveMQTextMessage) topic.getMessageStore().getMessage(message.getMessageId());
        message.setText("LargerMessageLargerMessage");

        //update the message
        topic.getMessageStore().updateMessage(message);

        //should be at least 10 bytes bigger and match the store size
        assertTrue(sub.getPendingMessageSize() > sizeBeforeUpdate + 10);
        assertEquals(sub.getPendingMessageSize(), topic.getMessageStore().getMessageSize());

        //Sub2 only has 1 message so should be less than sub, verify that the update message
        //didn't update the stats of sub2 and sub1 should be over twice as large since the
        //updated message is bigger
        assertTrue(sub.getPendingMessageSize() > 2 * sub2.getPendingMessageSize());
        assertEquals(sizeBeforeUpdate2, sub2.getPendingMessageSize());

    }

}
