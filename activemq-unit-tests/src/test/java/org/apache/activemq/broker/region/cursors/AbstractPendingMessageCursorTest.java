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

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.TopicSubscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.AbstractStoreStatTestSupport;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks that KahaDB properly sets the new storeMessageSize statistic.
 *
 * AMQ-5748
 *
 */
public abstract class AbstractPendingMessageCursorTest extends AbstractStoreStatTestSupport {
    protected static final Logger LOG = LoggerFactory
            .getLogger(AbstractPendingMessageCursorTest.class);


    protected BrokerService broker;
    protected URI brokerConnectURI;
    protected String defaultQueueName = "test.queue";
    protected String defaultTopicName = "test.topic";
    protected static int maxMessageSize = 1000;
    protected final boolean prioritizedMessages;
    protected boolean enableSubscriptionStatistics;

    @Rule
    public Timeout globalTimeout= new Timeout(60, TimeUnit.SECONDS);

    /**
     * @param prioritizedMessages
     */
    public AbstractPendingMessageCursorTest(final boolean prioritizedMessages) {
        super();
        this.prioritizedMessages = prioritizedMessages;
    }

    @Before
    public void startBroker() throws Exception {
        setUpBroker(true);
    }

    protected void setUpBroker(boolean clearDataDir) throws Exception {

        broker = new BrokerService();
        this.initPersistence(broker);
        //set up a transport
        TransportConnector connector = broker
                .addConnector(new TransportConnector());
        connector.setUri(new URI("tcp://0.0.0.0:0"));
        connector.setName("tcp");

        PolicyEntry policy = new PolicyEntry();
        policy.setTopicPrefetch(100);
        policy.setDurableTopicPrefetch(100);
        policy.setPrioritizedMessages(isPrioritizedMessages());
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);

        broker.start();
        broker.waitUntilStarted();
        brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();

    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Override
    protected BrokerService getBroker() {
        return this.broker;
    }

    @Override
    protected URI getBrokerConnectURI() {
        return this.brokerConnectURI;
    }

    protected abstract void initPersistence(BrokerService brokerService) throws IOException;

    protected boolean isPrioritizedMessages() {
        return prioritizedMessages;
    }

    @Test
    public void testQueueMessageSize() throws Exception {
        //doesn't apply to queues, only run once
        Assume.assumeFalse(enableSubscriptionStatistics);

        AtomicLong publishedMessageSize = new AtomicLong();

        org.apache.activemq.broker.region.Queue dest = publishTestQueueMessages(200, publishedMessageSize);
        verifyPendingStats(dest, 200, publishedMessageSize.get());
        verifyStoreStats(dest, 200, publishedMessageSize.get());
    }

    @Test
    public void testQueueBrowserMessageSize() throws Exception {
        //doesn't apply to queues, only run once
        Assume.assumeFalse(enableSubscriptionStatistics);

        AtomicLong publishedMessageSize = new AtomicLong();

        org.apache.activemq.broker.region.Queue dest = publishTestQueueMessages(200, publishedMessageSize);
        browseTestQueueMessages(dest.getName());
        verifyPendingStats(dest, 200, publishedMessageSize.get());
        verifyStoreStats(dest, 200, publishedMessageSize.get());
    }

    @Test
    public void testQueueMessageSizeNonPersistent() throws Exception {
        //doesn't apply to queues, only run once
        Assume.assumeFalse(enableSubscriptionStatistics);

        AtomicLong publishedMessageSize = new AtomicLong();

        org.apache.activemq.broker.region.Queue dest = publishTestQueueMessages(200,
                DeliveryMode.NON_PERSISTENT, publishedMessageSize);
        verifyPendingStats(dest, 200, publishedMessageSize.get());
    }

    @Test
    public void testQueueMessageSizePersistentAndNonPersistent() throws Exception {
        //doesn't apply to queues, only run once
        Assume.assumeFalse(enableSubscriptionStatistics);

        AtomicLong publishedNonPersistentMessageSize = new AtomicLong();
        AtomicLong publishedMessageSize = new AtomicLong();

        org.apache.activemq.broker.region.Queue dest = publishTestQueueMessages(100,
                DeliveryMode.PERSISTENT, publishedMessageSize);
        dest = publishTestQueueMessages(100,
                DeliveryMode.NON_PERSISTENT, publishedNonPersistentMessageSize);
        verifyPendingStats(dest, 200, publishedMessageSize.get() + publishedNonPersistentMessageSize.get());
        verifyStoreStats(dest, 100, publishedMessageSize.get());
    }

    @Test
    public void testQueueMessageSizeAfterConsumption() throws Exception {
        //doesn't apply to queues, only run once
        Assume.assumeFalse(enableSubscriptionStatistics);

        AtomicLong publishedMessageSize = new AtomicLong();

        org.apache.activemq.broker.region.Queue dest = publishTestQueueMessages(200, publishedMessageSize);
        verifyPendingStats(dest, 200, publishedMessageSize.get());

        consumeTestQueueMessages();

        verifyPendingStats(dest, 0, 0);
        verifyStoreStats(dest, 0, 0);
    }

    @Test
    public void testQueueMessageSizeAfterConsumptionNonPersistent() throws Exception {
        //doesn't apply to queues, only run once
        Assume.assumeFalse(enableSubscriptionStatistics);

        AtomicLong publishedMessageSize = new AtomicLong();

        org.apache.activemq.broker.region.Queue dest = publishTestQueueMessages(200, DeliveryMode.NON_PERSISTENT, publishedMessageSize);
        verifyPendingStats(dest, 200, publishedMessageSize.get());

        consumeTestQueueMessages();

        verifyPendingStats(dest, 0, 0);
        verifyStoreStats(dest, 0, 0);
    }

    @Test
    public void testTopicMessageSize() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(new ActiveMQTopic(this.defaultTopicName));

        org.apache.activemq.broker.region.Topic dest = publishTestTopicMessages(200, publishedMessageSize);

        //verify the count and size - there is a prefetch of 100 so only 100 are pending and 100
        //are dispatched because we have an active consumer online
        //verify that the size is greater than 100 messages times the minimum size of 100
        verifyPendingStats(dest, 100, 100 * 100);

        //consume all messages
        consumeTestMessages(consumer, 200);

        //All messages should now be gone
        verifyPendingStats(dest, 0, 0);

        connection.close();
    }

    @Test
    public void testTopicNonPersistentMessageSize() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(new ActiveMQTopic(this.defaultTopicName));

        org.apache.activemq.broker.region.Topic dest = publishTestTopicMessages(200,
                DeliveryMode.NON_PERSISTENT, publishedMessageSize);

        //verify the count and size - there is a prefetch of 100 so only 100 are pending and 100
        //are dispatched because we have an active consumer online
        //verify the size is at least as big as 100 messages times the minimum of 100 size
        verifyPendingStats(dest, 100, 100 * 100);

        //consume all messages
        consumeTestMessages(consumer, 200);

        //All messages should now be gone
        verifyPendingStats(dest, 0, 0);

        connection.close();
    }

    @Test
    public void testTopicPersistentAndNonPersistentMessageSize() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(new ActiveMQTopic(this.defaultTopicName));

        org.apache.activemq.broker.region.Topic dest = publishTestTopicMessages(100,
                DeliveryMode.NON_PERSISTENT, publishedMessageSize);

        dest = publishTestTopicMessages(100, DeliveryMode.PERSISTENT, publishedMessageSize);

        //verify the count and size - there is a prefetch of 100 so only 100 are pending and 100
        //are dispatched because we have an active consumer online
      //verify the size is at least as big as 100 messages times the minimum of 100 size
        verifyPendingStats(dest, 100, 100 * 100);

        //consume all messages
        consumeTestMessages(consumer, 200);

        //All messages should now be gone
        verifyPendingStats(dest, 0, 0);

        connection.close();
    }

    @Test
    public void testMessageSizeOneDurable() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        org.apache.activemq.broker.region.Topic dest = publishTestMessagesDurable(connection,
                new String[] {"sub1"}, 200, publishedMessageSize, DeliveryMode.PERSISTENT);

        //verify the count and size - durable is offline so all 200 should be pending since none are in prefetch
        verifyPendingStats(dest, subKey, 200, publishedMessageSize.get());
        verifyStoreStats(dest, 200, publishedMessageSize.get());

        //should be equal in this case
        assertEquals(dest.getDurableTopicSubs().get(subKey).getPendingMessageSize(),
                dest.getMessageStore().getMessageStoreStatistics().getMessageSize().getTotalSize());

        //consume all messages
        consumeDurableTestMessages(connection, "sub1", 200, publishedMessageSize);

        //All messages should now be gone
        verifyPendingStats(dest, subKey, 0, 0);
        verifyStoreStats(dest, 0, 0);

        connection.close();
    }

    @Test
    public void testMessageSizeOneDurablePartialConsumption() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        org.apache.activemq.broker.region.Topic dest = publishTestMessagesDurable(
                connection, new String[] {"sub1"}, 200, publishedMessageSize, DeliveryMode.PERSISTENT);

        //verify the count and size - durable is offline so all 200 should be pending since none are in prefetch
        verifyPendingStats(dest, subKey, 200, publishedMessageSize.get());
        verifyStoreStats(dest, 200, publishedMessageSize.get());

        //consume all messages
        consumeDurableTestMessages(connection, "sub1", 50, publishedMessageSize);

        //150 should be left
        verifyPendingStats(dest, subKey, 150, publishedMessageSize.get());
        verifyStoreStats(dest, 150, publishedMessageSize.get());

        connection.close();
    }

    @Test
    public void testMessageSizeTwoDurables() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        org.apache.activemq.broker.region.Topic dest =
                publishTestMessagesDurable(connection, new String[] {"sub1", "sub2"}, 200,
                        publishedMessageSize, DeliveryMode.PERSISTENT);

        //verify the count and size
        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        verifyPendingStats(dest, subKey, 200, publishedMessageSize.get());

        //consume messages just for sub1
        consumeDurableTestMessages(connection, "sub1", 200, publishedMessageSize);

        //There is still a durable that hasn't consumed so the messages should exist
        SubscriptionKey subKey2 = new SubscriptionKey("clientId", "sub2");
        verifyPendingStats(dest, subKey, 0, 0);
        verifyPendingStats(dest, subKey2, 200, publishedMessageSize.get());
        verifyStoreStats(dest, 200, publishedMessageSize.get());

        connection.stop();
    }


    protected void verifyPendingStats(final org.apache.activemq.broker.region.Queue queue,
            final int count, final long minimumSize) throws Exception {
        this.verifyPendingStats(queue, count, minimumSize, count, minimumSize);
    }

    protected void verifyPendingStats(final org.apache.activemq.broker.region.Queue queue,
            final int count, final long minimumSize, final int storeCount, final long minimumStoreSize) throws Exception {

        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return queue.getPendingMessageCount() == count;
            }
        }));

        verifySize(count, new MessageSizeCalculator() {
            @Override
            public long getMessageSize() throws Exception {
                return queue.getPendingMessageSize();
            }
        }, minimumSize);
    }

    //For a non-durable there won't necessarily be a message store
    protected void verifyPendingStats(org.apache.activemq.broker.region.Topic topic,
            final int count, final long minimumSize) throws Exception {

        final TopicSubscription sub = (TopicSubscription) topic.getConsumers().get(0);

        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return sub.getPendingQueueSize() == count;
            }
        }));

        verifySize(count, new MessageSizeCalculator() {
            @Override
            public long getMessageSize() throws Exception {
                return sub.getPendingMessageSize();
            }
        }, minimumSize);
    }

    protected void verifyPendingStats(org.apache.activemq.broker.region.Topic topic, SubscriptionKey subKey,
            final int count, final long minimumSize) throws Exception {

        final DurableTopicSubscription sub = topic.getDurableTopicSubs().get(subKey);

        //verify message count
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return sub.getPendingQueueSize() == count;
            }
        }));

        //verify message size
        verifySize(count, new MessageSizeCalculator() {
            @Override
            public long getMessageSize() throws Exception {
                return sub.getPendingMessageSize();
            }
        }, minimumSize);
    }

    protected void verifyStoreStats(org.apache.activemq.broker.region.Destination dest,
            final int storeCount, final long minimumStoreSize) throws Exception {
        final MessageStore messageStore = dest.getMessageStore();

        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return messageStore.getMessageCount() == storeCount;
            }
        }));
        verifySize(storeCount, new MessageSizeCalculator() {
            @Override
            public long getMessageSize() throws Exception {
                return messageStore.getMessageSize();
            }
        }, minimumStoreSize);

    }


    protected void verifySize(final int count, final MessageSizeCalculator messageSizeCalculator,
            final long minimumSize) throws Exception {
        if (count > 0) {
            assertTrue(Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return messageSizeCalculator.getMessageSize() > minimumSize ;
                }
            }));
        } else {
            assertTrue(Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return messageSizeCalculator.getMessageSize() == 0;
                }
            }));
        }
    }

    protected static interface MessageSizeCalculator {
        long getMessageSize() throws Exception;
    }


    protected Destination consumeTestMessages(MessageConsumer consumer, int size) throws Exception {
        return consumeTestMessages(consumer, size, defaultTopicName);
    }


    protected Destination consumeTestMessages(MessageConsumer consumer, int size, String topicName) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqTopic = new ActiveMQTopic(
                topicName);

        Destination dest = broker.getDestination(activeMqTopic);

        //Topic topic = session.createTopic(topicName);

        try {
            for (int i = 0; i < size; i++) {
                consumer.receive();
            }

        } finally {
            //session.close();
        }

        return dest;
    }

    protected Destination consumeDurableTestMessages(Connection connection, String sub, int size, AtomicLong publishedMessageSize) throws Exception {
        return consumeDurableTestMessages(connection, sub, size, defaultTopicName, publishedMessageSize);
    }

    protected org.apache.activemq.broker.region.Topic publishTestMessagesDurable(Connection connection,
            String[] subNames, int publishSize, AtomicLong publishedMessageSize, int deliveryMode) throws Exception {

        return publishTestMessagesDurable(connection, subNames, defaultTopicName,
                publishSize, 0, AbstractStoreStatTestSupport.defaultMessageSize,
                publishedMessageSize, false, deliveryMode);
    }

    protected org.apache.activemq.broker.region.Topic publishTestTopicMessages(int publishSize,
            AtomicLong publishedMessageSize) throws Exception {
        return publishTestTopicMessages(publishSize, DeliveryMode.PERSISTENT, publishedMessageSize);
    }

    protected org.apache.activemq.broker.region.Topic publishTestTopicMessages(int publishSize,
            int deliveryMode, AtomicLong publishedMessageSize) throws Exception {
        // create a new queue
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId2");
        connection.start();

        final ActiveMQDestination activeMqTopic = new ActiveMQTopic(
                defaultTopicName);

        org.apache.activemq.broker.region.Topic dest =
                (org.apache.activemq.broker.region.Topic) broker.getDestination(activeMqTopic);

        // Start the connection
        Session session = connection.createSession(false,
                TopicSession.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(defaultTopicName);

        try {
            // publish a bunch of non-persistent messages to fill up the temp
            // store
            MessageProducer prod = session.createProducer(topic);
            prod.setDeliveryMode(deliveryMode);
            for (int i = 0; i < publishSize; i++) {
                prod.send(createMessage(i, session, AbstractPendingMessageCursorTest.maxMessageSize, publishedMessageSize));
            }

        } finally {
            connection.close();
        }

        return dest;
    }

    protected org.apache.activemq.broker.region.Queue publishTestQueueMessages(int count,
            AtomicLong publishedMessageSize) throws Exception {
        return publishTestQueueMessages(count, defaultQueueName, DeliveryMode.PERSISTENT,
                AbstractPendingMessageCursorTest.maxMessageSize, publishedMessageSize);
    }

    protected org.apache.activemq.broker.region.Queue publishTestQueueMessages(int count, int deliveryMode,
            AtomicLong publishedMessageSize) throws Exception {
        return publishTestQueueMessages(count, defaultQueueName, deliveryMode,
                AbstractPendingMessageCursorTest.maxMessageSize, publishedMessageSize);
    }

    protected Destination consumeTestQueueMessages() throws Exception {
        return consumeTestQueueMessages(defaultQueueName);
    }

}
