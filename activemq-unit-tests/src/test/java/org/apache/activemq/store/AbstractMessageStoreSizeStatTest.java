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
package org.apache.activemq.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks that KahaDB properly sets the new storeMessageSize statistic.
 *
 * AMQ-5748
 *
 */
public abstract class AbstractMessageStoreSizeStatTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(AbstractMessageStoreSizeStatTest.class);


    protected BrokerService broker;
    protected URI brokerConnectURI;
    protected String defaultQueueName = "test.queue";
    protected String defaultTopicName = "test.topic";
    protected static int messageSize = 1000;

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

        broker.start();
        broker.waitUntilStarted();
        brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();

    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    protected abstract void initPersistence(BrokerService brokerService) throws IOException;

    @Test
    public void testMessageSize() throws Exception {
        Destination dest = publishTestQueueMessages(200);
        verifyStats(dest, 200, 200 * messageSize);
    }

    @Test
    public void testMessageSizeAfterConsumption() throws Exception {

        Destination dest = publishTestQueueMessages(200);
        verifyStats(dest, 200, 200 * messageSize);

        consumeTestQueueMessages();

        verifyStats(dest, 0, 0);
    }

    @Test
    public void testMessageSizeOneDurable() throws Exception {

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        Destination dest = publishTestMessagesDurable(connection, new String[] {"sub1"}, 200, 200);

        //verify the count and size
        verifyStats(dest, 200, 200 * messageSize);

        //consume all messages
        consumeDurableTestMessages(connection, "sub1", 200);

        //All messages should now be gone
        verifyStats(dest, 0, 0);

        connection.close();
    }

    @Test(timeout=10000)
    public void testMessageSizeTwoDurables() throws Exception {

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        Destination dest = publishTestMessagesDurable(connection, new String[] {"sub1", "sub2"}, 200, 200);

        //verify the count and size
        verifyStats(dest, 200, 200 * messageSize);

        //consume messages just for sub1
        consumeDurableTestMessages(connection, "sub1", 200);

        //There is still a durable that hasn't consumed so the messages should exist
        verifyStats(dest, 200, 200 * messageSize);

        connection.stop();

    }

    @Test
    public void testMessageSizeAfterDestinationDeletion() throws Exception {
        Destination dest = publishTestQueueMessages(200);
        verifyStats(dest, 200, 200 * messageSize);

        //check that the size is 0 after deletion
        broker.removeDestination(dest.getActiveMQDestination());
        verifyStats(dest, 0, 0);
    }

    protected void verifyStats(Destination dest, final int count, final long minimumSize) throws Exception {
        final MessageStore messageStore = dest.getMessageStore();
        final MessageStoreStatistics storeStats = dest.getMessageStore().getMessageStoreStatistics();

        Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return (count == messageStore.getMessageCount()) && (messageStore.getMessageCount() ==
                        storeStats.getMessageCount().getCount()) && (messageStore.getMessageSize() ==
                messageStore.getMessageStoreStatistics().getMessageSize().getTotalSize());
            }
        });

        if (count > 0) {
            assertTrue(storeStats.getMessageSize().getTotalSize() > minimumSize);
            Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return storeStats.getMessageSize().getTotalSize() > minimumSize;
                }
            });
        } else {
            Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return storeStats.getMessageSize().getTotalSize() == 0;
                }
            });
        }
    }

    /**
     * Generate random 1 megabyte messages
     * @param session
     * @return
     * @throws JMSException
     */
    protected BytesMessage createMessage(Session session) throws JMSException {
        final BytesMessage message = session.createBytesMessage();
        final byte[] data = new byte[messageSize];
        final Random rng = new Random();
        rng.nextBytes(data);
        message.writeBytes(data);
        return message;
    }


    protected Destination publishTestQueueMessages(int count) throws Exception {
        return publishTestQueueMessages(count, defaultQueueName);
    }

    protected Destination publishTestQueueMessages(int count, String queueName) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqQueue = new ActiveMQQueue(
                queueName);

        Destination dest = broker.getDestination(activeMqQueue);

        // Start the connection
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI)
        .createConnection();
        connection.setClientID("clientId" + queueName);
        connection.start();
        Session session = connection.createSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(queueName);

        try {
            // publish a bunch of non-persistent messages to fill up the temp
            // store
            MessageProducer prod = session.createProducer(queue);
            prod.setDeliveryMode(DeliveryMode.PERSISTENT);
            for (int i = 0; i < count; i++) {
                prod.send(createMessage(session));
            }

        } finally {
            connection.close();
        }

        return dest;
    }

    protected Destination consumeTestQueueMessages() throws Exception {
        return consumeTestQueueMessages(defaultQueueName);
    }

    protected Destination consumeDurableTestMessages(Connection connection, String sub, int size) throws Exception {
        return consumeDurableTestMessages(connection, sub, size, defaultTopicName);
    }

    protected Destination consumeTestQueueMessages(String queueName) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqQueue = new ActiveMQQueue(
                queueName);

        Destination dest = broker.getDestination(activeMqQueue);

        // Start the connection
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI)
        .createConnection();
        connection.setClientID("clientId2" + queueName);
        connection.start();
        Session session = connection.createSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(queueName);

        try {
            MessageConsumer consumer = session.createConsumer(queue);
            for (int i = 0; i < 200; i++) {
                consumer.receive();
            }

        } finally {
            connection.stop();
        }

        return dest;
    }

    protected Destination consumeDurableTestMessages(Connection connection, String sub, int size, String topicName) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqTopic = new ActiveMQTopic(
                topicName);

        Destination dest = broker.getDestination(activeMqTopic);

        Session session = connection.createSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(topicName);

        try {
            TopicSubscriber consumer = session.createDurableSubscriber(topic, sub);
            for (int i = 0; i < size; i++) {
                consumer.receive();
            }

        } finally {
            session.close();
        }

        return dest;
    }

    protected Destination publishTestMessagesDurable(Connection connection, String[] subNames, int publishSize, int expectedSize) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqTopic = new ActiveMQTopic(
                defaultTopicName);

        Destination dest = broker.getDestination(activeMqTopic);

        // Start the connection

        Session session = connection.createSession(false,
                TopicSession.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(defaultTopicName);
        for (String subName : subNames) {
            session.createDurableSubscriber(topic, subName);
        }

        // browse the durable sub - this test is to verify that browsing (which calls createTopicMessageStore)
        //in KahaDBStore will not create a brand new store (ie uses the cache) If the cache is not used,
        //then the statistics won't be updated properly because a new store would overwrite the old store
        //which is still in use
        ObjectName[] subs = broker.getAdminView().getDurableTopicSubscribers();

        try {
            // publish a bunch of non-persistent messages to fill up the temp
            // store
            MessageProducer prod = session.createProducer(topic);
            prod.setDeliveryMode(DeliveryMode.PERSISTENT);
            for (int i = 0; i < publishSize; i++) {
                prod.send(createMessage(session));
            }

            //verify the view has expected messages
            assertEquals(subNames.length, subs.length);
            ObjectName subName = subs[0];
            DurableSubscriptionViewMBean sub = (DurableSubscriptionViewMBean)
                    broker.getManagementContext().newProxyInstance(subName, DurableSubscriptionViewMBean.class, true);
            CompositeData[] data  = sub.browse();
            assertNotNull(data);
            assertEquals(expectedSize, data.length);

        } finally {
            session.close();
        }

        return dest;
    }

}
