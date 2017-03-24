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

import java.net.URI;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 */
public abstract class AbstractStoreStatTestSupport {

    protected static final Logger LOG = LoggerFactory
            .getLogger(AbstractStoreStatTestSupport.class);

    protected static int defaultMessageSize = 1000;

    protected abstract BrokerService getBroker();

    protected abstract URI getBrokerConnectURI();

    protected Destination consumeTestQueueMessages(String queueName) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqQueue = new ActiveMQQueue(
                queueName);

        Destination dest = getBroker().getDestination(activeMqQueue);

        // Start the connection
        Connection connection = new ActiveMQConnectionFactory(getBrokerConnectURI())
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

    protected Destination browseTestQueueMessages(String queueName) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqQueue = new ActiveMQQueue(
                queueName);

        Destination dest = getBroker().getDestination(activeMqQueue);

        // Start the connection
        Connection connection = new ActiveMQConnectionFactory(getBrokerConnectURI())
        .createConnection();
        connection.setClientID("clientId2" + queueName);
        connection.start();
        Session session = connection.createSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(queueName);

        try {
            QueueBrowser queueBrowser = session.createBrowser(queue);
            @SuppressWarnings("unchecked")
            Enumeration<Message> messages = queueBrowser.getEnumeration();
            while (messages.hasMoreElements()) {
                messages.nextElement();
            }

        } finally {
            connection.stop();
        }

        return dest;
    }

    protected Destination consumeDurableTestMessages(Connection connection, String sub,
            int size, String topicName, AtomicLong publishedMessageSize) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqTopic = new ActiveMQTopic(
                topicName);

        Destination dest = getBroker().getDestination(activeMqTopic);

        Session session = connection.createSession(false,
                QueueSession.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(topicName);

        try {
            TopicSubscriber consumer = session.createDurableSubscriber(topic, sub);
            for (int i = 0; i < size; i++) {
                ActiveMQMessage message = (ActiveMQMessage) consumer.receive();
                if (publishedMessageSize != null) {
                    publishedMessageSize.addAndGet(-message.getSize());
                }
            }

        } finally {
            session.close();
        }

        return dest;
    }

    protected org.apache.activemq.broker.region.Queue publishTestQueueMessages(int count, String queueName,
            int deliveryMode, int messageSize, AtomicLong publishedMessageSize) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqQueue = new ActiveMQQueue(
                queueName);

        Destination dest = getBroker().getDestination(activeMqQueue);

        // Start the connection
        Connection connection = new ActiveMQConnectionFactory(getBrokerConnectURI())
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
            prod.setDeliveryMode(deliveryMode);
            for (int i = 0; i < count; i++) {
                prod.send(createMessage(i, session, messageSize, publishedMessageSize));
            }

        } finally {
            connection.close();
        }

        return (org.apache.activemq.broker.region.Queue) dest;
    }

    protected org.apache.activemq.broker.region.Topic publishTestMessagesDurable(Connection connection, String[] subNames, String topicName,
            int publishSize, int expectedSize, int messageSize, AtomicLong publishedMessageSize,
            boolean verifyBrowsing) throws Exception {
        return this.publishTestMessagesDurable(connection, subNames, topicName, publishSize, expectedSize, messageSize,
                publishedMessageSize, verifyBrowsing, DeliveryMode.PERSISTENT);
    }

    protected org.apache.activemq.broker.region.Topic publishTestMessagesDurable(Connection connection, String[] subNames, String topicName,
            int publishSize, int expectedSize, int messageSize, AtomicLong publishedMessageSize,
            boolean verifyBrowsing, int deliveryMode) throws Exception {
        // create a new queue
        final ActiveMQDestination activeMqTopic = new ActiveMQTopic(
                topicName);

        Destination dest = getBroker().getDestination(activeMqTopic);

        // Start the connection

        Session session = connection.createSession(false,
                TopicSession.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(topicName);
        for (String subName : subNames) {
            session.createDurableSubscriber(topic, subName);
        }

        ObjectName[] subs = null;
        if (verifyBrowsing) {
            // browse the durable sub - this test is to verify that browsing (which calls createTopicMessageStore)
            //in KahaDBStore will not create a brand new store (ie uses the cache) If the cache is not used,
            //then the statistics won't be updated properly because a new store would overwrite the old store
            //which is still in use
            subs = getBroker().getAdminView().getDurableTopicSubscribers();
        }

        try {
            // publish a bunch of non-persistent messages to fill up the temp
            // store
            MessageProducer prod = session.createProducer(topic);
            prod.setDeliveryMode(deliveryMode);
            for (int i = 0; i < publishSize; i++) {
                prod.send(createMessage(i, session, messageSize, publishedMessageSize));
            }

            //verify the view has expected messages
            if (verifyBrowsing) {
                assertNotNull(subs);
                assertEquals(subNames.length, subs.length);
                ObjectName subName = subs[0];
                DurableSubscriptionViewMBean sub = (DurableSubscriptionViewMBean)
                        getBroker().getManagementContext().newProxyInstance(subName, DurableSubscriptionViewMBean.class, true);
                CompositeData[] data  = sub.browse();
                assertNotNull(data);
                assertEquals(expectedSize, data.length);
            }

        } finally {
            session.close();
        }

        return (org.apache.activemq.broker.region.Topic) dest;
    }

    /**
     * Generate random messages between 100 bytes and maxMessageSize
     * @param session
     * @return
     * @throws JMSException
     */
    protected BytesMessage createMessage(int count, Session session, int maxMessageSize, AtomicLong publishedMessageSize) throws JMSException {
        final BytesMessage message = session.createBytesMessage();

        final Random randomSize = new Random();
        int size = randomSize.nextInt((maxMessageSize - 100) + 1) + 100;
        LOG.info("Creating message to publish: " + count + ", size: " + size);
        if (publishedMessageSize != null) {
            publishedMessageSize.addAndGet(size);
        }

        final byte[] data = new byte[size];
        final Random rng = new Random();
        rng.nextBytes(data);
        message.writeBytes(data);
        return message;
    }
}
