/*
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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.usage.MemoryUsage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ6387Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ6387Test.class);

    private final String QUEUE_NAME = "testQueue";
    private final String TOPIC_NAME = "testTopic";
    private final String SUBSCRIPTION_NAME = "subscriberId";
    private final String CLIENT_ID = "client1";
    private final int MSG_COUNT = 150;

    private ActiveMQConnectionFactory connectionFactory;
    private BrokerService brokerService;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() throws Exception {

        LOG.info("=============== Starting test: {} ====================", testName.getMethodName());

        brokerService = new BrokerService();
        brokerService.setAdvisorySupport(false);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.setKeepDurableSubsActive(false);
        brokerService.start();
        connectionFactory = new ActiveMQConnectionFactory(brokerService.getVmConnectorURI());
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();

        LOG.info("=============== Finished test: {} ====================", testName.getMethodName());
    }

    @Test
    public void testQueueMessagesKeptAfterDelivery() throws Exception {
        createDurableSubscription();
        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        sendBytesMessage(Queue.class);

        logBrokerMemoryUsage(Queue.class);

        assertEquals(0, brokerService.getAdminView().getQueueSubscribers().length);

        receiveMessages(Queue.class);

        assertEquals(0, brokerService.getAdminView().getQueueSubscribers().length);

        logBrokerMemoryUsage(Queue.class);

        assertEquals(0, getCurrentMemoryUsage(Queue.class));
    }

    @Test
    public void testQueueMessagesKeptAfterPurge() throws Exception {
        createDurableSubscription();
        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        sendBytesMessage(Queue.class);

        logBrokerMemoryUsage(Queue.class);

        assertEquals(0, brokerService.getAdminView().getQueueSubscribers().length);

        getProxyToQueue(QUEUE_NAME).purge();

        assertEquals(0, brokerService.getAdminView().getQueueSubscribers().length);

        logBrokerMemoryUsage(Queue.class);

        assertEquals(0, getCurrentMemoryUsage(Queue.class));
    }

    @Test
    public void testDurableTopicSubscriptionMessagesKeptAfterDelivery() throws Exception {
        createDurableSubscription();
        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        sendBytesMessage(Topic.class);

        logBrokerMemoryUsage(Topic.class);

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        receiveMessages(Topic.class);

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        logBrokerMemoryUsage(Topic.class);

        assertEquals(0, getCurrentMemoryUsage(Topic.class));
    }

    @Test
    public void testDurableTopicSubscriptionMessagesKeptAfterUnsubscribe() throws Exception {
        createDurableSubscription();
        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        sendBytesMessage(Topic.class);

        logBrokerMemoryUsage(Topic.class);

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        unsubscribeDurableSubscription();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        logBrokerMemoryUsage(Topic.class);

        assertEquals(0, getCurrentMemoryUsage(Topic.class));
    }

    private void createDurableSubscription() throws JMSException {
        final Connection connection = connectionFactory.createConnection();
        connection.setClientID(CLIENT_ID);
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Topic topic = session.createTopic(TOPIC_NAME);
        connection.start();

        session.createDurableSubscriber(topic, SUBSCRIPTION_NAME, null, false);
        LOG.info("Created durable subscription.");

        connection.stop();
        connection.close();
    }

    private void receiveMessages(Class<? extends Destination> destType) throws JMSException {
        final Connection connection = connectionFactory.createConnection();
        connection.setClientID(CLIENT_ID);
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Destination destination;
        if (destType.equals(Queue.class)) {
            destination = session.createQueue(QUEUE_NAME);
        } else {
            destination = session.createTopic(TOPIC_NAME);
        }

        final MessageConsumer consumer;
        if (destType.equals(Queue.class)) {
            consumer = session.createConsumer(destination);
        } else {
            consumer = session.createDurableSubscriber((Topic) destination, SUBSCRIPTION_NAME, null, false);
        }

        connection.start();

        for (int i = 0; i < MSG_COUNT; ++i) {
            assertNotNull(consumer.receive(5000));
        }

        connection.close();
    }

    private void sendBytesMessage(Class<? extends Destination> destType) throws JMSException {
        final Connection connection = connectionFactory.createConnection();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Destination destination;
        if (destType.equals(Queue.class)) {
            destination = session.createQueue(QUEUE_NAME);
        } else {
            destination = session.createTopic(TOPIC_NAME);
        }
        final MessageProducer producer = session.createProducer(destination);
        final BytesMessage bytesMessage = session.createBytesMessage();

        bytesMessage.writeBytes(new byte[1024 * 1024]);

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(bytesMessage);
        }

        connection.close();
    }

    private void unsubscribeDurableSubscription() throws JMSException {
        final Connection connection = connectionFactory.createConnection();
        connection.setClientID(CLIENT_ID);
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.unsubscribe(SUBSCRIPTION_NAME);
        LOG.info("Unsubscribed durable subscription.");

        connection.stop();
        connection.close();
    }

    private long getCurrentMemoryUsage(Class<? extends Destination> destType) throws Exception {
        final MemoryUsage usage;
        if (destType.equals(Queue.class)) {
            usage = brokerService.getDestination(ActiveMQDestination.createDestination(QUEUE_NAME, ActiveMQDestination.QUEUE_TYPE)).getMemoryUsage();
        } else {
            usage = brokerService.getDestination(ActiveMQDestination.createDestination(TOPIC_NAME, ActiveMQDestination.TOPIC_TYPE)).getMemoryUsage();
        }

        return usage.getUsage();
    }

    private void logBrokerMemoryUsage(Class<? extends Destination> destType) throws Exception {
        LOG.info("Memory usage: broker={}% destination={}", brokerService.getAdminView().getMemoryPercentUsage(), getCurrentMemoryUsage(destType));
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}
