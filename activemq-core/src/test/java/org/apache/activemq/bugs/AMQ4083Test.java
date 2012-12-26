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
package org.apache.activemq.bugs;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AMQ4083Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ3992Test.class);
    private static BrokerService brokerService;
    private static String BROKER_ADDRESS = "tcp://localhost:0";
    private static String TEST_QUEUE = "testQueue";
    private static ActiveMQQueue queue = new ActiveMQQueue(TEST_QUEUE);

    private final int messageCount = 100;

    private String connectionUri;
    private String[] data;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.setDeleteAllMessagesOnStartup(true);
        connectionUri = brokerService.addConnector(BROKER_ADDRESS).getPublishableConnectString();
        brokerService.start();
        brokerService.waitUntilStarted();

        data = new String[messageCount];

        for (int i = 0; i < messageCount; i++) {
            data[i] = "Text for message: " + i + " at " + new Date();
        }
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test
    public void testExpiredMsgsBeforeNonExpired() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.getPrefetchPolicy().setQueuePrefetch(400);

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        connection.start();

        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);

        // send a batch that expires in a short time.
        for (int i = 0; i < 100; i++) {
            producer.send(session.createTextMessage(), DeliveryMode.PERSISTENT, 4, 4000);
        }

        // and send one that doesn't expire to we can ack it.
        producer.send(session.createTextMessage());

        // wait long enough so the first batch times out.
        TimeUnit.SECONDS.sleep(5);

        final QueueViewMBean queueView = getProxyToQueueViewMBean();

        assertEquals(101, queueView.getInFlightCount());

        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                try {
                    message.acknowledge();
                } catch (JMSException e) {
                }
            }
        });

        TimeUnit.SECONDS.sleep(5);

        assertEquals(0, queueView.getInFlightCount());

        for (int i = 0; i < 200; i++) {
            producer.send(session.createTextMessage());
        }

        assertTrue("Inflight count should reach zero, currently: " + queueView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            public boolean isSatisified() throws Exception {
                return queueView.getInFlightCount() == 0;
            }
        }));

        LOG.info("Dequeued Count: {}", queueView.getDequeueCount());
        LOG.info("Dispatch Count: {}", queueView.getDispatchCount());
        LOG.info("Enqueue Count: {}", queueView.getEnqueueCount());
        LOG.info("Expired Count: {}", queueView.getExpiredCount());
        LOG.info("InFlight Count: {}", queueView.getInFlightCount());
    }

    @Test
    public void testExpiredMsgsBeforeNonExpiredWithTX() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.getPrefetchPolicy().setQueuePrefetch(400);

        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        connection.start();

        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);

        // send a batch that expires in a short time.
        for (int i = 0; i < 100; i++) {
            producer.send(session.createTextMessage(), DeliveryMode.PERSISTENT, 4, 4000);
        }

        // and send one that doesn't expire to we can ack it.
        producer.send(session.createTextMessage());
        session.commit();

        // wait long enough so the first batch times out.
        TimeUnit.SECONDS.sleep(5);

        final QueueViewMBean queueView = getProxyToQueueViewMBean();

        assertEquals(101, queueView.getInFlightCount());

        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                try {
                    session.commit();
                } catch (JMSException e) {
                }
            }
        });

        TimeUnit.SECONDS.sleep(5);

        assertEquals(0, queueView.getInFlightCount());

        for (int i = 0; i < 200; i++) {
            producer.send(session.createTextMessage());
        }
        session.commit();

        assertTrue("Inflight count should reach zero, currently: " + queueView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getInFlightCount() == 0;
            }
        }));

        LOG.info("Dequeued Count: {}", queueView.getDequeueCount());
        LOG.info("Dispatch Count: {}", queueView.getDispatchCount());
        LOG.info("Enqueue Count: {}", queueView.getEnqueueCount());
        LOG.info("Expired Count: {}", queueView.getExpiredCount());
        LOG.info("InFlight Count: {}", queueView.getInFlightCount());
    }

    @Test
    public void testExpiredMsgsInterleavedWithNonExpired() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.getPrefetchPolicy().setQueuePrefetch(400);

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        connection.start();

        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);

        // send a batch that expires in a short time.
        for (int i = 0; i < 200; i++) {

            if ((i % 2) == 0) {
                producer.send(session.createTextMessage(), DeliveryMode.PERSISTENT, 4, 4000);
            } else {
                producer.send(session.createTextMessage());
            }
        }

        // wait long enough so the first batch times out.
        TimeUnit.SECONDS.sleep(5);

        final QueueViewMBean queueView = getProxyToQueueViewMBean();

        assertEquals(200, queueView.getInFlightCount());

        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                try {
                    LOG.debug("Acking message: {}", message);
                    message.acknowledge();
                } catch (JMSException e) {
                }
            }
        });

        TimeUnit.SECONDS.sleep(5);

        assertTrue("Inflight count should reach zero, currently: " + queueView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getInFlightCount() == 0;
            }
        }));

        for (int i = 0; i < 200; i++) {
            producer.send(session.createTextMessage());
        }

        assertTrue("Inflight count should reach zero, currently: " + queueView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getInFlightCount() == 0;
            }
        }));

        LOG.info("Dequeued Count: {}", queueView.getDequeueCount());
        LOG.info("Dispatch Count: {}", queueView.getDispatchCount());
        LOG.info("Enqueue Count: {}", queueView.getEnqueueCount());
        LOG.info("Expired Count: {}", queueView.getExpiredCount());
        LOG.info("InFlight Count: {}", queueView.getInFlightCount());
    }

    @Test
    public void testExpiredMsgsInterleavedWithNonExpiredCumulativeAck() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.getPrefetchPolicy().setQueuePrefetch(400);

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        connection.start();

        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);

        // send a batch that expires in a short time.
        for (int i = 0; i < 200; i++) {

            if ((i % 2) == 0) {
                producer.send(session.createTextMessage(), DeliveryMode.PERSISTENT, 4, 4000);
            } else {
                producer.send(session.createTextMessage());
            }
        }

        // wait long enough so the first batch times out.
        TimeUnit.SECONDS.sleep(5);

        final QueueViewMBean queueView = getProxyToQueueViewMBean();

        assertEquals(200, queueView.getInFlightCount());

        final AtomicInteger msgCount = new AtomicInteger();

        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                try {
                    if (msgCount.incrementAndGet() == 100) {
                        LOG.debug("Acking message: {}", message);
                        message.acknowledge();
                    }
                } catch (JMSException e) {
                }
            }
        });

        TimeUnit.SECONDS.sleep(5);

        assertTrue("Inflight count should reach zero, currently: " + queueView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getInFlightCount() == 0;
            }
        }));

        // Now we just ack each and see if our counters come out right in the end.
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                try {
                    LOG.debug("Acking message: {}", message);
                    message.acknowledge();
                } catch (JMSException e) {
                }
            }
        });

        for (int i = 0; i < 200; i++) {
            producer.send(session.createTextMessage());
        }

        assertTrue("Inflight count should reach zero, currently: " + queueView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getInFlightCount() == 0;
            }
        }));

        LOG.info("Dequeued Count: {}", queueView.getDequeueCount());
        LOG.info("Dispatch Count: {}", queueView.getDispatchCount());
        LOG.info("Enqueue Count: {}", queueView.getEnqueueCount());
        LOG.info("Expired Count: {}", queueView.getExpiredCount());
        LOG.info("InFlight Count: {}", queueView.getInFlightCount());
    }

    @Test
    public void testExpiredBatchBetweenNonExpiredMessages() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.getPrefetchPolicy().setQueuePrefetch(400);

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        connection.start();

        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);

        // Send one that doesn't expire so we can ack it.
        producer.send(session.createTextMessage());

        // send a batch that expires in a short time.
        for (int i = 0; i < 100; i++) {
            producer.send(session.createTextMessage(), DeliveryMode.PERSISTENT, 4, 4000);
        }

        // and send one that doesn't expire so we can ack it.
        producer.send(session.createTextMessage());

        // wait long enough so the first batch times out.
        TimeUnit.SECONDS.sleep(5);

        final QueueViewMBean queueView = getProxyToQueueViewMBean();

        assertEquals(102, queueView.getInFlightCount());

        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                try {
                    message.acknowledge();
                } catch (JMSException e) {
                }
            }
        });

        TimeUnit.SECONDS.sleep(5);

        assertTrue("Inflight count should reach zero, currently: " + queueView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getInFlightCount() == 0;
            }
        }));

        for (int i = 0; i < 200; i++) {
            producer.send(session.createTextMessage());
        }

        assertTrue("Inflight count should reach zero, currently: " + queueView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getInFlightCount() == 0;
            }
        }));

        LOG.info("Dequeued Count: {}", queueView.getDequeueCount());
        LOG.info("Dispatch Count: {}", queueView.getDispatchCount());
        LOG.info("Enqueue Count: {}", queueView.getEnqueueCount());
        LOG.info("Expired Count: {}", queueView.getExpiredCount());
        LOG.info("InFlight Count: {}", queueView.getInFlightCount());
    }

    @Test
    public void testConsumeExpiredQueueAndDlq() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        Connection connection = factory.createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producerNormal = session.createProducer(queue);
        MessageProducer producerExpire = session.createProducer(queue);
        producerExpire.setTimeToLive(500);

        MessageConsumer dlqConsumer = session.createConsumer(session.createQueue("ActiveMQ.DLQ"));
        connection.start();

        Connection consumerConnection = factory.createConnection();
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setAll(10);
        ((ActiveMQConnection)consumerConnection).setPrefetchPolicy(prefetchPolicy);
        Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(queue);
        consumerConnection.start();

        String msgBody = new String(new byte[20*1024]);
        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(msgBody);
            producerExpire.send(queue, message);
        }

        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(msgBody);
            producerNormal.send(queue, message);
        }

        ArrayList<Message> messages = new ArrayList<Message>();
        Message received;
        while ((received = consumer.receive(1000)) != null) {
            messages.add(received);
            if (messages.size() == 1) {
               TimeUnit.SECONDS.sleep(1);
            }
            received.acknowledge();
        };

        assertEquals("got messages", messageCount + 1, messages.size());

        ArrayList<Message> dlqMessages = new ArrayList<Message>();
        while ((received = dlqConsumer.receive(1000)) != null) {
            dlqMessages.add(received);
        };

        assertEquals("got dlq messages", data.length - 1, dlqMessages.size());

        final QueueViewMBean queueView = getProxyToQueueViewMBean();

        LOG.info("Dequeued Count: {}", queueView.getDequeueCount());
        LOG.info("Dispatch Count: {}", queueView.getDispatchCount());
        LOG.info("Enqueue Count: {}", queueView.getEnqueueCount());
        LOG.info("Expired Count: {}", queueView.getExpiredCount());
        LOG.info("InFlight Count: {}", queueView.getInFlightCount());
    }

    private QueueViewMBean getProxyToQueueViewMBean() throws Exception {
        final ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + queue.getQueueName());
        final QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext().newProxyInstance(
                queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}
