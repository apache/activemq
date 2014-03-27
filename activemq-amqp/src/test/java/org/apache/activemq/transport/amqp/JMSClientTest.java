/**                           >>>>>> pumping
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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.transport.amqp.joram.ActiveMQAdmin;
import org.apache.activemq.util.Wait;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.objectweb.jtests.jms.framework.TestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSClientTest extends AmqpTestSupport {
    protected static final Logger LOG = LoggerFactory.getLogger(JMSClientTest.class);
    @Rule public TestName name = new TestName();
    java.util.logging.Logger frameLoggger = java.util.logging.Logger.getLogger("FRM");


    @Override
    @Before
    public void setUp() throws Exception {
        LOG.debug("in setUp of {}", name.getMethodName());
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        LOG.debug("in tearDown of {}", name.getMethodName());
        super.tearDown();
        Thread.sleep(500);
    }

    @SuppressWarnings("rawtypes")
    @Test(timeout=30000)
    public void testProducerConsume() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(name.toString());
            MessageProducer p = session.createProducer(queue);

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            p.send(message);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            while (enumeration.hasMoreElements()) {
                Message m = (Message) enumeration.nextElement();
                assertTrue(m instanceof TextMessage);
            }

            MessageConsumer consumer = session.createConsumer(queue);
            Message msg = consumer.receive(TestConfig.TIMEOUT);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
        }
        connection.close();
    }

    @Test(timeout=30000)
    public void testAnonymousProducerConsume() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue1 = session.createQueue(name.toString() + "1");
            Queue queue2 = session.createQueue(name.toString() + "2");
            MessageProducer p = session.createProducer(null);

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            p.send(queue1, message);
            p.send(queue2, message);

            {
                MessageConsumer consumer = session.createConsumer(queue1);
                Message msg = consumer.receive(TestConfig.TIMEOUT);
                assertNotNull(msg);
                assertTrue(msg instanceof TextMessage);
                consumer.close();
            }
            {
                MessageConsumer consumer = session.createConsumer(queue2);
                Message msg = consumer.receive(TestConfig.TIMEOUT);
                assertNotNull(msg);
                assertTrue(msg instanceof TextMessage);
                consumer.close();
            }
        }
        connection.close();
    }

    @Test
    public void testTransactedConsumer() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        final int msgCount = 1;

        Connection connection = createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        sendMessages(connection, queue, msgCount);

        QueueViewMBean queueView = getProxyToQueue(name.toString());
        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);

        Message msg = consumer.receive(TestConfig.TIMEOUT);
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);

        LOG.info("Queue size before session commit is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        session.commit();

        LOG.info("Queue size after session commit is: {}", queueView.getQueueSize());
        assertEquals(0, queueView.getQueueSize());

        connection.close();
    }

    @Test(timeout=30000)
    public void testRollbackRececeivedMessage() throws Exception {

        ActiveMQAdmin.enableJMSFrameTracing();
        final int msgCount = 1;

        Connection connection = createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        sendMessages(connection, queue, msgCount);

        QueueViewMBean queueView = getProxyToQueue(name.toString());
        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);

        // Receive and roll back, first receive should not show redelivered.
        Message msg = consumer.receive(TestConfig.TIMEOUT);
        LOG.info("Test received msg: {}", msg);
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals(false, msg.getJMSRedelivered());

        session.rollback();

        // Receive and roll back, first receive should not show redelivered.
        msg = consumer.receive(TestConfig.TIMEOUT);
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals(true, msg.getJMSRedelivered());

        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        session.commit();

        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(0, queueView.getQueueSize());

        session.close();
        connection.close();
    }

    @Test(timeout=60000)
    public void testTXConsumerAndLargeNumberOfMessages() throws Exception {

        ActiveMQAdmin.enableJMSFrameTracing();
        final int msgCount = 500;

        Connection connection = createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        sendMessages(connection, queue, msgCount);

        QueueViewMBean queueView = getProxyToQueue(name.toString());
        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        // Consumer all in TX and commit.
        {
            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < msgCount; ++i) {
                if ((i % 100) == 0) {
                    LOG.info("Attempting receive of Message #{}", i);
                }
                Message msg = consumer.receive(TestConfig.TIMEOUT);
                assertNotNull("Should receive message: " + i, msg);
                assertTrue(msg instanceof TextMessage);
            }

            session.commit();
            consumer.close();
            session.close();
        }

        connection.close();

        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(0, queueView.getQueueSize());
    }

    @SuppressWarnings("rawtypes")
    @Test(timeout=30000)
    public void testSelectors() throws Exception{
        ActiveMQAdmin.enableJMSFrameTracing();

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(name.toString());
            MessageProducer p = session.createProducer(queue);

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            p.send(message, DeliveryMode.PERSISTENT, 5, 0);

            message = session.createTextMessage();
            message.setText("hello + 9");
            p.send(message, DeliveryMode.PERSISTENT, 9, 0);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            int count = 0;
            while (enumeration.hasMoreElements()) {
                Message m = (Message) enumeration.nextElement();
                assertTrue(m instanceof TextMessage);
                count ++;
            }

            assertEquals(2, count);

            MessageConsumer consumer = session.createConsumer(queue, "JMSPriority > 8");
            Message msg = consumer.receive(TestConfig.TIMEOUT);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("hello + 9", ((TextMessage) msg).getText());
        }
        connection.close();
    }

    @Test(timeout=30000)
    public void testProducerThrowsWhenBrokerStops() throws Exception {

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        connection.start();

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        Message m = session.createTextMessage("Sample text");

        Thread stopper = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    stopBroker();
                } catch (Exception ex) {}
            }
        });
        stopper.start();

        try {
            for (int i = 0; i < 10; ++i) {
                producer.send(m);
                TimeUnit.SECONDS.sleep(1);
            }
            fail("Should have thrown an IllegalStateException");
        } catch (Exception ex) {
            LOG.info("Caught exception on send: {}", ex);
        }
    }

    @Test(timeout=30000)
    public void testProducerCreateThrowsWhenBrokerStops() throws Exception {

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        connection.start();

        Thread stopper = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    stopBroker();
                } catch (Exception ex) {}
            }
        });
        stopper.start();

        try {
            for (int i = 0; i < 10; ++i) {
                MessageProducer producer = session.createProducer(queue);
                assertNotNull(producer);
                TimeUnit.SECONDS.sleep(1);
            }
            fail("Should have thrown an IllegalStateException");
        } catch (Exception ex) {
            LOG.info("Caught exception on create producer: {}", ex);
        }
    }

    @Test(timeout=30000)
    public void testConsumerCreateThrowsWhenBrokerStops() throws Exception {

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        connection.start();

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        Message m = session.createTextMessage("Sample text");
        producer.send(m);

        stopBroker();
        try {
            session.createConsumer(queue);
            fail("Should have thrown an IllegalStateException");
        } catch (Exception ex) {
            LOG.info("Caught exception on receive: {}", ex);
        }
    }

    @Test(timeout=90000)
    public void testConsumerReceiveNoWaitThrowsWhenBrokerStops() throws Exception {

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        connection.start();

        MessageConsumer consumer=session.createConsumer(queue);
        Thread stopper = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    stopBroker();
                } catch (Exception ex) {}
            }
        });
        stopper.start();

        try {
            for (int i = 0; i < 10; ++i) {
                consumer.receiveNoWait();
                TimeUnit.MILLISECONDS.sleep(1000 + (i * 100));
            }
            fail("Should have thrown an IllegalStateException");
        } catch (Exception ex) {
            LOG.info("Caught exception on receiveNoWait: {}", ex);
        }
    }

    @Test(timeout=60000)
    public void testConsumerReceiveTimedThrowsWhenBrokerStops() throws Exception {

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        connection.start();

        MessageConsumer consumer=session.createConsumer(queue);
        Thread stopper = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    stopBroker();
                } catch (Exception ex) {}
            }
        });
        stopper.start();

        try {
            for (int i = 0; i < 10; ++i) {
                consumer.receive(1000 + (i * 100));
            }
            fail("Should have thrown an IllegalStateException");
        } catch (Exception ex) {
            LOG.info("Caught exception on receive(1000): {}", ex);
        }
    }

    @Test(timeout=30000)
    public void testConsumerReceiveReturnsBrokerStops() throws Exception {

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        connection.start();

        MessageConsumer consumer=session.createConsumer(queue);
        Thread stopper = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    stopBroker();
                } catch (Exception ex) {}
            }
        });
        stopper.start();

        try {
            Message m = consumer.receive();
            assertNull(m);
        } catch (Exception ex) {
            LOG.info("Caught exception on receive(1000): {}", ex);
        }
    }

    @Test(timeout=30000)
    public void testBrokerRestartWontHangConnectionClose() throws Exception {

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        connection.start();

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        Message m = session.createTextMessage("Sample text");
        producer.send(m);

        restartBroker();

        try {
            connection.close();
        } catch (Exception ex) {
            LOG.error("Should not thrown on disconnected connection close(): {}", ex);
            fail("Should not have thrown an exception.");
        }
    }

    @Test(timeout=120000)
    public void testProduceAndConsumeLargeNumbersOfMessages() throws JMSException {

        int count = 2000;

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());
        connection.start();

        MessageProducer producer= session.createProducer(queue);
        for (int i = 0; i < count; i++) {
            Message m=session.createTextMessage("Test-Message:"+i);
            producer.send(m);
        }

        MessageConsumer  consumer=session.createConsumer(queue);
        for(int i = 0; i < count; i++) {
            Message message = consumer.receive(5000);
            assertNotNull(message);
            System.out.println(((TextMessage) message).getText());
            assertEquals("Test-Message:" + i,((TextMessage) message).getText());
        }

        Message message = consumer.receive(5000);
        assertNull(message);
    }

    @Test(timeout=30000)
    public void testSyncSends() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        Connection connection = null;
        try {
            connection = createConnection(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(name.toString());
            connection.start();
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            Message toSend = session.createTextMessage("Sample text");
            producer.send(toSend);
            MessageConsumer consumer = session.createConsumer(queue);
            Message received = consumer.receive(5000);
            assertNotNull(received);
        } finally {
            connection.close();
        }
    }

    @Test(timeout=30000)
    public void testDurableConsumerAsync() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> received = new AtomicReference<Message>();

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(name.toString());
            MessageConsumer consumer = session.createDurableSubscriber(topic, "DurbaleTopic");
            consumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    received.set(message);
                    latch.countDown();
                }
            });

            MessageProducer producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            producer.send(message);

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertNotNull("Should have received a message by now.", received.get());
            assertTrue("Should be an instance of TextMessage", received.get() instanceof TextMessage);
        }
        connection.close();
    }

    @Test(timeout=30000)
    public void testDurableConsumerSync() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(name.toString());
            final MessageConsumer consumer = session.createDurableSubscriber(topic, "DurbaleTopic");
            MessageProducer producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            producer.send(message);

            final AtomicReference<Message> msg = new AtomicReference<Message>();
            assertTrue(Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    msg.set(consumer.receiveNoWait());
                    return msg.get() != null;
                }
            }));

            assertNotNull("Should have received a message by now.", msg.get());
            assertTrue("Should be an instance of TextMessage", msg.get() instanceof TextMessage);
        }
        connection.close();
    }

    @Test(timeout=30000)
    public void testTopicConsumerAsync() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> received = new AtomicReference<Message>();

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(name.toString());
            MessageConsumer consumer = session.createConsumer(topic);
            consumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    received.set(message);
                    latch.countDown();
                }
            });

            MessageProducer producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            producer.send(message);

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertNotNull("Should have received a message by now.", received.get());
            assertTrue("Should be an instance of TextMessage", received.get() instanceof TextMessage);
        }
        connection.close();
    }

    @Test(timeout=45000)
    public void testTopicConsumerSync() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(name.toString());
            final MessageConsumer consumer = session.createConsumer(topic);
            MessageProducer producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            producer.send(message);

            final AtomicReference<Message> msg = new AtomicReference<Message>();
            assertTrue(Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    msg.set(consumer.receiveNoWait());
                    return msg.get() != null;
                }
            }));

            assertNotNull("Should have received a message by now.", msg.get());
            assertTrue("Should be an instance of TextMessage", msg.get() instanceof TextMessage);
        }
        connection.close();
    }

    @Test(timeout=60000)
    public void testConnectionsAreClosed() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        final ConnectorViewMBean connector = getProxyToConnectionView(getTargetConnectorName());
        LOG.info("Current number of Connections is: {}", connector.connectionCount());

        ArrayList<Connection> connections = new ArrayList<Connection>();

        for (int i = 0; i < 10; i++) {
            connections.add(createConnection(null));
        }

        LOG.info("Current number of Connections is: {}", connector.connectionCount());

        for (Connection connection : connections) {
            connection.close();
        }

        assertTrue("Should have no connections left.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Current number of Connections is: {}", connector.connectionCount());
                return connector.connectionCount() == 0;
            }
        }));
    }

    protected String getTargetConnectorName() {
        return "amqp";
    }

    @Test(timeout=30000)
    public void testExecptionListenerCalledOnBrokerStop() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        Connection connection = createConnection();
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        final CountDownLatch called = new CountDownLatch(1);

        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("Exception listener called: ", exception);
                called.countDown();
            }
        });

        Thread stopper = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    stopBroker();
                } catch (Exception ex) {}
            }
        });
        stopper.start();

        assertTrue("No exception listener event fired.", called.await(15, TimeUnit.SECONDS));
    }

    @Test(timeout=30000)
    public void testSessionTransactedCommit() throws JMSException, InterruptedException {
        ActiveMQAdmin.enableJMSFrameTracing();

        Connection connection = createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());

        connection.start();

        // transacted producer
        MessageProducer pr = session.createProducer(queue);
        for (int i = 0; i < 10; i++) {
            Message m = session.createTextMessage("TestMessage" + i);
            pr.send(m);
        }

        // No commit in place, so no message should be dispatched.
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage m = (TextMessage) consumer.receive(5000);

        assertNull(m);

        session.commit();

        // Messages should be available now.
        for (int i = 0; i < 10; i++) {
            Message msg = consumer.receive(5000);
            assertNotNull(msg);
        }

        session.close();
        connection.close();
    }

    @Test(timeout=30000)
    public void testSessionTransactedRollback() throws JMSException, InterruptedException {
        ActiveMQAdmin.enableJMSFrameTracing();

        Connection connection = createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.toString());

        connection.start();

        // transacted producer
        MessageProducer pr = session.createProducer(queue);
        for (int i = 0; i < 10; i++) {
            Message m = session.createTextMessage("TestMessage" + i);
            pr.send(m);
        }

        session.rollback();

        // No commit in place, so no message should be dispatched.
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage m = (TextMessage) consumer.receive(5000);
        assertNull(m);

        session.close();
        connection.close();
    }

    private String createLargeString(int sizeInBytes) {
        byte[] base = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < sizeInBytes; i++) {
            builder.append(base[i % base.length]);
        }

        LOG.debug("Created string with size : " + builder.toString().getBytes().length + " bytes");
        return builder.toString();
    }

    @Test(timeout = 60 * 1000)
    public void testSendLargeMessage() throws JMSException, InterruptedException {
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String queueName = name.toString();
        Queue queue = session.createQueue(queueName);

        MessageProducer producer=session.createProducer(queue);
        int messageSize = 1024 * 1024;
        String messageText = createLargeString(messageSize);
        Message m=session.createTextMessage(messageText);
        LOG.debug("Sending message of {} bytes on queue {}", messageSize, queueName);
        producer.send(m);

        MessageConsumer  consumer=session.createConsumer(queue);

        Message message = consumer.receive();
        assertNotNull(message);
        assertTrue(message instanceof TextMessage);
        TextMessage textMessage = (TextMessage) message;
        LOG.debug(">>>> Received message of length {}", textMessage.getText().length());
        assertEquals(messageSize, textMessage.getText().length());
        assertEquals(messageText, textMessage.getText());
    }

    private Connection createConnection() throws JMSException {
        return createConnection(name.toString(), false, false);
    }

    private Connection createConnection(boolean syncPublish) throws JMSException {
        return createConnection(name.toString(), syncPublish, false);
    }

    private Connection createConnection(String clientId) throws JMSException {
        return createConnection(clientId, false, false);
    }

    /**
     * Can be overridden in subclasses to test against a different transport suchs as NIO.
     *
     * @return the port to connect to on the Broker.
     */
    protected int getBrokerPort() {
        return port;
    }

    protected Connection createConnection(String clientId, boolean syncPublish, boolean useSsl) throws JMSException {

        int brokerPort = getBrokerPort();
        LOG.debug("Creating connection on port {}", brokerPort);
        final ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", brokerPort, "admin", "password", null, useSsl);

        factory.setSyncPublish(syncPublish);
        factory.setTopicPrefix("topic://");
        factory.setQueuePrefix("queue://");

        final Connection connection = factory.createConnection();
        if (clientId != null && !clientId.isEmpty()) {
            connection.setClientID(clientId);
        }
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                exception.printStackTrace();
            }
        });
        connection.start();
        return connection;
    }
}
