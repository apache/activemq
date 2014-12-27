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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
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

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.transport.amqp.joram.ActiveMQAdmin;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.objectweb.jtests.jms.framework.TestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSClientTest extends JMSClientTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JMSClientTest.class);

    protected java.util.logging.Logger frameLoggger = java.util.logging.Logger.getLogger("FRM");

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

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getDestinationName());
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
    }

    @Test(timeout=30000)
    public void testAnonymousProducerConsume() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue1 = session.createQueue(getDestinationName() + "1");
            Queue queue2 = session.createQueue(getDestinationName() + "2");
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
    }

    @Test(timeout=30*1000)
    public void testTransactedConsumer() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        final int msgCount = 1;

        connection = createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(getDestinationName());
        sendMessages(connection, queue, msgCount);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
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
    }

    @Test(timeout=30000)
    public void testRollbackRececeivedMessage() throws Exception {

        ActiveMQAdmin.enableJMSFrameTracing();
        final int msgCount = 1;

        connection = createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(getDestinationName());
        sendMessages(connection, queue, msgCount);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
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
    }

    @Test(timeout = 60000)
    public void testRollbackSomeThenReceiveAndCommit() throws Exception {
        int totalCount = 5;
        int consumeBeforeRollback = 2;

        connection = createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(getDestinationName());
        sendMessages(connection, queue, totalCount);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(totalCount, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);

        for(int i = 1; i <= consumeBeforeRollback; i++) {
            Message message = consumer.receive(1000);
            assertNotNull(message);
            assertEquals("Unexpected message number", i, message.getIntProperty(AmqpTestSupport.MESSAGE_NUMBER));
        }

        session.rollback();

        assertEquals(totalCount, proxy.getQueueSize());

        // Consume again..check we receive all the messages.
        Set<Integer> messageNumbers = new HashSet<Integer>();
        for(int i = 1; i <= totalCount; i++) {
            messageNumbers.add(i);
        }

        for(int i = 1; i <= totalCount; i++) {
            Message message = consumer.receive(1000);
            assertNotNull(message);
            int msgNum = message.getIntProperty(AmqpTestSupport.MESSAGE_NUMBER);
            messageNumbers.remove(msgNum);
        }

        session.commit();

        assertTrue("Did not consume all expected messages, missing messages: " + messageNumbers, messageNumbers.isEmpty());
        assertEquals("Queue should have no messages left after commit", 0, proxy.getQueueSize());
    }

    @Test(timeout=60000)
    public void testTXConsumerAndLargeNumberOfMessages() throws Exception {

        ActiveMQAdmin.enableJMSFrameTracing();
        final int msgCount = 500;

        connection = createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(getDestinationName());
        sendMessages(connection, queue, msgCount);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
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

        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(0, queueView.getQueueSize());
    }

    @SuppressWarnings("rawtypes")
    @Test(timeout=30000)
    public void testSelectors() throws Exception{
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getDestinationName());
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
    }

    abstract class Testable implements Runnable {
        protected String msg;
        synchronized boolean passed() {
            if (msg != null) {
                fail(msg);
            }
            return true;
        }
    }

    @Test(timeout=30000)
    public void testProducerThrowsWhenBrokerStops() throws Exception {

        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        connection.start();

        final MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        final Message m = session.createTextMessage("Sample text");

        Testable t = new Testable() {
            @Override
            public synchronized void run() {
                try {
                    for (int i = 0; i < 30; ++i) {
                        producer.send(m);
                        synchronized (producer) {
                            producer.notifyAll();
                        }
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                    msg = "Should have thrown an IllegalStateException";
                } catch (Exception ex) {
                    LOG.info("Caught exception on send: {}", ex);
                }
            }
        };
        synchronized(producer) {
            new Thread(t).start();
            //wait until we know that the producer was able to send a message
            producer.wait(10000);
        }

        stopBroker();
        assertTrue(t.passed());
    }

    @Test(timeout=30000)
    public void testProducerCreateThrowsWhenBrokerStops() throws Exception {
        connection = createConnection();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue queue = session.createQueue(getDestinationName());
        connection.start();


        Testable t = new Testable() {
            @Override
            public synchronized void run() {
                try {
                    for (int i = 0; i < 10; ++i) {
                        MessageProducer producer = session.createProducer(queue);
                        synchronized (session) {
                            session.notifyAll();
                        }
                        if (producer == null) {
                            msg = "Producer should not be null";
                        }
                        TimeUnit.SECONDS.sleep(1);
                    }
                    msg = "Should have thrown an IllegalStateException";
                } catch (Exception ex) {
                    LOG.info("Caught exception on create producer: {}", ex);
                }
            }
        };
        synchronized (session) {
            new Thread(t).start();
            session.wait(10000);
        }
        stopBroker();
        assertTrue(t.passed());
    }

    @Test(timeout=30000)
    public void testConsumerCreateThrowsWhenBrokerStops() throws Exception {
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
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

    @Test(timeout=30000)
    public void testConsumerReceiveNoWaitThrowsWhenBrokerStops() throws Exception {
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        connection.start();

        final MessageConsumer consumer=session.createConsumer(queue);
        Testable t = new Testable() {
            @Override
            public synchronized void run() {
                try {
                    for (int i = 0; i < 10; ++i) {
                        consumer.receiveNoWait();
                        synchronized (consumer) {
                            consumer.notifyAll();
                        }
                        TimeUnit.MILLISECONDS.sleep(1000 + (i * 100));
                    }
                    msg = "Should have thrown an IllegalStateException";
                } catch (Exception ex) {
                    LOG.info("Caught exception on receiveNoWait: {}", ex);
                }
            }

        };
        synchronized (consumer) {
            new Thread(t).start();
            consumer.wait(10000);
        }
        stopBroker();
        assertTrue(t.passed());
    }

    @Test(timeout=30000)
    public void testConsumerReceiveTimedThrowsWhenBrokerStops() throws Exception {
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        connection.start();

        final MessageConsumer consumer=session.createConsumer(queue);
        Testable t = new Testable() {
            @Override
            public synchronized void run() {
                try {
                    for (int i = 0; i < 10; ++i) {
                        consumer.receive(100 + (i * 1000));
                        synchronized (consumer) {
                            consumer.notifyAll();
                        }
                    }
                    msg = "Should have thrown an IllegalStateException";
                } catch (Exception ex) {
                    LOG.info("Caught exception on receive(1000): {}", ex);
                }
            }
        };
        synchronized (consumer) {
            new Thread(t).start();
            consumer.wait(10000);
            consumer.notifyAll();
        }
        stopBroker();
        assertTrue(t.passed());
    }

    @Test(timeout=30000)
    public void testConsumerReceiveReturnsBrokerStops() throws Exception {
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        connection.start();

        final MessageConsumer consumer=session.createConsumer(queue);

        Testable t = new Testable() {
            @Override
            public synchronized void run() {
                try {
                    Message m = consumer.receive(1);
                    synchronized (consumer) {
                        consumer.notifyAll();
                        if (m != null) {
                            msg = "Should have returned null";
                            return;
                        }
                    }
                    m = consumer.receive();
                    if (m != null) {
                        msg = "Should have returned null";
                    }
                } catch (Exception ex) {
                    LOG.info("Caught exception on receive(1000): {}", ex);
                }
            }
        };
        synchronized (consumer) {
            new Thread(t).start();
            consumer.wait(10000);
        }
        stopBroker();
        assertTrue(t.passed());

    }

    @Test(timeout=30000)
    public void testBrokerRestartWontHangConnectionClose() throws Exception {
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
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

    @Test(timeout=30 * 1000)
    public void testProduceAndConsumeLargeNumbersOfMessages() throws JMSException {
        int count = 2000;
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
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

        Message message = consumer.receive(500);
        assertNull(message);
    }

    @Test(timeout=30000)
    public void testSyncSends() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        connection = createConnection(true);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        connection.start();
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        Message toSend = session.createTextMessage("Sample text");
        producer.send(toSend);
        MessageConsumer consumer = session.createConsumer(queue);
        Message received = consumer.receive(5000);
        assertNotNull(received);
    }

    @Test(timeout=30000)
    public void testDurableConsumerAsync() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> received = new AtomicReference<Message>();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(getDestinationName());
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
    }

    @Test(timeout=30000)
    public void testDurableConsumerSync() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(getDestinationName());
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
    }

    @Test(timeout=30000)
    public void testTopicConsumerAsync() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> received = new AtomicReference<Message>();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(getDestinationName());
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

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(getDestinationName());
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
    }

    @Test(timeout=30000)
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

        connection = createConnection();
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        final CountDownLatch called = new CountDownLatch(1);

        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("Exception listener called: ", exception);
                called.countDown();
            }
        });

        // This makes sure the connection is completely up and connected
        Destination destination = s.createTemporaryQueue();
        MessageProducer producer = s.createProducer(destination);
        assertNotNull(producer);

        stopBroker();

        assertTrue("No exception listener event fired.", called.await(15, TimeUnit.SECONDS));
    }

    @Test(timeout=30000)
    public void testSessionTransactedCommit() throws JMSException, InterruptedException {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(getDestinationName());

        connection.start();

        // transacted producer
        MessageProducer pr = session.createProducer(queue);
        for (int i = 0; i < 10; i++) {
            Message m = session.createTextMessage("TestMessage" + i);
            pr.send(m);
        }

        // No commit in place, so no message should be dispatched.
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage m = (TextMessage) consumer.receive(500);

        assertNull(m);

        session.commit();

        // Messages should be available now.
        for (int i = 0; i < 10; i++) {
            Message msg = consumer.receive(5000);
            assertNotNull(msg);
        }

        session.close();
    }

    @Test(timeout=30000)
    public void testSessionTransactedRollback() throws JMSException, InterruptedException {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

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
        TextMessage m = (TextMessage) consumer.receive(500);
        assertNull(m);

        session.close();
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

    @Test(timeout = 30 * 1000)
    public void testSendLargeMessage() throws JMSException, InterruptedException {
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String queueName = getDestinationName();
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

    @Test(timeout=30000)
    public void testDurableConsumerUnsubscribe() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        final BrokerViewMBean broker = getProxyToBroker();

        connection = createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getDestinationName());
        MessageConsumer consumer = session.createDurableSubscriber(topic, "DurbaleTopic");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return broker.getInactiveDurableTopicSubscribers().length == 0 &&
                       broker.getDurableTopicSubscribers().length == 1;
            }
        }));

        consumer.close();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return broker.getInactiveDurableTopicSubscribers().length == 1 &&
                       broker.getDurableTopicSubscribers().length == 0;
            }
        }));

        session.unsubscribe("DurbaleTopic");
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return broker.getInactiveDurableTopicSubscribers().length == 0 &&
                       broker.getDurableTopicSubscribers().length == 0;
            }
        }));
    }

    @Test(timeout=30000)
    public void testDurableConsumerUnsubscribeWhileNoSubscription() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        final BrokerViewMBean broker = getProxyToBroker();

        connection = createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return broker.getInactiveDurableTopicSubscribers().length == 0 &&
                       broker.getDurableTopicSubscribers().length == 0;
            }
        }));

        try {
            session.unsubscribe("DurbaleTopic");
            fail("Should have thrown as subscription is in use.");
        } catch (JMSException ex) {
        }
    }

    @Test(timeout=30000)
    public void testDurableConsumerUnsubscribeWhileActive() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        final BrokerViewMBean broker = getProxyToBroker();

        connection = createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getDestinationName());
        session.createDurableSubscriber(topic, "DurbaleTopic");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return broker.getInactiveDurableTopicSubscribers().length == 0 &&
                       broker.getDurableTopicSubscribers().length == 1;
            }
        }));

        try {
            session.unsubscribe("DurbaleTopic");
            fail("Should have thrown as subscription is in use.");
        } catch (JMSException ex) {
        }
    }

    @Test(timeout=30000)
    public void testRedeliveredHeader() throws Exception {
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        connection.start();

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        for (int i = 1; i < 100; i++) {
            Message m = session.createTextMessage(i + ". Sample text");
            producer.send(m);
        }

        MessageConsumer consumer = session.createConsumer(queue);
        receiveMessages(consumer);
        consumer.close();

        consumer = session.createConsumer(queue);
        receiveMessages(consumer);
        consumer.close();
    }

    protected void receiveMessages(MessageConsumer consumer) throws Exception {
        for (int i = 0; i < 10; i++) {
            Message message = consumer.receive(1000);
            assertNotNull(message);
            assertFalse(message.getJMSRedelivered());
        }
    }
}
