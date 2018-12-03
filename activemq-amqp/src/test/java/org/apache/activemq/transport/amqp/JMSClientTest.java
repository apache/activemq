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
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.activemq.transport.amqp.joram.ActiveMQAdmin;
import org.apache.activemq.util.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Test;
import org.objectweb.jtests.jms.framework.TestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSClientTest extends JMSClientTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JMSClientTest.class);

    protected java.util.logging.Logger frameLoggger = java.util.logging.Logger.getLogger("FRM");

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

    @Test(timeout = 60000)
    public void testSendJMSMapMessage() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull(session);
            Queue queue = session.createQueue(name.getMethodName());
            MessageProducer producer = session.createProducer(queue);
            MapMessage message = session.createMapMessage();
            message.setBoolean("Boolean", false);
            message.setString("STRING", "TEST");
            producer.send(message);
            QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
            assertEquals(1, proxy.getQueueSize());

            MessageConsumer consumer = session.createConsumer(queue);
            Message received = consumer.receive(5000);
            assertNotNull(received);
            assertTrue(received instanceof MapMessage);
            MapMessage map = (MapMessage) received;
            assertEquals("TEST", map.getString("STRING"));
            assertEquals(false, map.getBooleanProperty("Boolean"));
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

        for (int i = 1; i <= consumeBeforeRollback; i++) {
            Message message = consumer.receive(1000);
            assertNotNull(message);
            assertEquals("Unexpected message number", i, message.getIntProperty(AmqpTestSupport.MESSAGE_NUMBER));
        }

        session.rollback();

        assertEquals(totalCount, proxy.getQueueSize());

        // Consume again..check we receive all the messages.
        Set<Integer> messageNumbers = new HashSet<>();
        for (int i = 1; i <= totalCount; i++) {
            messageNumbers.add(i);
        }

        for (int i = 1; i <= totalCount; i++) {
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
        final int msgCount = 300;

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

    @SuppressWarnings("rawtypes")
    @Test(timeout=30000)
    public void testSelectorsWithJMSType() throws Exception{
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getDestinationName());
            MessageProducer p = session.createProducer(queue);

            TextMessage message = session.createTextMessage();
            message.setText("text");
            p.send(message, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            TextMessage message2 = session.createTextMessage();
            String type = "myJMSType";
            message2.setJMSType(type);
            message2.setText("text + type");
            p.send(message2, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            int count = 0;
            while (enumeration.hasMoreElements()) {
                Message m = (Message) enumeration.nextElement();
                assertTrue(m instanceof TextMessage);
                count ++;
            }

            assertEquals(2, count);

            MessageConsumer consumer = session.createConsumer(queue, "JMSType = '"+ type +"'");
            Message msg = consumer.receive(TestConfig.TIMEOUT);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("Unexpected JMSType value", type, msg.getJMSType());
            assertEquals("Unexpected message content", "text + type", ((TextMessage) msg).getText());
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
            // wait until we know that the producer was able to send a message
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
            LOG.info("Caught exception on consumer create: {}", ex);
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
                    LOG.info("Caught exception on receive(): {}", ex);
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
    public void testProduceAndConsumeLargeNumbersOfMessages() throws Exception {
        int count = 1000;
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        connection.start();

        MessageProducer producer= session.createProducer(queue);
        for (int i = 0; i < count; i++) {
            Message m=session.createTextMessage("Test-Message:"+i);
            producer.send(m);
        }

        MessageConsumer consumer=session.createConsumer(queue);
        for(int i = 0; i < count; i++) {
            Message message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals("Test-Message:" + i,((TextMessage) message).getText());
        }

        assertNull(consumer.receiveNoWait());
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
        final AtomicReference<Message> received = new AtomicReference<>();
        String durableClientId = getDestinationName() + "-ClientId";

        connection = createConnection(durableClientId);
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
        String durableClientId = getDestinationName() + "-ClientId";

        connection = createConnection(durableClientId);
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

            final AtomicReference<Message> msg = new AtomicReference<>();
            assertTrue(Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    msg.set(consumer.receiveNoWait());
                    return msg.get() != null;
                }
            }, TimeUnit.SECONDS.toMillis(25), TimeUnit.MILLISECONDS.toMillis(200)));

            assertNotNull("Should have received a message by now.", msg.get());
            assertTrue("Should be an instance of TextMessage", msg.get() instanceof TextMessage);
        }
    }

    @Test(timeout=30000)
    public void testTopicConsumerAsync() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Message> received = new AtomicReference<>();

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

            final AtomicReference<Message> msg = new AtomicReference<>();
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

        ArrayList<Connection> connections = new ArrayList<>();

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
    public void testSessionTransactedCommit() throws Exception {
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
        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        assertEquals(0, queueView.getQueueSize());

        session.commit();

        // No commit in place, so no message should be dispatched.
        assertEquals(10, queueView.getQueueSize());

        session.close();
    }

    @Test(timeout=30000)
    public void testSessionTransactedRollback() throws Exception {
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

        MessageConsumer consumer = session.createConsumer(queue);

        // No commit in place, so no message should be dispatched.
        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        assertEquals(0, queueView.getQueueSize());

        assertNull(consumer.receive(100));
        consumer.close();

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

    @Test(timeout=30*1000)
    public void testDurableTopicStateAfterSubscriberClosed() throws Exception {
        String durableClientId = getDestinationName() + "-ClientId";
        String durableSubscriberName = getDestinationName() + "-SubscriptionName";

        BrokerView adminView = this.brokerService.getAdminView();
        int durableSubscribersAtStart = adminView.getDurableTopicSubscribers().length;
        int inactiveSubscribersAtStart = adminView.getInactiveDurableTopicSubscribers().length;
        LOG.debug(">>>> At Start, durable Subscribers {} inactiveDurableSubscribers {}", durableSubscribersAtStart, inactiveSubscribersAtStart);

        TopicConnection subscriberConnection =
            JMSClientContext.INSTANCE.createTopicConnection(getBrokerURI(), "admin", "password");
        subscriberConnection.setClientID(durableClientId);
        TopicSession subscriberSession = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = subscriberSession.createTopic(getDestinationName());
        TopicSubscriber messageConsumer = subscriberSession.createDurableSubscriber(topic, durableSubscriberName);

        assertNotNull(messageConsumer);

        int durableSubscribers = adminView.getDurableTopicSubscribers().length;
        int inactiveSubscribers = adminView.getInactiveDurableTopicSubscribers().length;
        LOG.debug(">>>> durable Subscribers after creation {} inactiveDurableSubscribers {}", durableSubscribers, inactiveSubscribers);
        assertEquals("Wrong number of durable subscribers after first subscription", 1, (durableSubscribers - durableSubscribersAtStart));
        assertEquals("Wrong number of inactive durable subscribers after first subscription", 0, (inactiveSubscribers - inactiveSubscribersAtStart));

        subscriberConnection.close();

        durableSubscribers = adminView.getDurableTopicSubscribers().length;
        inactiveSubscribers = adminView.getInactiveDurableTopicSubscribers().length;
        LOG.debug(">>>> durable Subscribers after close {} inactiveDurableSubscribers {}", durableSubscribers, inactiveSubscribers);
        assertEquals("Wrong number of durable subscribers after close", 0, (durableSubscribersAtStart));
        assertEquals("Wrong number of inactive durable subscribers after close", 1, (inactiveSubscribers - inactiveSubscribersAtStart));
    }

    @Test(timeout=30000)
    public void testDurableConsumerUnsubscribe() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        String durableClientId = getDestinationName() + "-ClientId";

        final BrokerViewMBean broker = getProxyToBroker();

        connection = createConnection(durableClientId);
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
        }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(250)));

        consumer.close();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return broker.getInactiveDurableTopicSubscribers().length == 1 &&
                       broker.getDurableTopicSubscribers().length == 0;
            }
        }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(250)));

        session.unsubscribe("DurbaleTopic");
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return broker.getInactiveDurableTopicSubscribers().length == 0 &&
                       broker.getDurableTopicSubscribers().length == 0;
            }
        }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(250)));
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
        }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(250)));

        try {
            session.unsubscribe("DurbaleTopic");
            fail("Should have thrown as subscription is in use.");
        } catch (JMSException ex) {
        }
    }

    @Test(timeout=30000)
    public void testDurableConsumerUnsubscribeWhileActive() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        String durableClientId = getDestinationName() + "-ClientId";

        final BrokerViewMBean broker = getProxyToBroker();

        connection = createConnection(durableClientId);
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
        }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(250)));

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

    @Test(timeout=30000)
    public void testCreateTemporaryQueue() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createTemporaryQueue();
            assertNotNull(queue);
            assertTrue(queue instanceof TemporaryQueue);

            final BrokerViewMBean broker = getProxyToBroker();
            assertEquals(1, broker.getTemporaryQueues().length);
        }
    }

    @Test(timeout=30000)
    public void testDeleteTemporaryQueue() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createTemporaryQueue();
            assertNotNull(queue);
            assertTrue(queue instanceof TemporaryQueue);

            final BrokerViewMBean broker = getProxyToBroker();
            assertEquals(1, broker.getTemporaryQueues().length);

            TemporaryQueue tempQueue = (TemporaryQueue) queue;
            tempQueue.delete();

            assertTrue("Temp Queue should be deleted.", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return broker.getTemporaryQueues().length == 0;
                }
            }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)));
        }
    }

    @Test(timeout=30000)
    public void testCreateTemporaryTopic() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTemporaryTopic();
            assertNotNull(topic);
            assertTrue(topic instanceof TemporaryTopic);

            final BrokerViewMBean broker = getProxyToBroker();
            assertEquals(1, broker.getTemporaryTopics().length);
        }
    }

    @Test(timeout=30000)
    public void testDeleteTemporaryTopic() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTemporaryTopic();
            assertNotNull(topic);
            assertTrue(topic instanceof TemporaryTopic);

            final BrokerViewMBean broker = getProxyToBroker();
            assertEquals(1, broker.getTemporaryTopics().length);

            TemporaryTopic tempTopic = (TemporaryTopic) topic;
            tempTopic.delete();

            assertTrue("Temp Topic should be deleted.", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return broker.getTemporaryTopics().length == 0;
                }
            }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)));
        }
    }

    @Test(timeout = 60000)
    public void testZeroPrefetchWithTwoConsumers() throws Exception {
        JmsConnectionFactory cf = new JmsConnectionFactory(getAmqpURI("jms.prefetchPolicy.all=0"));
        connection = cf.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));

        // now lets receive it
        MessageConsumer consumer1 = session.createConsumer(queue);
        MessageConsumer consumer2 = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer1.receive(5000);
        assertNotNull(answer);
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
        answer = (TextMessage)consumer2.receive(5000);
        assertNotNull(answer);
        assertEquals("Should have received a message!", answer.getText(), "Msg2");

        answer = (TextMessage)consumer2.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    @Test(timeout=30000)
    public void testRetroactiveConsumerSupported() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName() + "?consumer.retroactive=true");
        MessageConsumer consumer = session.createConsumer(queue);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        assertNotNull(queueView);
        assertEquals(1, queueView.getSubscriptions().length);

        SubscriptionViewMBean subscriber = getProxyToQueueSubscriber(getDestinationName());
        assertTrue(subscriber.isRetroactive());

        consumer.close();
    }

    @Test(timeout=30000)
    public void testExclusiveConsumerSupported() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName() + "?consumer.exclusive=true");
        MessageConsumer consumer = session.createConsumer(queue);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        assertNotNull(queueView);
        assertEquals(1, queueView.getSubscriptions().length);

        SubscriptionViewMBean subscriber = getProxyToQueueSubscriber(getDestinationName());
        assertTrue(subscriber.isExclusive());

        consumer.close();
    }

    @Test(timeout=30000)
    public void testUnpplicableDestinationOption() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName() + "?consumer.unknoen=true");
        try {
            session.createConsumer(queue);
            fail("Should have failed to create consumer");
        } catch (JMSException jmsEx) {
        }
    }

    protected void receiveMessages(MessageConsumer consumer) throws Exception {
        for (int i = 0; i < 10; i++) {
            Message message = consumer.receive(1000);
            assertNotNull(message);
            assertFalse(message.getJMSRedelivered());
        }
    }

    @Test(timeout = 30000)
    public void testProduceAndConsumeLargeNumbersOfTopicMessagesClientAck() throws Exception {
        doTestProduceAndConsumeLargeNumbersOfMessages(true, Session.CLIENT_ACKNOWLEDGE);
    }

    @Test(timeout = 30000)
    public void testProduceAndConsumeLargeNumbersOfQueueMessagesClientAck() throws Exception {
        doTestProduceAndConsumeLargeNumbersOfMessages(false, Session.CLIENT_ACKNOWLEDGE);
    }

    @Test(timeout = 30000)
    public void testProduceAndConsumeLargeNumbersOfTopicMessagesAutoAck() throws Exception {
        doTestProduceAndConsumeLargeNumbersOfMessages(true, Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout = 30000)
    public void testProduceAndConsumeLargeNumbersOfQueueMessagesAutoAck() throws Exception {
        doTestProduceAndConsumeLargeNumbersOfMessages(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void doTestProduceAndConsumeLargeNumbersOfMessages(boolean topic, int ackMode) throws Exception {

        final int MSG_COUNT = 1000;
        final CountDownLatch done = new CountDownLatch(MSG_COUNT);

        JmsConnectionFactory factory = new JmsConnectionFactory(getAmqpURI());
        factory.setForceSyncSend(true);

        connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, ackMode);
        final Destination destination;
        if (topic) {
            destination = session.createTopic(getDestinationName());
        } else {
            destination = session.createQueue(getDestinationName());
        }

        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                try {
                    message.acknowledge();
                    done.countDown();
                } catch (JMSException ex) {
                    LOG.info("Caught exception.", ex);
                }
            }
        });

        MessageProducer producer = session.createProducer(destination);

        TextMessage textMessage = session.createTextMessage();
        textMessage.setText("messageText");

        for (int i = 0; i < MSG_COUNT; i++) {
            producer.send(textMessage);
        }

        assertTrue("Did not receive all messages: " + MSG_COUNT, done.await(15, TimeUnit.SECONDS));
    }
}
