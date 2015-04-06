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
package org.apache.activemq;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.ObjectName;

import junit.framework.Test;

import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases used to test the JMS message consumer.
 */
public class JMSConsumerTest extends JmsTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JMSConsumerTest.class);

    public ActiveMQDestination destination;
    public int deliveryMode;
    public int prefetch;
    public int ackMode;
    public byte destinationType;
    public boolean durableConsumer;

    public static Test suite() {
        return suite(JMSConsumerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombosForTestMessageListenerWithConsumerCanBeStopped() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testMessageListenerWithConsumerCanBeStopped() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done1 = new CountDownLatch(1);
        final CountDownLatch done2 = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer)session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 1) {
                    done1.countDown();
                }
                if (counter.get() == 2) {
                    done2.countDown();
                }
            }
        });

        // Send a first message to make sure that the consumer dispatcher is
        // running
        sendMessages(session, destination, 1);
        assertTrue(done1.await(1, TimeUnit.SECONDS));
        assertEquals(1, counter.get());

        // Stop the consumer.
        consumer.stop();

        // Send a message, but should not get delivered.
        sendMessages(session, destination, 1);
        assertFalse(done2.await(500, TimeUnit.MILLISECONDS));
        assertEquals(1, counter.get());

        // Start the consumer, and the message should now get delivered.
        consumer.start();
        assertTrue(done2.await(500, TimeUnit.MILLISECONDS));
        assertEquals(2, counter.get());
    }

    public void testMessageListenerWithConsumerCanBeStoppedConcurently() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch closeDone = new CountDownLatch(1);

        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

        // preload the queue
        sendMessages(session, destination, 2000);

        final ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer)session.createConsumer(destination);

        final Map<Thread, Throwable> exceptions =
            Collections.synchronizedMap(new HashMap<Thread, Throwable>());
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Uncaught exception:", e);
                exceptions.put(t, e);
            }
        });

        final class AckAndClose implements Runnable {
            private final Message message;

            public AckAndClose(Message m) {
                this.message = m;
            }

            @Override
            public void run() {
                try {
                    int count = counter.incrementAndGet();
                    if (count == 590) {
                        // close in a separate thread is ok by jms
                        consumer.close();
                        closeDone.countDown();
                    }
                    if (count % 200 == 0) {
                        // ensure there are some outstanding messages
                        // ack every 200
                        message.acknowledge();
                    }
                } catch (Exception e) {
                    LOG.error("Exception on close or ack:", e);
                    exceptions.put(Thread.currentThread(), e);
                }
            }
        };

        final ExecutorService executor = Executors.newCachedThreadPool();
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                // ack and close eventually in separate thread
                executor.execute(new AckAndClose(m));
            }
        });

        assertTrue(closeDone.await(20, TimeUnit.SECONDS));

        // await possible exceptions
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }

    public void initCombosForTestMutiReceiveWithPrefetch1() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("ackMode", new Object[] {Integer.valueOf(Session.AUTO_ACKNOWLEDGE), Integer.valueOf(Session.DUPS_OK_ACKNOWLEDGE),
                                                      Integer.valueOf(Session.CLIENT_ACKNOWLEDGE)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testMutiReceiveWithPrefetch1() throws Exception {

        // Set prefetch to 1
        connection.getPrefetchPolicy().setAll(1);
        connection.start();

        // Use all the ack modes
        Session session = connection.createSession(false, ackMode);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 4);

        // Make sure 4 messages were delivered.
        Message message = null;
        for (int i = 0; i < 4; i++) {
            message = consumer.receive(1000);
            assertNotNull(message);
        }
        assertNull(consumer.receiveNoWait());
        message.acknowledge();
    }

    public void initCombosForTestDurableConsumerSelectorChange() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testDurableConsumerSelectorChange() throws Exception {

        // Receive a message with the JMS API
        connection.setClientID("test");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);
        MessageConsumer consumer = session.createDurableSubscriber((Topic)destination, "test", "color='red'", false);

        // Send the messages
        TextMessage message = session.createTextMessage("1st");
        message.setStringProperty("color", "red");
        producer.send(message);

        Message m = consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", ((TextMessage)m).getText());

        // Change the subscription.
        consumer.close();
        consumer = session.createDurableSubscriber((Topic)destination, "test", "color='blue'", false);

        message = session.createTextMessage("2nd");
        message.setStringProperty("color", "red");
        producer.send(message);
        message = session.createTextMessage("3rd");
        message.setStringProperty("color", "blue");
        producer.send(message);

        // Selector should skip the 2nd message.
        m = consumer.receive(1000);
        assertNotNull(m);
        assertEquals("3rd", ((TextMessage)m).getText());

        assertNull(consumer.receiveNoWait());
    }

    public void initCombosForTestSendReceiveBytesMessage() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testSendReceiveBytesMessage() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        BytesMessage message = session.createBytesMessage();
        message.writeBoolean(true);
        message.writeBoolean(false);
        producer.send(message);

        // Make sure only 1 message was delivered.
        BytesMessage m = (BytesMessage)consumer.receive(1000);
        assertNotNull(m);
        assertTrue(m.readBoolean());
        assertFalse(m.readBoolean());

        assertNull(consumer.receiveNoWait());
    }

    public void initCombosForTestSetMessageListenerAfterStart() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testSetMessageListenerAfterStart() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 4);

        // See if the message get sent to the listener
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 4) {
                    done.countDown();
                }
            }
        });

        assertTrue(done.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // Make sure only 4 messages were delivered.
        assertEquals(4, counter.get());
    }

    public void initCombosForTestPassMessageListenerIntoCreateConsumer() {
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testPassMessageListenerIntoCreateConsumer() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.start();
        ActiveMQSession session = (ActiveMQSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination, new MessageListener() {
            @Override
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 4) {
                    done.countDown();
                }
            }
        });
        assertNotNull(consumer);

        // Send the messages
        sendMessages(session, destination, 4);

        assertTrue(done.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // Make sure only 4 messages were delivered.
        assertEquals(4, counter.get());
    }

    public void initCombosForTestMessageListenerOnMessageCloseUnackedWithPrefetch1StayInQueue() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("ackMode", new Object[] {Integer.valueOf(Session.CLIENT_ACKNOWLEDGE)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE)});
    }

    public void testMessageListenerOnMessageCloseUnackedWithPrefetch1StayInQueue() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch sendDone = new CountDownLatch(1);
        final CountDownLatch got2Done = new CountDownLatch(1);

        // Set prefetch to 1
        connection.getPrefetchPolicy().setAll(1);
        // This test case does not work if optimized message dispatch is used as
        // the main thread send block until the consumer receives the
        // message. This test depends on thread decoupling so that the main
        // thread can stop the consumer thread.
        connection.setOptimizedMessageDispatch(false);
        connection.start();

        // Use all the ack modes
        Session session = connection.createSession(false, ackMode);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                try {
                    TextMessage tm = (TextMessage)m;
                    LOG.info("Got in first listener: " + tm.getText());
                    assertEquals("" + counter.get(), tm.getText());
                    counter.incrementAndGet();
                    if (counter.get() == 2) {
                        sendDone.await();
                        connection.close();
                        got2Done.countDown();
                    }
                    tm.acknowledge();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);
        sendDone.countDown();

        // Wait for first 2 messages to arrive.
        assertTrue(got2Done.await(100000, TimeUnit.MILLISECONDS));

        // Re-start connection.
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);

        connection.getPrefetchPolicy().setAll(1);
        connection.start();

        // Pickup the remaining messages.
        final CountDownLatch done2 = new CountDownLatch(1);
        session = connection.createSession(false, ackMode);
        consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                try {
                    TextMessage tm = (TextMessage)m;
                    LOG.info("Got in second listener: " + tm.getText());
                    // order is not guaranteed as the connection is started before the listener is set.
                    // assertEquals("" + counter.get(), tm.getText());
                    counter.incrementAndGet();
                    if (counter.get() == 4) {
                        done2.countDown();
                    }
                } catch (Throwable e) {
                    LOG.error("unexpected ex onMessage: ", e);
                }
            }
        });

        assertTrue(done2.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // assert msg 2 was redelivered as close() from onMessages() will only ack in auto_ack and dups_ok mode
        assertEquals(5, counter.get());
    }

    public void initCombosForTestMessageListenerAutoAckOnCloseWithPrefetch1() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("ackMode", new Object[] {Integer.valueOf(Session.AUTO_ACKNOWLEDGE), Integer.valueOf(Session.CLIENT_ACKNOWLEDGE)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE)});
    }

    public void testMessageListenerAutoAckOnCloseWithPrefetch1() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch sendDone = new CountDownLatch(1);
        final CountDownLatch got2Done = new CountDownLatch(1);

        // Set prefetch to 1
        connection.getPrefetchPolicy().setAll(1);
        // This test case does not work if optimized message dispatch is used as
        // the main thread send block until the consumer receives the
        // message. This test depends on thread decoupling so that the main
        // thread can stop the consumer thread.
        connection.setOptimizedMessageDispatch(false);
        connection.start();

        // Use all the ack modes
        Session session = connection.createSession(false, ackMode);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                try {
                    TextMessage tm = (TextMessage)m;
                    LOG.info("Got in first listener: " + tm.getText());
                    assertEquals("" + counter.get(), tm.getText());
                    counter.incrementAndGet();
                    m.acknowledge();
                    if (counter.get() == 2) {
                        sendDone.await();
                        connection.close();
                        got2Done.countDown();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);
        sendDone.countDown();

        // Wait for first 2 messages to arrive.
        assertTrue(got2Done.await(100000, TimeUnit.MILLISECONDS));

        // Re-start connection.
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);

        connection.getPrefetchPolicy().setAll(1);
        connection.start();

        // Pickup the remaining messages.
        final CountDownLatch done2 = new CountDownLatch(1);
        session = connection.createSession(false, ackMode);
        consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                try {
                    TextMessage tm = (TextMessage)m;
                    LOG.info("Got in second listener: " + tm.getText());
                    counter.incrementAndGet();
                    if (counter.get() == 4) {
                        done2.countDown();
                    }
                } catch (Throwable e) {
                    LOG.error("unexpected ex onMessage: ", e);
                }
            }
        });

        assertTrue(done2.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // close from onMessage with Auto_ack will ack
        // Make sure only 4 messages were delivered.
        assertEquals(4, counter.get());
    }

    public void initCombosForTestMessageListenerWithConsumerWithPrefetch1() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testMessageListenerWithConsumerWithPrefetch1() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.getPrefetchPolicy().setAll(1);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 4) {
                    done.countDown();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);

        assertTrue(done.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // Make sure only 4 messages were delivered.
        assertEquals(4, counter.get());
    }

    public void initCombosForTestMessageListenerWithConsumer() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testMessageListenerWithConsumer() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 4) {
                    done.countDown();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);

        assertTrue(done.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // Make sure only 4 messages were delivered.
        assertEquals(4, counter.get());
    }

    public void initCombosForTestUnackedWithPrefetch1StayInQueue() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("ackMode", new Object[] {Integer.valueOf(Session.AUTO_ACKNOWLEDGE), Integer.valueOf(Session.DUPS_OK_ACKNOWLEDGE),
                                                      Integer.valueOf(Session.CLIENT_ACKNOWLEDGE)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE)});
    }

    public void testUnackedWithPrefetch1StayInQueue() throws Exception {

        // Set prefetch to 1
        connection.getPrefetchPolicy().setAll(1);
        connection.start();

        // Use all the ack modes
        Session session = connection.createSession(false, ackMode);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 4);

        // Only pick up the first 2 messages.
        Message message = null;
        for (int i = 0; i < 2; i++) {
            message = consumer.receive(1000);
            assertNotNull(message);
        }
        message.acknowledge();

        connection.close();
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.getPrefetchPolicy().setAll(1);
        connection.start();

        // Use all the ack modes
        session = connection.createSession(false, ackMode);
        consumer = session.createConsumer(destination);

        // Pickup the rest of the messages.
        for (int i = 0; i < 2; i++) {
            message = consumer.receive(1000);
            assertNotNull(message);
        }
        message.acknowledge();
        assertNull(consumer.receiveNoWait());
    }

    public void initCombosForTestPrefetch1MessageNotDispatched() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    public void testPrefetch1MessageNotDispatched() throws Exception {

        // Set prefetch to 1
        connection.getPrefetchPolicy().setAll(1);
        connection.start();

        Session session = connection.createSession(true, 0);
        destination = new ActiveMQQueue("TEST");
        MessageConsumer consumer = session.createConsumer(destination);

        // Send 2 messages to the destination.
        sendMessages(session, destination, 2);
        session.commit();

        // The prefetch should fill up with 1 message.
        // Since prefetch is still full, the 2nd message should get dispatched
        // to another consumer.. lets create the 2nd consumer test that it does
        // make sure it does.
        ActiveMQConnection connection2 = (ActiveMQConnection)factory.createConnection();
        connection2.start();
        connections.add(connection2);
        Session session2 = connection2.createSession(true, 0);
        MessageConsumer consumer2 = session2.createConsumer(destination);

        // Pick up the first message.
        Message message1 = consumer.receive(1000);
        assertNotNull(message1);

        // Pick up the 2nd messages.
        Message message2 = consumer2.receive(5000);
        assertNotNull(message2);

        session.commit();
        session2.commit();

        assertNull(consumer.receiveNoWait());
    }

    public void initCombosForTestDontStart() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testDontStart() throws Exception {

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 1);

        // Make sure no messages were delivered.
        assertNull(consumer.receive(1000));
    }

    public void initCombosForTestStartAfterSend() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testStartAfterSend() throws Exception {

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 1);

        // Start the conncection after the message was sent.
        connection.start();

        // Make sure only 1 message was delivered.
        assertNotNull(consumer.receive(1000));
        assertNull(consumer.receiveNoWait());
    }

    public void initCombosForTestReceiveMessageWithConsumer() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testReceiveMessageWithConsumer() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 1);

        // Make sure only 1 message was delivered.
        Message m = consumer.receive(1000);
        assertNotNull(m);
        assertEquals("0", ((TextMessage)m).getText());
        assertNull(consumer.receiveNoWait());
    }

    public void testDupsOkConsumer() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 4);

        // Make sure only 4 message are delivered.
        for( int i=0; i < 4; i++){
            Message m = consumer.receive(1000);
            assertNotNull(m);
        }
        assertNull(consumer.receive(500));

        // Close out the consumer.. no other messages should be left on the queue.
        consumer.close();

        consumer = session.createConsumer(destination);
        assertNull(consumer.receive(500));
    }

    public void testRedispatchOfUncommittedTx() throws Exception {

        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

        sendMessages(connection, destination, 2);

        MessageConsumer consumer = session.createConsumer(destination);
        assertNotNull(consumer.receive(1000));
        assertNotNull(consumer.receive(1000));

        // install another consumer while message dispatch is unacked/uncommitted
        Session redispatchSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer redispatchConsumer = redispatchSession.createConsumer(destination);

        // no commit so will auto rollback and get re-dispatched to redisptachConsumer
        session.close();

        Message msg = redispatchConsumer.receive(1000);
        assertNotNull(msg);
        assertTrue("redelivered flag set", msg.getJMSRedelivered());
        assertEquals(2, msg.getLongProperty("JMSXDeliveryCount"));

        msg = redispatchConsumer.receive(1000);
        assertNotNull(msg);
        assertTrue(msg.getJMSRedelivered());
        assertEquals(2, msg.getLongProperty("JMSXDeliveryCount"));
        redispatchSession.commit();

        assertNull(redispatchConsumer.receive(500));
        redispatchSession.close();
    }

    public void testRedispatchOfRolledbackTx() throws Exception {

        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

        sendMessages(connection, destination, 2);

        MessageConsumer consumer = session.createConsumer(destination);
        assertNotNull(consumer.receive(1000));
        assertNotNull(consumer.receive(1000));

        // install another consumer while message dispatch is unacked/uncommitted
        Session redispatchSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer redispatchConsumer = redispatchSession.createConsumer(destination);

        session.rollback();
        session.close();

        Message msg = redispatchConsumer.receive(1000);
        assertNotNull(msg);
        assertTrue(msg.getJMSRedelivered());
        assertEquals(2, msg.getLongProperty("JMSXDeliveryCount"));
        msg = redispatchConsumer.receive(1000);
        assertNotNull(msg);
        assertTrue(msg.getJMSRedelivered());
        assertEquals(2, msg.getLongProperty("JMSXDeliveryCount"));
        redispatchSession.commit();

        assertNull(redispatchConsumer.receive(500));
        redispatchSession.close();
    }

    public void initCombosForTestAckOfExpired() {
        addCombinationValues("destinationType",
            new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testAckOfExpired() throws Exception {

        ActiveMQConnectionFactory fact = new ActiveMQConnectionFactory("vm://localhost?jms.prefetchPolicy.all=4&jms.sendAcksAsync=false");
        connection = fact.createActiveMQConnection();

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = (ActiveMQDestination) (destinationType == ActiveMQDestination.QUEUE_TYPE ?
                session.createQueue("test") : session.createTopic("test"));

        MessageConsumer consumer = session.createConsumer(destination);
        connection.setStatsEnabled(true);

        Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = sendSession.createProducer(destination);
        producer.setTimeToLive(500);
        final int count = 4;
        for (int i = 0; i < count; i++) {
            TextMessage message = sendSession.createTextMessage("" + i);
            producer.send(message);
        }

        // let first bunch in queue expire
        Thread.sleep(1000);

        producer.setTimeToLive(0);
        for (int i = 0; i < count; i++) {
            TextMessage message = sendSession.createTextMessage("no expiry" + i);
            producer.send(message);
        }

        ActiveMQMessageConsumer amqConsumer = (ActiveMQMessageConsumer) consumer;

        for (int i=0; i<count; i++) {
            TextMessage msg = (TextMessage) amqConsumer.receive();
            assertNotNull(msg);
            assertTrue("message has \"no expiry\" text: " + msg.getText(), msg.getText().contains("no expiry"));

            // force an ack when there are expired messages
            amqConsumer.acknowledge();
        }
        assertEquals("consumer has expiredMessages", count, amqConsumer.getConsumerStats().getExpiredMessageCount().getCount());

        DestinationViewMBean view = createView(destination);

        assertEquals("Wrong inFlightCount: " + view.getInFlightCount(), 0, view.getInFlightCount());
        assertEquals("Wrong dispatch count: " + view.getDispatchCount(), 8, view.getDispatchCount());
        assertEquals("Wrong dequeue count: " + view.getDequeueCount(), 8, view.getDequeueCount());
        assertEquals("Wrong expired count: " + view.getExpiredCount(), 4, view.getExpiredCount());
    }

    protected DestinationViewMBean createView(ActiveMQDestination destination) throws Exception {

        String domain = "org.apache.activemq";
        ObjectName name;
        if (destination.isQueue()) {
            name = new ObjectName(domain + ":type=Broker,brokerName=localhost,destinationType=Queue,destinationName=test");
        } else {
            name = new ObjectName(domain + ":type=Broker,brokerName=localhost,destinationType=Topic,destinationName=test");
        }
        return (DestinationViewMBean) broker.getManagementContext().newProxyInstance(name, DestinationViewMBean.class, true);
    }
}
