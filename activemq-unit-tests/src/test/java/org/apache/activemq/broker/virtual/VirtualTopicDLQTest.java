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
package org.apache.activemq.broker.virtual;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

/**
 * Unit test for virtual topics and DLQ messaging. See individual test for more
 * detail
 *
 */
public class VirtualTopicDLQTest extends TestCase {
    private static BrokerService broker;

    private static final Logger LOG = LoggerFactory.getLogger(VirtualTopicDLQTest.class);

    static final String jmsConnectionURI = "failover:(vm://localhost)";

    // Virtual Topic that the test publishes 10 messages to
    private static final String virtualTopicName = "VirtualTopic.Test";

    // Queues that receive all the messages send to the virtual topic
    private static final String consumer1Prefix = "Consumer.A.";
    private static final String consumer2Prefix = "Consumer.B.";
    private static final String consumer3Prefix = "Consumer.C.";

    // Expected Individual Dead Letter Queue names that are tied to the
    // Subscriber Queues
    private static final String dlqPrefix = "ActiveMQ.DLQ.Queue.";

    // Number of messages
    private static final int numberMessages = 6;

    @Override
    @Before
    public void setUp() throws Exception {
        try {
            broker = BrokerFactory.createBroker("xbean:org/apache/activemq/broker/virtual/virtual-individual-dlq.xml", true);
            broker.start();
            broker.waitUntilStarted();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            // Purge the DLQ's so counts are correct for next run
            purgeDestination(dlqPrefix + consumer1Prefix + virtualTopicName);
            purgeDestination(dlqPrefix + consumer2Prefix + virtualTopicName);
            purgeDestination(dlqPrefix + consumer3Prefix + virtualTopicName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    /*
     * This test verifies that all undelivered messages sent to a consumers
     * listening on a queue associated with a virtual topic with be forwarded to
     * separate DLQ's.
     *
     * Note that the broker config, deadLetterStrategy need to have the enable
     * audit set to false so that duplicate message sent from a topic to
     * individual consumers are forwarded to the DLQ
     *
     * <deadLetterStrategy> <bean
     * xmlns="http://www.springframework.org/schema/beans"
     * class="org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy"
     * > <property name="useQueueForQueueMessages" value="true"></property>
     * <property name="processNonPersistent" value="true"></property> <property
     * name="processExpired" value="false"></property> <property
     * name="enableAudit" value="false"></property>
     *
     * </bean> </deadLetterStrategy>
     */
    @Test
    public void testVirtualTopicSubscriberDeadLetterQueue() throws Exception {

        TestConsumer consumer1 = null;
        TestConsumer consumer2 = null;
        TestConsumer consumer3 = null;
        TestConsumer dlqConsumer1 = null;
        TestConsumer dlqConsumer2 = null;
        TestConsumer dlqConsumer3 = null;

        try {

            // The first 2 consumers will rollback, ultimately causing messages
            // to land on the DLQ
            consumer1 = new TestConsumer(consumer1Prefix + virtualTopicName, false, numberMessages, true);
            thread(consumer1, false);

            consumer2 = new TestConsumer(consumer2Prefix + virtualTopicName, false, numberMessages, true);
            thread(consumer2, false);

            // TestConsumer that does not throw exceptions, messages should not
            // land on DLQ
            consumer3 = new TestConsumer(consumer3Prefix + virtualTopicName, false, numberMessages, false);
            thread(consumer3, false);

            // TestConsumer to read the expected Dead Letter Queue
            dlqConsumer1 = new TestConsumer(dlqPrefix + consumer1Prefix + virtualTopicName, false, numberMessages, false);
            thread(dlqConsumer1, false);

            dlqConsumer2 = new TestConsumer(dlqPrefix + consumer2Prefix + virtualTopicName, false, numberMessages, false);
            thread(dlqConsumer2, false);

            dlqConsumer3 = new TestConsumer(dlqPrefix + consumer3Prefix + virtualTopicName, false, numberMessages, false);
            thread(dlqConsumer3, false);

            // Give the consumers a second to start
            Thread.sleep(1000);

            // Start the producer
            TestProducer producer = new TestProducer(virtualTopicName, true, numberMessages);
            thread(producer, false);

            assertTrue("sent all producer messages in time, count is: " + producer.getLatch().getCount(), producer.getLatch().await(10, TimeUnit.SECONDS));
            LOG.info("producer successful, count = " + producer.getLatch().getCount());

            assertTrue("remaining consumer1 count should be zero, is: " + consumer1.getLatch().getCount(), consumer1.getLatch().await(10, TimeUnit.SECONDS));
            LOG.info("consumer1 successful, count = " + consumer1.getLatch().getCount());

            assertTrue("remaining consumer2 count should be zero, is: " + consumer2.getLatch().getCount(), consumer2.getLatch().await(10, TimeUnit.SECONDS));
            LOG.info("consumer2 successful, count = " + consumer2.getLatch().getCount());

            assertTrue("remaining consumer3 count should be zero, is: " + consumer3.getLatch().getCount(), consumer3.getLatch().await(10, TimeUnit.SECONDS));
            LOG.info("consumer3 successful, count = " + consumer3.getLatch().getCount());

            assertTrue("remaining dlqConsumer1 count should be zero, is: " + dlqConsumer1.getLatch().getCount(),
                dlqConsumer1.getLatch().await(10, TimeUnit.SECONDS));
            LOG.info("dlqConsumer1 successful, count = " + dlqConsumer1.getLatch().getCount());

            assertTrue("remaining dlqConsumer2 count should be zero, is: " + dlqConsumer2.getLatch().getCount(),
                dlqConsumer2.getLatch().await(10, TimeUnit.SECONDS));
            LOG.info("dlqConsumer2 successful, count = " + dlqConsumer2.getLatch().getCount());

            assertTrue("remaining dlqConsumer3 count should be " + numberMessages + ", is: " + dlqConsumer3.getLatch().getCount(), dlqConsumer3.getLatch()
                .getCount() == numberMessages);
            LOG.info("dlqConsumer2 successful, count = " + dlqConsumer2.getLatch().getCount());

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            // Tell consumers to stop (don't read any more messages after this)
            if (consumer1 != null)
                consumer1.setStop(true);
            if (consumer2 != null)
                consumer2.setStop(true);
            if (consumer3 != null)
                consumer3.setStop(true);
            if (dlqConsumer1 != null)
                dlqConsumer1.setStop(true);
            if (dlqConsumer2 != null)
                dlqConsumer2.setStop(true);
            if (dlqConsumer3 != null)
                dlqConsumer3.setStop(true);
        }
    }

    private static Thread thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
        return brokerThread;
    }

    private class TestProducer implements Runnable {
        private String destinationName = null;
        private boolean isTopic = true;
        private int numberMessages = 0;
        private CountDownLatch latch = null;

        public TestProducer(String destinationName, boolean isTopic, int numberMessages) {
            this.destinationName = destinationName;
            this.isTopic = isTopic;
            this.numberMessages = numberMessages;
            latch = new CountDownLatch(numberMessages);
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        @Override
        public void run() {
            ActiveMQConnectionFactory connectionFactory = null;
            ActiveMQConnection connection = null;
            ActiveMQSession session = null;
            Destination destination = null;

            try {
                LOG.info("Started TestProducer for destination (" + destinationName + ")");

                connectionFactory = new ActiveMQConnectionFactory(jmsConnectionURI);
                connection = (ActiveMQConnection) connectionFactory.createConnection();
                connection.start();
                session = (ActiveMQSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                if (isTopic) {
                    destination = session.createTopic(this.destinationName);
                } else {
                    destination = session.createQueue(this.destinationName);
                }

                // Create a MessageProducer from the Session to the Topic or
                // Queue
                ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                for (int i = 0; i < numberMessages; i++) {
                    TextMessage message = session.createTextMessage("I am a message :: " + String.valueOf(i));
                    try {
                        producer.send(message);

                    } catch (Exception deeperException) {
                        LOG.info("Producer for destination (" + destinationName + ") Caught: " + deeperException);
                    }

                    latch.countDown();
                    Thread.sleep(1000);
                }

                LOG.info("Finished TestProducer for destination (" + destinationName + ")");

            } catch (Exception e) {
                LOG.error("Terminating TestProducer(" + destinationName + ")Caught: " + e);
            } finally {
                try {
                    // Clean up
                    if (connection != null) {
                        connection.close();
                    }
                } catch (Exception e) {
                    LOG.error("Closing connection/session (" + destinationName + ")Caught: " + e);
                }
            }
        }
    }

    private class TestConsumer implements Runnable, ExceptionListener, MessageListener {
        private String destinationName = null;
        private boolean isTopic = true;
        private CountDownLatch latch = null;
        private int maxRedeliveries = 0;
        private int receivedMessageCounter = 0;
        private boolean bFakeFail = false;
        private boolean bStop = false;

        private ActiveMQConnectionFactory connectionFactory = null;
        private ActiveMQConnection connection = null;
        private Session session = null;
        private MessageConsumer consumer = null;

        public TestConsumer(String destinationName, boolean isTopic, int expectedNumberMessages, boolean bFakeFail) {
            this.destinationName = destinationName;
            this.isTopic = isTopic;
            latch = new CountDownLatch(expectedNumberMessages * (this.bFakeFail ? (maxRedeliveries + 1) : 1));
            this.bFakeFail = bFakeFail;
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        @Override
        public void run() {

            try {
                LOG.info("Started TestConsumer for destination (" + destinationName + ")");

                connectionFactory = new ActiveMQConnectionFactory(jmsConnectionURI);
                connection = (ActiveMQConnection) connectionFactory.createConnection();
                connection.start();
                session = connection.createSession(true, Session.SESSION_TRANSACTED);

                RedeliveryPolicy policy = connection.getRedeliveryPolicy();
                policy.setInitialRedeliveryDelay(1);
                policy.setUseExponentialBackOff(false);
                policy.setMaximumRedeliveries(maxRedeliveries);

                connection.setExceptionListener(this);

                Destination destination = null;
                if (isTopic) {
                    destination = session.createTopic(destinationName);
                } else {
                    destination = session.createQueue(destinationName);
                }

                consumer = session.createConsumer(destination);
                consumer.setMessageListener(this);

                while (!bStop) {
                    Thread.sleep(100);
                }

                LOG.info("Finished TestConsumer for destination name (" + destinationName + ") remaining " + this.latch.getCount() + " messages "
                    + this.toString());

            } catch (Exception e) {
                LOG.error("Consumer (" + destinationName + ") Caught: " + e);
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (Exception e) {
                    LOG.error("Closing connection/session (" + destinationName + ")Caught: " + e);
                }
            }
        }

        @Override
        public synchronized void onException(JMSException ex) {
            ex.printStackTrace();
            LOG.error("Consumer for destination, (" + destinationName + "), JMS Exception occured.  Shutting down client.");
        }

        public synchronized void setStop(boolean bStop) {
            this.bStop = bStop;
        }

        @Override
        public synchronized void onMessage(Message message) {
            receivedMessageCounter++;
            latch.countDown();

            LOG.info("Consumer for destination (" + destinationName + ") latch countdown: " + latch.getCount() + " :: Number messages received "
                + this.receivedMessageCounter);

            try {
                LOG.info("Consumer for destination (" + destinationName + ") Received message id :: " + message.getJMSMessageID());

                if (!bFakeFail) {
                    LOG.info("Consumer on destination " + destinationName + " committing JMS Session for message: " + message.toString());
                    session.commit();
                } else {
                    LOG.info("Consumer on destination " + destinationName + " rolling back JMS Session for message: " + message.toString());
                    session.rollback(); // rolls back all the consumed messages
                                        // on the session to
                }

            } catch (JMSException ex) {
                LOG.error("Error reading JMS Message from destination " + destinationName + ".");
            }
        }
    }

    private static void purgeDestination(String destination) throws Exception {
        final Queue dest = (Queue) ((RegionBroker) broker.getRegionBroker()).getQueueRegion().getDestinationMap().get(new ActiveMQQueue(destination));
        dest.purge();
        assertEquals(0, dest.getDestinationStatistics().getMessages().getCount());
    }
}
