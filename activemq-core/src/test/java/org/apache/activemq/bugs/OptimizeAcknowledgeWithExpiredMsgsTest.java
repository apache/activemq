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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for AMQ-3965.
 * A consumer may be stalled in case it uses optimizeAcknowledge and receives
 * a number of messages that expire before being dispatched to application code.
 * See for more details.
 *
 */
public class OptimizeAcknowledgeWithExpiredMsgsTest {

    private final static Logger LOG = LoggerFactory.getLogger(OptimizeAcknowledgeWithExpiredMsgsTest.class);

    private BrokerService broker = null;

    private String connectionUri;

    /**
     * Creates a broker instance but does not start it.
     *
     * @param brokerUri - transport uri of broker
     * @param brokerName - name for the broker
     * @return a BrokerService instance with transport uri and broker name set
     * @throws Exception
     */
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setUseJmx(false);
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();
        return broker;
    }

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    /**
     * Tests for AMQ-3965
     * Creates connection into broker using optimzeAcknowledge and prefetch=100
     * Creates producer and consumer. Producer sends 45 msgs that will expire
     * at consumer (but before being dispatched to app code).
     * Producer then sends 60 msgs without expiry.
     *
     * Consumer receives msgs using a MessageListener and increments a counter.
     * Main thread sleeps for 5 seconds and checks the counter value.
     * If counter != 60 msgs (the number of msgs that should get dispatched
     * to consumer) the test fails.
     */
    @Test
    public void testOptimizedAckWithExpiredMsgs() throws Exception
    {
        ActiveMQConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(connectionUri + "?jms.optimizeAcknowledge=true&jms.prefetchPolicy.all=100");

        // Create JMS resources
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("TEST.FOO");

        // ***** Consumer code *****
        MessageConsumer consumer = session.createConsumer(destination);

        final MyMessageListener listener = new MyMessageListener();
        connection.setExceptionListener((ExceptionListener) listener);

        // ***** Producer Code *****
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
        TextMessage message;

        // Produce msgs that will expire quickly
        for (int i=0; i<45; i++) {
            message = session.createTextMessage(text);
            producer.send(message,1,1,100);
            LOG.trace("Sent message: "+ message.getJMSMessageID() +
                " with expiry 10 msec");
        }
        // Produce msgs that don't expire
        for (int i=0; i<60; i++) {
            message = session.createTextMessage(text);
            producer.send(message,1,1,60000);
            // producer.send(message);
            LOG.trace("Sent message: "+ message.getJMSMessageID() +
                " with expiry 30 sec");
        }
        consumer.setMessageListener(listener);

        sleep(1000);  // let the batch of 45 expire.

        connection.start();

        assertTrue("Should receive all expected messages, counter at " + listener.getCounter(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return listener.getCounter() == 60;
            }
        }));

        LOG.info("Received all expected messages with counter at: " + listener.getCounter());

        // Cleanup
        producer.close();
        consumer.close();
        session.close();
        connection.close();
    }

    @Test
    public void testOptimizedAckWithExpiredMsgsSync() throws Exception
    {
        ActiveMQConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(connectionUri + "?jms.optimizeAcknowledge=true&jms.prefetchPolicy.all=100");

        // Create JMS resources
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("TEST.FOO");

        // ***** Consumer code *****
        MessageConsumer consumer = session.createConsumer(destination);

        // ***** Producer Code *****
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
        TextMessage message;

        // Produce msgs that will expire quickly
        for (int i=0; i<45; i++) {
            message = session.createTextMessage(text);
            producer.send(message,1,1,10);
            LOG.trace("Sent message: "+ message.getJMSMessageID() +
                " with expiry 10 msec");
        }
        // Produce msgs that don't expire
        for (int i=0; i<60; i++) {
            message = session.createTextMessage(text);
            producer.send(message,1,1,30000);
            // producer.send(message);
            LOG.trace("Sent message: "+ message.getJMSMessageID() +
                " with expiry 30 sec");
        }
        sleep(200);

        int counter = 1;
        for (; counter <= 60; ++counter) {
            assertNotNull(consumer.receive(2000));
            LOG.info("counter at " + counter);
        }
        LOG.info("Received all expected messages with counter at: " + counter);

        // Cleanup
        producer.close();
        consumer.close();
        session.close();
        connection.close();
    }

    @Test
    public void testOptimizedAckWithExpiredMsgsSync2() throws Exception
    {
        ActiveMQConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(connectionUri + "?jms.optimizeAcknowledge=true&jms.prefetchPolicy.all=100");

        // Create JMS resources
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("TEST.FOO");

        // ***** Consumer code *****
        MessageConsumer consumer = session.createConsumer(destination);

        // ***** Producer Code *****
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
        TextMessage message;

        // Produce msgs that don't expire
        for (int i=0; i<56; i++) {
            message = session.createTextMessage(text);
            producer.send(message,1,1,30000);
            // producer.send(message);
            LOG.trace("Sent message: "+ message.getJMSMessageID() +
                " with expiry 30 sec");
        }
        // Produce msgs that will expire quickly
        for (int i=0; i<44; i++) {
            message = session.createTextMessage(text);
            producer.send(message,1,1,10);
            LOG.trace("Sent message: "+ message.getJMSMessageID() +
                " with expiry 10 msec");
        }
        // Produce some moremsgs that don't expire
        for (int i=0; i<4; i++) {
            message = session.createTextMessage(text);
            producer.send(message,1,1,30000);
            // producer.send(message);
            LOG.trace("Sent message: "+ message.getJMSMessageID() +
                " with expiry 30 sec");
        }

        sleep(200);

        int counter = 1;
        for (; counter <= 60; ++counter) {
            assertNotNull(consumer.receive(2000));
            LOG.info("counter at " + counter);
        }
        LOG.info("Received all expected messages with counter at: " + counter);

        // Cleanup
        producer.close();
        consumer.close();
        session.close();
        connection.close();
    }

    private void sleep(int milliSecondTime) {
        try {
            Thread.sleep(milliSecondTime);
        } catch (InterruptedException igonred) {
        }
    }

    /**
     * Standard JMS MessageListener
     */
    private class MyMessageListener implements MessageListener, ExceptionListener {

        private AtomicInteger counter = new AtomicInteger(0);

        public void onMessage(final Message message) {
            try {
                LOG.trace("Got Message " + message.getJMSMessageID());
                LOG.info("counter at " + counter.incrementAndGet());
            } catch (final Exception e) {
            }
        }

        public int getCounter() {
            return counter.get();
        }

        public synchronized void onException(JMSException ex) {
            LOG.error("JMS Exception occured.  Shutting down client.");
        }
    }
}
