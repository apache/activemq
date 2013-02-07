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
package org.apache.activemq.usecases;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;

/**
 * Test case intended to demonstrate delivery interruption to queue consumers when a JMS selector leaves some messages
 * on the queue (due to use of a JMS Selector)
 *
 * testNonDiscriminatingConsumer() demonstrates proper functionality for consumers that don't use a selector to qualify
 * their input.
 *
 * testDiscriminatingConsumer() demonstrates the failure condition in which delivery to the consumer eventually halts.
 *
 * The expected behavior is for the delivery to the client to be maintained regardless of the depth of the queue,
 * particularly when the messages in the queue do not meet the selector criteria of the client.
 *
 * https://issues.apache.org/activemq/browse/AMQ-2217
 *
 */
public class DiscriminatingConsumerLoadTest extends TestSupport {

    private static final org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory.getLog(DiscriminatingConsumerLoadTest.class);

    private Connection producerConnection;
    private Connection consumerConnection;

    public static final String JMSTYPE_EATME = "DiscriminatingLoadClient.EatMe";
    public static final String JMSTYPE_IGNOREME = "DiscriminatingLoadClient.IgnoreMe";

    private final int testSize = 5000; // setting this to a small number will pass all tests

    BrokerService broker;

    @Override
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);

        // workaround is to ensure sufficient dispatch buffer for the destination
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultPolicy = new PolicyEntry();
        defaultPolicy.setMaxPageSize(testSize);
        policyMap.setDefaultEntry(defaultPolicy);
        broker.setDestinationPolicy(policyMap);
        broker.start();

        super.setUp();
        this.producerConnection = this.createConnection();
        this.consumerConnection = this.createConnection();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        if (producerConnection != null) {
            producerConnection.close();
            producerConnection = null;
        }
        if (consumerConnection != null) {
            consumerConnection.close();
            consumerConnection = null;
        }
        super.tearDown();
        broker.stop();
    }

    /**
     * Test to check if a single consumer with no JMS selector will receive all intended messages
     *
     * @throws java.lang.Exception
     */
    public void testNonDiscriminatingConsumer() throws Exception {

        consumerConnection = createConnection();
        consumerConnection.start();
        LOG.info("consumerConnection = " + consumerConnection);

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }

        // here we pass in null for the JMS selector
        Consumer consumer = new Consumer(consumerConnection, null);
        Thread consumerThread = new Thread(consumer);

        consumerThread.start();

        producerConnection = createConnection();
        producerConnection.start();
        LOG.info("producerConnection = " + producerConnection);

        try {
            Thread.sleep(3000);
        } catch (Exception e) {
        }

        Producer producer = new Producer(producerConnection);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        // now that everything is running, let's wait for the consumer thread to finish ...
        consumerThread.join();
        producer.stop = true;

        if (consumer.getCount() == testSize)
            LOG.info("test complete .... all messsages consumed!!");
        else
            LOG.info("test failed .... Sent " + (testSize / 1) + " messages intended to be consumed ( " + testSize + " total), but only consumed "
                + consumer.getCount());

        assertTrue("Sent " + testSize + " messages intended to be consumed, but only consumed " + consumer.getCount(), (consumer.getCount() == testSize));
        assertFalse("Delivery of messages to consumer was halted during this test", consumer.deliveryHalted());
    }

    /**
     * Test to check if a single consumer with a JMS selector will receive all intended messages
     *
     * @throws java.lang.Exception
     */
    public void testDiscriminatingConsumer() throws Exception {

        consumerConnection = createConnection();
        consumerConnection.start();
        LOG.info("consumerConnection = " + consumerConnection);

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }

        // here we pass the JMS selector we intend to consume
        Consumer consumer = new Consumer(consumerConnection, JMSTYPE_EATME);
        Thread consumerThread = new Thread(consumer);

        consumerThread.start();

        producerConnection = createConnection();
        producerConnection.start();
        LOG.info("producerConnection = " + producerConnection);

        try {
            Thread.sleep(3000);
        } catch (Exception e) {
        }

        Producer producer = new Producer(producerConnection);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        // now that everything is running, let's wait for the consumer thread to finish ...
        consumerThread.join();
        producer.stop = true;

        if (consumer.getCount() == (testSize / 2)) {
            LOG.info("test complete .... all messsages consumed!!");
        } else {
            LOG.info("test failed .... Sent " + testSize + " original messages, only half of which (" + (testSize / 2)
                + ") were intended to be consumed: consumer paused at: " + consumer.getCount());
            // System.out.println("test failed .... Sent " + testSize + " original messages, only half of which (" +
            // (testSize / 2) +
            // ") were intended to be consumed: consumer paused at: " + consumer.getCount());

        }

        assertTrue("Sent " + testSize + " original messages, only half of which (" + (testSize / 2) + ") were intended to be consumed: consumer paused at: "
            + consumer.getCount(), (consumer.getCount() == (testSize / 2)));
        assertTrue("Delivery of messages to consumer was halted during this test as it only wants half", consumer.deliveryHalted());
    }

    /**
     * Helper class that will publish 2 * testSize messages. The messages will be distributed evenly between the
     * following two JMS types:
     *
     * @see JMSTYPE_INTENDED_FOR_CONSUMPTION
     * @see JMSTYPE_NOT_INTENDED_FOR_CONSUMPTION
     *
     */
    private class Producer extends Thread {
        private int counterSent = 0;
        private Connection connection = null;
        public boolean stop = false;

        public Producer(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final Queue queue = session.createQueue("test");

                // wait for 10 seconds to allow consumer.receive to be run
                // first
                Thread.sleep(10000);
                MessageProducer producer = session.createProducer(queue);

                while (!stop && (counterSent < testSize)) {
                    // first send a message intended to be consumed ....
                    TextMessage message = session.createTextMessage("*** Ill ....... Ini ***"); // alma mater ...
                    message.setJMSType(JMSTYPE_EATME);
                    // LOG.info("sending .... JMSType = " + message.getJMSType());
                    producer.send(message, DeliveryMode.NON_PERSISTENT, 0, 1800000);

                    counterSent++;

                    // now send a message intended to be consumed by some other consumer in the the future
                    // ... we expect these messages to accrue in the queue
                    message = session.createTextMessage("*** Ill ....... Ini ***"); // alma mater ...
                    message.setJMSType(JMSTYPE_IGNOREME);
                    // LOG.info("sending .... JMSType = " + message.getJMSType());
                    producer.send(message, DeliveryMode.NON_PERSISTENT, 0, 1800000);

                    counterSent++;
                }

                session.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
            LOG.info("producer thread complete ... " + counterSent + " messages sent to the queue");
        }
    }

    /**
     * Helper class that will consume messages from the queue based on the supplied JMS selector. Thread will stop after
     * the first receive(..) timeout, or once all expected messages have been received (see testSize). If the thread
     * stops due to a timeout, it is experiencing the delivery pause that is symptomatic of a bug in the broker.
     *
     */
    private class Consumer extends Thread {
        protected int counterReceived = 0;
        private String jmsSelector = null;
        private boolean deliveryHalted = false;

        public Consumer(Connection connection, String jmsSelector) {
            this.jmsSelector = jmsSelector;
        }

        @Override
        public void run() {
            try {
                Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final Queue queue = session.createQueue("test");
                MessageConsumer consumer = null;
                if (null != this.jmsSelector) {
                    consumer = session.createConsumer(queue, "JMSType='" + this.jmsSelector + "'");
                } else {
                    consumer = session.createConsumer(queue);
                }

                while (!deliveryHalted && (counterReceived < testSize)) {
                    TextMessage result = (TextMessage) consumer.receive(30000);
                    if (result != null) {
                        counterReceived++;
                        // System.out.println("consuming .... JMSType = " + result.getJMSType() + " received = " +
                        // counterReceived);
                        LOG.info("consuming .... JMSType = " + result.getJMSType() + " received = " + counterReceived);
                    } else {
                        LOG.info("consuming .... timeout while waiting for a message ... broker must have stopped delivery ...  received = " + counterReceived);
                        deliveryHalted = true;
                    }
                }
                session.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        public int getCount() {
            return this.counterReceived;
        }

        public boolean deliveryHalted() {
            return this.deliveryHalted;
        }
    }

}
