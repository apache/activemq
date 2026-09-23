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
package org.apache.activemq.jms2;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.Queue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * JMS 2.0 delivery delay is implemented by the scheduler broker, which the
 * embedded TCK broker runs without persistence. This pins the in-memory job
 * scheduler path: a non-persistent broker with schedulerSupport enabled must
 * hold a delayed message for the full delay.
 */
public class DeliveryDelayNonPersistentBrokerTest {

    private static final long DELIVERY_DELAY_MS = 3000;

    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(true);
        broker.addConnector("vm://localhost");
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testDelayedMessagesHeldWithPrefetchOneAcrossDeliveryModes() throws Exception {
        // TCK jmsproducer deliveryDelayTest shape: prefetch 1, a message property,
        // a PERSISTENT send followed by a NON_PERSISTENT send on the same producer,
        // each expected to be invisible to receive() until the delay elapses
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.getPrefetchPolicy().setAll(0);
        factory.getPrefetchPolicy().setQueuePrefetch(1);
        factory.getPrefetchPolicy().setTopicPrefetch(1);

        try (JMSContext context = factory.createContext()) {
            Queue queue = context.createQueue("test.delivery.delay.prefetch.one");
            var consumer = context.createConsumer(queue);
            var producer = context.createProducer().setDeliveryDelay(DELIVERY_DELAY_MS);

            for (int deliveryMode : new int[] {jakarta.jms.DeliveryMode.PERSISTENT, jakarta.jms.DeliveryMode.NON_PERSISTENT}) {
                producer.setDeliveryMode(deliveryMode);
                var message = context.createTextMessage("delayed " + deliveryMode);
                message.setStringProperty("COM_SUN_JMS_TESTNAME", "deliveryDelayTest");
                producer.send(queue, message);

                assertNull("Mode " + deliveryMode + ": message must not be delivered before the delay elapses",
                    consumer.receive(DELIVERY_DELAY_MS / 2));
                assertNotNull("Mode " + deliveryMode + ": message must be delivered after the delay",
                    consumer.receive(DELIVERY_DELAY_MS * 3));
            }
        }
    }

    @Test(timeout = 60000)
    public void testResentDelayedMessageIsDelayedAgain() throws Exception {
        // TCK deliveryDelayTest re-sends the received message object for its second
        // leg. The delivered copy carries the scheduler's scheduledJobId marker, which
        // must not suppress the fresh delay the producer applies on the re-send.
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        try (JMSContext context = factory.createContext()) {
            Queue queue = context.createQueue("test.delivery.delay.resend");
            var consumer = context.createConsumer(queue);
            var producer = context.createProducer().setDeliveryDelay(DELIVERY_DELAY_MS);

            producer.send(queue, "first");
            Message received = consumer.receive(DELIVERY_DELAY_MS * 3);
            assertNotNull(received);

            producer.setDeliveryMode(jakarta.jms.DeliveryMode.NON_PERSISTENT);
            producer.send(queue, received);
            assertNull("Re-sent message must honor the producer's delivery delay",
                consumer.receive(DELIVERY_DELAY_MS / 2));
            assertNotNull("Re-sent message must arrive after the delay",
                consumer.receive(DELIVERY_DELAY_MS * 3));
        }
    }

    @Test(timeout = 60000)
    public void testDelayedMessageIsHeldForTheDelay() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        try (JMSContext context = factory.createContext()) {
            Queue queue = context.createQueue("test.delivery.delay.nonpersistent");
            var consumer = context.createConsumer(queue);

            long sentAt = System.currentTimeMillis();
            context.createProducer().setDeliveryDelay(DELIVERY_DELAY_MS).send(queue, "delayed");

            assertNull("Message must not be delivered before the delivery delay elapses",
                consumer.receive(DELIVERY_DELAY_MS / 2));

            Message received = consumer.receive(DELIVERY_DELAY_MS * 3);
            assertNotNull("Delayed message must be delivered after the delay", received);
            assertTrue("Delivered before the delay elapsed",
                System.currentTimeMillis() - sentAt >= DELIVERY_DELAY_MS);
            assertTrue("JMSDeliveryTime must reflect the delay",
                received.getJMSDeliveryTime() >= sentAt + DELIVERY_DELAY_MS);
        }
    }
}
