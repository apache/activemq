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
package org.apache.activemq.jms.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PooledJMSConsumerTest extends JmsPoolTestSupport {

    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;
    private String connectionUri;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        var connector = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        connectionUri = connector.getPublishableConnectString();
        factory = new ActiveMQConnectionFactory(connectionUri);
        pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);
        pooledFactory.setMaxConnections(1);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            pooledFactory.stop();
        } catch (Exception ex) {
            // ignored
        }
        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testReceive() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.consumer.receive");
            context.createProducer().send(queue, "hello");

            try (var consumer = context.createConsumer(queue)) {
                var msg = consumer.receive(5000);
                assertNotNull("Should have received a message", msg);
            }
        }
    }

    @Test(timeout = 60000)
    public void testReceiveNoWaitReturnsNull() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.consumer.nowait");

            try (var consumer = context.createConsumer(queue)) {
                var msg = consumer.receiveNoWait();
                assertNull("Should not have received a message", msg);
            }
        }
    }

    @Test(timeout = 60000)
    public void testReceiveBody() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.consumer.body");
            context.createProducer().send(queue, "payload");

            try (var consumer = context.createConsumer(queue)) {
                var body = consumer.receiveBody(String.class, 5000);
                assertEquals("payload", body);
            }
        }
    }

    @Test(timeout = 60000)
    public void testReceiveBodyNoWaitReturnsNull() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.consumer.body.nowait");

            try (var consumer = context.createConsumer(queue)) {
                var body = consumer.receiveBodyNoWait(String.class);
                assertNull(body);
            }
        }
    }

    @Test(timeout = 60000)
    public void testClose() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.consumer.close");
            var consumer = context.createConsumer(queue);
            consumer.close();
        }
    }

    @Test(timeout = 60000)
    public void testMessageListenerDelivery() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.consumer.listener");
            var consumer = context.createConsumer(queue);

            final var delivered = new CountDownLatch(3);
            final var bodies = Collections.synchronizedList(new ArrayList<String>());
            MessageListener listener = message -> {
                try {
                    bodies.add(((TextMessage) message).getText());
                } catch (JMSException e) {
                    // body stays missing and the containsAll assert below fails
                }
                delivered.countDown();
            };

            consumer.setMessageListener(listener);
            assertSame(listener, consumer.getMessageListener());

            var producer = context.createProducer();
            producer.send(queue, "one");
            producer.send(queue, "two");
            producer.send(queue, "three");

            assertTrue("Listener should have received all messages",
                delivered.await(10, TimeUnit.SECONDS));
            assertTrue("Listener should have seen all bodies: " + bodies,
                bodies.containsAll(Arrays.asList("one", "two", "three")));
            consumer.close();
        }
    }

    @Test(timeout = 60000)
    public void testMessageListenerDeliveryNotTrackedForAcknowledge() throws Exception {
        // Listener-delivered messages are not tracked by the context's acknowledge()
        // support (documented limitation), so without an explicit ack on the message
        // itself the delivery stays unacknowledged and the broker redelivers it.
        try (var context = pooledFactory.createContext(Session.CLIENT_ACKNOWLEDGE)) {
            var queue = context.createQueue("test.consumer.listener.ack");
            context.createProducer().send(queue, "needs explicit ack");

            var consumer = context.createConsumer(queue);
            final var delivered = new CountDownLatch(1);
            consumer.setMessageListener(message -> delivered.countDown());
            assertTrue(delivered.await(10, TimeUnit.SECONDS));

            // no-op for listener-delivered messages: the context never saw them
            context.acknowledge();
            consumer.close();
        }

        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.consumer.listener.ack");
            try (var consumer = context.createConsumer(queue)) {
                var redelivered = consumer.receive(5000);
                assertNotNull("Unacknowledged listener-delivered message should be redelivered", redelivered);
                assertTrue(redelivered.getJMSRedelivered());
            }
        }
    }

    @Test(timeout = 60000)
    public void testMessageListenerExplicitMessageAcknowledge() throws Exception {
        // The documented workaround: acknowledge the delivered Message directly
        try (var context = pooledFactory.createContext(Session.CLIENT_ACKNOWLEDGE)) {
            var queue = context.createQueue("test.consumer.listener.msgack");
            context.createProducer().send(queue, "ack in listener");

            var consumer = context.createConsumer(queue);
            final var acked = new CountDownLatch(1);
            consumer.setMessageListener(message -> {
                try {
                    message.acknowledge();
                    acked.countDown();
                } catch (JMSException e) {
                    // latch never counts down and the await below fails
                }
            });
            assertTrue("Listener should have acknowledged the message",
                acked.await(10, TimeUnit.SECONDS));
            consumer.close();
        }

        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.consumer.listener.msgack");
            try (var consumer = context.createConsumer(queue)) {
                assertNull("Acknowledged message must not be redelivered", consumer.receive(1000));
            }
        }
    }

    @Test(timeout = 60000)
    public void testConsumerUseAfterCloseThrows() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.consumer.use.after.close");
            var consumer = context.createConsumer(queue);
            consumer.close();

            // close is idempotent
            consumer.close();

            try {
                consumer.receive(100);
                fail("Expected IllegalStateRuntimeException from receive on closed consumer");
            } catch (IllegalStateRuntimeException expected) {
            }

            try {
                consumer.receiveNoWait();
                fail("Expected IllegalStateRuntimeException from receiveNoWait on closed consumer");
            } catch (IllegalStateRuntimeException expected) {
            }

            try {
                consumer.receiveBodyNoWait(String.class);
                fail("Expected IllegalStateRuntimeException from receiveBodyNoWait on closed consumer");
            } catch (IllegalStateRuntimeException expected) {
            }

            try {
                consumer.getMessageSelector();
                fail("Expected IllegalStateRuntimeException from getMessageSelector on closed consumer");
            } catch (IllegalStateRuntimeException expected) {
            }
        }
    }

}
