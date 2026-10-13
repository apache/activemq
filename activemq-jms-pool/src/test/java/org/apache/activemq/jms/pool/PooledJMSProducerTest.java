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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import jakarta.jms.CompletionListener;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PooledJMSProducerTest extends JmsPoolTestSupport {

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
    public void testSendTextMessage() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.producer.text");
            context.createProducer().send(queue, "hello world");

            try (var consumer = context.createConsumer(queue)) {
                var body = consumer.receiveBody(String.class, 5000);
                assertEquals("hello world", body);
            }
        }
    }

    @Test(timeout = 60000)
    public void testSendMapMessage() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.producer.map");
            // Deliberately not 'var': HashMap is both Map and Serializable, and the
            // declared Map type selects the send(Destination, Map) overload.
            Map<String, Object> body = new HashMap<>();
            body.put("key1", "value1");
            body.put("key2", 42);
            context.createProducer().send(queue, body);

            try (var consumer = context.createConsumer(queue)) {
                var msg = consumer.receive(5000);
                assertNotNull(msg);
                assertTrue(msg instanceof jakarta.jms.MapMessage);
                assertEquals("value1", ((jakarta.jms.MapMessage) msg).getString("key1"));
                assertEquals(42, ((jakarta.jms.MapMessage) msg).getInt("key2"));
            }
        }
    }

    @Test(timeout = 60000)
    public void testSendBytesMessage() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.producer.bytes");
            var payload = new byte[]{1, 2, 3, 4, 5};
            context.createProducer().send(queue, payload);

            try (var consumer = context.createConsumer(queue)) {
                var msg = consumer.receive(5000);
                assertNotNull(msg);
                assertTrue(msg instanceof jakarta.jms.BytesMessage);
            }
        }
    }

    @Test(timeout = 60000)
    public void testSendMessageObject() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.producer.message");
            var msg = context.createTextMessage("direct message");
            context.createProducer().send(queue, msg);

            try (var consumer = context.createConsumer(queue)) {
                var body = consumer.receiveBody(String.class, 5000);
                assertEquals("direct message", body);
            }
        }
    }

    @Test(timeout = 60000)
    public void testFluentQoSSetters() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer();

            var returned = producer
                .setDeliveryMode(DeliveryMode.NON_PERSISTENT)
                .setPriority(7)
                .setTimeToLive(30000)
                .setDeliveryDelay(1000)
                .setDisableMessageID(true)
                .setDisableMessageTimestamp(true);

            assertEquals(producer, returned);
            assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());
            assertEquals(7, producer.getPriority());
            assertEquals(30000, producer.getTimeToLive());
            assertEquals(1000, producer.getDeliveryDelay());
            assertTrue(producer.getDisableMessageID());
            assertTrue(producer.getDisableMessageTimestamp());
        }
    }

    @Test(timeout = 60000)
    public void testDefaultQoSValues() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer();

            assertEquals(DeliveryMode.PERSISTENT, producer.getDeliveryMode());
            assertEquals(4, producer.getPriority());
            assertEquals(0, producer.getTimeToLive());
            assertEquals(0, producer.getDeliveryDelay());
            assertFalse(producer.getDisableMessageID());
            assertFalse(producer.getDisableMessageTimestamp());
            assertNull(producer.getAsync());
        }
    }

    @Test(timeout = 60000, expected = JMSRuntimeException.class)
    public void testSetInvalidDeliveryMode() throws Exception {
        try (var context = pooledFactory.createContext()) {
            context.createProducer().setDeliveryMode(99);
        }
    }

    @Test(timeout = 60000, expected = JMSRuntimeException.class)
    public void testSetInvalidPriority() throws Exception {
        try (var context = pooledFactory.createContext()) {
            context.createProducer().setPriority(10);
        }
    }

    @Test(timeout = 60000, expected = MessageFormatRuntimeException.class)
    public void testSendNullMessage() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.producer.null");
            context.createProducer().send(queue, (Message) null);
        }
    }

    @Test(timeout = 60000)
    public void testJMSHeaders() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.producer.headers");
            var replyTo = context.createQueue("test.producer.replyto");

            context.createProducer()
                .setJMSCorrelationID("corr-123")
                .setJMSType("myType")
                .setJMSReplyTo(replyTo)
                .send(queue, "with headers");

            try (var consumer = context.createConsumer(queue)) {
                var msg = consumer.receive(5000);
                assertNotNull(msg);
                assertEquals("corr-123", msg.getJMSCorrelationID());
                assertEquals("myType", msg.getJMSType());
                assertNotNull(msg.getJMSReplyTo());
            }
        }
    }

    @Test(timeout = 60000)
    public void testMessageProperties() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.producer.props");

            var producer = context.createProducer()
                .setProperty("strProp", "hello")
                .setProperty("intProp", 42)
                .setProperty("boolProp", true)
                .setProperty("longProp", 100L)
                .setProperty("doubleProp", 3.14);

            assertTrue(producer.propertyExists("strProp"));
            assertFalse(producer.propertyExists("nonexistent"));
            assertEquals("hello", producer.getStringProperty("strProp"));
            assertEquals(42, producer.getIntProperty("intProp"));
            assertTrue(producer.getBooleanProperty("boolProp"));
            assertEquals(100L, producer.getLongProperty("longProp"));
            assertEquals(3.14, producer.getDoubleProperty("doubleProp"), 0.001);

            producer.send(queue, "with props");

            try (var consumer = context.createConsumer(queue)) {
                var msg = consumer.receive(5000);
                assertNotNull(msg);
                assertEquals("hello", msg.getStringProperty("strProp"));
                assertEquals(42, msg.getIntProperty("intProp"));
                assertTrue(msg.getBooleanProperty("boolProp"));
            }
        }
    }

    @Test(timeout = 60000)
    public void testClearProperties() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer()
                .setProperty("key", "value");
            assertTrue(producer.propertyExists("key"));

            producer.clearProperties();
            assertFalse(producer.propertyExists("key"));
        }
    }

    @Test(timeout = 60000)
    public void testGetPropertyNames() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer()
                .setProperty("a", 1)
                .setProperty("b", 2);
            var names = producer.getPropertyNames();
            assertEquals(2, names.size());
            assertTrue(names.contains("a"));
            assertTrue(names.contains("b"));
        }
    }

    @Test(timeout = 60000)
    public void testGetPropertyNamesEmpty() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer();
            var names = producer.getPropertyNames();
            assertTrue(names.isEmpty());
        }
    }

    @Test(timeout = 60000)
    public void testGetPropertyNamesIsUnmodifiableLiveView() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer();

            // a set obtained before any property is set must be a live view
            // backed by the producer (Jakarta Messaging 3.1 JMSProducer#getPropertyNames)
            var names = producer.getPropertyNames();
            assertTrue(names.isEmpty());
            producer.setProperty("added", 1);
            assertTrue("Returned set must be a live view backed by the producer", names.contains("added"));

            // mutation attempts must throw, directly and via the iterator
            try {
                names.remove("added");
                fail("Expected UnsupportedOperationException from direct mutation");
            } catch (UnsupportedOperationException expected) {
            }
            try {
                var iterator = names.iterator();
                iterator.next();
                iterator.remove();
                fail("Expected UnsupportedOperationException from iterator mutation");
            } catch (UnsupportedOperationException expected) {
            }

            // producer state must be untouched by the rejected mutations
            assertTrue(producer.propertyExists("added"));

            // the view stays live through clearProperties
            producer.clearProperties();
            assertTrue(names.isEmpty());
        }
    }

    @Test(timeout = 60000)
    public void testCorrelationIDAsBytes() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var corrId = new byte[]{10, 20, 30};
            var producer = context.createProducer()
                .setJMSCorrelationIDAsBytes(corrId);
            var result = producer.getJMSCorrelationIDAsBytes();
            assertEquals(3, result.length);
            assertEquals(10, result[0]);
        }
    }

    @Test(timeout = 60000)
    public void testPropertyStringConversion() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer()
                .setProperty("num", "42");
            assertEquals(42, producer.getIntProperty("num"));
            assertEquals(42L, producer.getLongProperty("num"));
        }
    }

    @Test(timeout = 60000)
    public void testNumericPropertyWidening() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer()
                .setProperty("byteProp", (byte) 5)
                .setProperty("shortProp", (short) 6)
                .setProperty("intProp", 7)
                .setProperty("floatProp", 1.5f);

            assertEquals(5, producer.getShortProperty("byteProp"));
            assertEquals(5, producer.getIntProperty("byteProp"));
            assertEquals(5L, producer.getLongProperty("byteProp"));
            assertEquals(6, producer.getIntProperty("shortProp"));
            assertEquals(6L, producer.getLongProperty("shortProp"));
            assertEquals(7L, producer.getLongProperty("intProp"));
            assertEquals(1.5d, producer.getDoubleProperty("floatProp"), 0.0);
        }
    }

    @Test(timeout = 60000)
    public void testInvalidPropertyConversions() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer().setProperty("intProp", 7);

            try {
                producer.getByteProperty("intProp"); // narrowing is not a valid conversion
                fail("Expected MessageFormatRuntimeException");
            } catch (MessageFormatRuntimeException expected) {
            }

            try {
                producer.getIntProperty("missing"); // null numeric read
                fail("Expected NumberFormatException");
            } catch (NumberFormatException expected) {
            }

            try {
                producer.getFloatProperty("missing"); // null float read
                fail("Expected NullPointerException");
            } catch (NullPointerException expected) {
            }

            assertFalse(producer.getBooleanProperty("missing"));
            assertNull(producer.getStringProperty("missing"));
            assertEquals("7", producer.getStringProperty("intProp"));
        }
    }

    @Test(timeout = 60000)
    public void testProducerUseAfterContextCloseThrows() throws Exception {
        var context = pooledFactory.createContext();
        var queue = context.createQueue("test.producer.use.after.close");
        var producer = context.createProducer();
        var prebuilt = context.createTextMessage("prebuilt");
        producer.send(queue, "before close");
        context.close();

        // locally stored producer state remains readable after the context closes
        assertEquals(DeliveryMode.PERSISTENT, producer.getDeliveryMode());

        try {
            producer.send(queue, "after close");
            fail("Expected IllegalStateRuntimeException sending through a closed context's producer");
        } catch (jakarta.jms.IllegalStateRuntimeException expected) {
        }

        // the pre-built message path must not silently send on the pooled session
        try {
            producer.send(queue, prebuilt);
            fail("Expected IllegalStateRuntimeException sending a pre-built message through a closed context's producer");
        } catch (jakarta.jms.IllegalStateRuntimeException expected) {
        }
    }

    @Test(timeout = 60000)
    public void testAsyncSendPassesListenerToProvider() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.producer.async");
            var producer = context.createProducer();

            var listener = new CompletionListener() {
                @Override
                public void onCompletion(Message message) {
                }

                @Override
                public void onException(Message message, Exception exception) {
                }
            };

            assertSame(producer, producer.setAsync(listener));
            assertSame(listener, producer.getAsync());

            try {
                producer.send(queue, "async message");
                fail("Expected UnsupportedOperationException from ActiveMQ Classic client");
            } catch (UnsupportedOperationException expected) {
            }

            // clearing the listener restores synchronous sends
            producer.setAsync(null);
            producer.send(queue, "sync message");
            try (var consumer = context.createConsumer(queue)) {
                assertEquals("sync message", consumer.receiveBody(String.class, 5000));
            }
        }
    }

    @Test(timeout = 60000)
    public void testSetPropertyNameValidation() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer();

            try {
                producer.setProperty(null, "value");
                fail("Expected IllegalArgumentException for null property name");
            } catch (IllegalArgumentException expected) {
            }

            try {
                producer.setProperty("", 1);
                fail("Expected IllegalArgumentException for empty property name");
            } catch (IllegalArgumentException expected) {
            }
        }
    }
}
