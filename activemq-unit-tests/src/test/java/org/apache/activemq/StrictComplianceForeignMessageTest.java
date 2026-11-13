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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A message from another provider lists the JMSX properties that provider
 * set. A strict connection refuses those when a client sets them, so the
 * conversion to an ActiveMQ message has to leave them out instead of failing.
 */
public class StrictComplianceForeignMessageTest {

    private static final String QUEUE_NAME = "test.strict.foreign";
    private static final List<String> STORED_PROVIDER_PROPERTIES =
        List.of("JMSXRcvTimestamp", "JMSXState", "JMSXProducerTXID", "JMSXConsumerTXID");

    private BrokerService broker;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("vm://localhost");
        broker.start();
        broker.waitUntilStarted();
        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testStrictConnectionSendsForeignMessage() throws Exception {
        try (var connection = createConnection(true)) {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var queue = session.createQueue(QUEUE_NAME);
            final var consumer = session.createConsumer(queue);

            session.createProducer(queue).send(foreignMessage());

            final var received = consumer.receive(5000);
            assertNotNull(received);
            assertEquals("red", received.getStringProperty("color"));
            assertEquals("group-a", received.getStringProperty("JMSXGroupID"));
            assertEquals("the other provider's delivery count is not carried over",
                1, received.getIntProperty("JMSXDeliveryCount"));
            assertFalse(received.propertyExists("JMSXState"));
        }
    }

    // on the way in the values come from a provider, so a strict connection keeps them
    @Test
    public void testStrictConnectionKeepsProviderPropertiesFromConsumerTransformer() throws Exception {
        try (var connection = createConnection(true)) {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var queue = session.createQueue(QUEUE_NAME);
            final var consumer = (ActiveMQMessageConsumer) session.createConsumer(queue);
            consumer.setTransformer(new MessageTransformer() {
                @Override
                public Message producerTransform(Session session, MessageProducer producer, Message message) {
                    return message;
                }

                @Override
                public Message consumerTransform(Session session, MessageConsumer consumer, Message message) {
                    return foreignMessage();
                }
            });

            session.createProducer(queue).send(session.createMessage());

            final var received = (ActiveMQMessage) consumer.receive(5000);
            assertNotNull(received);
            assertProviderPropertiesCopied(received);
        }
    }

    @Test
    public void testTransformFollowsConnection() throws Exception {
        try (var strict = createConnection(true); var lenient = createConnection(false)) {
            assertProviderPropertiesSkipped(ActiveMQMessageTransformation.transformMessage(foreignMessage(), strict));
            assertProviderPropertiesCopied(ActiveMQMessageTransformation.transformMessage(foreignMessage(), lenient));
        }
    }

    @Test
    public void testTransformOverrideCopiesProviderPropertiesOnStrictConnection() throws Exception {
        try (var strict = createConnection(true); var lenient = createConnection(false)) {
            assertProviderPropertiesCopied(
                ActiveMQMessageTransformation.transformMessage(foreignMessage(), strict, true));
            assertProviderPropertiesSkipped(
                ActiveMQMessageTransformation.transformMessage(foreignMessage(), strict, false));

            // the override has nothing to do on a connection that is not strict
            assertProviderPropertiesCopied(
                ActiveMQMessageTransformation.transformMessage(foreignMessage(), lenient, true));
            assertProviderPropertiesCopied(
                ActiveMQMessageTransformation.transformMessage(foreignMessage(), lenient, false));
        }
    }

    @Test
    public void testTransformWithoutConnectionCopiesProviderProperties() throws Exception {
        assertProviderPropertiesCopied(ActiveMQMessageTransformation.transformMessage(foreignMessage(), null));
    }

    @Test
    public void testCopyPropertiesFollowsConnectionOfTarget() throws Exception {
        try (var strict = createConnection(true)) {
            final var followsConnection = new ActiveMQMessage();
            followsConnection.setConnection(strict);
            ActiveMQMessageTransformation.copyProperties(foreignMessage(), followsConnection);
            assertProviderPropertiesSkipped(followsConnection);

            final var overridden = new ActiveMQMessage();
            overridden.setConnection(strict);
            ActiveMQMessageTransformation.copyProperties(foreignMessage(), overridden, true);
            assertProviderPropertiesCopied(overridden);
        }

        final var noConnection = new ActiveMQMessage();
        ActiveMQMessageTransformation.copyProperties(foreignMessage(), noConnection);
        assertProviderPropertiesCopied(noConnection);
    }

    private ActiveMQConnection createConnection(final boolean strictCompliance) throws Exception {
        final var factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(strictCompliance);
        return (ActiveMQConnection) factory.createConnection();
    }

    private static void assertProviderPropertiesCopied(final ActiveMQMessage message) throws Exception {
        assertEquals("red", message.getStringProperty("color"));
        assertEquals("JMSXDeliveryCount 3 is a redelivery counter of 2", 2, message.getRedeliveryCounter());
        for (final var name : STORED_PROVIDER_PROPERTIES) {
            assertTrue(name, message.getProperties().containsKey(name));
        }
    }

    private static void assertProviderPropertiesSkipped(final ActiveMQMessage message) throws Exception {
        assertEquals("red", message.getStringProperty("color"));
        assertEquals("group-a", message.getStringProperty("JMSXGroupID"));
        assertEquals(0, message.getRedeliveryCounter());
        for (final var name : STORED_PROVIDER_PROPERTIES) {
            assertFalse(name, message.getProperties().containsKey(name));
        }
    }

    // a message that is not an ActiveMQ message and lists every provider-set JMSX property
    private static Message foreignMessage() {
        final Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("color", "red");
        properties.put("JMSXGroupID", "group-a");
        properties.put("JMSXDeliveryCount", 3);
        properties.put("JMSXRcvTimestamp", 1000L);
        properties.put("JMSXState", 1);
        properties.put("JMSXProducerTXID", "producer-tx");
        properties.put("JMSXConsumerTXID", "consumer-tx");

        return (Message) Proxy.newProxyInstance(Message.class.getClassLoader(), new Class<?>[] {Message.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                case "getPropertyNames":
                    return Collections.enumeration(properties.keySet());
                case "getObjectProperty":
                    return properties.get(args[0]);
                default:
                    return defaultValue(method.getReturnType());
                }
            });
    }

    private static Object defaultValue(final Class<?> type) {
        if (type == boolean.class) {
            return false;
        } else if (type == int.class) {
            return 0;
        } else if (type == long.class) {
            return 0L;
        }
        return null;
    }
}
