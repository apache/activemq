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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.function.BiConsumer;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
public class StrictComplianceProviderJMSXPropertyTest {

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
    public void testLegacyModeAllowsProviderSetJMSXProperties() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // Legacy mode (default)
        factory.setStrictCompliance(false);

        try (Connection connection = factory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            connection.start();
            Message message = session.createMessage();

            try {
                // ActiveMQ allows clients to overwrite the delivery count
                message.setIntProperty("JMSXDeliveryCount", 5);
                assertEquals(5, message.getIntProperty("JMSXDeliveryCount"));
                // PASS: Should not throw an exception in legacy mode
            } catch (Exception e) {
                fail("Legacy mode should not restrict setting provider-only JMSX properties");
            }
        }
    }

    @Test
    public void testStrictModeRejectsProviderSetJMSXProperties() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // Enforce Jakarta 3.1 compliance
        factory.setStrictCompliance(true);

        try (Connection connection = factory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            connection.start();
            Message message = session.createMessage();

            String[] providerOnlyProperties = {
                    "JMSXAppID",
                    "JMSXConsumerTXID",
                    "JMSXDeliveryCount",
                    "JMSXProducerTXID",
                    "JMSXRcvTimestamp",
                    "JMSXState",
                    "JMSXUserID"
            };

            for (String property : providerOnlyProperties) {
                try {
                    // Attempting to set these should fail
                    message.setObjectProperty(property, 1);
                    fail("Strict mode must reject client attempt to set provider-only property: " + property);
                } catch (JMSException e) {
                    assertEquals("exception type for " + property, JMSException.class, e.getClass());
                }
            }
        }
    }

    @Test
    public void testStrictModeAllowsClientSetJMSXProperties() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // Enforce Jakarta 3.1 compliance
        factory.setStrictCompliance(true);

        try (Connection connection = factory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            connection.start();
            Message message = session.createMessage();

            try {
                // The spec explicitly allows clients to set these specific JMSX properties
                message.setStringProperty("JMSXGroupID", "Group-A");
                message.setIntProperty("JMSXGroupSeq", 1);

                assertEquals("Group-A", message.getStringProperty("JMSXGroupID"));
                assertEquals(1, message.getIntProperty("JMSXGroupSeq"));
                // PASS: Valid client-set properties succeeded
            } catch (JMSException e) {
                fail("Strict mode incorrectly rejected a valid client-set JMSX property");
            }
        }
    }

    @Test
    public void testStrictProducerRejectsProviderPropertiesBeforeNativeAndForeignSends() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(true);
        try (var context = factory.createContext()) {
            final var producer = context.createProducer();
            final List<BiConsumer<JMSProducer, String>> setters = List.of(
                (p, name) -> p.setProperty(name, true),
                (p, name) -> p.setProperty(name, (byte) 1),
                (p, name) -> p.setProperty(name, (short) 1),
                (p, name) -> p.setProperty(name, 1),
                (p, name) -> p.setProperty(name, 1L),
                (p, name) -> p.setProperty(name, 1.0f),
                (p, name) -> p.setProperty(name, 1.0d),
                (p, name) -> p.setProperty(name, "provider-value"),
                (p, name) -> p.setProperty(name, (Object) "provider-value"));
            for (String name : List.of("JMSXAppID", "JMSXConsumerTXID", "JMSXDeliveryCount",
                "JMSXProducerTXID", "JMSXRcvTimestamp", "JMSXState", "JMSXUserID")) {
                for (var setter : setters) {
                    assertThrows(JMSRuntimeException.class, () -> setter.accept(producer, name));
                    assertFalse(producer.propertyExists(name));
                }
            }

            producer.setProperty("color", "red");
            producer.setProperty("JMSXGroupID", "group-a");
            producer.setProperty("JMSXGroupSeq", 1);
            final var queue = context.createQueue("test.strict.producer");
            try (var consumer = context.createConsumer(queue)) {
                for (Message message : List.of(context.createMessage(), foreignMessage())) {
                    producer.send(queue, message);
                    final var received = consumer.receive(5000);
                    assertNotNull(received);
                    assertEquals("red", received.getStringProperty("color"));
                    assertEquals("group-a", received.getStringProperty("JMSXGroupID"));
                    assertEquals(1, received.getIntProperty("JMSXGroupSeq"));
                    assertEquals(1, received.getIntProperty("JMSXDeliveryCount"));
                    assertFalse(received.propertyExists("JMSXAppID"));
                }
            }
        }
    }

    @Test
    public void testLegacyProducerRetainsProviderPropertiesOnNativeAndForeignSends() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(false);
        try (var context = factory.createContext()) {
            final var producer = context.createProducer().setProperty("JMSXAppID", "provider-app")
                .setProperty("JMSXUserID", "provider-user");
            final var queue = context.createQueue("test.lenient.producer");
            try (var consumer = context.createConsumer(queue)) {
                for (Message message : List.of(context.createMessage(), foreignMessage())) {
                    producer.send(queue, message);
                    final var received = consumer.receive(5000);
                    assertNotNull(received);
                    assertEquals("provider-app", received.getStringProperty("JMSXAppID"));
                    assertEquals("provider-user", received.getStringProperty("JMSXUserID"));
                }
            }
        }
    }

    private static Message foreignMessage() {
        final var backing = new ActiveMQMessage();
        return (Message) Proxy.newProxyInstance(Message.class.getClassLoader(), new Class<?>[] {Message.class},
            (proxy, method, args) -> {
                try {
                    return method.invoke(backing, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    }
}