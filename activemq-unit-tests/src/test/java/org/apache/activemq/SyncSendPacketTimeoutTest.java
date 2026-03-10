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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.transport.RequestTimedOutIOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that {@link ActiveMQConnection#syncSendPacket(org.apache.activemq.command.Command)}
 * applies the configured {@code requestTimeout}.
 */
public class SyncSendPacketTimeoutTest {

    private BrokerService broker;
    private String brokerUrl;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        brokerUrl = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testRequestTimeoutDefaultIsZero() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        try (ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection()) {
            assertEquals("Default requestTimeout should be 0", 0, connection.getRequestTimeout());
        }
    }

    @Test
    public void testRequestTimeoutConfiguredViaFactory() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        factory.setRequestTimeout(5000);
        try (ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection()) {
            assertEquals("requestTimeout should be propagated from factory", 5000, connection.getRequestTimeout());
        }
    }

    @Test
    public void testSyncSendPacketSucceedsWithRequestTimeout() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        factory.setRequestTimeout(5000);
        try (ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection()) {
            connection.start();
            // Creating a session triggers syncSendPacket internally — should succeed within
            // timeout
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    // Creating a consumer triggers syncSendPacket internally — should succeed
                    // within timeout
                    MessageConsumer consumer = session.createConsumer(session.createQueue("TEST.QUEUE"))) {
                assertNotNull("Session should be created successfully", session);
                assertNotNull("Consumer should be created successfully", consumer);
            }
        }
    }

    @Test
    public void testSyncSendPacketSucceedsWithoutRequestTimeout() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        // requestTimeout=0 means no timeout (default)
        try (ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    // Creating a consumer triggers syncSendPacket internally — should succeed
                    // no timeout
                    MessageConsumer consumer = session.createConsumer(session.createQueue("TEST.QUEUE"))) {
                assertNotNull("Session should be created successfully with no timeout", session);
                assertNotNull("Consumer should be created successfully", consumer);
            }
        }
    }

    @Test
    public void testRequestTimeoutConfiguredViaUrl() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl + "?jms.requestTimeout=3000");
        try (ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection()) {
            assertEquals("requestTimeout should be set via URL parameter", 3000, connection.getRequestTimeout());
        }
    }

    @Test
    public void testSyncSendPacketFailFromTimeout() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        // Set to super short 1 millisecond so we always time out
        factory.setRequestTimeout(1);
        try (ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection()) {
            Exception exception = null;
            try {
                connection.start();
                try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                     MessageConsumer consumer = session.createConsumer(session.createQueue("test"))) {
                    assertNotNull("Consumer should be created successfully", consumer);
                }
                fail("Expected JMSException due to request timeout");
            } catch (JMSException expected) {
                exception = expected;
            }
            assertEquals(RequestTimedOutIOException.class,
                    TransportConnector.getRootCause(exception).getClass());
        }
    }

    @Test
    public void testSyncSendPacketOverrideDefaultRequestTimeout() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        try (ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection()) {
            connection.start();
            ActiveMQSession session = (ActiveMQSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // After session creation set the timeout default to be very short to test that
            // overriding directly works
            connection.setRequestTimeout(1);
            ConsumerInfo info = new ConsumerInfo(session.getSessionInfo(),
                    session.getNextConsumerId().getValue());
            info.setDestination(new ActiveMQQueue("test"));
            // Send info packet with timeout override
            assertNotNull("Consumer should be created successfully with no timeout",
                    connection.syncSendPacket(info, 5000));
        }
    }
}
