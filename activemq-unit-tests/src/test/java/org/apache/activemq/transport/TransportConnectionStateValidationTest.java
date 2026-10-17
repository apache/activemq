/*
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
package org.apache.activemq.transport;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnectionState;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class TransportConnectionStateValidationTest {

    private static final Logger LOG = LoggerFactory.getLogger(
            TransportConnectionStateValidationTest.class);

    private TransportConnector connector;
    private URI connectionURI;
    private BrokerService brokerService;
    private final AtomicInteger counter = new AtomicInteger();

    @Before
    public void setUp() throws Exception {
        startBroker();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void testMismatchedConsumerProducer() throws Exception {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectionURI);
        try (ActiveMQConnection conn1 = (ActiveMQConnection) cf.createConnection()) {
            // Create a connection with a consumer
            conn1.start();
            TransportConnection conn1Tc = findTransportConnection(conn1);

            // Create a second connection and grab its state
            ActiveMQConnection conn2 = (ActiveMQConnection) cf.createConnection();
            conn2.start();
            TransportConnection conn2Tc = findTransportConnection(conn2);
            TransportConnectionState secondState = conn2Tc.lookupConnectionState(conn2.getConnectionInfo().getConnectionId());
            assertNotSame(conn1Tc, conn2Tc);

            // Try to add a session to connection 2 that uses connection 1's connection id
            SessionId conn1SessionId = new SessionId();
            conn1SessionId.setConnectionId(conn1Tc.getConnectionId());
            conn1SessionId.setValue(counter.incrementAndGet());
            SessionInfo conn1SessionInfo = new SessionInfo(conn1SessionId);

            // processAddSession should skip this because it doesn't match
            conn2.getTransport().request(conn1SessionInfo);
            // The second connection should not be tracking the session because it didn't match
            // and was skipped
            assertTrue(secondState.getSessionStates().stream()
                    .noneMatch(ss -> ss.getInfo().getSessionId().equals(conn1SessionId)));

            // Attempting to add a consumer using the wrong connection id should throw an exception
            ExceptionResponse response = (ExceptionResponse) conn2.getTransport().request(
                    new ConsumerInfo(conn1SessionInfo, counter.incrementAndGet()));
            assertTrue(response.getException() instanceof IllegalStateException);
            assertTrue(response.getException().getMessage().contains(
                    "Cannot add a consumer to a connection that had not been registered"));

            // Attempting to add a consumer using the wrong connection id should throw an exception
            response = (ExceptionResponse) conn2.getTransport().request(
                    new ProducerInfo(conn1SessionInfo, counter.incrementAndGet()));
            assertTrue(response.getException() instanceof IllegalStateException);
            assertTrue(response.getException().getMessage().contains(
                    "Cannot add a producer to a connection that had not been registered"));
        }
    }

    @Test
    public void testMismatchedConsumerAck() throws Exception {
        ActiveMQQueue queue = new ActiveMQQueue("test");

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectionURI);
        try (ActiveMQConnection conn1 = (ActiveMQConnection) cf.createConnection()) {
            conn1.start();
            Session session = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Create a consumer/producer on a test queue and send a single message
            ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(queue);
            MessageProducer producer = session.createProducer(queue);
            ActiveMQMessage message = (ActiveMQMessage) session.createTextMessage("test");
            producer.send(message);
            ConsumerId conn1ConsumerId = consumer.getConsumerId();

            // create a second connection
            ActiveMQConnection conn2 = (ActiveMQConnection) cf.createConnection();
            conn2.start();
            TransportConnection conn2Tc = findTransportConnection(conn2);
            TransportConnectionState secondState = conn2Tc.lookupConnectionState(conn2.getConnectionInfo().getConnectionId());

            // copy session id from connection 1 and attempt to add to connection 2
            SessionInfo sessionInfo = new SessionInfo(conn1ConsumerId.getParentId());
            conn2.getTransport().request(sessionInfo);
            // The second connection should not be tracking the session because it didn't match
            // and was skipped
            assertTrue(secondState.getSessionStates().stream()
                    .noneMatch(ss -> ss.getInfo().getSessionId().equals(sessionInfo.getSessionId())));

            // copy consumer from connection 1 and try to add to connection 2
            ConsumerInfo ci = new ConsumerInfo(conn1ConsumerId);
            ci.setDestination(new ActiveMQQueue("test"));

            // This should throw an error
            ExceptionResponse response = (ExceptionResponse) conn2.getTransport().request(ci);
            assertTrue(response.getException() instanceof IllegalStateException);
            assertTrue(response.getException().getMessage().contains(
                    "Cannot add a consumer to a connection that had not been registered"));

            // Try to ack on connection 2 with a consumer 1 for connection 1
            // This should be ignored
            MessageAck ack = new MessageAck();
            ack.setConsumerId(conn1ConsumerId);
            ack.setDestination(new ActiveMQQueue("test"));
            ack.setAckType(org.apache.activemq.command.MessageAck.STANDARD_ACK_TYPE);
            ack.setMessageID(message.getMessageId());
            ack.setCommandId(counter.incrementAndGet());
            conn2.getTransport().request(ack);

            // Message should not be acked and still pending
            assertEquals(1, brokerService.getDestination(queue)
                    .getDestinationStatistics().getMessages().getCount());
        }
    }

    @Test
    public void testMismatchedConsumerControl() throws Exception {
        ActiveMQQueue queue = new ActiveMQQueue("test");

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectionURI);
        try (ActiveMQConnection conn1 = (ActiveMQConnection) cf.createConnection()) {
            conn1.start();
            Session session = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Create a consumer/producer on a test queue and send a single message
            ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(queue);
            MessageProducer producer = session.createProducer(queue);
            ActiveMQMessage message = (ActiveMQMessage) session.createTextMessage("test");
            producer.send(message);
            ConsumerId conn1ConsumerId = consumer.getConsumerId();

            // create a second connection
            ActiveMQConnection conn2 = (ActiveMQConnection) cf.createConnection();
            conn2.start();
            TransportConnection conn2Tc = findTransportConnection(conn2);
            TransportConnectionState secondState = conn2Tc.lookupConnectionState(conn2.getConnectionInfo().getConnectionId());

            // copy session id from connection 1 and attempt to add to connection 2
            SessionInfo sessionInfo = new SessionInfo(conn1ConsumerId.getParentId());
            conn2.getTransport().request(sessionInfo);
            // The second connection should not be tracking the session because it didn't match
            // and was skipped
            assertTrue(secondState.getSessionStates().stream()
                    .noneMatch(ss -> ss.getInfo().getSessionId().equals(sessionInfo.getSessionId())));

            ConsumerControl control = new ConsumerControl();
            control.setConsumerId(conn1ConsumerId);
            control.setDestination(queue);

            // This should throw an error
            ExceptionResponse response = (ExceptionResponse) conn2.getTransport().request(control);
            assertTrue(response.getException() instanceof IllegalStateException);
            assertTrue(response.getException().getMessage().contains(
                    "Cannot process ConsumerControl for a consumer that had not been registered"));

        }
    }

    @Test
    public void testMismatchedSend() throws Exception {
        ActiveMQQueue queue = new ActiveMQQueue("test");

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectionURI);
        try (ActiveMQConnection conn1 = (ActiveMQConnection) cf.createConnection()) {
            conn1.start();
            ActiveMQSession session = (ActiveMQSession) conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ActiveMQMessage message = (ActiveMQMessage) session.createTextMessage("test");

            // create a second connection
            ActiveMQConnection conn2 = (ActiveMQConnection) cf.createConnection();
            conn2.start();

            // Create a session/producer id that use connection 1's ID info and set on message
            SessionId conn1SessionId = new SessionId();
            conn1SessionId.setConnectionId(conn1.getConnectionInfo().getConnectionId().toString());
            conn1SessionId.setValue(counter.incrementAndGet());
            message.setProducerId(new ProducerId(conn1SessionId, counter.incrementAndGet()));

            // This should throw an error because we are trying to send a message on a transport
            // for connection 2 but the message contains a producer id for connection 1
            ExceptionResponse response = (ExceptionResponse) conn2.getTransport().request(message);
            assertTrue(response.getException() instanceof IllegalStateException);
            assertTrue(response.getException().getMessage().contains(
                    "Cannot lookup a producer from a connection that had not been registered"));
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        connector = broker.addConnector("tcp://0.0.0.0:0");
        connectionURI = connector.getPublishableConnectURI();
        return broker;
    }

    public void startBroker() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    private TransportConnection findTransportConnection(ActiveMQConnection connection) {
        return connector.getConnections().stream().filter(
                tc -> tc.lookupConnectionState(
                        connection.getConnectionInfo().getConnectionId()) != null).findFirst().orElseThrow();
    }
}

