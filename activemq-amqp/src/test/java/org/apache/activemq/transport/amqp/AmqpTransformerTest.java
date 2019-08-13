/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpTransformerTest {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransformerTest.class);

    private static final String AMQP_URL = "amqp://0.0.0.0:0%s";
    private BrokerService brokerService;
    private URI amqpConnectionURI;
    private URI openwireConnectionURI;
    private static final String TEST_QUEUE = "txqueue";

    @Test(timeout = 30 * 1000)
    public void testNativeTransformation() throws Exception {

        // default is native
        startBrokerWithAmqpTransport(String.format(AMQP_URL, "?transport.transformer=native"));

        // send "text message" with AMQP JMS API
        Connection amqpConnection = JMSClientContext.INSTANCE.createConnection(amqpConnectionURI);
        amqpConnection.start();

        Session amqpSession = amqpConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = amqpSession.createQueue(TEST_QUEUE);
        MessageProducer p = amqpSession.createProducer(queue);
        p.setPriority(7);

        TextMessage amqpMessage = amqpSession.createTextMessage();
        amqpMessage.setText("hello");
        p.send(amqpMessage);

        p.close();
        amqpSession.close();
        amqpConnection.close();

        // receive with openwire JMS
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(openwireConnectionURI);
        Connection openwireConn = factory.createConnection();
        openwireConn.start();
        Session session = openwireConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue jmsDest = session.createQueue(TEST_QUEUE);

        MessageConsumer c = session.createConsumer(jmsDest);

        Message message = c.receive(1000);

        assertTrue(message instanceof BytesMessage);
        Boolean nativeTransformationUsed = message.getBooleanProperty("JMS_AMQP_NATIVE");
        assertTrue("Didn't use the correct transformation, expected NATIVE", nativeTransformationUsed);
        assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
        assertEquals(7, message.getJMSPriority());

        c.close();
        session.close();
        openwireConn.close();
    }

    @Test(timeout = 30000)
    public void testRawTransformation() throws Exception {

        // default is native
        startBrokerWithAmqpTransport(String.format(AMQP_URL, "?transport.transformer=raw"));

        // send "text message" with AMQP JMS API
        Connection amqpConnection = JMSClientContext.INSTANCE.createConnection(amqpConnectionURI);
        amqpConnection.start();

        Session amqpSession = amqpConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = amqpSession.createQueue(TEST_QUEUE);
        MessageProducer p = amqpSession.createProducer(queue);
        p.setPriority(7);

        TextMessage amqpMessage = amqpSession.createTextMessage();
        amqpMessage.setText("hello");
        p.send(amqpMessage);

        p.close();
        amqpSession.close();
        amqpConnection.close();

        // receive with openwire JMS
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(openwireConnectionURI);
        Connection openwireConn = factory.createConnection();
        openwireConn.start();
        Session session = openwireConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue jmsDest = session.createQueue(TEST_QUEUE);

        MessageConsumer c = session.createConsumer(jmsDest);

        Message message = c.receive(2000);

        assertNotNull("Should have received a message", message);
        LOG.info("Recieved message: {}", message);
        assertTrue(message instanceof BytesMessage);
        Boolean nativeTransformationUsed = message.getBooleanProperty("JMS_AMQP_NATIVE");
        assertTrue("Didn't use the correct transformation, expected NATIVE", nativeTransformationUsed);
        assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());

        // should not equal 7 (should equal the default) because "raw" does not map headers
        assertEquals(4, message.getJMSPriority());

        c.close();
        session.close();
        openwireConn.close();
    }

    @Test(timeout = 30 * 1000)
    public void testJmsTransformation() throws Exception {

        // default is native
        startBrokerWithAmqpTransport(String.format(AMQP_URL, "?transport.transformer=jms"));

        // send "text message" with AMQP JMS API
        Connection amqpConnection = JMSClientContext.INSTANCE.createConnection(amqpConnectionURI);
        amqpConnection.start();

        Session amqpSession = amqpConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = amqpSession.createQueue(TEST_QUEUE);
        MessageProducer p = amqpSession.createProducer(queue);

        TextMessage amqpMessage = amqpSession.createTextMessage();
        amqpMessage.setText("hello");
        p.send(amqpMessage);

        p.close();
        amqpSession.close();
        amqpConnection.close();

        // receive with openwire JMS
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(openwireConnectionURI);
        Connection openwireConn = factory.createConnection();
        openwireConn.start();
        Session session = openwireConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue jmsDest = session.createQueue(TEST_QUEUE);

        MessageConsumer c = session.createConsumer(jmsDest);

        Message message = c.receive(1000);

        assertTrue(message instanceof TextMessage);
        Boolean nativeTransformationUsed = message.getBooleanProperty("JMS_AMQP_NATIVE");
        assertFalse("Didn't use the correct transformation, expected NOT to be NATIVE", nativeTransformationUsed);
        assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());

        c.close();
        session.close();
        openwireConn.close();
    }

    @Test(timeout = 60000)
    public void testSendAMQPMessageWithComplexAnnotationsReceiveCore() throws Exception {
        startBrokerWithAmqpTransport(String.format(AMQP_URL, "?transport.transformer=jms"));

        URI remoteURI = new URI("tcp://" + amqpConnectionURI.getHost() + ":" + amqpConnectionURI.getPort());
        AmqpClient client = new AmqpClient(remoteURI, null, null);
        AmqpConnection connection = client.connect();
        try {
            connection.connect();

            String annotation = "x-opt-embedded-map";
            Map<String, String> embeddedMap = new LinkedHashMap<>();
            embeddedMap.put("test-key-1", "value-1");
            embeddedMap.put("test-key-2", "value-2");
            embeddedMap.put("test-key-3", "value-3");

            AmqpSession session = connection.createSession();
            AmqpSender sender = session.createSender(TEST_QUEUE);
            AmqpMessage message = createAmqpMessage((byte) 'A', 65535);

            message.setApplicationProperty("IntProperty", 42);
            message.setDurable(true);
            message.setMessageAnnotation(annotation, embeddedMap);
            sender.send(message);

            session.close();

            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(openwireConnectionURI);
            Connection connection2 = factory.createConnection();
            try {

                Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                connection2.start();
                MessageConsumer consumer = session2.createConsumer(session2.createQueue(TEST_QUEUE));

                Message received = consumer.receive(5000);
                assertNotNull(received);
                assertEquals(42, received.getIntProperty("IntProperty"));

                connection2.close();
            } finally {
                connection2.close();
            }
        } finally {
            connection.close();
        }
    }

    public void startBrokerWithAmqpTransport(String amqpUrl) throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setUseJmx(false);
        brokerService.setDeleteAllMessagesOnStartup(true);

        TransportConnector connector = brokerService.addConnector(amqpUrl);
        amqpConnectionURI = connector.getPublishableConnectURI();
        connector = brokerService.addConnector("tcp://0.0.0.0:0");
        openwireConnectionURI = connector.getPublishableConnectURI();

        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService = null;
        }
    }

    private AmqpMessage createAmqpMessage(byte value, int payloadSize) {
        AmqpMessage message = new AmqpMessage();
        byte[] payload = new byte[payloadSize];
        for (int i = 0; i < payload.length; i++) {
           payload[i] = value;
        }
        message.setBytes(payload);
        return message;
     }
}
