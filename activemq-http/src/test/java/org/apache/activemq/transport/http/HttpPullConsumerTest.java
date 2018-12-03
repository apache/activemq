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
package org.apache.activemq.transport.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.net.ServerSocketFactory;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.transport.ws.WSTransportTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HttpPullConsumerTest {

    private static final Logger LOG = LoggerFactory.getLogger(WSTransportTestSupport.class);

    @Rule
    public TestName name = new TestName();

    private int proxyPort = 0;

    protected ActiveMQConnectionFactory factory;
    protected ActiveMQConnection connection;
    protected BrokerService broker;
    protected URI httpConnectUri;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== Starting test: {} ==========", name.getMethodName());
        broker = createBroker(true, true);

        factory = new ActiveMQConnectionFactory(getHttpConnectionURI());
        factory.getPrefetchPolicy().setAll(0);
    }

    @After
    public void tearDown() throws Exception {
        try {
            stopBroker();
        } catch(Exception e) {
            LOG.warn("Error on Broker stop.");
        }

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
        }

        LOG.info("========== Finished test: {} ==========", name.getMethodName());
    }

    @Test(timeout = 30000)
    public void testTextMessage() throws Exception {
        connection = (ActiveMQConnection) factory.createConnection();

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(getTestName());
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // Send the message.
        {
            TextMessage message = session.createTextMessage();
            message.setText("Hi");
            producer.send(message);
        }

        // Check the Message
        {
            TextMessage message = (TextMessage)consumer.receive(2000);
            assertNotNull(message);
            assertEquals("Hi", message.getText());
        }

        assertNull(consumer.receiveNoWait());
    }

    @Test(timeout = 30000)
    public void testBytesMessageLength() throws Exception {
        connection = (ActiveMQConnection) factory.createConnection();

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(getTestName());
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // Send the message
        {
            BytesMessage message = session.createBytesMessage();
            message.writeInt(1);
            message.writeInt(2);
            message.writeInt(3);
            message.writeInt(4);
            producer.send(message);
        }

        // Check the message.
        {
            BytesMessage message = (BytesMessage)consumer.receive(1000);
            assertNotNull(message);
            assertEquals(16, message.getBodyLength());
        }

        assertNull(consumer.receiveNoWait());
    }

    protected String getHttpConnectionURI() {
        return "http://127.0.0.1:" + getProxyPort() + "?trace=true";
    }

    protected String getHttpConnectorURI() {
        return "http://127.0.0.1:" + getProxyPort() +
               "?trace=true&transport.trace=true";
    }

    protected BrokerService createBroker(boolean deleteMessages, boolean advisorySupport) throws Exception {

        BrokerService broker = new BrokerService();

        httpConnectUri = broker.addConnector(getHttpConnectorURI()).getPublishableConnectURI();

        broker.setAdvisorySupport(advisorySupport);
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        broker.setPersistent(isPersistent());
        broker.setDeleteAllMessagesOnStartup(deleteMessages);
        broker.start();
        broker.waitUntilStarted();

        return broker;
    }

    protected boolean isPersistent() {
        return false;
    }

    protected String getTestName() {
        return name.getMethodName();
    }

    protected int getProxyPort() {
        if (proxyPort == 0) {
            ServerSocket ss = null;
            try {
                ss = ServerSocketFactory.getDefault().createServerSocket(0);
                proxyPort = ss.getLocalPort();
            } catch (IOException e) { // ignore
            } finally {
                try {
                    if (ss != null ) {
                        ss.close();
                    }
                } catch (IOException e) { // ignore
                }
            }
        }

        return proxyPort;
    }

    protected void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }
}
