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
package org.apache.activemq.transport.nio;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.net.ssl.SSLHandshakeException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertTrue;

public class NIOSSLBasicTest {

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/org/apache/activemq/security/broker1.ks";
    public static final String TRUST_KEYSTORE = "src/test/resources/org/apache/activemq/security/broker1.ks";

    public static final int MESSAGE_COUNT = 1000;

    @Before
    public void before() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        // Choose a value that's informative: ssl,handshake,data,trustmanager or all
        //System.setProperty("javax.net.debug", "handshake");
    }

    @After
    public void after() throws Exception {
    }

    public BrokerService createBroker(String connectorName, String connectorString) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        TransportConnector connector = broker.addConnector(connectorString);
        connector.setName(connectorName);
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }

    public void stopBroker(BrokerService broker) throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void basicConnector() throws Exception {
        BrokerService broker = createBroker("nio+ssl", getTransportType() + "://localhost:0?transport.needClientAuth=true");
        basicSendReceive("ssl://localhost:" + broker.getConnectorByName("nio+ssl").getConnectUri().getPort() + "?socket.verifyHostName=false");
        stopBroker(broker);
    }

    @Test
    public void enabledCipherSuites() throws Exception {
        BrokerService broker = createBroker("nio+ssl", getTransportType() + "://localhost:0?transport.needClientAuth=true&transport.verifyHostName=false&transport.enabledCipherSuites=TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256");
        basicSendReceive("ssl://localhost:" + broker.getConnectorByName("nio+ssl").getConnectUri().getPort() + "?socket.verifyHostName=false");
        stopBroker(broker);
    }

    @Test
    public void enabledProtocols() throws Exception {
        BrokerService broker = createBroker("nio+ssl", getTransportType() + "://localhost:61616?transport.needClientAuth=true&transport.enabledProtocols=TLSv1,TLSv1.1,TLSv1.2");
        basicSendReceive("ssl://localhost:" + broker.getConnectorByName("nio+ssl").getConnectUri().getPort() + "?socket.verifyHostName=false");
        stopBroker(broker);
    }

    //Client is missing verifyHostName=false so it should fail as cert doesn't have right host name
    @Test(expected = Exception.class)
    public void verifyHostNameErrorClient() throws Exception {

        final CountDownLatch gotLogMessage = new CountDownLatch(1);
        final AtomicBoolean gotRemoteAddressInLog = new AtomicBoolean();
        final DefaultTestAppender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.WARN) && event.getRenderedMessage().contains("Could not accept connection")) {
                    gotLogMessage.countDown();
                    if (event.getRenderedMessage().contains("tcp")) {
                        // got remote address
                        gotRemoteAddressInLog.set(true);
                    }
                }
            }
        };
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.addAppender(appender);

        BrokerService broker = null;
        try {
            broker = createBroker("nio+ssl", getTransportType() + "://localhost:61616?transport.needClientAuth=true");
            basicSendReceive("ssl://localhost:" + broker.getConnectorByName("nio+ssl").getConnectUri().getPort());
        } finally {
            gotLogMessage.await(5, TimeUnit.SECONDS);
            if (broker != null) {
                stopBroker(broker);
            }
            rootLogger.removeAppender(appender);
            assertTrue("Got remote address in log", gotRemoteAddressInLog.get());
        }
    }

    public void basicSendReceive(String uri) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        String body = "hello world!";
        Queue destination = session.createQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        producer.send(session.createTextMessage(body));

        MessageConsumer consumer = session.createConsumer(destination);
        Message received = consumer.receive(2000);
        TestCase.assertEquals(body, ((TextMessage)received).getText());
    }

    protected String getTransportType() {
        return "nio+ssl";
    }
}
