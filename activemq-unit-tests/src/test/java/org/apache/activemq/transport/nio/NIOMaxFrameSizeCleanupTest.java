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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;

import static junit.framework.TestCase.assertTrue;

//Test for AMQ-8183
public class NIOMaxFrameSizeCleanupTest {

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/org/apache/activemq/security/broker1.ks";
    public static final String TRUST_KEYSTORE = "src/test/resources/org/apache/activemq/security/broker1.ks";

    private BrokerService broker;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
    }

    @After
    public void after() throws Exception {
        stopBroker(broker);
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
    public void testMaxFrameSizeCleanupNio() throws Exception {
        String transportType = "nio";
        broker = createBroker(transportType, transportType + "://localhost:0?wireFormat.maxFrameSize=1024");
        testMaxFrameSizeCleanup(transportType, "tcp://localhost:" + broker.getConnectorByName(transportType).getConnectUri().getPort() + "?wireFormat.maxFrameSizeEnabled=false");
    }

    @Test
    public void testMaxFrameSizeCleanupAutoNio() throws Exception {
        String transportType = "auto+nio";
        broker = createBroker(transportType, transportType + "://localhost:0?wireFormat.maxFrameSize=1024");
        testMaxFrameSizeCleanup(transportType, "tcp://localhost:" + broker.getConnectorByName(transportType).getConnectUri().getPort() + "?wireFormat.maxFrameSizeEnabled=false");
    }

    @Test
    public void testMaxFrameSizeCleanupNioSsl() throws Exception {
        String transportType = "nio+ssl";
        broker = createBroker(transportType, transportType +
                "://localhost:0?transport.needClientAuth=true&wireFormat.maxFrameSize=1024");
        testMaxFrameSizeCleanup(transportType, "ssl://localhost:" + broker.getConnectorByName(transportType).getConnectUri().getPort()
                + "?socket.verifyHostName=false&wireFormat.maxFrameSizeEnabled=false");
    }

    @Test
    public void testMaxFrameSizeCleanupAutoNioSsl() throws Exception {
        String transportType = "auto+nio+ssl";
        broker = createBroker(transportType, transportType +
                "://localhost:0?transport.needClientAuth=true&wireFormat.maxFrameSize=1024");
        testMaxFrameSizeCleanup(transportType, "ssl://localhost:" + broker.getConnectorByName(transportType).getConnectUri().getPort()
                + "?socket.verifyHostName=false&wireFormat.maxFrameSizeEnabled=false");
    }

    protected void testMaxFrameSizeCleanup(String transportType, String clientUri) throws Exception {
        final List<Connection> connections = new ArrayList<>();
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(clientUri);
        for (int i = 0; i < 10; i++) {
            Connection connection = factory.createConnection();
            connection.start();
            connections.add(connection);
        }

        //Generate a body that is too large
        StringBuffer body = new StringBuffer();
        Random r = new Random();
        for (int i = 0; i < 10000; i++) {
            body.append(r.nextInt());
        }

        //Try sending 10 large messages rapidly in a loop to make sure all
        //nio threads are properly terminated
        for (int i = 0; i < 10; i++) {
            boolean exception = false;
            try {
                Connection connection = connections.get(i);
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue destination = session.createQueue("TEST");
                MessageProducer producer = session.createProducer(destination);
                producer.send(session.createTextMessage(body.toString()));
            } catch (Exception e) {
                //expected
                exception = true;
            }
            assertTrue("Should have gotten a transport exception", exception);
        }

        final ThreadPoolExecutor e = (ThreadPoolExecutor) SelectorManager.getInstance().getSelectorExecutor();
        //Verify that all connections are removed
        assertTrue(Wait.waitFor(() -> broker.getConnectorByName(transportType).getConnections().size() == 0,
                5000, 500));
        //Verify no more active transport connections in the selector thread pool. This was broken
        //due to AMQ-7106 before the fix in AMQ-8183
        assertTrue(Wait.waitFor(() -> e.getActiveCount() == 0, 5000, 500));
    }
}
