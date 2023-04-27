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

package org.apache.activemq.broker.replica;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

import static org.apache.activemq.broker.replica.ReplicaPluginTestSupport.LONG_TIMEOUT;

@RunWith(Parameterized.class)
public class ReplicaProtocolStompConnectionTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaProtocolStompConnectionTest.class);
    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";
    public static final String PRIMARY_BROKER_CONFIG = "org/apache/activemq/broker/replica/transport-protocol-test-primary.xml";
    public static final String REPLICA_BROKER_CONFIG = "org/apache/activemq/broker/replica/transport-protocol-test-replica.xml";
    protected static final String SECOND_BROKER_BINDING_ADDRESS = "vm://secondBrokerLocalhost";
    private static final DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.S");
    private final String protocol;
    protected BrokerService firstBroker;
    protected BrokerService secondBroker;
    private StompConnection firstBrokerConnection;
    private ConnectionFactory secondBrokerConnectionFactory;
    private Connection secondBrokerConnection;

    @Before
    public void setUp() throws Exception {
        firstBroker =  setUpBrokerService(PRIMARY_BROKER_CONFIG);
        secondBroker =  setUpBrokerService(REPLICA_BROKER_CONFIG);

        firstBroker.start();
        secondBroker.start();
        firstBroker.waitUntilStarted();
        secondBroker.waitUntilStarted();

        firstBrokerConnection = new StompConnection();
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(SECOND_BROKER_BINDING_ADDRESS);
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();
    }

    @After
    public void tearDown() throws Exception {
        firstBrokerConnection.disconnect();
        secondBrokerConnection.stop();
        if (firstBroker != null) {
            try {
                firstBroker.stop();
                firstBroker.waitUntilStopped();
            } catch (Exception e) {
            }
        }
        if (secondBroker != null) {
            try {
                secondBroker.stop();
                secondBroker.waitUntilStopped();
            } catch (Exception e) {
            }
        }
    }

    @Parameterized.Parameters(name="protocol={0}")
    public static Collection<String[]> getTestParameters() {
        return Arrays.asList(new String[][] {
            {"stomp"}, {"stomp+ssl"}, {"stomp+nio+ssl"}, {"stomp+nio"},
        });
    }

    static {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
    }

    @Test
    public void testMessageSendAndReceive() throws Exception {
        startConnection(firstBroker.getTransportConnectorByScheme(protocol), firstBrokerConnection);
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        String type = "queue";
        String body = "testMessageSendAndReceiveOnPrimarySide body";
        String destination = "ReplicaProtocolStompConnectionTestQueue";

        firstBrokerConnection.begin("tx1");
        String message = String.format("[%s://%s] %s", type, destination, body);
        HashMap<String, String> headers = new HashMap<>();
        headers.put("persistent", "true");
        firstBrokerConnection.send(String.format("/%s/%s", type, destination), message, "tx1", headers);
        firstBrokerConnection.commit("tx1");
        Thread.sleep(LONG_TIMEOUT);

        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(new ActiveMQQueue(destination));
        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(message, ((TextMessage) receivedMessage).getText());

        firstBrokerConnection.subscribe(String.format("/%s/%s", type, destination), Stomp.Headers.Subscribe.AckModeValues.AUTO);
        StompFrame receivedStompMessage = firstBrokerConnection.receive(LONG_TIMEOUT);
        assertNotNull(receivedStompMessage);
        assertEquals(message, receivedStompMessage.getBody());

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        secondBrokerSession.close();
    }

    private void startConnection(TransportConnector connector, StompConnection brokerConnection) throws Exception {
        if (protocol.contains("ssl")) {
            SocketFactory factory = SSLSocketFactory.getDefault();
            Socket socket = factory.createSocket("localhost", connector.getConnectUri().getPort());
            brokerConnection.open(socket);
            brokerConnection.connect(null, null, UUID.randomUUID().toString());
        } else {
            brokerConnection.open("localhost", connector.getConnectUri().getPort());
            brokerConnection.connect(null, null, UUID.randomUUID().toString());
        }
    }

    public ReplicaProtocolStompConnectionTest(String protocol) {
        this.protocol = protocol;
    }

    protected BrokerService setUpBrokerService(String configurationUri) throws Exception {
        BrokerService broker = createBroker(configurationUri);
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(false);
        return broker;
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }
}
