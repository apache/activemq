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
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.activemq.broker.replica.ReplicaPluginTestSupport.LONG_TIMEOUT;

@RunWith(Parameterized.class)
public class ReplicaProtocolConnectionTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaProtocolConnectionTest.class);
    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";
    public static final String PRIMARY_BROKER_CONFIG = "org/apache/activemq/broker/replica/transport-protocol-test-primary.xml";
    public static final String REPLICA_BROKER_CONFIG = "org/apache/activemq/broker/replica/transport-protocol-test-replica.xml";
    private static final DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.S");
    private final String protocol;
    protected BrokerService firstBroker;
    protected BrokerService secondBroker;
    protected ActiveMQDestination destination;

    @Before
    public void setUp() throws Exception {
        firstBroker =  setUpBrokerService(PRIMARY_BROKER_CONFIG);
        secondBroker =  setUpBrokerService(REPLICA_BROKER_CONFIG);

        firstBroker.start();
        secondBroker.start();
        firstBroker.waitUntilStarted();
        secondBroker.waitUntilStarted();

        destination = new ActiveMQQueue(getClass().getName() + "." + getName());
    }

    @After
    public void tearDown() throws Exception {
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
            {"auto"}, {"auto+ssl"}, {"auto+nio+ssl"}, {"auto+nio"},
            {"tcp"}, {"ssl"}, {"nio+ssl"}, {"nio"}
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
    public void testBrokerConnection() throws Exception {
        Connection firstBrokerConnection = getClientConnectionFactory(firstBroker.getTransportConnectorByScheme(protocol)).createConnection();
        Connection secondBrokerConnection = getClientConnectionFactory(secondBroker.getTransportConnectorByScheme(protocol)).createConnection();
        firstBrokerConnection.start();
        secondBrokerConnection.start();
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination);
        receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        receivedMessage.acknowledge();

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
        firstBrokerConnection.close();
        secondBrokerConnection.close();
    }

    private ActiveMQConnectionFactory getClientConnectionFactory(TransportConnector connector) throws IOException, URISyntaxException {
        String connectionUri = protocol + "://localhost:" + connector.getConnectUri().getPort();
        if (protocol.contains("ssl")) {
            return new ActiveMQSslConnectionFactory(connectionUri);
        } else {
            return new ActiveMQConnectionFactory(connectionUri);
        }
    }



    public ReplicaProtocolConnectionTest(String protocol) {
        this.protocol = protocol;
    }

    protected BrokerService setUpBrokerService(String configurationUri) throws Exception {
        BrokerService broker = createBroker(configurationUri);
        broker.setPersistent(false);
        return broker;
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }
}
