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
package org.apache.activemq.usecases;

import static org.junit.Assert.assertNotEquals;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientRebalanceTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRebalanceTest.class);
    private static final String QUEUE_NAME = "Test.ClientRebalanceTest";
    private static final AtomicInteger BROKER_SEQUENCE = new AtomicInteger(0);

    protected void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
    }

    public void testRebalance() throws Exception {
        // Use unique broker names to avoid VM transport registry collisions between tests
        final String b1Name = "rb1-" + BROKER_SEQUENCE.incrementAndGet();
        final String b2Name = "rb2-" + BROKER_SEQUENCE.incrementAndGet();
        final String b3Name = "rb3-" + BROKER_SEQUENCE.incrementAndGet();

        final BrokerService b1 = createBrokerProgrammatically(b1Name);
        final BrokerService b2 = createBrokerProgrammatically(b2Name);

        // Start b1 and b2 first to get their assigned ports
        b1.start();
        b1.waitUntilStarted();
        b2.start();
        b2.waitUntilStarted();

        final URI b1Uri = getConnectUri(b1);
        final URI b2Uri = getConnectUri(b2);

        // Wire network connectors between b1 and b2
        final NetworkConnector nc_b1_b2 = addAndStartNetworkConnector(b1, b1Name + "_" + b2Name, b2Uri);
        final NetworkConnector nc_b2_b1 = addAndStartNetworkConnector(b2, b2Name + "_" + b1Name, b1Uri);

        // Wait for bridges to form between b1 and b2
        waitForBridgeOnConnector(nc_b1_b2, b1Name + "->" + b2Name);
        waitForBridgeOnConnector(nc_b2_b1, b2Name + "->" + b1Name);

        LOG.info("Starting connection");

        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
            "failover:(" + b1Uri + "," + b2Uri + ")?randomize=false");
        final Connection conn = factory.createConnection();
        conn.start();
        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue theQueue = session.createQueue(QUEUE_NAME);
        final MessageProducer producer = session.createProducer(theQueue);
        final MessageConsumer consumer = session.createConsumer(theQueue);
        final Message message = session.createTextMessage("Test message");
        producer.send(message);
        final Message msg = consumer.receive(5000);
        assertNotNull(msg);

        // Introduce third broker
        final BrokerService b3 = createBrokerProgrammatically(b3Name);
        b3.setUseJmx(true);
        b3.start();
        b3.waitUntilStarted();

        final URI b3Uri = getConnectUri(b3);

        // Wire network connectors for b3
        final NetworkConnector nc_b1_b3 = addAndStartNetworkConnector(b1, b1Name + "_" + b3Name, b3Uri);
        addAndStartNetworkConnector(b2, b2Name + "_" + b3Name, b3Uri);
        final NetworkConnector nc_b3_b1 = addAndStartNetworkConnector(b3, b3Name + "_" + b1Name, b1Uri);
        addAndStartNetworkConnector(b3, b3Name + "_" + b2Name, b2Uri);

        // Wait for bridges involving b3 to form
        waitForBridgeOnConnector(nc_b1_b3, b1Name + "->" + b3Name);
        waitForBridgeOnConnector(nc_b3_b1, b3Name + "->" + b1Name);

        LOG.info("Stopping broker 1");

        b1.stop();
        b1.waitUntilStopped();
        brokers.remove(b1Name);

        // Wait for failover reconnect: the client should reconnect to b2 or b3
        // Verify by successfully sending and receiving a message
        assertTrue("should be able to send/receive after b1 stop",
            Wait.waitFor(() -> {
                try {
                    producer.send(message);
                    final Message received = consumer.receive(2000);
                    return received != null;
                } catch (final Exception e) {
                    return false;
                }
            }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(500)));

        LOG.info("Stopping broker 2");

        b2.stop();
        b2.waitUntilStopped();
        brokers.remove(b2Name);

        // Should reconnect to broker3
        assertTrue("should be able to send/receive after b2 stop",
            Wait.waitFor(() -> {
                try {
                    producer.send(message);
                    final Message received = consumer.receive(2000);
                    return received != null;
                } catch (final Exception e) {
                    return false;
                }
            }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(500)));
    }

    public void testReconnect() throws Exception {
        // Use unique broker names to avoid VM transport registry collisions between tests
        final String b1Name = "rc1-" + BROKER_SEQUENCE.incrementAndGet();
        final String b2Name = "rc2-" + BROKER_SEQUENCE.incrementAndGet();

        final BrokerService b1 = createBrokerProgrammatically(b1Name);
        final BrokerService b2 = createBrokerProgrammatically(b2Name);

        b1.start();
        b1.waitUntilStarted();
        b2.start();
        b2.waitUntilStarted();

        final URI b1Uri = getConnectUri(b1);
        final URI b2Uri = getConnectUri(b2);

        // Wire network connectors between b1 and b2
        final NetworkConnector nc_b1_b2 = addAndStartNetworkConnector(b1, b1Name + "_" + b2Name, b2Uri);
        addAndStartNetworkConnector(b2, b2Name + "_" + b1Name, b1Uri);

        // Wait for bridge to form
        waitForBridgeOnConnector(nc_b1_b2, b1Name + "->" + b2Name);

        LOG.info("Starting connection");

        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
            "failover:(" + b1Uri + ")?randomize=false");
        final Connection conn = factory.createConnection();
        conn.start();
        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue theQueue = session.createQueue("Test.ClientReconnectTest");
        final MessageProducer producer = session.createProducer(theQueue);
        final MessageConsumer consumer = session.createConsumer(theQueue);
        final Message message = session.createTextMessage("Test message");
        producer.send(message);
        final Message msg = consumer.receive(5000);
        assertNotNull(msg);

        final TransportConnector transportConnector = b1.getTransportConnectorByName("openwire");
        assertNotNull(transportConnector);

        final TransportConnection startFailoverConnection = findClientFailoverTransportConnection(transportConnector);
        assertNotNull(startFailoverConnection);

        final String startConnectionId = startFailoverConnection.getConnectionId();
        final String startRemoteAddress = startFailoverConnection.getRemoteAddress();
        final ConnectionControl simulateRebalance = new ConnectionControl();
        simulateRebalance.setReconnectTo(b1Uri.toString());
        startFailoverConnection.dispatchSync(simulateRebalance);

        // Wait for the failover reconnection to complete
        assertTrue("client should reconnect with different remote address",
            Wait.waitFor(() -> {
                final TransportConnection afterConnection = findClientFailoverTransportConnection(transportConnector);
                return afterConnection != null
                    && startConnectionId.equals(afterConnection.getConnectionId())
                    && !startRemoteAddress.equals(afterConnection.getRemoteAddress());
            }, TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(200)));

        final TransportConnection afterFailoverConnection = findClientFailoverTransportConnection(transportConnector);
        assertNotNull(afterFailoverConnection);
        assertEquals(startConnectionId, afterFailoverConnection.getConnectionId());
        assertNotEquals(startRemoteAddress, afterFailoverConnection.getRemoteAddress());

        // Should still be able to send without exception
        producer.send(message);
        final Message receivedMsg = consumer.receive(5000);
        assertNotNull(receivedMsg);
        conn.close();
    }

    private BrokerService createBrokerProgrammatically(final String brokerName) throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setBrokerName(brokerName);
        broker.setPersistent(false);
        broker.setUseJmx(false);

        final TransportConnector tc = broker.addConnector("tcp://localhost:0");
        tc.setName("openwire");
        tc.setRebalanceClusterClients(true);
        tc.setUpdateClusterClients(true);

        brokers.put(brokerName, new BrokerItem(broker));
        return broker;
    }

    /**
     * Get the connect URI for a broker's openwire transport connector.
     */
    private URI getConnectUri(final BrokerService broker) throws Exception {
        return broker.getTransportConnectorByName("openwire").getConnectUri();
    }

    private NetworkConnector addAndStartNetworkConnector(final BrokerService broker, final String name, final URI remoteUri) throws Exception {
        final DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector(
            new URI("static:(" + remoteUri + ")?useExponentialBackOff=false"));
        nc.setName(name);
        nc.setDuplex(false);
        broker.addNetworkConnector(nc);
        broker.startNetworkConnector(nc, null);
        return nc;
    }

    private void waitForBridgeOnConnector(final NetworkConnector connector, final String description) throws Exception {
        assertTrue(description + " bridge should form",
            Wait.waitFor(() -> {
                for (final NetworkBridge bridge : connector.activeBridges()) {
                    if (bridge.getRemoteBrokerName() != null) {
                        LOG.info("Bridge formed: {} -> {}", description, bridge.getRemoteBrokerName());
                        return true;
                    }
                }
                return false;
            }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(200)));
    }

    protected TransportConnection findClientFailoverTransportConnection(final TransportConnector transportConnector) {
        TransportConnection failoverConnection = null;
        for (final TransportConnection tmpConnection : transportConnector.getConnections()) {
            if (tmpConnection.isNetworkConnection()) {
                continue;
            }
            if (tmpConnection.isFaultTolerantConnection()) {
                failoverConnection = tmpConnection;
            }
        }
        return failoverConnection;
    }
}
