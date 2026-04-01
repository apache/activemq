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
package org.apache.activemq.network;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that verifies network bridges reconnect after the remote broker is
 * stopped abruptly during or before the broker info handshake completes.
 *
 * This covers the bug where {@code onException()} in
 * {@link DemandForwardingBridgeSupport} returned early when
 * {@code futureBrokerInfo} was not done, preventing
 * {@code serviceLocalException()}/{@code serviceRemoteException()} from
 * firing and thus blocking the reconnection chain.
 */
public class NetworkBridgeReconnectOnHandshakeFailureTest {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkBridgeReconnectOnHandshakeFailureTest.class);

    private BrokerService localBroker;
    private BrokerService remoteBroker;

    @After
    public void tearDown() throws Exception {
        if (localBroker != null) {
            try { localBroker.stop(); } catch (Exception ignored) {}
            localBroker.waitUntilStopped();
        }
        if (remoteBroker != null) {
            try { remoteBroker.stop(); } catch (Exception ignored) {}
            remoteBroker.waitUntilStopped();
        }
    }

    /**
     * Verify that when the remote broker is abruptly stopped (causing a
     * transport exception potentially during the broker info handshake),
     * the network bridge reconnects once the remote broker is restarted.
     */
    @Test(timeout = 60_000)
    public void testBridgeReconnectsAfterRemoteBrokerRestart() throws Exception {
        remoteBroker = createRemoteBroker(0);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        int remotePort = remoteBroker.getTransportConnectors().get(0).getConnectUri().getPort();
        LOG.info("Remote broker started on port {}", remotePort);

        localBroker = createLocalBroker(remotePort);
        localBroker.start();
        localBroker.waitUntilStarted();
        DiscoveryNetworkConnector nc = (DiscoveryNetworkConnector) localBroker.getNetworkConnectors().get(0);

        // Wait for the bridge to fully establish
        assertTrue("Bridge should be established", Wait.waitFor(() ->
                !nc.activeBridges().isEmpty(), 15_000, 200));
        LOG.info("Bridge established");

        // Abruptly stop the remote broker — this triggers onException on the
        // bridge transports, potentially while futureBrokerInfo is still pending
        remoteBroker.stop();
        remoteBroker.waitUntilStopped();
        LOG.info("Remote broker stopped abruptly");

        // Wait for the bridge to go down
        assertTrue("Bridge should go down after remote stop", Wait.waitFor(() ->
                nc.activeBridges().isEmpty(), 10_000, 200));

        // Restart the remote broker on the same port
        remoteBroker = createRemoteBroker(remotePort);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        LOG.info("Remote broker restarted on port {}", remotePort);

        // The bridge should reconnect — this is what failed before the fix,
        // because onException returned early without calling serviceRemoteException()
        assertTrue("Bridge should reconnect after remote broker restart", Wait.waitFor(() ->
                !nc.activeBridges().isEmpty(), 30_000, 500));
        LOG.info("Bridge reconnected successfully");

        // Verify messages can flow across the re-established bridge
        verifyMessageFlow(localBroker, remoteBroker);
    }

    /**
     * A more aggressive variant: stop the remote broker multiple times in
     * quick succession to increase the chance of hitting the onException
     * path during broker info exchange.
     */
    @Test(timeout = 120_000)
    public void testBridgeReconnectsAfterMultipleRemoteBrokerRestarts() throws Exception {
        remoteBroker = createRemoteBroker(0);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        int remotePort = remoteBroker.getTransportConnectors().get(0).getConnectUri().getPort();

        localBroker = createLocalBroker(remotePort);
        localBroker.start();
        localBroker.waitUntilStarted();
        DiscoveryNetworkConnector nc = (DiscoveryNetworkConnector) localBroker.getNetworkConnectors().get(0);

        for (int i = 0; i < 3; i++) {
            LOG.info("=== Restart cycle {} ===", i + 1);

            assertTrue("Bridge should be established (cycle " + (i + 1) + ")", Wait.waitFor(() ->
                    !nc.activeBridges().isEmpty(), 30_000, 500));

            remoteBroker.stop();
            remoteBroker.waitUntilStopped();

            assertTrue("Bridge should go down (cycle " + (i + 1) + ")", Wait.waitFor(() ->
                    nc.activeBridges().isEmpty(), 10_000, 200));

            remoteBroker = createRemoteBroker(remotePort);
            remoteBroker.start();
            remoteBroker.waitUntilStarted();
        }

        assertTrue("Bridge should be established after all restarts", Wait.waitFor(() ->
                !nc.activeBridges().isEmpty(), 30_000, 500));
        verifyMessageFlow(localBroker, remoteBroker);
    }

    private BrokerService createRemoteBroker(int port) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("remoteBroker");
        broker.setUseJmx(false);
        broker.setPersistent(false);
        broker.setUseShutdownHook(false);
        broker.addConnector("tcp://localhost:" + port);
        return broker;
    }

    private BrokerService createLocalBroker(int remotePort) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("localBroker");
        broker.setUseJmx(false);
        broker.setPersistent(false);
        broker.setUseShutdownHook(false);
        DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector(
                new URI("static:(tcp://localhost:" + remotePort + ")?useExponentialBackOff=false&initialReconnectDelay=1000"));
        nc.setName("bridge-reconnect-test");
        broker.addNetworkConnector(nc);
        return broker;
    }

    private void verifyMessageFlow(BrokerService local, BrokerService remote) throws Exception {
        ActiveMQQueue dest = new ActiveMQQueue("RECONNECT.HANDSHAKE.TEST");

        // Create consumer on remote broker
        ActiveMQConnectionFactory remoteFac = new ActiveMQConnectionFactory(remote.getVmConnectorURI());
        Connection remoteConn = remoteFac.createConnection();
        remoteConn.start();
        Session remoteSession = remoteConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = remoteSession.createConsumer(dest);

        // Wait for the demand subscription to propagate across the bridge
        assertTrue("Demand subscription should propagate", Wait.waitFor(() -> {
            try {
                return local.getDestination(dest) != null
                        && local.getDestination(dest).getConsumers().size() > 0;
            } catch (Exception e) {
                return false;
            }
        }, 15_000, 200));

        // Send message from local broker
        ActiveMQConnectionFactory localFac = new ActiveMQConnectionFactory(local.getVmConnectorURI());
        Connection localConn = localFac.createConnection();
        localConn.start();
        Session localSession = localConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = localSession.createProducer(dest);
        producer.send(localSession.createTextMessage("test-after-reconnect"));
        producer.close();

        // Receive on remote
        TextMessage received = (TextMessage) consumer.receive(TimeUnit.SECONDS.toMillis(10));
        assertNotNull("Message should flow across the re-established bridge", received);

        localConn.close();
        remoteConn.close();
    }
}
