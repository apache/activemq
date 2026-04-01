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

import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
     * Reproduces the exact bug path: a transport exception fires while
     * futureBrokerInfo is NOT done (during handshake).
     *
     * Strategy: Use a fake server socket that accepts connections but never
     * sends BrokerInfo. The bridge connects, starts the handshake, but
     * futureBrokerInfo never completes. When the fake server closes the
     * socket, onException fires with futureBrokerInfo not done — this is
     * the exact bug path. The bridge must still trigger reconnection so
     * that when the real broker comes up, it connects successfully.
     */
    @Test(timeout = 60_000)
    public void testBridgeReconnectsAfterHandshakeFailure() throws Exception {
        // Start a fake server that accepts connections but never responds
        // This ensures futureBrokerInfo is never set when onException fires
        ServerSocket fakeServer = new ServerSocket(0);
        int port = fakeServer.getLocalPort();
        LOG.info("Fake server listening on port {}", port);

        // Accept connections in background and close them after a short delay
        // to trigger IOException on the bridge while futureBrokerInfo is pending
        CountDownLatch connectionReceived = new CountDownLatch(1);
        AtomicReference<Socket> clientSocket = new AtomicReference<>();
        Thread acceptThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Socket s = fakeServer.accept();
                    LOG.info("Fake server accepted connection from {}", s.getRemoteSocketAddress());
                    clientSocket.set(s);
                    connectionReceived.countDown();
                    // Keep accepting — the bridge will retry
                }
            } catch (Exception e) {
                // Expected when we close the server socket
            }
        }, "fake-server-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        // Start local broker with network connector pointing at the fake server
        localBroker = new BrokerService();
        localBroker.setBrokerName("localBroker");
        localBroker.setUseJmx(false);
        localBroker.setPersistent(false);
        localBroker.setUseShutdownHook(false);
        DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector(
                new URI("static:(tcp://localhost:" + port
                        + "?wireFormat.maxInactivityDuration=3000"
                        + "&wireFormat.maxInactivityDurationInitalDelay=3000"
                        + ")?useExponentialBackOff=false&initialReconnectDelay=1000"));
        nc.setName("bridge-handshake-failure-test");
        localBroker.addNetworkConnector(nc);
        localBroker.start();
        localBroker.waitUntilStarted();

        // Wait for the fake server to receive a connection — the bridge TCP
        // connected but will never get BrokerInfo
        assertTrue("Bridge should connect to fake server",
                connectionReceived.await(10, TimeUnit.SECONDS));
        LOG.info("Bridge connected to fake server, futureBrokerInfo will NOT be set");

        // Close the accepted socket — this fires onException while
        // futureBrokerInfo is NOT done (the exact bug path)
        Socket s = clientSocket.get();
        if (s != null) {
            s.close();
            LOG.info("Fake server closed client socket — triggering onException with futureBrokerInfo not done");
        }

        // Wait a bit for the bridge to process the exception and attempt reconnection
        Thread.sleep(2000);

        // Now shut down the fake server and start the real remote broker on the same port
        fakeServer.close();
        acceptThread.interrupt();

        remoteBroker = new BrokerService();
        remoteBroker.setBrokerName("remoteBroker");
        remoteBroker.setUseJmx(false);
        remoteBroker.setPersistent(false);
        remoteBroker.setUseShutdownHook(false);
        remoteBroker.addConnector("tcp://localhost:" + port);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        LOG.info("Real remote broker started on port {}", port);

        // The bridge should reconnect to the real broker.
        // WITHOUT the fix: onException returned early, serviceFailed() was never
        // called, the bridge is permanently dead — this assertion will FAIL.
        // WITH the fix: serviceRemoteException() fires, triggering reconnection.
        assertTrue("Bridge should reconnect to real broker after handshake failure",
                Wait.waitFor(() -> !nc.activeBridges().isEmpty(), 30_000, 500));
        LOG.info("Bridge reconnected to real broker successfully!");

        // Verify messages flow
        verifyMessageFlow(localBroker, remoteBroker);
    }

    /**
     * Basic reconnection test: verify bridge reconnects after remote broker restart.
     */
    @Test(timeout = 60_000)
    public void testBridgeReconnectsAfterRemoteBrokerRestart() throws Exception {
        remoteBroker = createRemoteBroker(0);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        int remotePort = remoteBroker.getTransportConnectors().get(0).getConnectUri().getPort();

        localBroker = createLocalBroker(remotePort);
        localBroker.start();
        localBroker.waitUntilStarted();
        DiscoveryNetworkConnector nc = (DiscoveryNetworkConnector) localBroker.getNetworkConnectors().get(0);

        assertTrue("Bridge should be established", Wait.waitFor(() ->
                !nc.activeBridges().isEmpty(), 15_000, 200));

        remoteBroker.stop();
        remoteBroker.waitUntilStopped();

        assertTrue("Bridge should go down", Wait.waitFor(() ->
                nc.activeBridges().isEmpty(), 10_000, 200));

        remoteBroker = createRemoteBroker(remotePort);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();

        assertTrue("Bridge should reconnect", Wait.waitFor(() ->
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

        ActiveMQConnectionFactory remoteFac = new ActiveMQConnectionFactory(remote.getVmConnectorURI());
        Connection remoteConn = remoteFac.createConnection();
        remoteConn.start();
        Session remoteSession = remoteConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = remoteSession.createConsumer(dest);

        assertTrue("Demand subscription should propagate", Wait.waitFor(() -> {
            try {
                return local.getDestination(dest) != null
                        && local.getDestination(dest).getConsumers().size() > 0;
            } catch (Exception e) {
                return false;
            }
        }, 30_000, 200));

        ActiveMQConnectionFactory localFac = new ActiveMQConnectionFactory(local.getVmConnectorURI());
        Connection localConn = localFac.createConnection();
        localConn.start();
        Session localSession = localConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = localSession.createProducer(dest);
        producer.send(localSession.createTextMessage("test-after-reconnect"));
        producer.close();

        TextMessage received = (TextMessage) consumer.receive(TimeUnit.SECONDS.toMillis(10));
        assertNotNull("Message should flow across the re-established bridge", received);

        localConn.close();
        remoteConn.close();
    }
}
