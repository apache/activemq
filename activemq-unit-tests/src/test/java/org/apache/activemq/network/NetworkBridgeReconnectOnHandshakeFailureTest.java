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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that verifies network bridges properly handle transport exceptions
 * during the broker info handshake phase and recover by reconnecting
 * to a real broker.
 *
 * <p>The bug: {@code onException()} in {@link DemandForwardingBridgeSupport}
 * returned early when {@code futureBrokerInfo} was not done (i.e. during
 * the handshake), preventing {@code serviceRemoteException()} from being
 * called with the original {@code IOException}. While
 * {@code collectBrokerInfos()} provided a fallback reconnection path
 * (via {@code TimeoutException}), the original error was lost.</p>
 *
 * <p>The fix removes the early {@code return} so that
 * {@code serviceRemoteException()} is always called with the original
 * {@code IOException}, ensuring proper error reporting and direct
 * exception handling in {@code onException()}.</p>
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
     * Simulates a handshake failure end-to-end and verifies recovery:
     *
     * <ol>
     *   <li>A fake server accepts TCP connections but never sends
     *       {@code BrokerInfo}, so {@code futureBrokerInfo} is never
     *       completed — the bridge is stuck mid-handshake.</li>
     *   <li>The fake server abruptly closes the socket, triggering
     *       {@code onException()} while {@code futureBrokerInfo} is
     *       not done — the exact bug path.</li>
     *   <li>A real broker starts on the same port.</li>
     *   <li>The bridge reconnects to the real broker.</li>
     *   <li>Messages flow across the re-established bridge.</li>
     * </ol>
     *
     * <p>Additionally, this test uses a custom {@link BridgeFactory} to
     * verify that {@code serviceRemoteException()} receives the original
     * {@code IOException} (from {@code onException()}), not only a
     * {@code TimeoutException} (from the {@code collectBrokerInfos()}
     * fallback). Without the fix, the early {@code return} prevents the
     * {@code IOException} from reaching {@code serviceRemoteException()}.
     * </p>
     */
    @Test(timeout = 60_000)
    public void testBridgeReconnectsAfterHandshakeFailure() throws Exception {
        // Track exceptions passed to serviceRemoteException to verify
        // the original IOException is properly propagated
        CopyOnWriteArrayList<Throwable> remoteExceptions = new CopyOnWriteArrayList<>();
        CountDownLatch exceptionLatch = new CountDownLatch(1);

        BridgeFactory trackingFactory = new BridgeFactory() {
            @Override
            public DemandForwardingBridge createNetworkBridge(
                    NetworkBridgeConfiguration configuration,
                    Transport localTransport, Transport remoteTransport,
                    NetworkBridgeListener listener) {
                DemandForwardingBridge bridge = new DemandForwardingBridge(configuration, localTransport, remoteTransport) {
                    @Override
                    public void serviceRemoteException(Throwable error) {
                        LOG.info("serviceRemoteException called with: {} ({})",
                                error.getClass().getSimpleName(), error.getMessage());
                        remoteExceptions.add(error);
                        exceptionLatch.countDown();
                        super.serviceRemoteException(error);
                    }
                };
                bridge.setNetworkBridgeListener(listener);
                return bridge;
            }
        };

        // Phase 1: Start a fake server that accepts connections but never
        // sends BrokerInfo — this keeps futureBrokerInfo incomplete
        ServerSocket fakeServer = new ServerSocket(0);
        int port = fakeServer.getLocalPort();
        LOG.info("Fake server listening on port {}", port);

        CountDownLatch connectionReceived = new CountDownLatch(1);
        AtomicReference<Socket> clientSocket = new AtomicReference<>();
        Thread acceptThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Socket s = fakeServer.accept();
                    LOG.info("Fake server accepted connection from {}", s.getRemoteSocketAddress());
                    clientSocket.set(s);
                    connectionReceived.countDown();
                }
            } catch (Exception e) {
                // Expected when we close the server socket
            }
        }, "fake-server-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        // Start the local broker with network connector pointing at the fake server
        localBroker = new BrokerService();
        localBroker.setBrokerName("localBroker");
        localBroker.setUseJmx(false);
        localBroker.setPersistent(false);
        localBroker.setUseShutdownHook(false);
        DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector(
                new URI("static:(tcp://localhost:" + port
                        + "?wireFormat.maxInactivityDuration=0"
                        + ")?useExponentialBackOff=false&initialReconnectDelay=1000"));
        nc.setName("bridge-handshake-failure-test");
        nc.setBridgeFactory(trackingFactory);
        localBroker.addNetworkConnector(nc);
        localBroker.start();
        localBroker.waitUntilStarted();

        // Wait for the bridge to TCP-connect to the fake server.
        // At this point futureBrokerInfo is NOT done — the handshake
        // is stuck because the fake server never sends BrokerInfo.
        assertTrue("Bridge should connect to fake server",
                connectionReceived.await(10, TimeUnit.SECONDS));
        LOG.info("Bridge connected to fake server, futureBrokerInfo will NOT be set");

        // Phase 2: Simulate a handshake failure — close the socket to
        // trigger onException() while futureBrokerInfo is not done
        Socket s = clientSocket.get();
        if (s != null) {
            s.close();
            LOG.info("Closed fake server socket — simulating handshake failure");
        }

        // Verify serviceRemoteException is called with the original IOException.
        // Without the fix, only a TimeoutException from collectBrokerInfos
        // would reach serviceRemoteException.
        assertTrue("serviceRemoteException should be called",
                exceptionLatch.await(10, TimeUnit.SECONDS));

        // Allow time for both code paths (onException and collectBrokerInfos)
        // to call serviceRemoteException
        assertTrue("Should receive exception(s)", Wait.waitFor(() ->
                !remoteExceptions.isEmpty(), 5_000, 100));

        for (int i = 0; i < remoteExceptions.size(); i++) {
            Throwable ex = remoteExceptions.get(i);
            LOG.info("serviceRemoteException call [{}]: {} ({})",
                    i, ex.getClass().getName(), ex.getMessage());
        }

        boolean hasIOException = remoteExceptions.stream()
                .anyMatch(ex -> ex instanceof IOException);
        assertTrue(
                "serviceRemoteException should receive the original IOException "
                        + "(from onException handler), not only TimeoutException "
                        + "(from collectBrokerInfos fallback). Exceptions received: "
                        + remoteExceptions.stream()
                                .map(ex -> ex.getClass().getSimpleName())
                                .reduce((a, b) -> a + ", " + b).orElse("none"),
                hasIOException);

        // Phase 3: Shut down the fake server and start a real broker
        // on the same port
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

        // Phase 4: The bridge should reconnect to the real broker
        assertTrue("Bridge should reconnect to real broker after handshake failure",
                Wait.waitFor(() -> !nc.activeBridges().isEmpty(), 30_000, 500));
        LOG.info("Bridge reconnected successfully after handshake failure");

        // Phase 5: Verify messages flow across the re-established bridge
        verifyMessageFlow(localBroker, remoteBroker);
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
