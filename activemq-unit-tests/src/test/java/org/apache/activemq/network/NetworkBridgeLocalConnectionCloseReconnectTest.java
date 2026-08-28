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
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When a network bridge's local connection is closed server-side while the broker keeps running,
 * the bridge should fail and reconnect.
 */
public class NetworkBridgeLocalConnectionCloseReconnectTest {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkBridgeLocalConnectionCloseReconnectTest.class);

    private static final String QUEUE_NAME = "BRIDGE.RECONNECT.TEST";

    private BrokerService localBroker;
    private BrokerService remoteBroker;
    private Connection remoteConnection;
    private final AtomicInteger remoteReceived = new AtomicInteger();

    @Before
    public void setUp() throws Exception {
        remoteBroker = new BrokerService();
        remoteBroker.setBrokerName("remote");
        remoteBroker.setPersistent(false);
        remoteBroker.setUseJmx(false);
        remoteBroker.addConnector("tcp://127.0.0.1:0");
        remoteBroker.start();
        remoteBroker.waitUntilStarted();

        localBroker = new BrokerService();
        localBroker.setBrokerName("local");
        localBroker.setPersistent(false);
        localBroker.setUseJmx(false);
        localBroker.start();
        localBroker.waitUntilStarted();

        var remoteUri = remoteBroker.getTransportConnectors().get(0).getPublishableConnectURI();
        remoteConnection = new ActiveMQConnectionFactory(remoteUri).createConnection();
        remoteConnection.start();
        remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
                .createConsumer(new ActiveMQQueue(QUEUE_NAME))
                .setMessageListener(m -> remoteReceived.incrementAndGet());
    }

    @After
    public void tearDown() throws Exception {
        if (remoteConnection != null) {
            try { remoteConnection.close(); } catch (Exception ignored) {}
        }
        if (localBroker != null) {
            localBroker.stop();
            localBroker.waitUntilStopped();
        }
        if (remoteBroker != null) {
            remoteBroker.stop();
            remoteBroker.waitUntilStopped();
        }
    }

    @Test(timeout = 120_000)
    public void testBridgeReconnectsAfterLocalConnectionClosedServerSide() throws Exception {
        var nc = startBridge();

        produce(5);
        assertTrue("bridge should forward before the close",
                Wait.waitFor(() -> remoteReceived.get() >= 5, 20_000, 10));

        // The bridge's local side is a vm:// client of the local broker.
        var oldBridge = nc.activeBridges().iterator().next();
        var bridgeLocalConnection = findVmConnection(localBroker);
        assertNotNull("expected the bridge's local vm:// connection on the local broker", bridgeLocalConnection);
        LOG.info("stopping the bridge's local connection server-side: {}", bridgeLocalConnection);
        bridgeLocalConnection.serviceException(new IOException("stopping the bridge's local connection server-side"));

        // a connection stop is not a broker stop
        var reconnected = Wait.waitFor(() -> {
            for (var bridge : nc.activeBridges()) {
                if (bridge != oldBridge) {
                    return true;
                }
            }
            return false;
        }, 30_000, 10);
        assertTrue("bridge must reconnect after local-side connection closed and not a broker shutdown", reconnected);
        assertTrue("stopped bridge instance should be removed from activeBridges",
                Wait.waitFor(() -> !nc.activeBridges().contains(oldBridge), 20_000, 10));

        var before = remoteReceived.get();
        produce(5);
        assertTrue("messages produced after the reconnect must flow across the new bridge "
                        + "(remote had " + before + ", waiting for " + (before + 5) + ")",
                Wait.waitFor(() -> remoteReceived.get() >= before + 5, 20_000, 10));
        LOG.info("bridge reconnected and resumed forwarding after server-side local connection stop");
        nc.stop();
    }

    /**
     * A local broker shutdown should stop the bridge cleanly, with no failure handling or
     * reconnect attempt.
     */
    @Test(timeout = 120_000)
    public void testBridgeStopsCleanlyOnLocalBrokerShutdown() throws Exception {
        var nc = startBridge();
        produce(5);
        assertTrue("bridge should forward before the shutdown",
                Wait.waitFor(() -> remoteReceived.get() >= 5, 20_000, 10));

        localBroker.stop();
        localBroker.waitUntilStopped();

        assertTrue("bridge should be gone after a local broker shutdown",
                Wait.waitFor(() -> nc.activeBridges().isEmpty(), 20_000, 10));
        // the remote broker should be left with only the test's consumer connection - a
        // reconnect attempt during shutdown would show up as an extra inbound client
        assertTrue("no bridge connection should linger or reconnect on the remote broker",
                Wait.waitFor(() -> ((RegionBroker) remoteBroker.getRegionBroker()).getClients().length == 1, 20_000, 10));
        Thread.sleep(2000);
        assertTrue("and none may appear afterwards (no reconnect attempts after broker stop)",
                ((RegionBroker) remoteBroker.getRegionBroker()).getClients().length == 1);
    }

    /**
     * Stopping the connector delivers a ShutdownInfo to the bridge's own local transport as part
     * of its shutdown. That echo must not be treated as a failure and reconnect, since the bridge
     * is already disposed.
     */
    @Test(timeout = 120_000)
    public void testConnectorStopStaysStopped() throws Exception {
        var nc = startBridge();
        produce(5);
        assertTrue("bridge should forward before the stop",
                Wait.waitFor(() -> remoteReceived.get() >= 5, 20_000, 10));

        nc.stop();
        assertTrue("bridge should be gone after connector stop",
                Wait.waitFor(() -> nc.activeBridges().isEmpty(), 20_000, 10));

        var receivedAtStop = remoteReceived.get();
        produce(5);
        Thread.sleep(3000);
        assertTrue("bridge must not reconnect ", nc.activeBridges().isEmpty());
        assertTrue("no messages move after the stop (remote had " + receivedAtStop
                + ", now " + remoteReceived.get() + ")", remoteReceived.get() == receivedAtStop);
    }

    /**
     * Every server-side close should be recovered from, not just the first. After each close the
     * bridge reconnects, only one bridge stays registered, and forwarding still works at the end.
     */
    @Test(timeout = 120_000)
    public void testBridgeReconnectsAfterRepeatedLocalConnectionCloses() throws Exception {
        var nc = startBridge();
        produce(5);
        assertTrue("bridge should forward before the closes",
                Wait.waitFor(() -> remoteReceived.get() >= 5, 20_000, 10));

        for (var round = 1; round <= 3; round++) {
            var oldBridge = nc.activeBridges().iterator().next();
            // after a reconnect the new bridge's vm connection may not be registered yet
            assertTrue("round " + round + ": bridge's local vm:// connection should be present",
                    Wait.waitFor(() -> findVmConnection(localBroker) != null, 20_000, 10));
            var bridgeLocalConnection = findVmConnection(localBroker);
            bridgeLocalConnection.serviceException(new IOException("stopping the bridge's local connection server-side"));

            final var expectRound = round;
            assertTrue("round " + expectRound + ": bridge must reconnect with a new instance",
                    Wait.waitFor(() -> {
                        for (var bridge : nc.activeBridges()) {
                            if (bridge != oldBridge) {
                                return true;
                            }
                        }
                        return false;
                    }, 30_000, 10));
            assertTrue("round " + expectRound + ": old bridge instance must be deregistered, only one live bridge",
                    Wait.waitFor(() -> nc.activeBridges().size() == 1
                            && !nc.activeBridges().contains(oldBridge), 20_000, 10));
        }

        var before = remoteReceived.get();
        produce(5);
        assertTrue("forwarding must still work after repeated close/reconnect cycles",
                Wait.waitFor(() -> remoteReceived.get() >= before + 5, 20_000, 10));
        nc.stop();
    }

    /**
     * Once the bridge is stopped, a late ShutdownInfo must be a no-op: serviceLocalException() on
     * a disposed bridge must not fire bridgeFailed() or schedule a reconnect.
     */
    @Test(timeout = 120_000)
    public void testStoppedBridgeIgnoresShutdownInfo() throws Exception {
        var nc = startBridge();
        produce(5);
        assertTrue("bridge should forward before the stop",
                Wait.waitFor(() -> remoteReceived.get() >= 5, 20_000, 10));

        var bridge = (DemandForwardingBridgeSupport) nc.activeBridges().iterator().next();
        nc.stop();
        assertTrue("bridge should be gone after connector stop",
                Wait.waitFor(() -> nc.activeBridges().isEmpty(), 20_000, 10));

        // deliver a late ShutdownInfo to the stopped bridge; the broker is still running, so
        // without the disposed guard this would take the failure path and reconnect
        bridge.serviceLocalCommand(new ShutdownInfo());

        Thread.sleep(3000);
        assertTrue("a ShutdownInfo delivered to a stopped bridge must not resurrect it",
                nc.activeBridges().isEmpty());
        assertTrue("and no bridge connection may reappear on the remote broker",
                ((RegionBroker) remoteBroker.getRegionBroker()).getClients().length == 1);
    }

    /**
     * AbortSlowConsumerStrategy and AbortSlowAckConsumerStrategy with abortConnection=true call
     * connection.serviceException(InactivityIOException) on the bridge's local connection. The
     * bridge must restart and resume forwarding.
     */
    @Test(timeout = 120_000)
    public void testBridgeRestartsAfterConnectionServiceException() throws Exception {
        var nc = startBridge();
        produce(5);
        assertTrue("bridge should forward before the abort",
                Wait.waitFor(() -> remoteReceived.get() >= 5, 20_000, 10));

        var oldBridge = nc.activeBridges().iterator().next();
        var bridgeLocalConnection = findVmConnection(localBroker);
        assertNotNull("expected the bridge's local vm:// connection", bridgeLocalConnection);
        LOG.info("servicing an InactivityIOException on the bridge's local connection: {}", bridgeLocalConnection);
        bridgeLocalConnection.serviceException(
                new InactivityIOException("1 Consumers was slow too often or too long"));

        var restarted = Wait.waitFor(() -> {
            for (var bridge : nc.activeBridges()) {
                if (bridge != oldBridge) {
                    return true;
                }
            }
            return false;
        }, 30_000, 10);
        assertTrue("bridge must restart after connection.serviceException(..) on its local connection", restarted);
        assertTrue("aborted bridge instance must be deregistered from activeBridges",
                Wait.waitFor(() -> !nc.activeBridges().contains(oldBridge), 20_000, 10));

        var before = remoteReceived.get();
        produce(5);
        assertTrue("messages must flow across the restarted bridge (remote had " + before + ")",
                Wait.waitFor(() -> remoteReceived.get() >= before + 5, 20_000, 10));
        LOG.info("bridge restarted and resumed forwarding after serviceException abort");
        nc.stop();
    }

    private NetworkConnector startBridge() throws Exception {
        var remoteUri = remoteBroker.getTransportConnectors().get(0).getPublishableConnectURI();
        var nc = localBroker.addNetworkConnector("static:(" + remoteUri + ")");
        nc.setName("to-remote");
        nc.setDuplex(false);
        nc.addStaticallyIncludedDestination(new ActiveMQQueue(QUEUE_NAME));
        nc.start();
        assertTrue("bridge should establish",
                Wait.waitFor(() -> !nc.activeBridges().isEmpty(), 20_000, 10));
        return nc;
    }

    private void produce(int n) throws Exception {
        try (var c = new ActiveMQConnectionFactory(
                localBroker.getTransportConnectors().isEmpty()
                        ? localBroker.getVmConnectorURI()
                        : localBroker.getTransportConnectors().get(0).getPublishableConnectURI()).createConnection();
             var session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(new ActiveMQQueue(QUEUE_NAME))) {
            c.start();

            for (var i = 0; i < n; i++) {
                producer.send(session.createTextMessage("m-" + i));
            }
        }
    }

    // The bridge's local side is the only vm:// client connection on the local broker
    private TransportConnection findVmConnection(BrokerService broker) throws Exception {
        for (var connection :
                ((RegionBroker) broker.getRegionBroker()).getClients()) {
            var address = connection.getRemoteAddress();
            if (address != null && address.startsWith("vm:") && connection instanceof TransportConnection) {
                return (TransportConnection) connection;
            }
        }
        return null;
    }
}
