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
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ShutdownInfo handling for a duplex network bridge, on both sides.
 *
 * A duplex bridge has two halves, each with its own local vm:// connection: the initiator's
 * bridge on the initiating broker, and the responder's bridge embedded in the inbound
 * TransportConnection on the accepting broker. Closing either vm:// connection server-side while
 * its broker keeps running delivers a ShutdownInfo to that half, and both halves should fail and
 * recover so message flow resumes in both directions. An orderly broker shutdown, which sends
 * ShutdownInfo over the remote side of the shared connection, should still stop the bridge without
 * trying to reconnect to the stopped broker.
 */
public class NetworkBridgeDuplexLocalConnectionCloseReconnectTest {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkBridgeDuplexLocalConnectionCloseReconnectTest.class);

    // one queue per direction, consumed only on the receiving side; a consumer on the producing
    // side would compete with the bridge's subscription for the same messages
    private static final String QUEUE_TO_RESPONDER = "BRIDGE.DUPLEX.TO.RESPONDER";
    private static final String QUEUE_TO_INITIATOR = "BRIDGE.DUPLEX.TO.INITIATOR";

    private BrokerService initiatorBroker;
    private BrokerService responderBroker;
    private Connection initiatorConnection;
    private Connection responderConnection;
    private final AtomicInteger initiatorReceived = new AtomicInteger();
    private final AtomicInteger responderReceived = new AtomicInteger();
    private NetworkConnector nc;

    @Before
    public void setUp() throws Exception {
        responderBroker = new BrokerService();
        responderBroker.setBrokerName("responder");
        responderBroker.setPersistent(false);
        responderBroker.setUseJmx(false);
        responderBroker.addConnector("tcp://127.0.0.1:0");
        responderBroker.start();
        responderBroker.waitUntilStarted();

        initiatorBroker = new BrokerService();
        initiatorBroker.setBrokerName("initiator");
        initiatorBroker.setPersistent(false);
        initiatorBroker.setUseJmx(false);
        initiatorBroker.addConnector("tcp://127.0.0.1:0");
        initiatorBroker.start();
        initiatorBroker.waitUntilStarted();

        var responderUri = responderBroker.getTransportConnectors().get(0).getPublishableConnectURI();
        nc = initiatorBroker.addNetworkConnector("static:(" + responderUri + ")");
        nc.setName("duplex-to-responder");
        nc.setDuplex(true);
        // static inclusion is mirrored onto the responder half via the BrokerInfo config, so
        // forwarding works in both directions; this test covers lifecycle, not demand propagation
        nc.addStaticallyIncludedDestination(new ActiveMQQueue(QUEUE_TO_RESPONDER));
        nc.addStaticallyIncludedDestination(new ActiveMQQueue(QUEUE_TO_INITIATOR));
        nc.start();
        assertTrue("duplex bridge should establish",
                Wait.waitFor(() -> !nc.activeBridges().isEmpty(), 20_000, 10));

        // consumers on both brokers create demand in both directions across the one duplex bridge
        responderConnection = new ActiveMQConnectionFactory(responderUri).createConnection();
        responderConnection.start();
        responderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
                .createConsumer(new ActiveMQQueue(QUEUE_TO_RESPONDER))
                .setMessageListener(m -> responderReceived.incrementAndGet());

        var initiatorUri = initiatorBroker.getTransportConnectors().get(0).getPublishableConnectURI();
        initiatorConnection = new ActiveMQConnectionFactory(initiatorUri).createConnection();
        initiatorConnection.start();
        initiatorConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
                .createConsumer(new ActiveMQQueue(QUEUE_TO_INITIATOR))
                .setMessageListener(m -> initiatorReceived.incrementAndGet());
    }

    @After
    public void tearDown() throws Exception {
        for (var c : new Connection[] { initiatorConnection, responderConnection }) {
            if (c != null) {
                try { c.close(); } catch (Exception ignored) {}
            }
        }
        if (initiatorBroker != null) {
            initiatorBroker.stop();
            initiatorBroker.waitUntilStopped();
        }
        if (responderBroker != null) {
            responderBroker.stop();
            responderBroker.waitUntilStopped();
        }
    }

    /** Server-side close of the initiator half's local vm:// connection. */
    @Test(timeout = 120_000)
    public void testDuplexBridgeReconnectsAfterInitiatorLocalConnectionClose() throws Exception {
        assertFlowBothWays("before initiator-side close");

        var oldBridge = nc.activeBridges().iterator().next();
        var vmConnection = findVmConnection(initiatorBroker);
        assertNotNull("expected the initiator bridge's local vm:// connection", vmConnection);
        LOG.info("stopping initiator-side local connection server-side: {}", vmConnection);
        vmConnection.serviceException(new IOException("stopping initiator-side local connection server-side"));

        awaitNewBridge(oldBridge, "after initiator-side close");
        assertFlowBothWays("after initiator-side close and reconnect");
    }

    /** Server-side close of the responder half's local vm:// connection. */
    @Test(timeout = 120_000)
    public void testDuplexBridgeReconnectsAfterResponderLocalConnectionClose() throws Exception {
        assertFlowBothWays("before responder-side close");

        var oldBridge = nc.activeBridges().iterator().next();
        var vmConnection = findVmConnection(responderBroker);
        assertNotNull("expected the responder bridge half's local vm:// connection", vmConnection);
        LOG.info("stopping responder-side local connection server-side: {}", vmConnection);
        vmConnection.stop();

        awaitNewBridge(oldBridge, "after responder-side close");
        assertFlowBothWays("after responder-side close and reconnect");
    }

    /**
     * An orderly responder broker shutdown sends ShutdownInfo over the remote side. The initiator
     * bridge should stop and stay stopped, not linger in activeBridges retrying against the
     * stopped broker.
     */
    @Test(timeout = 120_000)
    public void testDuplexBridgeStopsOnResponderBrokerShutdown() throws Exception {
        assertFlowBothWays("before responder broker shutdown");

        responderBroker.stop();
        responderBroker.waitUntilStopped();

        assertTrue("initiator bridge should be gone after an orderly responder broker shutdown",
                Wait.waitFor(() -> nc.activeBridges().isEmpty(), 20_000, 10));
    }

    private void awaitNewBridge(NetworkBridge oldBridge, String label) throws Exception {
        assertTrue("duplex bridge must reconnect with a new instance " + label,
                Wait.waitFor(() -> {
                    for (var bridge : nc.activeBridges()) {
                        if (bridge != oldBridge) {
                            return true;
                        }
                    }
                    return false;
                }, 30_000, 10));
        assertTrue("old duplex bridge instance must be deregistered " + label,
                Wait.waitFor(() -> nc.activeBridges().size() == 1
                        && !nc.activeBridges().contains(oldBridge), 20_000, 10));
        LOG.info("duplex bridge reconnected {}", label);
    }

    /** Messages must cross both ways over the duplex bridge. */
    private void assertFlowBothWays(String label) throws Exception {
        var responderBefore = responderReceived.get();
        produce(initiatorBroker, QUEUE_TO_RESPONDER, 5);
        assertTrue("initiator->responder flow " + label + " (responder had " + responderBefore + ")",
                Wait.waitFor(() -> responderReceived.get() >= responderBefore + 5, 30_000, 10));

        var initiatorBefore = initiatorReceived.get();
        produce(responderBroker, QUEUE_TO_INITIATOR, 5);
        assertTrue("responder->initiator flow " + label + " (initiator had " + initiatorBefore + ")",
                Wait.waitFor(() -> initiatorReceived.get() >= initiatorBefore + 5, 30_000, 10));
    }

    private void produce(BrokerService broker, String queueName, int n) throws Exception {
        try (var c = new ActiveMQConnectionFactory(
                broker.getTransportConnectors().get(0).getPublishableConnectURI()).createConnection();
             var session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(new ActiveMQQueue(queueName))) {
            c.start();

            for (var i = 0; i < n; i++) {
                producer.send(session.createTextMessage("m-" + i));
            }
        }
    }

    private TransportConnection findVmConnection(BrokerService broker) throws Exception {
        for (var connection : ((RegionBroker) broker.getRegionBroker()).getClients()) {
            var address = connection.getRemoteAddress();
            if (address != null && address.startsWith("vm:") && connection instanceof TransportConnection) {
                return (TransportConnection) connection;
            }
        }
        return null;
    }
}
