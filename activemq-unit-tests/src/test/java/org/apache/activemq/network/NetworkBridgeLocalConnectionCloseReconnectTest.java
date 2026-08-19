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

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A network bridge whose LOCAL connection is closed server-side while the broker keeps running
 * must fail the bridge to signal reconnect.
 */
@Category(ParallelTest.class)
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
        var remoteUri = remoteBroker.getTransportConnectors().get(0).getPublishableConnectURI();
        var nc = localBroker.addNetworkConnector("static:(" + remoteUri + ")");
        nc.setName("to-remote");
        nc.setDuplex(false);
        nc.addStaticallyIncludedDestination(new ActiveMQQueue(QUEUE_NAME));
        nc.start();
        assertTrue("bridge should establish",
                Wait.waitFor(() -> !nc.activeBridges().isEmpty(), 20_000, 10));

        produce(5);
        assertTrue("bridge should forward before the close",
                Wait.waitFor(() -> remoteReceived.get() >= 5, 20_000, 10));

        // The bridge's local side is a vm:// client of the local broker.
        var oldBridge = nc.activeBridges().iterator().next();
        var bridgeLocalConnection = findVmConnection(localBroker);
        assertNotNull("expected the bridge's local vm:// connection on the local broker", bridgeLocalConnection);
        LOG.info("stopping the bridge's local connection server-side: {}", bridgeLocalConnection);
        bridgeLocalConnection.stop();

        // Connection stop is different from broker stop
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

    private void produce(int n) throws Exception {
        try (var c = new ActiveMQConnectionFactory(
                localBroker.getTransportConnectors().isEmpty()
                        ? localBroker.getVmConnectorURI()
                        : localBroker.getTransportConnectors().get(0).getPublishableConnectURI()).createConnection()) {
            c.start();
            var session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            var producer = session.createProducer(new ActiveMQQueue(QUEUE_NAME));
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
