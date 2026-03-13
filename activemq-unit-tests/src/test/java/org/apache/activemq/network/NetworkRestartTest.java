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

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkRestartTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkRestartTest.class);

    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;

    protected final ActiveMQQueue included = new ActiveMQQueue("include.test.foo");

    public void testConnectorRestart() throws Exception {
        final MessageConsumer remoteConsumer = remoteSession.createConsumer(included);
        final MessageProducer localProducer = localSession.createProducer(included);

        localProducer.send(localSession.createTextMessage("before"));
        final Message before = remoteConsumer.receive(5000);
        assertNotNull(before);
        assertEquals("before", ((TextMessage) before).getText());

        // Wait for the network connector bridge to be fully established before stopping
        final NetworkConnector connector = localBroker.getNetworkConnectorByName("networkConnector");
        assertNotNull("networkConnector must exist", connector);

        assertTrue("bridge should be active before stop",
            Wait.waitFor(() -> !connector.activeBridges().isEmpty(),
                TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(100)));

        LOG.info("Stopping connector");
        connector.stop();

        // Wait until the connector has fully stopped (no active bridges)
        assertTrue("bridge should stop",
            Wait.waitFor(() -> connector.activeBridges().isEmpty(),
                TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(100)));

        LOG.info("Starting connector");
        connector.start();

        // Wait until the bridge is re-established and fully started
        waitForBridgeFullyStarted(connector);

        localProducer.send(localSession.createTextMessage("after"));
        final Message after = remoteConsumer.receive(5000);
        assertNotNull("should receive message after connector restart", after);
        assertEquals("after", ((TextMessage) after).getText());
    }

    public void testConnectorReAdd() throws Exception {
        final MessageConsumer remoteConsumer = remoteSession.createConsumer(included);
        final MessageProducer localProducer = localSession.createProducer(included);

        localProducer.send(localSession.createTextMessage("before"));
        final Message before = remoteConsumer.receive(5000);
        assertNotNull(before);
        assertEquals("before", ((TextMessage) before).getText());

        // Wait for the network connector bridge to be fully established before removing
        final NetworkConnector connector = localBroker.getNetworkConnectorByName("networkConnector");
        assertNotNull("networkConnector must exist", connector);

        assertTrue("bridge should be active before remove",
            Wait.waitFor(() -> !connector.activeBridges().isEmpty(),
                TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(100)));

        LOG.info("Removing connector");
        connector.stop();
        localBroker.removeNetworkConnector(connector);

        // Wait until the connector has fully stopped
        assertTrue("bridge should stop after removal",
            Wait.waitFor(() -> connector.activeBridges().isEmpty(),
                TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(100)));

        LOG.info("Re-adding connector");
        localBroker.addNetworkConnector(connector);
        connector.start();

        // Wait until the bridge is re-established and fully started
        waitForBridgeFullyStarted(connector);

        localProducer.send(localSession.createTextMessage("after"));
        final Message after = remoteConsumer.receive(5000);
        assertNotNull("should receive message after connector re-add", after);
        assertEquals("after", ((TextMessage) after).getText());
    }

    protected void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
        doSetUp();
    }

    protected void tearDown() throws Exception {
        localBroker.deleteAllMessages();
        remoteBroker.deleteAllMessages();
        doTearDown();
        super.tearDown();
    }

    protected void doTearDown() throws Exception {
        localConnection.close();
        remoteConnection.close();
        localBroker.stop();
        localBroker.waitUntilStopped();
        remoteBroker.stop();
        remoteBroker.waitUntilStopped();
    }

    protected void doSetUp() throws Exception {
        remoteBroker = createRemoteBroker();
        remoteBroker.setDeleteAllMessagesOnStartup(true);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();

        final URI remoteConnectUri = remoteBroker.getTransportConnectorByScheme("tcp").getConnectUri();

        localBroker = createLocalBroker(remoteConnectUri);
        localBroker.setDeleteAllMessagesOnStartup(true);
        localBroker.start();
        localBroker.waitUntilStarted();

        final URI localConnectUri = localBroker.getTransportConnectorByScheme("tcp").getConnectUri();

        // Wait for the network bridge to be fully started before creating connections
        final NetworkConnector nc = localBroker.getNetworkConnectorByName("networkConnector");
        assertNotNull("networkConnector should exist after broker start", nc);
        waitForBridgeFullyStarted(nc);

        final ActiveMQConnectionFactory localFac = new ActiveMQConnectionFactory(localConnectUri);
        localConnection = localFac.createConnection();
        localConnection.setClientID("local");
        localConnection.start();

        final ActiveMQConnectionFactory remoteFac = new ActiveMQConnectionFactory(remoteConnectUri);
        remoteFac.setWatchTopicAdvisories(false);
        remoteConnection = remoteFac.createConnection();
        remoteConnection.setClientID("remote");
        remoteConnection.start();

        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected BrokerService createRemoteBroker() throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setBrokerName("remoteBroker");
        broker.setUseJmx(false);
        broker.setPersistent(true);
        broker.setUseShutdownHook(false);
        broker.setMonitorConnectionSplits(false);
        broker.addConnector("tcp://localhost:0");
        return broker;
    }

    protected BrokerService createLocalBroker(final URI remoteUri) throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setBrokerName("localBroker");
        broker.setPersistent(true);
        broker.setUseShutdownHook(false);
        broker.setMonitorConnectionSplits(true);
        broker.addConnector("tcp://localhost:0");

        final DiscoveryNetworkConnector networkConnector = new DiscoveryNetworkConnector(
            new URI("static:(" + remoteUri + ")"));
        networkConnector.setName("networkConnector");
        configureNetworkConnector(networkConnector);
        broker.addNetworkConnector(networkConnector);

        return broker;
    }

    /**
     * Configure the network connector. Subclasses can override to customize
     * (e.g., remove destination filtering for plain mode).
     */
    protected void configureNetworkConnector(final DiscoveryNetworkConnector networkConnector) {
        networkConnector.setDynamicOnly(false);
        networkConnector.setConduitSubscriptions(true);
        networkConnector.setDecreaseNetworkConsumerPriority(false);
        networkConnector.setDynamicallyIncludedDestinations(
            List.of(new ActiveMQQueue("include.test.foo"),
                new ActiveMQTopic("include.test.bar")));
        networkConnector.setExcludedDestinations(
            List.of(new ActiveMQQueue("exclude.test.foo"),
                new ActiveMQTopic("exclude.test.bar")));
    }

    private void waitForBridgeFullyStarted(final NetworkConnector connector) throws Exception {
        assertTrue("bridge should be fully started",
            Wait.waitFor(() -> {
                for (final NetworkBridge bridge : connector.activeBridges()) {
                    if (bridge instanceof DemandForwardingBridgeSupport) {
                        final DemandForwardingBridgeSupport dfBridge = (DemandForwardingBridgeSupport) bridge;
                        if (dfBridge.startedLatch.getCount() == 0) {
                            return true;
                        }
                    }
                }
                return false;
            }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(100)));
    }
}
