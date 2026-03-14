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

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import jakarta.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Show that both directions of a duplex bridge will properly restart the
 * network durable consumers if dynamicOnly is false.
 */
public class AMQ6366Test extends JmsMultipleBrokersTestSupport {
    protected static final Logger LOG = LoggerFactory.getLogger(AMQ6366Test.class);
    final ActiveMQTopic dest = new ActiveMQTopic("TEST.FOO");


    /**
     * This test works even before AMQ6366
     * @throws Exception
     */
    public void testDuplexDurableSubRestarted() throws Exception {
        testNonDurableReceiveThrougRestart("BrokerA", "BrokerB");
    }

    /**
     * This test failed before AMQ6366 because the NC durable consumer was
     * never properly activated.
     *
     * @throws Exception
     */
    public void testDuplexDurableSubRestartedReverse() throws Exception {
        testNonDurableReceiveThrougRestart("BrokerB", "BrokerA");
    }

    protected void testNonDurableReceiveThrougRestart(final String pubBroker, final String conBroker) throws Exception {
        final NetworkConnector networkConnector = bridgeBrokerPair("BrokerA", "BrokerB");

        startAllBrokers();
        waitForBridgeFormation();

        final MessageConsumer client = createDurableSubscriber(conBroker, dest, "sub1");
        client.close();

        // Wait for the durable subscription to become inactive after closing the consumer
        final Topic conBrokerDest = (Topic) brokers.get(conBroker).broker.getDestination(dest);
        assertTrue("Durable sub should become inactive after close",
                Wait.waitFor(() -> {
                    final DurableTopicSubscription[] subs = conBrokerDest.getDurableTopicSubs()
                            .values().toArray(new DurableTopicSubscription[0]);
                    return subs.length > 0 && !subs[0].isActive();
                }, 5000, 100));

        networkConnector.stop();

        // Wait for the network connector to fully stop
        assertTrue("Network connector should stop",
                Wait.waitFor(networkConnector::isStopped, 5000, 100));

        final Set<ActiveMQDestination> durableDests = new HashSet<>();
        durableDests.add(dest);
        //Normally set on broker start from the persistence layer but
        //simulate here since we just stopped and started the network connector
        //without a restart
        networkConnector.setDurableDestinations(durableDests);
        networkConnector.start();
        waitForBridgeFormation();

        // Wait for the network bridge to re-establish its demand subscription on the
        // publishing broker. waitForBridgeFormation() only verifies the bridge is connected,
        // but setupStaticDestinations() (which creates the durable demand subscription)
        // runs asynchronously after bridge connection. Without this wait, sendMessages()
        // can fire before the demand subscription is set up, causing the message to be
        // published with no subscriber to forward it to the consumer broker.
        // We check for an active durable subscription because the durable sub may already
        // exist (inactive) from the previous bridge; we need it to be reactivated.
        final Topic pubBrokerDest = (Topic) brokers.get(pubBroker).broker.getDestination(dest);
        assertTrue("Network durable subscription should be active on " + pubBroker,
                Wait.waitFor(() -> pubBrokerDest.getDurableTopicSubs().values().stream()
                                .anyMatch(DurableTopicSubscription::isActive),
                        10000, 100));

        // Send messages
        sendMessages(pubBroker, dest, 1);

        // Wait for the message to be enqueued through the network bridge
        final Topic destination = (Topic) brokers.get(conBroker).broker.getDestination(dest);
        assertTrue("Message should be enqueued to durable subscription",
                Wait.waitFor(() -> {
                    final DurableTopicSubscription sub = destination.getDurableTopicSubs()
                            .values().toArray(new DurableTopicSubscription[0])[0];
                    return sub.getSubscriptionStatistics().getEnqueues().getCount() == 1;
                }, 10000, 100));
    }

    @Override
    protected void configureBroker(BrokerService broker) {
        broker.getManagementContext().setCreateConnector(false);
        broker.setAdvisorySupport(true);
    }

    protected NetworkConnector bridgeBrokerPair(final String localBrokerName, final String remoteBrokerName) throws Exception {
        final BrokerService localBroker = brokers.get(localBrokerName).broker;
        final BrokerService remoteBroker = brokers.get(remoteBrokerName).broker;

        final List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        if (!transportConnectors.isEmpty()) {
            final URI remoteURI = transportConnectors.get(0).getConnectUri();
            final String uri = "static:(" + remoteURI + ")";
            final NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
            connector.setDynamicOnly(false); // so matching durable subs are loaded on start
            connector.setStaticBridge(false);
            connector.setDuplex(true);
            connector.addDynamicallyIncludedDestination(dest);
            localBroker.addNetworkConnector(connector);
            return connector;
        } else {
            throw new Exception("Remote broker has no registered connectors.");
        }
    }

    @Override
    public void setUp() throws Exception {
        final File dataDir = new File(IOHelper.getDefaultDataDirectory());
        LOG.info("Delete dataDir.." + dataDir.getCanonicalPath());
        org.apache.activemq.TestSupport.recursiveDelete(dataDir);
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI(
                "broker:(tcp://0.0.0.0:0)/BrokerA"));
        createBroker(new URI(
                "broker:(tcp://0.0.0.0:0)/BrokerB"));

    }
}
