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

import javax.jms.MessageConsumer;

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

    protected void testNonDurableReceiveThrougRestart(String pubBroker, String conBroker) throws Exception {
        NetworkConnector networkConnector = bridgeBrokerPair("BrokerA", "BrokerB");

        startAllBrokers();
        waitForBridgeFormation();

        MessageConsumer client = createDurableSubscriber(conBroker, dest, "sub1");
        client.close();

        Thread.sleep(1000);
        networkConnector.stop();
        Thread.sleep(1000);

        Set<ActiveMQDestination> durableDests = new HashSet<>();
        durableDests.add(dest);
        //Normally set on broker start from the persistence layer but
        //simulate here since we just stopped and started the network connector
        //without a restart
        networkConnector.setDurableDestinations(durableDests);
        networkConnector.start();
        waitForBridgeFormation();

        // Send messages
        sendMessages(pubBroker, dest, 1);
        Thread.sleep(1000);

        Topic destination = (Topic) brokers.get(conBroker).broker.getDestination(dest);
        DurableTopicSubscription sub = destination.getDurableTopicSubs().
                values().toArray(new DurableTopicSubscription[0])[0];

        //Assert that the message made it to the other broker
        assertEquals(1, sub.getSubscriptionStatistics().getEnqueues().getCount());
    }

    @Override
    protected void configureBroker(BrokerService broker) {
        broker.getManagementContext().setCreateConnector(false);
        broker.setAdvisorySupport(true);
    }

    protected NetworkConnector bridgeBrokerPair(String localBrokerName, String remoteBrokerName) throws Exception {
        BrokerService localBroker = brokers.get(localBrokerName).broker;
        BrokerService remoteBroker = brokers.get(remoteBrokerName).broker;

        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI;
        if (!transportConnectors.isEmpty()) {
            remoteURI = transportConnectors.get(0).getConnectUri();
            String uri = "static:(" + remoteURI + ")";
            NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
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
        File dataDir = new File(IOHelper.getDefaultDataDirectory());
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
