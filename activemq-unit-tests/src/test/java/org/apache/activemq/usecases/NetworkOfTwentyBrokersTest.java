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

import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.ThreadTracker;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkOfTwentyBrokersTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkOfTwentyBrokersTest.class);

    // This will interconnect all brokers using multicast
    protected void bridgeAllBrokers() throws Exception {
        bridgeAllBrokers("TwentyBrokersTest", 1, false, false);
    }

    protected void bridgeAllBrokers(String groupName, int ttl, boolean suppressduplicateQueueSubs) throws Exception {
        bridgeAllBrokers(groupName, ttl, suppressduplicateQueueSubs, false);
    }

    protected void bridgeAllBrokers(String groupName, int ttl, boolean suppressduplicateQueueSubs, boolean decreasePriority) throws Exception {
        Collection<BrokerItem> brokerList = brokers.values();
        for (Iterator<BrokerItem> i = brokerList.iterator(); i.hasNext();) {
            BrokerService broker = i.next().broker;
            List<TransportConnector> transportConnectors = broker.getTransportConnectors();

            if (transportConnectors.isEmpty()) {
                broker.addConnector(new URI(AUTO_ASSIGN_TRANSPORT));
                transportConnectors = broker.getTransportConnectors();
            }

            TransportConnector transport = transportConnectors.get(0);
            if (transport.getDiscoveryUri() == null) {
                transport.setDiscoveryUri(new URI("multicast://default?group=" + groupName));
            }

            List<NetworkConnector> networkConnectors = broker.getNetworkConnectors();
            if (networkConnectors.isEmpty()) {
                broker.addNetworkConnector("multicast://default?group=" + groupName);
                networkConnectors = broker.getNetworkConnectors();
            }

            NetworkConnector nc = networkConnectors.get(0);
            nc.setNetworkTTL(ttl);
            nc.setSuppressDuplicateQueueSubscriptions(suppressduplicateQueueSubs);
            nc.setDecreaseNetworkConsumerPriority(decreasePriority);
        }

        // Multicasting may take longer to setup
        maxSetupTime = 8000;
    }

    protected BrokerService createBroker(String brokerName) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setBrokerName(brokerName);
        broker.addConnector(new URI(AUTO_ASSIGN_TRANSPORT));
        brokers.put(brokerName, new BrokerItem(broker));

        return broker;
    }

    /* AMQ-3077 Bug */
    public void testBrokers() throws Exception {
        int X = 20;
        int i;

        LOG.info("Creating X Brokers");
        for (i = 0; i < X; i++) {
            createBroker("Broker" + i);
        }

        bridgeAllBrokers();
        startAllBrokers();
        waitForBridgeFormation(X-1);

        LOG.info("Waiting for complete formation");
        try {
            Thread.sleep(20000);
        } catch (Exception e) {
        }

        verifyPeerBrokerInfos(X-1);

        LOG.info("Stopping half the brokers");
        for (i = 0; i < X/2; i++) {
            destroyBroker("Broker" + i);
        }

        LOG.info("Waiting for complete stop");
        try {
            Thread.sleep(20000);
        } catch (Exception e) {
        }

        verifyPeerBrokerInfos((X/2) - 1);

        LOG.info("Recreating first half");
        for (i = 0; i < X/2; i++) {
            createBroker("Broker" + i);
        }

        bridgeAllBrokers();
        startAllBrokers();
        waitForBridgeFormation(X-1);

        LOG.info("Waiting for complete reformation");
        try {
            Thread.sleep(20000);
        } catch (Exception e) {
        }

        verifyPeerBrokerInfos(X-1);
    }

    public void testPeerBrokerCountHalfPeer() throws Exception {
        createBroker("A");
        createBroker("B");
        bridgeBrokers("A", "B");
        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);
        verifyPeerBrokerInfo(brokers.get("B"), 0);
    }

    public void testPeerBrokerCountHalfPeerTwice() throws Exception {
        createBroker("A");
        createBroker("B");
        bridgeBrokers("A", "B");
        bridgeBrokers("A", "B");
        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);
        verifyPeerBrokerInfo(brokers.get("B"), 0);
    }

    public void testPeerBrokerCountFullPeer() throws Exception {
        createBroker("A");
        createBroker("B");
        bridgeBrokers("A", "B");
        bridgeBrokers("B", "A");
        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);
        verifyPeerBrokerInfo(brokers.get("B"), 1);
    }

    public void testPeerBrokerCountFullPeerDuplex() throws Exception {
        createBroker("A");
        createBroker("B");
        NetworkConnector nc = bridgeBrokers("A", "B");
        nc.setDuplex(true);
        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);
        verifyPeerBrokerInfo(brokers.get("B"), 1);
    }


    private void verifyPeerBrokerInfo(BrokerItem brokerItem, final int max) throws Exception {
        final BrokerService broker = brokerItem.broker;
        final RegionBroker regionBroker = (RegionBroker) broker.getRegionBroker();
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("verify infos " + broker.getBrokerName() + ", len: " + regionBroker.getPeerBrokerInfos().length);
                return max == regionBroker.getPeerBrokerInfos().length;
            }
         }, 120 * 1000);
        LOG.info("verify infos " + broker.getBrokerName() + ", len: " + regionBroker.getPeerBrokerInfos().length);
        for (BrokerInfo info : regionBroker.getPeerBrokerInfos()) {
            LOG.info(info.getBrokerName());
        }
        assertEquals(broker.getBrokerName(), max, regionBroker.getPeerBrokerInfos().length);
    }

    private void verifyPeerBrokerInfos(final int max) throws Exception {
        Collection<BrokerItem> brokerList = brokers.values();
        for (Iterator<BrokerItem> i = brokerList.iterator(); i.hasNext();) {
            verifyPeerBrokerInfo(i.next(), max);
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadTracker.result();
    }
}
