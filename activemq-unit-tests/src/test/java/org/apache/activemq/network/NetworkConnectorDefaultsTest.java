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

import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ConsumerInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkConnectorDefaultsTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkConnectorDefaultsTest.class);

    private static final String LOCAL_BROKER_TRANSPORT_URI = "tcp://localhost:61616";
    private static final String REMOTE_BROKER_TRANSPORT_URI = "tcp://localhost:61617";
    private static final String DESTINATION_NAME = "TEST.RECONNECT";

    private BrokerService localBroker;
    private BrokerService remoteBroker;

    @Test
    public void testDefaultValues() throws Exception {
        LOG.info("testIsStarted is starting...");

        LOG.info("Adding network connector...");
        NetworkConnector nc = localBroker.addNetworkConnector("static:(" + REMOTE_BROKER_TRANSPORT_URI + ")");
        nc.setName("NC1");

        // Check values before calling .start()
        assertEquals(Integer.valueOf(75), Integer.valueOf(nc.getAdvisoryAckPercentage()));
        assertEquals(Integer.valueOf(0), Integer.valueOf(nc.getAdvisoryPrefetchSize()));
        assertEquals(Integer.valueOf(ConsumerInfo.NETWORK_CONSUMER_PRIORITY), Integer.valueOf(nc.getConsumerPriorityBase()));
        assertEquals(Integer.valueOf(1), Integer.valueOf(nc.getConsumerTTL()));
        assertEquals(Long.valueOf(60 * 1000l), Long.valueOf(nc.getGcSweepTime()));
        assertEquals(Integer.valueOf(1), Integer.valueOf(nc.getMessageTTL()));
        assertEquals(Integer.valueOf(1), Integer.valueOf(nc.getNetworkTTL()));
        assertEquals(Integer.valueOf(1000), Integer.valueOf(nc.getPrefetchSize()));
        assertFalse(nc.isAdvisoryForFailedForward());
        assertTrue(nc.isAlwaysSyncSend());
        assertTrue(nc.isBridgeTempDestinations());
        assertFalse(nc.isCheckDuplicateMessagesOnDuplex());
        assertFalse(nc.isConduitNetworkQueueSubscriptions());
        assertTrue(nc.isDecreaseNetworkConsumerPriority());
        assertTrue(nc.isDispatchAsync());
        assertFalse(nc.isDuplex());
        assertFalse(nc.isDynamicOnly());
        assertTrue(nc.isGcDestinationViews());
        assertFalse(nc.isStaticBridge());
        assertFalse(nc.isSuppressDuplicateQueueSubscriptions());
        assertTrue(nc.isSuppressDuplicateTopicSubscriptions());
        assertFalse(nc.isSyncDurableSubs());
        assertTrue(nc.isUseBrokerNamesAsIdSeed());
        assertFalse(nc.isUseCompression());
        assertFalse(nc.isUseVirtualDestSubs());
    }

   

    @Override
    protected void setUp() throws Exception {
        LOG.info("Setting up LocalBroker");
        localBroker = new BrokerService();
        localBroker.setBrokerName("LocalBroker");
        localBroker.setUseJmx(false);
        localBroker.setPersistent(false);
        localBroker.setTransportConnectorURIs(new String[]{LOCAL_BROKER_TRANSPORT_URI});
        localBroker.start();
        localBroker.waitUntilStarted();

        LOG.info("Setting up RemoteBroker");
        remoteBroker = new BrokerService();
        remoteBroker.setBrokerName("RemoteBroker");
        remoteBroker.setUseJmx(false);
        remoteBroker.setPersistent(false);
        remoteBroker.setTransportConnectorURIs(new String[]{REMOTE_BROKER_TRANSPORT_URI});
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
    }

    @Override
    protected void tearDown() throws Exception {
        if (localBroker.isStarted()) {
            LOG.info("Stopping LocalBroker");
            localBroker.stop();
            localBroker.waitUntilStopped();
            localBroker = null;
        }

        if (remoteBroker.isStarted()) {
            LOG.info("Stopping RemoteBroker");
            remoteBroker.stop();
            remoteBroker.waitUntilStopped();
            remoteBroker = null;
        }
    }
}