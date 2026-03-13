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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import jakarta.jms.MessageProducer;
import jakarta.jms.TemporaryQueue;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuplexNetworkTest extends SimpleNetworkTest {
    private static final Logger LOG = LoggerFactory.getLogger(DuplexNetworkTest.class);

    @Override
    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/duplexLocalBroker-ephemeral.xml";
    }

    @Override
    protected BrokerService createRemoteBroker() throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setBrokerName("remoteBroker");
        broker.addConnector("tcp://localhost:0");
        return broker;
    }

    @Override
    protected void addNetworkConnectors() throws Exception {
        // Add a duplex network connector from localBroker to remoteBroker using the actual
        // assigned ephemeral port (matching the original duplexLocalBroker.xml config but
        // without hardcoded ports).
        final URI remoteConnectURI = remoteBroker.getTransportConnectors().get(0).getConnectUri();

        final DiscoveryNetworkConnector duplexConnector = new DiscoveryNetworkConnector(
                new URI("static:(" + remoteConnectURI + ")"));
        duplexConnector.setName("networkConnector");
        duplexConnector.setDuplex(true);
        duplexConnector.setDynamicOnly(false);
        duplexConnector.setConduitSubscriptions(true);
        duplexConnector.setDecreaseNetworkConsumerPriority(false);

        final List<ActiveMQDestination> excluded = new ArrayList<>();
        excluded.add(new ActiveMQQueue("exclude.test.foo"));
        excluded.add(new ActiveMQTopic("exclude.test.bar"));
        duplexConnector.setExcludedDestinations(excluded);

        localBroker.addNetworkConnector(duplexConnector);
        localBroker.startNetworkConnector(duplexConnector, null);
    }

    @Test
    public void testTempQueues() throws Exception {
        final TemporaryQueue temp = localSession.createTemporaryQueue();
        final MessageProducer producer = localSession.createProducer(temp);
        producer.send(localSession.createTextMessage("test"));

        assertTrue("Destination not created", Wait.waitFor(
            () -> remoteBroker.getAdminView().getTemporaryQueues().length == 1,
            TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(100)));

        temp.delete();

        assertTrue("Destination not deleted", Wait.waitFor(
            () -> remoteBroker.getAdminView().getTemporaryQueues().length == 0));
    }

    @Test
    public void testStaysUp() throws Exception {
        final int bridgeIdentity = getBridgeId();
        LOG.info("Bridges: " + bridgeIdentity);
        TimeUnit.SECONDS.sleep(5);
        assertEquals("Same bridges", bridgeIdentity, getBridgeId());
    }

    private int getBridgeId() {
        int id = 0;
        while (id == 0) {
            try {
                id = localBroker.getNetworkConnectors().get(0).activeBridges().iterator().next().hashCode();
            } catch (Throwable tryAgainInABit) {
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException ignored) {
                }
            }
        }
        return id;
    }

    @Override
    protected void assertNetworkBridgeStatistics(final long expectedLocalSent, final long expectedRemoteSent) throws Exception {

        final NetworkBridge localBridge = localBroker.getNetworkConnectors().get(0).activeBridges().iterator().next();

        assertTrue(Wait.waitFor(() ->
            expectedLocalSent == localBridge.getNetworkBridgeStatistics().getDequeues().getCount() &&
            expectedRemoteSent == localBridge.getNetworkBridgeStatistics().getReceivedCount().getCount()
        ));

    }

}
