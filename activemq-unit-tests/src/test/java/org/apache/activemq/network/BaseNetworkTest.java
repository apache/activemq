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

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class BaseNetworkTest {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;

    @Before
    public final void setUp() throws Exception {
        doSetUp(true);
    }

    @After
    public final void tearDown() throws Exception {
        doTearDown();
    }

    protected void doTearDown() throws Exception {
        if(localConnection != null)
            localConnection.close();

        if(remoteConnection != null)
            remoteConnection.close();

        if(localBroker != null) {
            localBroker.stop();
            localBroker.waitUntilStopped();
        }

        if(remoteBroker != null) {
            remoteBroker.stop();
            remoteBroker.waitUntilStopped();
        }
    }

    protected void doSetUp(boolean deleteAllMessages) throws Exception {
        remoteBroker = createRemoteBroker();
        remoteBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        localBroker = createLocalBroker();
        localBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        localBroker.start();
        localBroker.waitUntilStarted();

        // Programmatically add network connectors using the actual assigned ephemeral ports.
        // Use startNetworkConnector() instead of connector.start() to ensure proper JMX MBean registration.
        addNetworkConnectors();

        // Wait for both network bridges to be FULLY started (advisory consumers registered).
        // activeBridges().isEmpty() is NOT sufficient because bridges are added to the map
        // before start() completes asynchronously. We must wait for the startedLatch.
        waitForBridgeFullyStarted(localBroker, "Local");
        waitForBridgeFullyStarted(remoteBroker, "Remote");

        final URI localURI = localBroker.getVmConnectorURI();
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(localURI);
        fac.setAlwaysSyncSend(true);
        fac.setDispatchAsync(false);
        localConnection = fac.createConnection();
        localConnection.setClientID("clientId");
        localConnection.start();
        final URI remoteURI = remoteBroker.getVmConnectorURI();
        fac = new ActiveMQConnectionFactory(remoteURI);
        remoteConnection = fac.createConnection();
        remoteConnection.setClientID("clientId");
        remoteConnection.start();
        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Programmatically adds network connectors between the local and remote brokers
     * using the actual assigned ephemeral ports. This avoids hardcoding ports in XML
     * config files which causes port conflicts on CI.
     */
    protected void addNetworkConnectors() throws Exception {
        final URI remoteConnectURI = remoteBroker.getTransportConnectors().get(0).getConnectUri();
        final URI localConnectURI = localBroker.getTransportConnectors().get(0).getConnectUri();

        // Local -> Remote network connector (matches the original localBroker.xml config)
        final DiscoveryNetworkConnector localToRemote = new DiscoveryNetworkConnector(
                new URI("static:(" + remoteConnectURI + ")"));
        localToRemote.setName("networkConnector");
        localToRemote.setDynamicOnly(false);
        localToRemote.setConduitSubscriptions(true);
        localToRemote.setDecreaseNetworkConsumerPriority(false);

        final List<ActiveMQDestination> dynamicallyIncluded = new ArrayList<>();
        dynamicallyIncluded.add(new ActiveMQQueue("include.test.foo"));
        dynamicallyIncluded.add(new ActiveMQTopic("include.test.bar"));
        localToRemote.setDynamicallyIncludedDestinations(dynamicallyIncluded);

        final List<ActiveMQDestination> excluded = new ArrayList<>();
        excluded.add(new ActiveMQQueue("exclude.test.foo"));
        excluded.add(new ActiveMQTopic("exclude.test.bar"));
        localToRemote.setExcludedDestinations(excluded);

        localBroker.addNetworkConnector(localToRemote);
        // startNetworkConnector handles JMX MBean registration and connector startup
        localBroker.startNetworkConnector(localToRemote, null);

        // Remote -> Local network connector (matches the original remoteBroker.xml config)
        final DiscoveryNetworkConnector remoteToLocal = new DiscoveryNetworkConnector(
                new URI("static:(" + localConnectURI + ")"));
        remoteToLocal.setName("networkConnector");
        remoteBroker.addNetworkConnector(remoteToLocal);
        remoteBroker.startNetworkConnector(remoteToLocal, null);
    }

    protected void waitForBridgeFullyStarted(final BrokerService broker, final String label) throws Exception {
        // Skip if broker has no network connectors (e.g., duplex target broker receives
        // bridge connections but doesn't initiate them)
        if (broker.getNetworkConnectors().isEmpty()) {
            return;
        }
        assertTrue(label + " broker bridge should be fully started", Wait.waitFor(() -> {
            if (broker.getNetworkConnectors().get(0).activeBridges().isEmpty()) {
                return false;
            }
            final NetworkBridge bridge = broker.getNetworkConnectors().get(0).activeBridges().iterator().next();
            if (bridge instanceof DemandForwardingBridgeSupport) {
                return ((DemandForwardingBridgeSupport) bridge).startedLatch.getCount() == 0;
            }
            return true;
        }, TimeUnit.SECONDS.toMillis(10), 100));
    }

    protected String getRemoteBrokerURI() {
        return "org/apache/activemq/network/remoteBroker-ephemeral.xml";
    }

    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/localBroker-ephemeral.xml";
    }

    protected BrokerService createBroker(String uri) throws Exception {
        final Resource resource = new ClassPathResource(uri);
        final BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();
        final BrokerService result = factory.getBroker();
        return result;
    }

    protected BrokerService createLocalBroker() throws Exception {
        return createBroker(getLocalBrokerURI());
    }

    protected BrokerService createRemoteBroker() throws Exception {
        return createBroker(getRemoteBrokerURI());
    }
}
