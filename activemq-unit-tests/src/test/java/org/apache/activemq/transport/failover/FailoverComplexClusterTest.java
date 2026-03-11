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
package org.apache.activemq.transport.failover;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.PublishedAddressPolicy;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Complex cluster test that will exercise the dynamic failover capabilities of
 * a network of brokers. Using a networking of 3 brokers where the 3rd broker is
 * removed and then added back in it is expected in each test that the number of
 * connections on the client should start with 3, then have two after the 3rd
 * broker is removed and then show 3 after the 3rd broker is reintroduced.
 */
public class FailoverComplexClusterTest extends FailoverClusterTestSupport {
    protected final Logger LOG = LoggerFactory.getLogger(FailoverComplexClusterTest.class);

    private static final String BROKER_A_NAME = "BROKERA";
    private static final String BROKER_B_NAME = "BROKERB";
    private static final String BROKER_C_NAME = "BROKERC";
    private static final String EPHEMERAL_BIND_ADDRESS = "tcp://127.0.0.1:0";

    // Resolved addresses after broker start (set dynamically per test)
    private String brokerAClientAddr;
    private String brokerBClientAddr;
    private String brokerCClientAddr;
    private String brokerANobAddr;
    private String brokerBNobAddr;
    private String brokerCNobAddr;

    /**
     * Basic dynamic failover 3 broker test
     *
     * @throws Exception
     */
    public void testThreeBrokerClusterSingleConnectorBasic() throws Exception {

        initSingleTcBroker("", null, null);

        setClientUrl("failover://(" + brokerAClientAddr + "," + brokerBClientAddr + ")");
        createClients();

        runTests(false, null, null, null);
    }

    /**
     * Tests a 3 broker configuration to ensure that the backup is random and
     * supported in a cluster. useExponentialBackOff is set to false and
     * maxReconnectAttempts is set to 1 to move through the list quickly for
     * this test.
     *
     * @throws Exception
     */
    public void testThreeBrokerClusterSingleConnectorBackupFailoverConfig() throws Exception {

        initSingleTcBroker("", null, null);

        setClientUrl("failover://(" + brokerAClientAddr + "," + brokerBClientAddr + ")?backup=true&backupPoolSize=2&useExponentialBackOff=false&initialReconnectDelay=500");
        createClients();

        runTests(false, null, null, null);
    }

    /**
     * Tests a 3 broker cluster that passes in connection params on the
     * transport connector. Prior versions of AMQ passed the TC connection
     * params to the client and this should not happen. The chosen param is not
     * compatible with the client and will throw an error if used.
     *
     * @throws Exception
     */
    public void testThreeBrokerClusterSingleConnectorWithParams() throws Exception {

        initSingleTcBroker("?transport.closeAsync=false", null, null);

        setClientUrl("failover://(" + brokerAClientAddr + "," + brokerBClientAddr + ")");
        createClients();

        runTests(false, null, null, null);
    }

    /**
     * Tests a 3 broker cluster using a cluster filter of *
     *
     * @throws Exception
     */
    public void testThreeBrokerClusterWithClusterFilter() throws Exception {

        initSingleTcBroker("?transport.closeAsync=false", null, null);

        setClientUrl("failover://(" + brokerAClientAddr + "," + brokerBClientAddr + ")");
        createClients();

        runTests(false, null, "*", null);
    }

    /**
     * Test to verify that a broker with multiple transport connections only the
     * one marked to update clients is propagate
     *
     * @throws Exception
     */
    public void testThreeBrokerClusterMultipleConnectorBasic() throws Exception {

        initMultiTcCluster("", null);

        setClientUrl("failover://(" + brokerAClientAddr + "," + brokerBClientAddr + ")");
        createClients();

        runTests(true, null, null, null);
    }

    /**
     * Test to verify the reintroduction of the A Broker
     *
     * @throws Exception
     */
    public void testOriginalBrokerRestart() throws Exception {
        initSingleTcBroker("", null, null);

        setClientUrl("failover://(" + brokerAClientAddr + "," + brokerBClientAddr + ")");
        createClients();

        assertClientsConnectedToThreeBrokers();

        getBroker(BROKER_A_NAME).stop();
        getBroker(BROKER_A_NAME).waitUntilStopped();
        removeBroker(BROKER_A_NAME);

        assertClientsConnectedToTwoBrokers();

        createBrokerA(false, null, null, null);
        getBroker(BROKER_A_NAME).waitUntilStarted();

        // Wait for bridges from the recreated broker A to form
        waitForBridgesFromBroker(BROKER_A_NAME);

        assertClientsConnectedToThreeBrokers();
    }

    /**
     * Test to ensure clients are evenly to all available brokers in the
     * network.
     *
     * @throws Exception
     */
    public void testThreeBrokerClusterClientDistributions() throws Exception {

        initSingleTcBroker("", null, null);

        setClientUrl("failover://(" + brokerAClientAddr + "," + brokerBClientAddr + ")?useExponentialBackOff=false&initialReconnectDelay=500&randomize=false");
        createClients(100);

        runClientDistributionTests(false, null, null, null);
    }

    /**
     * Test to verify that clients are distributed with no less than 20% of the
     * clients on any one broker.
     *
     * @throws Exception
     */
    public void testThreeBrokerClusterDestinationFilter() throws Exception {

        initSingleTcBroker("", null, null);

        setClientUrl("failover://(" + brokerAClientAddr + "," + brokerBClientAddr + ")");
        createClients();

        runTests(false, null, null, "Queue.TEST.FOO.>");
    }

    public void testFailOverWithUpdateClientsOnRemove() throws Exception {
        // Broker A - start first with ephemeral port
        addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
        final TransportConnector connectorA = getBroker(BROKER_A_NAME).addConnector(EPHEMERAL_BIND_ADDRESS);
        connectorA.setName("openwire");
        connectorA.setRebalanceClusterClients(true);
        connectorA.setUpdateClusterClients(true);
        connectorA.setUpdateClusterClientsOnRemove(true); //If set to false the test succeeds.
        connectorA.getPublishedAddressPolicy().setPublishedHostStrategy(PublishedAddressPolicy.PublishedHostStrategy.IPADDRESS);
        getBroker(BROKER_A_NAME).start();
        getBroker(BROKER_A_NAME).waitUntilStarted();
        brokerAClientAddr = connectorA.getPublishableConnectString();

        // Broker B - start with ephemeral port, bridge to A using resolved address
        addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
        final TransportConnector connectorB = getBroker(BROKER_B_NAME).addConnector(EPHEMERAL_BIND_ADDRESS);
        connectorB.setName("openwire");
        connectorB.setRebalanceClusterClients(true);
        connectorB.setUpdateClusterClients(true);
        connectorB.setUpdateClusterClientsOnRemove(true); //If set to false the test succeeds.
        connectorB.getPublishedAddressPolicy().setPublishedHostStrategy(PublishedAddressPolicy.PublishedHostStrategy.IPADDRESS);
        addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + brokerAClientAddr + ")?useExponentialBackOff=false", false, null);
        getBroker(BROKER_B_NAME).start();
        getBroker(BROKER_B_NAME).waitUntilStarted();
        brokerBClientAddr = connectorB.getPublishableConnectString();

        // Now add A->B bridge using B's resolved address and start it
        addAndStartNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + brokerBClientAddr + ")?useExponentialBackOff=false", false, null);

        // Wait for both bridges to form
        assertTrue("A->B bridge should form",
            Wait.waitFor(() -> !getBroker(BROKER_A_NAME).getNetworkConnectors().get(0).activeBridges().isEmpty(),
                TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(500)));
        assertTrue("B->A bridge should form",
            Wait.waitFor(() -> !getBroker(BROKER_B_NAME).getNetworkConnectors().get(0).activeBridges().isEmpty(),
                TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(500)));

        // create client connecting only to A. It should receive broker B address when it connects to A.
        setClientUrl("failover:(" + brokerAClientAddr + ")?useExponentialBackOff=true");
        createClients(1);

        // Wait for all clients to be connected
        assertAllConnected(1);

        // We stop broker A.
        logger.info("Stopping broker A whose address is: {}", brokerAClientAddr);
        getBroker(BROKER_A_NAME).stop();
        getBroker(BROKER_A_NAME).waitUntilStopped();

        // Client should failover to B - wait for it
        assertAllConnectedTo(brokerBClientAddr);
    }

    public void testStaticInfoAvailableAfterPattialUpdate() throws Exception {

        addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
        final TransportConnector connectorA = getBroker(BROKER_A_NAME).addConnector(EPHEMERAL_BIND_ADDRESS);
        connectorA.setName("openwire");
        connectorA.setRebalanceClusterClients(true);
        connectorA.setUpdateClusterClients(true);
        connectorA.getPublishedAddressPolicy().setPublishedHostStrategy(PublishedAddressPolicy.PublishedHostStrategy.IPADDRESS);

        getBroker(BROKER_A_NAME).start();
        getBroker(BROKER_A_NAME).waitUntilStarted();
        brokerAClientAddr = connectorA.getPublishableConnectString();

        // Reserve a port for broker B by using a temporary ServerSocket
        final int brokerBPort;
        try (final ServerSocket ss = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            brokerBPort = ss.getLocalPort();
        }
        final String brokerBBindAddr = "tcp://127.0.0.1:" + brokerBPort;

        setClientUrl("failover://(" + brokerAClientAddr + "?trace=true," + brokerBBindAddr + "?trace=true)?useExponentialBackOff=false&initialReconnectDelay=500");
        createClients(1);

        assertAllConnectedTo(brokerAClientAddr);

        getBroker(BROKER_A_NAME).stop();
        getBroker(BROKER_A_NAME).waitUntilStopped();

        // Now start broker B on the reserved port
        addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
        final TransportConnector connectorB = getBroker(BROKER_B_NAME).addConnector(brokerBBindAddr);
        connectorB.setName("openwire");
        connectorB.setRebalanceClusterClients(true);
        connectorB.setUpdateClusterClients(true);
        connectorB.getPublishedAddressPolicy().setPublishedHostStrategy(PublishedAddressPolicy.PublishedHostStrategy.IPADDRESS);

        getBroker(BROKER_B_NAME).start();
        getBroker(BROKER_B_NAME).waitUntilStarted();

        // verify client connects to B
        assertAllConnectedTo(brokerBBindAddr);
    }

    /**
     * Runs a 3 Broker dynamic failover test: <br/>
     * <ul>
     * <li>asserts clients are distributed across all 3 brokers</li>
     * <li>asserts clients are distributed across 2 brokers after removing the 3rd</li>
     * <li>asserts clients are distributed across all 3 brokers after
     * reintroducing the 3rd broker</li>
     * </ul>
     *
     * @param multi
     * @param tcParams
     * @param clusterFilter
     * @param destinationFilter
     * @throws Exception
     * @throws InterruptedException
     */
    private void runTests(final boolean multi, final String tcParams, final String clusterFilter, final String destinationFilter) throws Exception, InterruptedException {
        assertClientsConnectedToThreeBrokers();

        LOG.info("Stopping BrokerC in prep for restart");
        getBroker(BROKER_C_NAME).stop();
        getBroker(BROKER_C_NAME).waitUntilStopped();
        removeBroker(BROKER_C_NAME);

        assertClientsConnectedToTwoBrokers();

        LOG.info("Recreating BrokerC after stop");
        createBrokerC(multi, tcParams, clusterFilter, destinationFilter);
        getBroker(BROKER_C_NAME).waitUntilStarted();

        // Wait for network bridges from the recreated broker C to form
        waitForBridgesFromBroker(BROKER_C_NAME);

        assertClientsConnectedToThreeBrokers();
    }

    /**
     * @param multi
     * @param tcParams
     * @param clusterFilter
     * @param destinationFilter
     * @throws Exception
     * @throws InterruptedException
     */
    private void runClientDistributionTests(final boolean multi, final String tcParams, final String clusterFilter, final String destinationFilter) throws Exception, InterruptedException {
        assertClientsConnectedToThreeBrokers();
        assertClientsConnectionsEvenlyDistributed(.25);

        getBroker(BROKER_C_NAME).stop();
        getBroker(BROKER_C_NAME).waitUntilStopped();
        removeBroker(BROKER_C_NAME);

        assertClientsConnectedToTwoBrokers();
        assertClientsConnectionsEvenlyDistributed(.35);

        createBrokerC(multi, tcParams, clusterFilter, destinationFilter);
        getBroker(BROKER_C_NAME).waitUntilStarted();

        // Wait for network bridges from the recreated broker C to form
        waitForBridgesFromBroker(BROKER_C_NAME);

        assertClientsConnectedToThreeBrokers();
        assertClientsConnectionsEvenlyDistributed(.20);
    }

    @Override
    protected void setUp() throws Exception {
    }

    @Override
    protected void tearDown() throws Exception {
        shutdownClients();
        destroyBrokerCluster();
    }

    private void initSingleTcBroker(final String params, final String clusterFilter, final String destinationFilter) throws Exception {
        final String tcParams = (params == null) ? "" : params;

        // Phase 1: Create and start all 3 brokers with transport connectors only
        addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
        addTransportConnector(getBroker(BROKER_A_NAME), "openwire", EPHEMERAL_BIND_ADDRESS + tcParams, true);
        getBroker(BROKER_A_NAME).start();
        getBroker(BROKER_A_NAME).waitUntilStarted();
        brokerAClientAddr = getBroker(BROKER_A_NAME).getTransportConnectors().get(0).getPublishableConnectString();

        addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
        addTransportConnector(getBroker(BROKER_B_NAME), "openwire", EPHEMERAL_BIND_ADDRESS + tcParams, true);
        getBroker(BROKER_B_NAME).start();
        getBroker(BROKER_B_NAME).waitUntilStarted();
        brokerBClientAddr = getBroker(BROKER_B_NAME).getTransportConnectors().get(0).getPublishableConnectString();

        addBroker(BROKER_C_NAME, createBroker(BROKER_C_NAME));
        addTransportConnector(getBroker(BROKER_C_NAME), "openwire", EPHEMERAL_BIND_ADDRESS + tcParams, true);
        getBroker(BROKER_C_NAME).start();
        getBroker(BROKER_C_NAME).waitUntilStarted();
        brokerCClientAddr = getBroker(BROKER_C_NAME).getTransportConnectors().get(0).getPublishableConnectString();

        // Phase 2: Add network bridges using resolved addresses and start them
        addAndStartNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + brokerBClientAddr + ")?useExponentialBackOff=false", false, clusterFilter);
        addAndStartNetworkBridge(getBroker(BROKER_A_NAME), "A_2_C_Bridge", "static://(" + brokerCClientAddr + ")?useExponentialBackOff=false", false, null);

        addAndStartNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + brokerAClientAddr + ")?useExponentialBackOff=false", false, clusterFilter);
        addAndStartNetworkBridge(getBroker(BROKER_B_NAME), "B_2_C_Bridge", "static://(" + brokerCClientAddr + ")?useExponentialBackOff=false", false, null);

        addAndStartNetworkBridge(getBroker(BROKER_C_NAME), "C_2_A_Bridge", "static://(" + brokerAClientAddr + ")?useExponentialBackOff=false", false, clusterFilter);
        addAndStartNetworkBridge(getBroker(BROKER_C_NAME), "C_2_B_Bridge", "static://(" + brokerBClientAddr + ")?useExponentialBackOff=false", false, null);

        // Wait for all bridges to form
        waitForAllBridges();
    }

    private void initMultiTcCluster(final String params, final String clusterFilter) throws Exception {
        final String tcParams = (params == null) ? "" : params;

        // Phase 1: Create and start all 3 brokers with both transport connectors
        addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
        addTransportConnector(getBroker(BROKER_A_NAME), "openwire", EPHEMERAL_BIND_ADDRESS + tcParams, true);
        addTransportConnector(getBroker(BROKER_A_NAME), "network", EPHEMERAL_BIND_ADDRESS + tcParams, false);
        getBroker(BROKER_A_NAME).start();
        getBroker(BROKER_A_NAME).waitUntilStarted();
        brokerAClientAddr = getBroker(BROKER_A_NAME).getTransportConnectorByName("openwire").getPublishableConnectString();
        brokerANobAddr = getBroker(BROKER_A_NAME).getTransportConnectorByName("network").getPublishableConnectString();

        addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
        addTransportConnector(getBroker(BROKER_B_NAME), "openwire", EPHEMERAL_BIND_ADDRESS + tcParams, true);
        addTransportConnector(getBroker(BROKER_B_NAME), "network", EPHEMERAL_BIND_ADDRESS + tcParams, false);
        getBroker(BROKER_B_NAME).start();
        getBroker(BROKER_B_NAME).waitUntilStarted();
        brokerBClientAddr = getBroker(BROKER_B_NAME).getTransportConnectorByName("openwire").getPublishableConnectString();
        brokerBNobAddr = getBroker(BROKER_B_NAME).getTransportConnectorByName("network").getPublishableConnectString();

        addBroker(BROKER_C_NAME, createBroker(BROKER_C_NAME));
        addTransportConnector(getBroker(BROKER_C_NAME), "openwire", EPHEMERAL_BIND_ADDRESS + tcParams, true);
        addTransportConnector(getBroker(BROKER_C_NAME), "network", EPHEMERAL_BIND_ADDRESS + tcParams, false);
        getBroker(BROKER_C_NAME).start();
        getBroker(BROKER_C_NAME).waitUntilStarted();
        brokerCClientAddr = getBroker(BROKER_C_NAME).getTransportConnectorByName("openwire").getPublishableConnectString();
        brokerCNobAddr = getBroker(BROKER_C_NAME).getTransportConnectorByName("network").getPublishableConnectString();

        // Phase 2: Add network bridges using network connector addresses and start them
        addAndStartNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + brokerBNobAddr + ")?useExponentialBackOff=false", false, clusterFilter);
        addAndStartNetworkBridge(getBroker(BROKER_A_NAME), "A_2_C_Bridge", "static://(" + brokerCNobAddr + ")?useExponentialBackOff=false", false, null);

        addAndStartNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + brokerANobAddr + ")?useExponentialBackOff=false", false, clusterFilter);
        addAndStartNetworkBridge(getBroker(BROKER_B_NAME), "B_2_C_Bridge", "static://(" + brokerCNobAddr + ")?useExponentialBackOff=false", false, null);

        addAndStartNetworkBridge(getBroker(BROKER_C_NAME), "C_2_A_Bridge", "static://(" + brokerANobAddr + ")?useExponentialBackOff=false", false, clusterFilter);
        addAndStartNetworkBridge(getBroker(BROKER_C_NAME), "C_2_B_Bridge", "static://(" + brokerBNobAddr + ")?useExponentialBackOff=false", false, null);

        // Wait for all bridges to form
        waitForAllBridges();
    }

    /**
     * Creates broker A with transport connector and network bridges to B and C.
     * Rebinds to the same port that was previously used (stored in brokerAClientAddr).
     * Used for re-creating broker A after it has been stopped.
     */
    private void createBrokerA(final boolean multi, final String params, final String clusterFilter, final String destinationFilter) throws Exception {
        final String tcParams = (params == null) ? "" : params;
        if (getBroker(BROKER_A_NAME) == null) {
            final String bindAddr = extractBindAddress(brokerAClientAddr);

            addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
            if (multi) {
                final String nobBindAddr = extractBindAddress(brokerANobAddr);
                addTransportConnector(getBroker(BROKER_A_NAME), "openwire", bindAddr + tcParams, true);
                addTransportConnector(getBroker(BROKER_A_NAME), "network", nobBindAddr + tcParams, false);
            } else {
                addTransportConnector(getBroker(BROKER_A_NAME), "openwire", bindAddr + tcParams, true);
            }
            getBroker(BROKER_A_NAME).start();
            getBroker(BROKER_A_NAME).waitUntilStarted();
            brokerAClientAddr = getBroker(BROKER_A_NAME).getTransportConnectorByName("openwire").getPublishableConnectString();
            if (multi) {
                brokerANobAddr = getBroker(BROKER_A_NAME).getTransportConnectorByName("network").getPublishableConnectString();
                addAndStartNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + brokerBNobAddr + ")?useExponentialBackOff=false", false, clusterFilter);
                addAndStartNetworkBridge(getBroker(BROKER_A_NAME), "A_2_C_Bridge", "static://(" + brokerCNobAddr + ")?useExponentialBackOff=false", false, null);
            } else {
                addAndStartNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + brokerBClientAddr + ")?useExponentialBackOff=false", false, clusterFilter);
                addAndStartNetworkBridge(getBroker(BROKER_A_NAME), "A_2_C_Bridge", "static://(" + brokerCClientAddr + ")?useExponentialBackOff=false", false, null);
            }
        }
    }

    /**
     * Creates broker C with transport connector and network bridges to A and B.
     * Rebinds to the same port that was previously used (stored in brokerCClientAddr).
     * Used for re-creating broker C after it has been stopped.
     */
    private void createBrokerC(final boolean multi, final String params, final String clusterFilter, final String destinationFilter) throws Exception {
        final String tcParams = (params == null) ? "" : params;
        if (getBroker(BROKER_C_NAME) == null) {
            final String bindAddr = extractBindAddress(brokerCClientAddr);

            addBroker(BROKER_C_NAME, createBroker(BROKER_C_NAME));
            if (multi) {
                final String nobBindAddr = extractBindAddress(brokerCNobAddr);
                addTransportConnector(getBroker(BROKER_C_NAME), "openwire", bindAddr + tcParams, true);
                addTransportConnector(getBroker(BROKER_C_NAME), "network", nobBindAddr + tcParams, false);
            } else {
                addTransportConnector(getBroker(BROKER_C_NAME), "openwire", bindAddr + tcParams, true);
            }
            getBroker(BROKER_C_NAME).start();
            getBroker(BROKER_C_NAME).waitUntilStarted();
            brokerCClientAddr = getBroker(BROKER_C_NAME).getTransportConnectorByName("openwire").getPublishableConnectString();
            if (multi) {
                brokerCNobAddr = getBroker(BROKER_C_NAME).getTransportConnectorByName("network").getPublishableConnectString();
                addAndStartNetworkBridge(getBroker(BROKER_C_NAME), "C_2_A_Bridge", "static://(" + brokerANobAddr + ")?useExponentialBackOff=false", false, clusterFilter);
                addAndStartNetworkBridge(getBroker(BROKER_C_NAME), "C_2_B_Bridge", "static://(" + brokerBNobAddr + ")?useExponentialBackOff=false", false, null);
            } else {
                addAndStartNetworkBridge(getBroker(BROKER_C_NAME), "C_2_A_Bridge", "static://(" + brokerAClientAddr + ")?useExponentialBackOff=false", false, clusterFilter);
                addAndStartNetworkBridge(getBroker(BROKER_C_NAME), "C_2_B_Bridge", "static://(" + brokerBClientAddr + ")?useExponentialBackOff=false", false, null);
            }
        }
    }

    /**
     * Extracts the bind address (tcp://host:port) from a publishable connect string.
     */
    private static String extractBindAddress(final String publishableAddr) throws Exception {
        final URI uri = new URI(publishableAddr);
        return "tcp://127.0.0.1:" + uri.getPort();
    }

    /**
     * Adds a network bridge to a broker and starts it immediately.
     * This is used when the broker is already running.
     */
    private void addAndStartNetworkBridge(final BrokerService broker, final String bridgeName,
                                          final String uri, final boolean duplex, final String destinationFilter) throws Exception {
        final NetworkConnector network = broker.addNetworkConnector(uri);
        network.setName(bridgeName);
        network.setDuplex(duplex);
        if (destinationFilter != null && !destinationFilter.isEmpty()) {
            network.setDestinationFilter(bridgeName);
        }
        broker.startNetworkConnector(network, null);
    }

    /**
     * Waits for all network bridges on all brokers to become active.
     */
    private void waitForAllBridges() throws Exception {
        for (final String brokerName : new String[]{BROKER_A_NAME, BROKER_B_NAME, BROKER_C_NAME}) {
            final BrokerService broker = getBroker(brokerName);
            if (broker != null) {
                for (final NetworkConnector nc : broker.getNetworkConnectors()) {
                    assertTrue("bridge " + nc.getName() + " on " + brokerName + " should form",
                        Wait.waitFor(() -> !nc.activeBridges().isEmpty(),
                            TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(500)));
                }
            }
        }
    }

    /**
     * Waits for network bridges FROM the specified broker to become active.
     */
    private void waitForBridgesFromBroker(final String brokerName) throws Exception {
        final BrokerService broker = getBroker(brokerName);
        if (broker != null) {
            for (final NetworkConnector nc : broker.getNetworkConnectors()) {
                assertTrue("bridge " + nc.getName() + " on " + brokerName + " should form",
                    Wait.waitFor(() -> !nc.activeBridges().isEmpty(),
                        TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(500)));
            }
        }
    }
}
