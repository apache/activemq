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

import org.apache.activemq.broker.TransportConnector;
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

    private static final String BROKER_A_CLIENT_TC_ADDRESS = "tcp://127.0.0.1:61616";
    private static final String BROKER_B_CLIENT_TC_ADDRESS = "tcp://127.0.0.1:61617";
    private static final String BROKER_C_CLIENT_TC_ADDRESS = "tcp://127.0.0.1:61618";
    private static final String BROKER_A_NOB_TC_ADDRESS = "tcp://127.0.0.1:61626";
    private static final String BROKER_B_NOB_TC_ADDRESS = "tcp://127.0.0.1:61627";
    private static final String BROKER_C_NOB_TC_ADDRESS = "tcp://127.0.0.1:61628";
    private static final String BROKER_A_NAME = "BROKERA";
    private static final String BROKER_B_NAME = "BROKERB";
    private static final String BROKER_C_NAME = "BROKERC";

    /**
     * Basic dynamic failover 3 broker test
     *
     * @throws Exception
     */
    public void testThreeBrokerClusterSingleConnectorBasic() throws Exception {

        initSingleTcBroker("", null, null);

        Thread.sleep(2000);

        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")");
        createClients();
        Thread.sleep(2000);

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

        Thread.sleep(2000);

        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")?backup=true&backupPoolSize=2&useExponentialBackOff=false&initialReconnectDelay=500");
        createClients();
        Thread.sleep(2000);

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

        Thread.sleep(2000);
        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")");
        createClients();
        Thread.sleep(2000);

        runTests(false, null, null, null);
    }

    /**
     * Tests a 3 broker cluster using a cluster filter of *
     *
     * @throws Exception
     */
    public void testThreeBrokerClusterWithClusterFilter() throws Exception {

        initSingleTcBroker("?transport.closeAsync=false", null, null);

        Thread.sleep(2000);
        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")");
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

        Thread.sleep(2000);

        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")");
        createClients();
        Thread.sleep(2000);

        runTests(true, null, null, null);
    }

    /**
     * Test to verify the reintroduction of the A Broker
     *
     * @throws Exception
     */
    public void testOriginalBrokerRestart() throws Exception {
        initSingleTcBroker("", null, null);

        Thread.sleep(2000);

        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")");
        createClients();

        assertClientsConnectedToThreeBrokers();

        getBroker(BROKER_A_NAME).stop();
        getBroker(BROKER_A_NAME).waitUntilStopped();
        removeBroker(BROKER_A_NAME);

        assertClientsConnectedToTwoBrokers();

        createBrokerA(false, null, null, null);
        getBroker(BROKER_A_NAME).waitUntilStarted();

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

        Thread.sleep(2000);
        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false&initialReconnectDelay=500");
        createClients(100);
        Thread.sleep(5000);

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

        Thread.sleep(2000);
        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")");
        createClients();

        runTests(false, null, null, "Queue.TEST.FOO.>");
    }

    public void testFailOverWithUpdateClientsOnRemove() throws Exception{
        // Broker A
        addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
        TransportConnector connectorA = getBroker(BROKER_A_NAME).addConnector(BROKER_A_CLIENT_TC_ADDRESS);
        connectorA.setName("openwire");
        connectorA.setRebalanceClusterClients(true);
        connectorA.setUpdateClusterClients(true);
        connectorA.setUpdateClusterClientsOnRemove(true); //If set to false the test succeeds.
        addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        getBroker(BROKER_A_NAME).start();

        // Broker B
        addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
        TransportConnector connectorB = getBroker(BROKER_B_NAME).addConnector(BROKER_B_CLIENT_TC_ADDRESS);
        connectorB.setName("openwire");
        connectorB.setRebalanceClusterClients(true);
        connectorB.setUpdateClusterClients(true);
        connectorB.setUpdateClusterClientsOnRemove(true); //If set to false the test succeeds.
        addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        getBroker(BROKER_B_NAME).start();

        getBroker(BROKER_B_NAME).waitUntilStarted();
        Thread.sleep(1000);

        // create client connecting only to A. It should receive broker B address whet it connects to A.
        setClientUrl("failover:(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=true");
        createClients(1);
        Thread.sleep(5000);

        // We stop broker A.
        logger.info("Stopping broker A whose address is: {}", BROKER_A_CLIENT_TC_ADDRESS);
        getBroker(BROKER_A_NAME).stop();
        getBroker(BROKER_A_NAME).waitUntilStopped();
        Thread.sleep(5000);

        // Client should failover to B.
        assertAllConnectedTo(BROKER_B_CLIENT_TC_ADDRESS);
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
    private void runTests(boolean multi, String tcParams, String clusterFilter, String destinationFilter) throws Exception, InterruptedException {
        assertClientsConnectedToThreeBrokers();

        LOG.info("Stopping BrokerC in prep for restart");
        getBroker(BROKER_C_NAME).stop();
        getBroker(BROKER_C_NAME).waitUntilStopped();
        removeBroker(BROKER_C_NAME);

        assertClientsConnectedToTwoBrokers();

        LOG.info("Recreating BrokerC after stop");
        createBrokerC(multi, tcParams, clusterFilter, destinationFilter);
        getBroker(BROKER_C_NAME).waitUntilStarted();

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
    private void runClientDistributionTests(boolean multi, String tcParams, String clusterFilter, String destinationFilter) throws Exception, InterruptedException {
        assertClientsConnectedToThreeBrokers();
        assertClientsConnectionsEvenlyDistributed(.25);

        getBroker(BROKER_C_NAME).stop();
        getBroker(BROKER_C_NAME).waitUntilStopped();
        removeBroker(BROKER_C_NAME);

        assertClientsConnectedToTwoBrokers();
        assertClientsConnectionsEvenlyDistributed(.35);

        createBrokerC(multi, tcParams, clusterFilter, destinationFilter);
        getBroker(BROKER_C_NAME).waitUntilStarted();

        assertClientsConnectedToThreeBrokers();
        assertClientsConnectionsEvenlyDistributed(.20);
    }

    @Override
    protected void setUp() throws Exception {
    }

    @Override
    protected void tearDown() throws Exception {
        shutdownClients();
        Thread.sleep(2000);
        destroyBrokerCluster();
    }

    private void initSingleTcBroker(String params, String clusterFilter, String destinationFilter) throws Exception {
        createBrokerA(false, params, clusterFilter, null);
        createBrokerB(false, params, clusterFilter, null);
        createBrokerC(false, params, clusterFilter, null);
        getBroker(BROKER_C_NAME).waitUntilStarted();
    }

    private void initMultiTcCluster(String params, String clusterFilter) throws Exception {
        createBrokerA(true, params, clusterFilter, null);
        createBrokerB(true, params, clusterFilter, null);
        createBrokerC(true, params, clusterFilter, null);
        getBroker(BROKER_C_NAME).waitUntilStarted();
    }

    private void createBrokerA(boolean multi, String params, String clusterFilter, String destinationFilter) throws Exception {
        final String tcParams = (params == null)?"":params;
        if (getBroker(BROKER_A_NAME) == null) {
            addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
            addTransportConnector(getBroker(BROKER_A_NAME), "openwire", BROKER_A_CLIENT_TC_ADDRESS + tcParams, true);
            if (multi) {
                addTransportConnector(getBroker(BROKER_A_NAME), "network", BROKER_A_NOB_TC_ADDRESS + tcParams, false);
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_C_Bridge", "static://(" + BROKER_C_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            } else {
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_C_Bridge", "static://(" + BROKER_C_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            }
            getBroker(BROKER_A_NAME).start();
        }
    }

    private void createBrokerB(boolean multi, String params, String clusterFilter, String destinationFilter) throws Exception {
        final String tcParams = (params == null)?"":params;
        if (getBroker(BROKER_B_NAME) == null) {
            addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
            addTransportConnector(getBroker(BROKER_B_NAME), "openwire", BROKER_B_CLIENT_TC_ADDRESS + tcParams, true);
            if (multi) {
                addTransportConnector(getBroker(BROKER_B_NAME), "network", BROKER_B_NOB_TC_ADDRESS + tcParams, false);
                addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + BROKER_A_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_C_Bridge", "static://(" + BROKER_C_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            } else {
                addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_C_Bridge", "static://(" + BROKER_C_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            }
            getBroker(BROKER_B_NAME).start();
        }
    }

    private void createBrokerC(boolean multi, String params, String clusterFilter, String destinationFilter) throws Exception {
        final String tcParams = (params == null)?"":params;
        if (getBroker(BROKER_C_NAME) == null) {
            addBroker(BROKER_C_NAME, createBroker(BROKER_C_NAME));
            addTransportConnector(getBroker(BROKER_C_NAME), "openwire", BROKER_C_CLIENT_TC_ADDRESS + tcParams, true);
            if (multi) {
                addTransportConnector(getBroker(BROKER_C_NAME), "network", BROKER_C_NOB_TC_ADDRESS + tcParams, false);
                addNetworkBridge(getBroker(BROKER_C_NAME), "C_2_A_Bridge", "static://(" + BROKER_A_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_C_NAME), "C_2_B_Bridge", "static://(" + BROKER_B_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            } else {
                addNetworkBridge(getBroker(BROKER_C_NAME), "C_2_A_Bridge", "static://(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_C_NAME), "C_2_B_Bridge", "static://(" + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            }
            getBroker(BROKER_C_NAME).start();
        }
    }
}
