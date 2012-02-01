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

/**
 * Complex cluster test that will exercise the dynamic failover capabilities of
 * a network of brokers. Using a networking of 3 brokers where the 3rd broker is
 * removed and then added back in it is expected in each test that the number of
 * connections on the client should start with 3, then have two after the 3rd
 * broker is removed and then show 3 after the 3rd broker is reintroduced.
 */
public class FailoverComplexClusterTest extends FailoverClusterTestSupport {

    private static final String BROKER_A_CLIENT_TC_ADDRESS = "tcp://localhost:61616";
    private static final String BROKER_B_CLIENT_TC_ADDRESS = "tcp://localhost:61617";
    private static final String BROKER_C_CLIENT_TC_ADDRESS = "tcp://localhost:61618";
    private static final String BROKER_A_NOB_TC_ADDRESS = "tcp://localhost:61626";
    private static final String BROKER_B_NOB_TC_ADDRESS = "tcp://localhost:61627";
    private static final String BROKER_C_NOB_TC_ADDRESS = "tcp://localhost:61628";
    private static final String BROKER_A_NAME = "BROKERA";
    private static final String BROKER_B_NAME = "BROKERB";
    private static final String BROKER_C_NAME = "BROKERC";
    
    

    public void testThreeBrokerClusterSingleConnectorBasic() throws Exception {

        initSingleTcBroker("", null);

        Thread.sleep(2000);

        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")");
        createClients();
        Thread.sleep(2000);

        runTests(false);
    }


    public void testThreeBrokerClusterSingleConnectorBackup() throws Exception {

        initSingleTcBroker("", null);

        Thread.sleep(2000);

        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")?backup=true&backupPoolSize=2");
        createClients();
        Thread.sleep(2000);

        runTests(false);
    }


    public void testThreeBrokerClusterSingleConnectorWithParams() throws Exception {

        initSingleTcBroker("?transport.closeAsync=false", null);

        Thread.sleep(2000);
        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")");
        createClients();

        runTests(false);
    }

    public void testThreeBrokerClusterMultipleConnectorBasic() throws Exception {

        initMultiTcCluster("", null);

        Thread.sleep(2000);

        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")");
        createClients();

        runTests(true);
    }


    /**
     * Runs a 3 tests: <br/>
     * <ul>
     * <li>asserts clients are distributed across all 3 brokers</li>
     * <li>asserts clients are distributed across 2 brokers after removing the 3rd</li>
     * <li>asserts clients are distributed across all 3 brokers after reintroducing the 3rd broker</li>
     * </ul>
     * @throws Exception
     * @throws InterruptedException
     */
    private void runTests(boolean multi) throws Exception, InterruptedException {
        assertClientsConnectedToThreeBrokers();

        getBroker(BROKER_C_NAME).stop();
        getBroker(BROKER_C_NAME).waitUntilStopped();
        removeBroker(BROKER_C_NAME);

        Thread.sleep(5000);

        assertClientsConnectedToTwoBrokers();

        createBrokerC(multi, "", null);
        getBroker(BROKER_C_NAME).waitUntilStarted();
        Thread.sleep(5000);

        assertClientsConnectedToThreeBrokers();
    }

    @Override
    protected void setUp() throws Exception {
    }

    @Override
    protected void tearDown() throws Exception {
        shutdownClients();
        destroyBrokerCluster();
        Thread.sleep(2000);
    }

    private void initSingleTcBroker(String params, String clusterFilter) throws Exception {
        createBrokerA(false, params, clusterFilter);
        createBrokerB(false, params, clusterFilter);
        createBrokerC(false, params, clusterFilter);
        getBroker(BROKER_C_NAME).waitUntilStarted();
    }

    private void initMultiTcCluster(String params, String clusterFilter) throws Exception {
        createBrokerA(true, params, clusterFilter);
        createBrokerB(true, params, clusterFilter);
        createBrokerC(true, params, clusterFilter);
        getBroker(BROKER_C_NAME).waitUntilStarted();
    }
    
    private void createBrokerA(boolean multi, String params, String clusterFilter) throws Exception {
        if (getBroker(BROKER_A_NAME) == null) {
            addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
            addTransportConnector(getBroker(BROKER_A_NAME), "openwire", BROKER_A_CLIENT_TC_ADDRESS + params, true);
            if (multi) {
                addTransportConnector(getBroker(BROKER_A_NAME), "network", BROKER_A_NOB_TC_ADDRESS, false);
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_C_Bridge", "static://(" + BROKER_C_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            } else {
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_C_Bridge", "static://(" + BROKER_C_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            }
            getBroker(BROKER_A_NAME).start();
        }
    }

    private void createBrokerB(boolean multi, String params, String clusterFilter) throws Exception {
        if (getBroker(BROKER_B_NAME) == null) {
            addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
            addTransportConnector(getBroker(BROKER_B_NAME), "openwire", BROKER_B_CLIENT_TC_ADDRESS + params, true);
            if (multi) {
                addTransportConnector(getBroker(BROKER_B_NAME), "network", BROKER_B_NOB_TC_ADDRESS, false);
                addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + BROKER_A_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_C_Bridge", "static://(" + BROKER_C_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            } else {
                addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
                addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_C_Bridge", "static://(" + BROKER_C_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            }
            getBroker(BROKER_B_NAME).start();
        }
    }

    private void createBrokerC(boolean multi, String params, String clusterFilter) throws Exception {
        if (getBroker(BROKER_C_NAME) == null) {
            addBroker(BROKER_C_NAME, createBroker(BROKER_C_NAME));
            addTransportConnector(getBroker(BROKER_C_NAME), "openwire", BROKER_C_CLIENT_TC_ADDRESS + params, true);
            if (multi) {
                addTransportConnector(getBroker(BROKER_C_NAME), "network", BROKER_C_NOB_TC_ADDRESS, false);
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
