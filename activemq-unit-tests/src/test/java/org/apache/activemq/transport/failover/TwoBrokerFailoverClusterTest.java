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

public class TwoBrokerFailoverClusterTest extends FailoverClusterTestSupport {

    private static final String BROKER_A_CLIENT_TC_ADDRESS = "tcp://127.0.0.1:61616";
    private static final String BROKER_B_CLIENT_TC_ADDRESS = "tcp://127.0.0.1:61617";
    private static final String BROKER_A_NOB_TC_ADDRESS = "tcp://127.0.0.1:61626";
    private static final String BROKER_B_NOB_TC_ADDRESS = "tcp://127.0.0.1:61627";
    private static final String BROKER_A_NAME = "BROKERA";
    private static final String BROKER_B_NAME = "BROKERB";

    public void testTwoBrokersRestart() throws Exception {
        createBrokerA(false, "", null, null);
        createBrokerB(false, "", null, null);
        getBroker(BROKER_B_NAME).waitUntilStarted();

        Thread.sleep(2000);
        setClientUrl("failover://(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")?randomize=false&jms.watchTopicAdvisories=false");
        createClients();

        Thread.sleep(5000);

        assertClientsConnectedToTwoBrokers();
        assertClientsConnectionsEvenlyDistributed(.35);


        getBroker(BROKER_A_NAME).stop();
        getBroker(BROKER_A_NAME).waitUntilStopped();
        removeBroker(BROKER_A_NAME);

        Thread.sleep(1000);

        assertAllConnected(NUMBER_OF_CLIENTS);
        assertAllConnectedTo(BROKER_B_CLIENT_TC_ADDRESS);

        Thread.sleep(5000);

        logger.info("Restarting A");
        createBrokerA(false, "", null, null);
        getBroker(BROKER_A_NAME).waitUntilStarted();
        Thread.sleep(5000);

        assertAllConnected(NUMBER_OF_CLIENTS);
        assertClientsConnectedToTwoBrokers();
        assertClientsConnectionsEvenlyDistributed(.35);
    }


    private void createBrokerA(boolean multi, String params, String clusterFilter, String destinationFilter) throws Exception {
    	final String tcParams = (params == null)?"":params;
        if (getBroker(BROKER_A_NAME) == null) {
            addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
            addTransportConnector(getBroker(BROKER_A_NAME), "openwire", BROKER_A_CLIENT_TC_ADDRESS + tcParams, true);
            if (multi) {
                addTransportConnector(getBroker(BROKER_A_NAME), "network", BROKER_A_NOB_TC_ADDRESS + tcParams, false);
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_NOB_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
            } else {
                addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
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
            } else {
                addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, clusterFilter);
            }
            getBroker(BROKER_B_NAME).start();
        }
    }

}
