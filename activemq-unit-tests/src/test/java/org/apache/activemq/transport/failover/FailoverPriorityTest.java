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

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverPriorityTest extends FailoverClusterTestSupport {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private static final String BROKER_A_CLIENT_TC_ADDRESS = "tcp://127.0.0.1:61616";
    private static final String BROKER_B_CLIENT_TC_ADDRESS = "tcp://127.0.0.1:61617";
    private static final String BROKER_C_CLIENT_TC_ADDRESS = "tcp://127.0.0.1:61618";
    private final HashMap<String,String> urls = new HashMap<String,String>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        urls.put(BROKER_A_NAME, BROKER_A_CLIENT_TC_ADDRESS);
        urls.put(BROKER_B_NAME, BROKER_B_CLIENT_TC_ADDRESS);
    }

    private static final String BROKER_A_NAME = "BROKERA";
    private static final String BROKER_B_NAME = "BROKERB";
    private static final String BROKER_C_NAME = "BROKERC";


    public void testPriorityBackup() throws Exception {
        createBrokerA();
        createBrokerB();
        getBroker(BROKER_B_NAME).waitUntilStarted();
        Thread.sleep(1000);

        setClientUrl("failover:(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")?randomize=false&priorityBackup=true&initialReconnectDelay=1000&useExponentialBackOff=false");
        createClients(5);

        assertAllConnectedTo(urls.get(BROKER_A_NAME));
        assertBrokerInfo(BROKER_A_NAME);

        restart(false, BROKER_A_NAME, BROKER_B_NAME);

        for (int i = 0; i < 3; i++) {
            restart(true, BROKER_A_NAME, BROKER_B_NAME);
        }

        Thread.sleep(5000);

        restart(false, BROKER_A_NAME, BROKER_B_NAME);

    }

    public void testPriorityBackupList() throws Exception {
        createBrokerA();
        createBrokerB();
        getBroker(BROKER_B_NAME).waitUntilStarted();
        Thread.sleep(1000);

        setClientUrl("failover:(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")?randomize=false&priorityBackup=true&priorityURIs=tcp://127.0.0.1:61617&initialReconnectDelay=1000&useExponentialBackOff=false");
        createClients(5);

        Thread.sleep(3000);

        assertAllConnectedTo(urls.get(BROKER_B_NAME));

        restart(false, BROKER_B_NAME, BROKER_A_NAME);

        for (int i = 0; i < 3; i++) {
            restart(true, BROKER_B_NAME, BROKER_A_NAME);
        }

        restart(false, BROKER_B_NAME, BROKER_A_NAME);

    }

    public void testThreeBrokers() throws Exception {
        // Broker A
        addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
        addTransportConnector(getBroker(BROKER_A_NAME), "openwire", BROKER_A_CLIENT_TC_ADDRESS, false);
        addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_C_Bridge", "static://(" + BROKER_C_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        getBroker(BROKER_A_NAME).start();

        // Broker B
        addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
        addTransportConnector(getBroker(BROKER_B_NAME), "openwire", BROKER_B_CLIENT_TC_ADDRESS, false);
        addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_C_Bridge", "static://(" + BROKER_C_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        getBroker(BROKER_B_NAME).start();

        // Broker C
        addBroker(BROKER_C_NAME, createBroker(BROKER_C_NAME));
        addTransportConnector(getBroker(BROKER_C_NAME), "openwire", BROKER_C_CLIENT_TC_ADDRESS, false);
        addNetworkBridge(getBroker(BROKER_C_NAME), "C_2_A_Bridge", "static://(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        addNetworkBridge(getBroker(BROKER_C_NAME), "C_2_B_Bridge", "static://(" + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        getBroker(BROKER_C_NAME).start();


        getBroker(BROKER_C_NAME).waitUntilStarted();
        Thread.sleep(1000);

        setClientUrl("failover:(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + "," + BROKER_C_CLIENT_TC_ADDRESS + ")?randomize=false&priorityBackup=true&initialReconnectDelay=1000&useExponentialBackOff=false");

        createClients(5);

        assertAllConnectedTo(urls.get(BROKER_A_NAME));

        restart(true, BROKER_A_NAME, BROKER_B_NAME);

    }

    public void testPriorityBackupAndUpdateClients() throws Exception {
        // Broker A
        addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
        addTransportConnector(getBroker(BROKER_A_NAME), "openwire", BROKER_A_CLIENT_TC_ADDRESS, true);
        addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        getBroker(BROKER_A_NAME).start();

        // Broker B
        addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
        addTransportConnector(getBroker(BROKER_B_NAME), "openwire", BROKER_B_CLIENT_TC_ADDRESS, true);
        addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
        getBroker(BROKER_B_NAME).start();

        getBroker(BROKER_B_NAME).waitUntilStarted();
        Thread.sleep(1000);

        setClientUrl("failover:(" + BROKER_A_CLIENT_TC_ADDRESS + "," + BROKER_B_CLIENT_TC_ADDRESS + ")?randomize=false&priorityBackup=true&initialReconnectDelay=1000&useExponentialBackOff=false");

        LOG.info("Client URI will be: " + getClientUrl());

        createClients(5);

        // Let's wait a little bit longer just in case it takes a while to realize that the
        // Broker A is the one with higher priority.
        Thread.sleep(5000);

        assertAllConnectedTo(urls.get(BROKER_A_NAME));
    }

    private void restart(boolean primary, String primaryName, String secondaryName) throws Exception {

        Thread.sleep(1000);

        if (primary) {
            LOG.info("Stopping " + primaryName);
            stopBroker(primaryName);
        } else {
            LOG.info("Stopping " + secondaryName);
            stopBroker(secondaryName);
        }
        Thread.sleep(5000);

        if (primary) {
            assertAllConnectedTo(urls.get(secondaryName));
            assertBrokerInfo(secondaryName);
        } else {
            assertAllConnectedTo(urls.get(primaryName));
            assertBrokerInfo(primaryName);
        }

        if (primary) {
            LOG.info("Starting " + primaryName);
            createBrokerByName(primaryName);
            getBroker(primaryName).waitUntilStarted();
        } else {
            LOG.info("Starting " + secondaryName);
            createBrokerByName(secondaryName);
            getBroker(secondaryName).waitUntilStarted();
        }

        Thread.sleep(5000);

        assertAllConnectedTo(urls.get(primaryName));
        assertBrokerInfo(primaryName);

    }

    private void createBrokerByName(String name) throws Exception {
        if (name.equals(BROKER_A_NAME)) {
            createBrokerA();
        } else if (name.equals(BROKER_B_NAME)) {
            createBrokerB();
        } else {
            throw new Exception("Unknown broker " + name);
        }
    }

    private void createBrokerA() throws Exception {
        if (getBroker(BROKER_A_NAME) == null) {
            addBroker(BROKER_A_NAME, createBroker(BROKER_A_NAME));
            addTransportConnector(getBroker(BROKER_A_NAME), "openwire", BROKER_A_CLIENT_TC_ADDRESS, false);
            addNetworkBridge(getBroker(BROKER_A_NAME), "A_2_B_Bridge", "static://(" + BROKER_B_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            getBroker(BROKER_A_NAME).start();
        }
    }

    private void createBrokerB() throws Exception {
        if (getBroker(BROKER_B_NAME) == null) {
            addBroker(BROKER_B_NAME, createBroker(BROKER_B_NAME));
            addTransportConnector(getBroker(BROKER_B_NAME), "openwire", BROKER_B_CLIENT_TC_ADDRESS, false);
            addNetworkBridge(getBroker(BROKER_B_NAME), "B_2_A_Bridge", "static://(" + BROKER_A_CLIENT_TC_ADDRESS + ")?useExponentialBackOff=false", false, null);
            getBroker(BROKER_B_NAME).start();
        }
    }

    @Override
    protected void tearDown() throws Exception {
        shutdownClients();
        destroyBrokerCluster();
    }

}
