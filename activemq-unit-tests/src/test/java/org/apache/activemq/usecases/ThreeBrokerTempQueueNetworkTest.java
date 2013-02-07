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
import java.util.Iterator;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.network.NetworkConnector;

/**
 *
 */
public class ThreeBrokerTempQueueNetworkTest extends JmsMultipleBrokersTestSupport {
    protected static final int MESSAGE_COUNT = 100;
    boolean enableTempDestinationBridging = true;

    /**
     * BrokerA -> BrokerB -> BrokerC
     */
    public void testTempQueueCleanup() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB", false, 2);
        bridgeBrokers("BrokerB", "BrokerC", false, 2);
        startAllBrokers();
        BrokerItem brokerItem = brokers.get("BrokerC");
        Connection conn = brokerItem.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue tempQ = sess.createTemporaryQueue();
        Thread.sleep(5000);
        for (Iterator<BrokerItem> i = brokers.values().iterator(); i.hasNext();) {
            BrokerItem bi = i.next();
            assertEquals("No queues on broker " + bi.broker.getBrokerName(), 1, bi.broker.getAdminView().getTemporaryQueues().length);
        }
        tempQ.delete();
        Thread.sleep(2000);
        for (Iterator<BrokerItem> i = brokers.values().iterator(); i.hasNext();) {
            BrokerItem bi = i.next();
            assertEquals("Temp queue left behind on broker " + bi.broker.getBrokerName(), 0, bi.broker.getAdminView().getTemporaryQueues().length);
        }
    }

    // this actually uses 4 brokers ...
    public void testTempQueueRecovery() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB", false, 3);
        bridgeBrokers("BrokerB", "BrokerC", false, 3);
        startAllBrokers();
        BrokerItem brokerItem = brokers.get("BrokerC");
        Connection conn = brokerItem.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue tempQ = sess.createTemporaryQueue();
        Thread.sleep(5000);
        for (Iterator<BrokerItem> i = brokers.values().iterator(); i.hasNext();) {
            BrokerItem bi = i.next();
            assertEquals("No queues on broker " + bi.broker.getBrokerName(), 1, bi.broker.getAdminView().getTemporaryQueues().length);
        }
        createBroker(new URI("broker:(tcp://localhost:61619)/BrokerD?persistent=false&useJmx=true"));
        bridgeBrokers("BrokerD", "BrokerA", false, 3);
        BrokerItem newBroker = brokers.get("BrokerD");
        newBroker.broker.start();
        Thread.sleep(1000);
        assertEquals("No queues on broker D", 1, newBroker.broker.getAdminView().getTemporaryQueues().length);
        tempQ.delete();
        Thread.sleep(2000);
        for (Iterator<BrokerItem> i = brokers.values().iterator(); i.hasNext();) {
            BrokerItem bi = i.next();
            assertEquals("Temp queue left behind on broker " + bi.broker.getBrokerName(), 0, bi.broker.getAdminView().getTemporaryQueues().length);
        }
    }

    public void testTempDisable() throws Exception {
        enableTempDestinationBridging = false;
        try {
            testTempQueueCleanup();
        } catch (Throwable e) {
            // Expecting an error
            return;
        }
        fail("Test should have failed since temp queues are disabled.");
    }

    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=true"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=true"));
        createBroker(new URI("broker:(tcp://localhost:61618)/BrokerC?persistent=false&useJmx=true"));
    }

    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName, boolean dynamicOnly, int networkTTL) throws Exception {
        NetworkConnector connector = super.bridgeBrokers(localBrokerName, remoteBrokerName, dynamicOnly, networkTTL, true);
        connector.setBridgeTempDestinations(enableTempDestinationBridging);
        return connector;
    }

}
