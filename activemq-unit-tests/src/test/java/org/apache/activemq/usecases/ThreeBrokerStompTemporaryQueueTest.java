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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThreeBrokerStompTemporaryQueueTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ThreeBrokerStompTemporaryQueueTest.class);
    private StompConnection stompConnection;

    @Override
    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker, boolean dynamicOnly, int networkTTL, boolean conduit, boolean failover) throws Exception {
        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI;
        if (!transportConnectors.isEmpty()) {
            remoteURI = transportConnectors.get(0).getConnectUri();
            NetworkConnector connector = new DiscoveryNetworkConnector(new URI("static:" + remoteURI));
            connector.setName(localBroker.getBrokerName() + remoteBroker.getBrokerName());
            localBroker.addNetworkConnector(connector);
            maxSetupTime = 2000;
            return connector;
        } else {
            throw new Exception("Remote broker has no registered connectors.");
        }
    }

    public void testStompTemporaryQueue() throws Exception {
        // Setup broker networks
        bridgeAndConfigureBrokers("BrokerA", "BrokerB");
        bridgeAndConfigureBrokers("BrokerA", "BrokerC");
        bridgeAndConfigureBrokers("BrokerB", "BrokerA");
        bridgeAndConfigureBrokers("BrokerB", "BrokerC");
        bridgeAndConfigureBrokers("BrokerC", "BrokerA");
        bridgeAndConfigureBrokers("BrokerC", "BrokerB");

        startAllBrokers();
        waitForBridgeFormation();

        Thread.sleep(1000);

        stompConnection = new StompConnection();
        stompConnection.open("localhost", 61614);
        // Creating a temp queue
        stompConnection.sendFrame("CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL);

        StompFrame frame = stompConnection.receive();
        assertTrue(frame.toString().startsWith("CONNECTED"));

        stompConnection.subscribe("/temp-queue/meaningless", "auto");
        stompConnection.send("/temp-queue/meaningless", "Hello World");

        frame = stompConnection.receive(3000);
        assertEquals("Hello World", frame.getBody());

        Thread.sleep(1000);

        assertEquals("Destination", 1, brokers.get("BrokerA").broker.getAdminView().getTemporaryQueues().length);
        assertEquals("Destination", 1, brokers.get("BrokerB").broker.getAdminView().getTemporaryQueues().length);
        assertEquals("Destination", 1, brokers.get("BrokerC").broker.getAdminView().getTemporaryQueues().length);

        int advisoryTopicsForTempQueues;
        advisoryTopicsForTempQueues = countTopicsByName("BrokerA", "ActiveMQ.Advisory.Consumer.Queue.ID");
        assertEquals("Advisory topic should be present", 1, advisoryTopicsForTempQueues);

        advisoryTopicsForTempQueues = countTopicsByName("BrokerB", "ActiveMQ.Advisory.Consumer.Queue.ID");
        assertEquals("Advisory topic should be present", 1, advisoryTopicsForTempQueues);

        advisoryTopicsForTempQueues = countTopicsByName("BrokerC", "ActiveMQ.Advisory.Consumer.Queue.ID");
        assertEquals("Advisory topic should be present", 1, advisoryTopicsForTempQueues);

        stompConnection.disconnect();

        Thread.sleep(1000);

        advisoryTopicsForTempQueues = countTopicsByName("BrokerA", "ActiveMQ.Advisory.Consumer.Queue.ID");
        assertEquals("Advisory topic should have been deleted", 0, advisoryTopicsForTempQueues);
        advisoryTopicsForTempQueues = countTopicsByName("BrokerB", "ActiveMQ.Advisory.Consumer.Queue.ID");
        assertEquals("Advisory topic should have been deleted", 0, advisoryTopicsForTempQueues);
        advisoryTopicsForTempQueues = countTopicsByName("BrokerC", "ActiveMQ.Advisory.Consumer.Queue.ID");
        assertEquals("Advisory topic should have been deleted", 0, advisoryTopicsForTempQueues);

        LOG.info("Restarting brokerA");
        BrokerItem brokerItem = brokers.remove("BrokerA");
        if (brokerItem != null) {
            brokerItem.destroy();
        }

        BrokerService restartedBroker = createAndConfigureBroker(new URI("broker:(tcp://localhost:61616,stomp://localhost:61613)/BrokerA"));
        bridgeAndConfigureBrokers("BrokerA", "BrokerB");
        bridgeAndConfigureBrokers("BrokerA", "BrokerC");
        restartedBroker.start();
        waitForBridgeFormation();

        Thread.sleep(3000);

        assertEquals("Destination", 0, brokers.get("BrokerA").broker.getAdminView().getTemporaryQueues().length);
        assertEquals("Destination", 0, brokers.get("BrokerB").broker.getAdminView().getTemporaryQueues().length);
        assertEquals("Destination", 0, brokers.get("BrokerC").broker.getAdminView().getTemporaryQueues().length);

        advisoryTopicsForTempQueues = countTopicsByName("BrokerA", "ActiveMQ.Advisory.Consumer.Queue.ID");
        assertEquals("Advisory topic should have been deleted", 0, advisoryTopicsForTempQueues);
        advisoryTopicsForTempQueues = countTopicsByName("BrokerB", "ActiveMQ.Advisory.Consumer.Queue.ID");
        assertEquals("Advisory topic should have been deleted", 0, advisoryTopicsForTempQueues);
        advisoryTopicsForTempQueues = countTopicsByName("BrokerC", "ActiveMQ.Advisory.Consumer.Queue.ID");
        assertEquals("Advisory topic should have been deleted", 0, advisoryTopicsForTempQueues);
    }

    private int countTopicsByName(String broker, String name)
            throws Exception {
        int advisoryTopicsForTempQueues = 0;
        for(int i=0; i<brokers.get(broker).broker.getAdminView().getTopics().length; i++){
            if(brokers.get(broker).broker.getAdminView().getTopics()[i].toString().contains(name)){
                advisoryTopicsForTempQueues++;
            }
        }
        return advisoryTopicsForTempQueues;
    }

    private void bridgeAndConfigureBrokers(String local, String remote) throws Exception {
        NetworkConnector bridge = bridgeBrokers(local, remote);
        assertNotNull(bridge);
    }

    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = new String("?deleteAllMessagesOnStartup=true");
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61616,stomp://localhost:61613)/BrokerA" + options));
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61617,stomp://localhost:61614)/BrokerB" + options));
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61618,stomp://localhost:61615)/BrokerC" + options));
    }

    private BrokerService createAndConfigureBroker(URI uri) throws Exception {
        BrokerService broker = createBroker(uri);
        configurePersistenceAdapter(broker);
        return broker;
    }

    protected void configurePersistenceAdapter(BrokerService broker) throws IOException {
        File dataFileDir = new File("target/test-amq-data/kahadb/" + broker.getBrokerName());
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        broker.setPersistenceAdapter(kaha);
    }
}
