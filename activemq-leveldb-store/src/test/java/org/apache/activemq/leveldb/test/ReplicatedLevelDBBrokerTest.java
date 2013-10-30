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
package org.apache.activemq.leveldb.test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.leveldb.replicated.ElectingLevelDBStore;
import org.junit.After;
import org.junit.Test;

import javax.jms.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Holds broker unit tests of the replicated leveldb store.
 */
public class ReplicatedLevelDBBrokerTest extends ZooKeeperTestSupport {

    final SynchronousQueue<BrokerService> masterQueue = new SynchronousQueue<BrokerService>();
    ArrayList<BrokerService> brokers = new ArrayList<BrokerService>();

    /**
     * Tries to replicate the problem reported at:
     * https://issues.apache.org/jira/browse/AMQ-4837
     */
    @Test(timeout = 1000*60*10)
    public void testAMQ4837() throws Exception {

        // 1.	Start 3 activemq nodes.
        startBrokerAsync(createBrokerNode("node-1"));
        startBrokerAsync(createBrokerNode("node-2"));
        startBrokerAsync(createBrokerNode("node-3"));

        // 2.	Push a message to the master and browse the queue
        System.out.println("Wait for master to start up...");
        BrokerService master = masterQueue.poll(60, TimeUnit.SECONDS);
        assertNotNull("Master elected", master);
        sendMessage(master, "Hello World #1");
        assertEquals(1, browseMessages(master).size());

        // 3.	Stop master node
        System.out.println("Stopping master...");
        master.stop();
        master.waitUntilStopped();
        BrokerService prevMaster = master;

        // 4.	Push a message to the new master (Node2) and browse the queue using the web UI. Message summary and queue content ok.
        System.out.println("Wait for new master to start up...");
        master = masterQueue.poll(60, TimeUnit.SECONDS);
        assertNotNull("Master elected", master);
        sendMessage(master, "Hello World #2");
        assertEquals(2, browseMessages(master).size());

        // 5.	Start Node1
        System.out.println("Starting previous master...");
        prevMaster = createBrokerNode(prevMaster.getBrokerName());
        startBrokerAsync(prevMaster);

        // 6.	Stop master node (Node2)
        System.out.println("Stopping master...");
        master.stop();
        master.waitUntilStopped();

        // 7.	Browse the queue using the web UI on new master (Node3). Message summary ok however when clicking on the queue, no message details.
        // An error (see below) is logged by the master, which attempts a restart.
        System.out.println("Wait for new master to start up...");
        master = masterQueue.poll(60, TimeUnit.SECONDS);
        assertNotNull("Master elected", master);
        assertEquals(2, browseMessages(master).size());

    }

    private void startBrokerAsync(BrokerService b) {
        final BrokerService broker = b;
        new Thread("Starting broker node: "+b.getBrokerName()){
            @Override
            public void run() {
                try {
                    broker.start();
                    broker.waitUntilStarted();
                    masterQueue.put(broker);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    private void sendMessage(BrokerService brokerService, String body) throws Exception {
        TransportConnector connector = brokerService.getTransportConnectors().get(0);
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connector.getConnectUri());
        Connection connection = factory.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue("FOO"));
            producer.send(session.createTextMessage(body));
        } finally {
            connection.close();
        }
    }

    private ArrayList<String> browseMessages(BrokerService brokerService) throws Exception {
        ArrayList<String> rc = new ArrayList<String>();
        TransportConnector connector = brokerService.getTransportConnectors().get(0);
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connector.getConnectUri());
        Connection connection = factory.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueBrowser browser = session.createBrowser(session.createQueue("FOO"));
            Enumeration enumeration = browser.getEnumeration();
            while (enumeration.hasMoreElements()) {
                TextMessage textMessage = (TextMessage) enumeration.nextElement();
                rc.add(textMessage.getText());
            }
        } finally {
            connection.close();
        }
        return rc;
    }

    @After
    public void closeBrokers() throws Exception {
        for (BrokerService broker : brokers) {
            try {
                broker.stop();
                broker.waitUntilStopped();
            } catch (Exception e) {
            }
        }
    }

    private BrokerService createBrokerNode(String id) throws Exception {
        BrokerService bs = new BrokerService();
        bs.getManagementContext().setCreateConnector(false);
        brokers.add(bs);
        bs.setBrokerName(id);
        bs.setPersistenceAdapter(createStoreNode(id));
        bs.addConnector("tcp://0.0.0.0:0");
        return bs;
    }


    private ElectingLevelDBStore createStoreNode(String id) {

        // This little hack is in here because we give each of the 3 brokers
        // different broker names so they can show up in JMX correctly,
        // but the store needs to be configured with the same broker name
        // so that they can find each other in ZK properly.
        ElectingLevelDBStore store = new ElectingLevelDBStore() {
            @Override
            public void start() throws Exception {
                this.setBrokerName("localhost");
                super.start();
            }
        };
        store.setDirectory(new File(data_dir(), id));
        store.setReplicas(3);
        store.setZkAddress("localhost:" + connector.getLocalPort());
        store.setHostname("localhost");
        store.setBind("tcp://0.0.0.0:0");
        return store;
    }
}
