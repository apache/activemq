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
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

import static org.junit.Assert.*;

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
    public void testAMQ4837viaJMS() throws Throwable {
        testAMQ4837(false);
    }

  /**
     * Tries to replicate the problem reported at:
     * https://issues.apache.org/jira/browse/AMQ-4837
     */
    @Test(timeout = 1000*60*10)
    public void testAMQ4837viaJMX() throws Throwable {
        for (int i = 0; i < 2; i++) {
            resetDataDirs();
            testAMQ4837(true);
            stopBrokers();
        }
    }

    @Before
    public void resetDataDirs() throws IOException {
        deleteDirectory("node-1");
        deleteDirectory("node-2");
        deleteDirectory("node-3");
    }

    protected void deleteDirectory(String s) throws IOException {
        try {
            FileUtils.deleteDirectory(new File(data_dir(), s));
        } catch (IOException e) {
        }
    }


    public void testAMQ4837(boolean jmx) throws Throwable {

        try {
            System.out.println("======================================");
            System.out.println("1.	Start 3 activemq nodes.");
            System.out.println("======================================");
            startBrokerAsync(createBrokerNode("node-1"));
            startBrokerAsync(createBrokerNode("node-2"));
            startBrokerAsync(createBrokerNode("node-3"));

            BrokerService master = waitForNextMaster();
            System.out.println("======================================");
            System.out.println("2.	Push a message to the master and browse the queue");
            System.out.println("======================================");
            sendMessage(master, pad("Hello World #1", 1024));
            assertEquals(1, browseMessages(master, jmx).size());

            System.out.println("======================================");
            System.out.println("3.	Stop master node");
            System.out.println("======================================");
            stop(master);
            BrokerService prevMaster = master;
            master = waitForNextMaster();

            System.out.println("======================================");
            System.out.println("4.	Push a message to the new master and browse the queue. Message summary and queue content ok.");
            System.out.println("======================================");
            assertEquals(1, browseMessages(master, jmx).size());
            sendMessage(master, pad("Hello World #2", 1024));
            assertEquals(2, browseMessages(master, jmx).size());

            System.out.println("======================================");
            System.out.println("5.	Restart the stopped node & 6. stop current master");
            System.out.println("======================================");
            prevMaster = createBrokerNode(prevMaster.getBrokerName());
            startBrokerAsync(prevMaster);
            stop(master);

            master = waitForNextMaster();
            System.out.println("======================================");
            System.out.println("7.	Browse the queue on new master");
            System.out.println("======================================");
            assertEquals(2, browseMessages(master, jmx).size());
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }

    }

    private void stop(BrokerService master) throws Exception {
        System.out.println("Stopping "+master.getBrokerName());
        master.stop();
        master.waitUntilStopped();
    }

    private BrokerService waitForNextMaster() throws InterruptedException {
        System.out.println("Wait for master to start up...");
        BrokerService master = masterQueue.poll(60, TimeUnit.SECONDS);
        assertNotNull("Master elected", master);
        assertFalse(master.isSlave());
        assertNull("Only one master elected at a time..", masterQueue.peek());
        System.out.println("Master started: " + master.getBrokerName());
        return master;
    }

    private String pad(String value, int size) {
        while( value.length() < size ) {
            value += " ";
        }
        return value;
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
    private ArrayList<String> browseMessages(BrokerService brokerService, boolean jmx) throws Exception {
        if( jmx ) {
            return browseMessagesViaJMX(brokerService);
        } else {
            return browseMessagesViaJMS(brokerService);
        }
    }

    private ArrayList<String> browseMessagesViaJMX(BrokerService brokerService) throws Exception {
        ArrayList<String> rc = new ArrayList<String>();
        ObjectName on = new ObjectName("org.apache.activemq:type=Broker,brokerName="+brokerService.getBrokerName()+",destinationType=Queue,destinationName=FOO");
        CompositeData[] browse = (CompositeData[]) ManagementFactory.getPlatformMBeanServer().invoke(on, "browse", null, null);
        for (CompositeData cd : browse) {
            rc.add(cd.get("Text").toString()) ;
        }
        return rc;
    }

    private ArrayList<String> browseMessagesViaJMS(BrokerService brokerService) throws Exception {
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
    public void stopBrokers() throws Exception {
        for (BrokerService broker : brokers) {
            try {
                stop(broker);
            } catch (Exception e) {
            }
        }
        brokers.clear();
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
        store.setContainer(id);
        store.setReplicas(3);
        store.setZkAddress("localhost:" + connector.getLocalPort());
        store.setHostname("localhost");
        store.setBind("tcp://0.0.0.0:0");
        return store;
    }
}
