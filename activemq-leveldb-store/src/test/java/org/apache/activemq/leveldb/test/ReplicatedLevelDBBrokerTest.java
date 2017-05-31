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
import org.junit.Ignore;
import org.junit.Test;

import javax.jms.*;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;

/**
 * Holds broker unit tests of the replicated leveldb store.
 */
public class ReplicatedLevelDBBrokerTest extends ZooKeeperTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(ReplicatedLevelDBBrokerTest.class);
    final SynchronousQueue<BrokerService> masterQueue = new SynchronousQueue<BrokerService>();
    ArrayList<BrokerService> brokers = new ArrayList<BrokerService>();

    /**
     * Tries to replicate the problem reported at:
     * https://issues.apache.org/jira/browse/AMQ-4837
     */
    @Ignore("https://issues.apache.org/jira/browse/AMQ-5512")
    @Test(timeout = 1000*60*10)
    public void testAMQ4837viaJMS() throws Throwable {
        testAMQ4837(false);
    }

  /**
     * Tries to replicate the problem reported at:
     * https://issues.apache.org/jira/browse/AMQ-4837
     */
   @Ignore("https://issues.apache.org/jira/browse/AMQ-5512")
    @Test(timeout = 1000*60*10)
    public void testAMQ4837viaJMX() throws Throwable {
        for (int i = 0; i < 2; i++) {
            LOG.info("testAMQ4837viaJMX - Iteration: " + i);
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

    public interface Client{
        public void execute(Connection connection) throws Exception;
    }

    protected Thread startFailoverClient(String name, final Client client) throws IOException, URISyntaxException {
        String url = "failover://(tcp://localhost:"+port+")?maxReconnectDelay=500&nested.wireFormat.maxInactivityDuration=1000";
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        Thread rc = new Thread(name) {
            @Override
            public void run() {
                Connection connection = null;
                try {
                    connection = factory.createConnection();
                    client.execute(connection);
                } catch (Throwable e) {
                    e.printStackTrace();
                } finally {
                    try {
                        connection.close();
                    } catch (JMSException e) {
                    }
                }
            }
        };
        rc.start();
        return rc;
    }

    @Test
    @Ignore
    public void testReplicationQuorumLoss() throws Throwable {

        System.out.println("======================================");
        System.out.println(" Start 2 ActiveMQ nodes.");
        System.out.println("======================================");
        startBrokerAsync(createBrokerNode("node-1", port));
        startBrokerAsync(createBrokerNode("node-2", port));
        BrokerService master = waitForNextMaster();
        System.out.println("======================================");
        System.out.println(" Start the producer and consumer");
        System.out.println("======================================");

        final AtomicBoolean stopClients = new AtomicBoolean(false);
        final ArrayBlockingQueue<String> errors = new ArrayBlockingQueue<String>(100);
        final AtomicLong receivedCounter = new AtomicLong();
        final AtomicLong sentCounter = new AtomicLong();
        Thread producer = startFailoverClient("producer", new Client() {
            @Override
            public void execute(Connection connection) throws Exception {
                Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(session.createQueue("test"));
                long actual = 0;
                while(!stopClients.get()) {
                    TextMessage msg = session.createTextMessage("Hello World");
                    msg.setLongProperty("id", actual++);
                    producer.send(msg);
                    sentCounter.incrementAndGet();
                }
            }
        });

        Thread consumer = startFailoverClient("consumer", new Client() {
            @Override
            public void execute(Connection connection) throws Exception {
                connection.start();
                Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(session.createQueue("test"));
                long expected = 0;
                while(!stopClients.get()) {
                    Message msg = consumer.receive(200);
                    if( msg!=null ) {
                        long actual = msg.getLongProperty("id");
                        if( actual != expected ) {
                            errors.offer("Received got unexpected msg id: "+actual+", expected: "+expected);
                        }
                        msg.acknowledge();
                        expected = actual+1;
                        receivedCounter.incrementAndGet();
                    }
                }
            }
        });

        try {
            assertCounterMakesProgress(sentCounter, 10, TimeUnit.SECONDS);
            assertCounterMakesProgress(receivedCounter, 5, TimeUnit.SECONDS);
            assertNull(errors.poll());

            System.out.println("======================================");
            System.out.println(" Master should stop once the quorum is lost.");
            System.out.println("======================================");
            ArrayList<BrokerService> stopped = stopSlaves();// stopping the slaves should kill the quorum.
            assertStopsWithin(master, 10, TimeUnit.SECONDS);
            assertNull(errors.poll()); // clients should not see an error since they are failover clients.
            stopped.add(master);

            System.out.println("======================================");
            System.out.println(" Restart the slave. Clients should make progress again..");
            System.out.println("======================================");
            startBrokersAsync(createBrokerNodes(stopped));
            assertCounterMakesProgress(sentCounter, 10, TimeUnit.SECONDS);
            assertCounterMakesProgress(receivedCounter, 5, TimeUnit.SECONDS);
            assertNull(errors.poll());
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            // Wait for the clients to stop..
            stopClients.set(true);
            producer.join();
            consumer.join();
        }
    }

    protected void startBrokersAsync(ArrayList<BrokerService> brokers) {
        for (BrokerService broker : brokers) {
            startBrokerAsync(broker);
        }
    }

    protected ArrayList<BrokerService> createBrokerNodes(ArrayList<BrokerService> brokers) throws Exception {
        ArrayList<BrokerService> rc = new ArrayList<BrokerService>();
        for (BrokerService b : brokers) {
            rc.add(createBrokerNode(b.getBrokerName(), connectPort(b)));
        }
        return rc;
    }

    protected ArrayList<BrokerService> stopSlaves() throws Exception {
        ArrayList<BrokerService> rc = new ArrayList<BrokerService>();
        for (BrokerService broker : brokers) {
            if( broker.isSlave() ) {
                System.out.println("Stopping slave: "+broker.getBrokerName());
                broker.stop();
                broker.waitUntilStopped();
                rc.add(broker);
            }
        }
        brokers.removeAll(rc);
        return rc;
    }

    protected void assertStopsWithin(final BrokerService master, int timeout, TimeUnit unit) throws InterruptedException {
        within(timeout, unit, new Task(){
            @Override
            public void run() throws Exception {
                assertTrue(master.isStopped());
            }
        });
    }

    protected void assertCounterMakesProgress(final AtomicLong counter, int timeout, TimeUnit unit) throws InterruptedException {
        final long initial = counter.get();
        within(timeout, unit, new Task(){
            public void run() throws Exception {
                assertTrue(initial < counter.get());
            }
        });
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
            brokers.remove(prevMaster);
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
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:"+ connectPort(brokerService));
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

    private int connectPort(BrokerService brokerService) throws IOException, URISyntaxException {
        TransportConnector connector = brokerService.getTransportConnectors().get(0);
        return connector.getConnectUri().getPort();
    }

    int port;
    @Before
    public void findFreePort() throws Exception {
        ServerSocket socket = new ServerSocket(0);
        port = socket.getLocalPort();
        socket.close();
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
        resetDataDirs();
    }

    private BrokerService createBrokerNode(String id) throws Exception {
        return createBrokerNode(id, 0);
    }

    private BrokerService createBrokerNode(String id, int port) throws Exception {
        BrokerService bs = new BrokerService();
        bs.getManagementContext().setCreateConnector(false);
        brokers.add(bs);
        bs.setBrokerName(id);
        bs.setPersistenceAdapter(createStoreNode(id));
        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI("tcp://0.0.0.0:" + port));
        bs.addConnector(connector);
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
        store.setSync("quorum_disk");
        store.setZkAddress("localhost:" + connector.getLocalPort());
        store.setZkSessionTimeout("15s");
        store.setHostname("localhost");
        store.setBind("tcp://0.0.0.0:0");
        return store;
    }
}
