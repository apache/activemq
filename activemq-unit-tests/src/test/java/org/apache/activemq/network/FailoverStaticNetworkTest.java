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
package org.apache.activemq.network;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.management.ObjectName;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.transport.tcp.SslBrokerServiceTest;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.JMXSupport;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverStaticNetworkTest {
    protected static final Logger LOG = LoggerFactory.getLogger(FailoverStaticNetworkTest.class);

    private final static String DESTINATION_NAME = "testQ";
    protected BrokerService brokerA;
    protected BrokerService brokerA1;
    protected BrokerService brokerB;
    protected BrokerService brokerC;


    private SslContext sslContext;

    protected BrokerService createBroker(String scheme, String listenPort, String[] networkToPorts) throws Exception {
        return createBroker(scheme, listenPort, networkToPorts, null);
    }

    protected BrokerService createBroker(String scheme, String listenPort, String[] networkToPorts,
                                         HashMap<String, String> networkProps) throws Exception {
        BrokerService broker = new BrokerService();
        broker.getManagementContext().setCreateConnector(false);
        broker.setSslContext(sslContext);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setBrokerName("Broker_" + listenPort);
        // lazy init listener on broker start
        TransportConnector transportConnector = new TransportConnector();
        transportConnector.setUri(new URI(scheme + "://localhost:" + listenPort));
        List<TransportConnector> transportConnectors = new ArrayList<TransportConnector>();
        transportConnectors.add(transportConnector);
        broker.setTransportConnectors(transportConnectors);
        if (networkToPorts != null && networkToPorts.length > 0) {
            StringBuilder builder = new StringBuilder("static:(failover:(" + scheme + "://localhost:");
            builder.append(networkToPorts[0]);
            for (int i=1;i<networkToPorts.length; i++) {
                builder.append("," + scheme + "://localhost:" + networkToPorts[i]);
            }
            // limit the reconnects in case of initial random connection to slave
            // leaving randomize on verifies that this config is picked up
            builder.append(")?maxReconnectAttempts=0)?useExponentialBackOff=false");
            NetworkConnector nc = broker.addNetworkConnector(builder.toString());
            if (networkProps != null) {
                IntrospectionSupport.setProperties(nc, networkProps);
            }
        }
        return broker;
    }

    private BrokerService createBroker(String listenPort, String dataDir) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.getManagementContext().setCreateConnector(false);
        broker.setBrokerName("Broker_Shared");
        // lazy create transport connector on start completion
        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI("tcp://localhost:" + listenPort));
        broker.addConnector(connector);
        broker.setDataDirectory(dataDir);
        return broker;
    }

    @Before
    public void setUp() throws Exception {
        KeyManager[] km = SslBrokerServiceTest.getKeyManager();
        TrustManager[] tm = SslBrokerServiceTest.getTrustManager();
        sslContext = new SslContext(km, tm, null);
    }

    @After
    public void tearDown() throws Exception {
        brokerB.stop();
        brokerB.waitUntilStopped();

        brokerA.stop();
        brokerA.waitUntilStopped();

        if (brokerA1 != null) {
            brokerA1.stop();
            brokerA1.waitUntilStopped();
        }

        if (brokerC != null) {
            brokerC.stop();
            brokerC.waitUntilStopped();
        }
    }

    @Test
    public void testSendReceiveAfterReconnect() throws Exception {
        brokerA = createBroker("tcp", "61617", null);
        brokerA.start();
        brokerB = createBroker("tcp", "62617", new String[]{"61617"});
        brokerB.start();
        doTestNetworkSendReceive();

        LOG.info("stopping brokerA");
        brokerA.stop();
        brokerA.waitUntilStopped();

        LOG.info("restarting brokerA");
        brokerA = createBroker("tcp", "61617", null);
        brokerA.start();

        doTestNetworkSendReceive();
    }

    @Test
    public void testSendReceiveFailover() throws Exception {
        brokerA = createBroker("tcp", "61617", null);
        brokerA.start();
        brokerB = createBroker("tcp", "62617", new String[]{"61617", "63617"});
        brokerB.start();
        doTestNetworkSendReceive();

        // check mbean
        Set<String> bridgeNames = getNetworkBridgeMBeanName(brokerB);
        assertEquals("only one bridgeName: " + bridgeNames, 1, bridgeNames.size());

        LOG.info("stopping brokerA");
        brokerA.stop();
        brokerA.waitUntilStopped();

        LOG.info("restarting brokerA");
        brokerA = createBroker("tcp", "63617", null);
        brokerA.start();

        doTestNetworkSendReceive();

        Set<String> otherBridgeNames = getNetworkBridgeMBeanName(brokerB);
        assertEquals("only one bridgeName: " + otherBridgeNames, 1, otherBridgeNames.size());

        assertTrue("there was an addition", bridgeNames.addAll(otherBridgeNames));
    }

    private Set<String> getNetworkBridgeMBeanName(BrokerService brokerB) throws Exception {
        Set<String> names = new HashSet<String>();
        for (ObjectName objectName : brokerB.getManagementContext().queryNames(null, null)) {
            if (objectName.getKeyProperty("networkBridge") != null) {
                names.add(objectName.getKeyProperty("networkBridge"));
            }
        }
        return names;
    }

    @Test
    public void testSendReceiveFailoverDuplex() throws Exception {
        final Vector<Throwable> errors = new Vector<Throwable>();
        final String dataDir = "target/data/shared";
        brokerA = createBroker("61617", dataDir);
        brokerA.start();

        final BrokerService slave = createBroker("63617", dataDir);
        brokerA1 = slave;
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    slave.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.add(e);
                }
            }
        });
        executor.shutdown();

        HashMap<String, String> networkConnectorProps = new HashMap<String, String>();
        networkConnectorProps.put("duplex", "true");
        brokerB = createBroker("tcp", "62617", new String[]{"61617", "63617"}, networkConnectorProps);
        brokerB.start();

        doTestNetworkSendReceive(brokerA, brokerB);
        doTestNetworkSendReceive(brokerB, brokerA);

        LOG.info("stopping brokerA (master shared_broker)");
        brokerA.stop();
        brokerA.waitUntilStopped();

        // wait for slave to start
        brokerA1.waitUntilStarted();

        doTestNetworkSendReceive(brokerA1, brokerB);
        doTestNetworkSendReceive(brokerB, brokerA1);

        assertTrue("No unexpected exceptions " + errors, errors.isEmpty());
    }

    @Test
    // master slave piggy in the middle setup
    public void testSendReceiveFailoverDuplexWithPIM() throws Exception {
        final String dataDir = "target/data/shared/pim";
        brokerA = createBroker("61617", dataDir);
        brokerA.start();

        final BrokerService slave = createBroker("63617", dataDir);
        brokerA1 = slave;
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    slave.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        executor.shutdown();

        HashMap<String, String> networkConnectorProps = new HashMap<String, String>();
        networkConnectorProps.put("duplex", "true");
        networkConnectorProps.put("networkTTL", "2");

        brokerB = createBroker("tcp", "62617", new String[]{"61617", "63617"}, networkConnectorProps);
        brokerB.start();

        assertTrue("all props applied", networkConnectorProps.isEmpty());
        networkConnectorProps.put("duplex", "true");
        networkConnectorProps.put("networkTTL", "2");

        brokerC = createBroker("tcp", "64617", new String[]{"61617", "63617"}, networkConnectorProps);
        brokerC.start();
        assertTrue("all props applied a second time", networkConnectorProps.isEmpty());

        doTestNetworkSendReceive(brokerC, brokerB);
        doTestNetworkSendReceive(brokerB, brokerC);

        LOG.info("stopping brokerA (master shared_broker)");
        brokerA.stop();
        brokerA.waitUntilStopped();

        doTestNetworkSendReceive(brokerC, brokerB);
        doTestNetworkSendReceive(brokerB, brokerC);

        brokerC.stop();
        brokerC.waitUntilStopped();
    }

    /**
     * networked broker started after target so first connect attempt succeeds
     * start order is important
     */
    @Test
    public void testSendReceive() throws Exception {

        brokerA = createBroker("tcp", "61617", null);
        brokerA.start();
        brokerB = createBroker("tcp", "62617", new String[]{"61617","1111"});
        brokerB.start();

        doTestNetworkSendReceive();
    }

    @Test
    public void testSendReceiveSsl() throws Exception {

        brokerA = createBroker("ssl", "61617", null);
        brokerA.start();
        brokerB = createBroker("ssl", "62617", new String[]{"61617", "1111"});
        brokerB.start();

        doTestNetworkSendReceive();
    }

    @Test
    public void testRepeatedSendReceiveWithMasterSlaveAlternate() throws Exception {
        doTestRepeatedSendReceiveWithMasterSlaveAlternate(null);
    }

    @Test
    public void testRepeatedSendReceiveWithMasterSlaveAlternateDuplex() throws Exception {
        HashMap<String, String> networkConnectorProps = new HashMap<String, String>();
        networkConnectorProps.put("duplex", "true");

        doTestRepeatedSendReceiveWithMasterSlaveAlternate(networkConnectorProps);
    }

    public void doTestRepeatedSendReceiveWithMasterSlaveAlternate(HashMap<String, String> networkConnectorProps) throws Exception {

        brokerB = createBroker("tcp", "62617", new String[]{"61610","61611"}, networkConnectorProps);
        brokerB.start();

        final AtomicBoolean done = new AtomicBoolean(false);
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!done.get()) {
                        brokerA = createBroker("tcp", "61610", null);
                        brokerA.setBrokerName("Pair");
                        brokerA.setBrokerObjectName(new ObjectName(brokerA.getManagementContext().getJmxDomainName() + ":" + "brokerName="
                                + JMXSupport.encodeObjectNamePart("A") + "," + "Type=Broker"));
                        ((KahaDBPersistenceAdapter)brokerA.getPersistenceAdapter()).getLocker().setLockAcquireSleepInterval(1000);
                        brokerA.start();
                        brokerA.waitUntilStopped();

                        // restart after peer taken over
                        brokerA1.waitUntilStarted();
                    }
                } catch (Exception ignored) {
                    LOG.info("A create/start, unexpected: " + ignored, ignored);
                }
            }
        });

        // start with brokerA as master
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerA != null && brokerA.waitUntilStarted();
            }
        });

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!done.get()) {
                        brokerA1 = createBroker("tcp", "61611", null);
                        brokerA1.setBrokerName("Pair");
                        // so they can coexist in local jmx we set the object name b/c the brokername identifies the shared store
                        brokerA1.setBrokerObjectName(new ObjectName(brokerA.getManagementContext().getJmxDomainName() + ":" + "brokerName="
                            + JMXSupport.encodeObjectNamePart("A1") + "," + "Type=Broker"));
                        ((KahaDBPersistenceAdapter)brokerA1.getPersistenceAdapter()).getLocker().setLockAcquireSleepInterval(1000);
                        brokerA1.start();
                        brokerA1.waitUntilStopped();

                        // restart after peer taken over
                        brokerA.waitUntilStarted();
                    }
                } catch (Exception ignored) {
                    LOG.info("A1 create/start, unexpected: " + ignored, ignored);
                }
            }
        });

        for (int i=0; i<4; i++) {
            BrokerService currentMaster =  (i%2 == 0 ? brokerA : brokerA1);
            LOG.info("iteration: " + i + ", using: " + currentMaster.getBrokerObjectName().getKeyProperty("brokerName"));
            currentMaster.waitUntilStarted();

            doTestNetworkSendReceive(brokerB, currentMaster);

            LOG.info("Stopping " + currentMaster.getBrokerObjectName().getKeyProperty("brokerName"));
            currentMaster.stop();
            currentMaster.waitUntilStopped();
        }

        done.set(true);
        LOG.info("all done");
        executorService.shutdownNow();
    }

    private void doTestNetworkSendReceive() throws Exception, JMSException {
        doTestNetworkSendReceive(brokerB, brokerA);
    }

    private void doTestNetworkSendReceive(final BrokerService to, final BrokerService from) throws Exception, JMSException {

        LOG.info("Creating Consumer on the networked broker ..." + from);

        SslContext.setCurrentSslContext(sslContext);
        // Create a consumer on brokerA
        ConnectionFactory consFactory = createConnectionFactory(from);
        Connection consConn = consFactory.createConnection();
        consConn.start();
        Session consSession = consConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQDestination destination = (ActiveMQDestination) consSession.createQueue(DESTINATION_NAME);
        final MessageConsumer consumer = consSession.createConsumer(destination);

        LOG.info("publishing to " + to);

        sendMessageTo(destination, to);

        boolean gotMessage = Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Message message = consumer.receive(5000);
                LOG.info("from:  " + from.getBrokerObjectName().getKeyProperty("brokerName") +  ", received: " + message);
                return message != null;
            }
        });
        if (!gotMessage) {
            AutoFailTestSupport.dumpAllThreads("noMessage");
        }
        try {
            consConn.close();
        } catch (JMSException ignored) {
        }
        assertTrue("consumer on A got message", gotMessage);
    }

    private void sendMessageTo(ActiveMQDestination destination, BrokerService brokerService) throws Exception {
        ConnectionFactory factory = createConnectionFactory(brokerService);
        Connection conn = factory.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createProducer(destination).send(session.createTextMessage("Hi"));
        conn.close();
    }

    protected ConnectionFactory createConnectionFactory(final BrokerService broker) throws Exception {
        String url = broker.getTransportConnectors().get(0).getServer().getConnectURI().toString();
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        connectionFactory.setOptimizedMessageDispatch(true);
        connectionFactory.setDispatchAsync(false);
        connectionFactory.setUseAsyncSend(false);
        connectionFactory.setOptimizeAcknowledge(false);
        connectionFactory.setAlwaysSyncSend(true);
        return connectionFactory;
    }
}
