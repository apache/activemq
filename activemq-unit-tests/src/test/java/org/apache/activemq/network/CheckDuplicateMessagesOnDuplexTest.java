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

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.net.ServerSocketFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.command.Response;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.leveldb.LevelDBPersistenceAdapter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.nio.NIOTransport;
import org.apache.activemq.transport.nio.NIOTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.wireformat.WireFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

/**
 *
 * @author x22koe
 */
public class CheckDuplicateMessagesOnDuplexTest {

    private static final Logger log = LoggerFactory.getLogger(CheckDuplicateMessagesOnDuplexTest.class);
    private BrokerService localBroker;
    private BrokerService remoteBroker;
    private ActiveMQConnectionFactory localFactory;
    private ActiveMQConnectionFactory remoteFactory;
    private Session localSession;
    private MessageConsumer consumer;
    private Session remoteSession;
    private MessageProducer producer;
    private Connection remoteConnection;
    private Connection localConnection;
    private DebugTransportFilter debugTransportFilter;
    private boolean useLevelDB = false;

    public CheckDuplicateMessagesOnDuplexTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testConnectionLossBehaviorBeforeAckIsSent() throws Exception {
        createBrokers();
        localBroker.deleteAllMessages();
        remoteBroker.deleteAllMessages();
        startBrokers();
        openConnections();

        Thread.sleep(1000);
        log.info("\n\n==============================================\nsend hello1\n");

        // simulate network failure between REMOTE and LOCAL just before the reception response is sent back to REMOTE
        debugTransportFilter.closeOnResponse = true;

        producer.send(remoteSession.createTextMessage("hello1"));
        Message msg = consumer.receive(30000);

        assertNotNull("expected hello1", msg);
        assertEquals("hello1", ((TextMessage) msg).getText());

        Thread.sleep(1000);
        log.info("\n\n------------------------------------------\nsend hello2\n");

        producer.send(remoteSession.createTextMessage("hello2"));
        msg = consumer.receive(30000);

        assertNotNull("expected hello2", msg);
        assertEquals("hello2", ((TextMessage) msg).getText());

        closeLocalConnection();

        Thread.sleep(1000);
        log.info("\n\n------------------------------------------\nsend hello3\n");

        openLocalConnection();

        Thread.sleep(1000);

        producer.send(remoteSession.createTextMessage("hello3"));
        msg = consumer.receive(30000);

        assertNotNull("expected hello3", msg);
        assertEquals("hello3", ((TextMessage) msg).getText());

        Thread.sleep(1000);
        log.info("\n\n==============================================\n\n");

        closeConnections();
        stopBrokers();

        // restart the local broker, which should be empty

        Thread.sleep(1000);
        log.info("\n\n##############################################\n\n");

        createLocalBroker();
        startLocalBroker();
        openLocalConnection();

        // this should not return the "hello1" message
        msg = consumer.receive(1000);

        closeLocalConnection();
        stopLocalBroker();

        assertNull(msg);
    }

    private void createBrokers() throws Exception {
        createLocalBroker();
        createRemoteBroker();
    }

    private void createLocalBroker() throws Exception {
        localBroker = new BrokerService();
        localBroker.setBrokerName("LOCAL");
        localBroker.setUseJmx(true);
        localBroker.setSchedulePeriodForDestinationPurge(5000);
        ManagementContext managementContext = new ManagementContext();
        managementContext.setCreateConnector(false);
        localBroker.setManagementContext(managementContext);
        PersistenceAdapter persistenceAdapter = persistanceAdapterFactory("target/local");
        localBroker.setPersistenceAdapter(persistenceAdapter);
        List<TransportConnector> transportConnectors = new ArrayList<TransportConnector>();
        DebugTransportFactory tf = new DebugTransportFactory();
        TransportServer transport = tf.doBind(URI.create("nio://127.0.0.1:23539"));
        TransportConnector transportConnector = new TransportConnector(transport);
        transportConnector.setName("tc");
        transportConnector.setAuditNetworkProducers(true);
        transportConnectors.add(transportConnector);
        localBroker.setTransportConnectors(transportConnectors);
    }

    private void createRemoteBroker() throws Exception {
        remoteBroker = new BrokerService();
        remoteBroker.setBrokerName("REMOTE");
        remoteBroker.setUseJmx(true);
        remoteBroker.setSchedulePeriodForDestinationPurge(5000);
        ManagementContext managementContext = new ManagementContext();
        managementContext.setCreateConnector(false);
        remoteBroker.setManagementContext(managementContext);
        PersistenceAdapter persistenceAdapter = persistanceAdapterFactory("target/remote");
        remoteBroker.setPersistenceAdapter(persistenceAdapter);
        List<NetworkConnector> networkConnectors = new ArrayList<NetworkConnector>();
        DiscoveryNetworkConnector networkConnector = new DiscoveryNetworkConnector();
        networkConnector.setName("to local");
        // set maxInactivityDuration to 0, otherwise the broker restarts while you are in the debugger
        networkConnector.setUri(URI.create("static://(tcp://127.0.0.1:23539?wireFormat.maxInactivityDuration=0)"));
        networkConnector.setDuplex(true);
        //networkConnector.setNetworkTTL(5);
        //networkConnector.setDynamicOnly(true);
        networkConnector.setAlwaysSyncSend(true);
        networkConnector.setDecreaseNetworkConsumerPriority(false);
        networkConnector.setPrefetchSize(1);
        networkConnector.setCheckDuplicateMessagesOnDuplex(true);
        networkConnectors.add(networkConnector);
        remoteBroker.setNetworkConnectors(networkConnectors);
    }

    private void startBrokers() throws Exception {
        startLocalBroker();
        startRemoteBroker();
    }

    private void startLocalBroker() throws Exception {
        localBroker.start();
        localBroker.waitUntilStarted();
    }

    private void startRemoteBroker() throws Exception {
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
    }

    private void openConnections() throws JMSException {
        openLocalConnection();
        openRemoteConnection();
    }

    private void openLocalConnection() throws JMSException {
        localFactory = new ActiveMQConnectionFactory(localBroker.getVmConnectorURI());
        //localFactory.setSendAcksAsync(false);
        localConnection = localFactory.createConnection();
        localConnection.start();
        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = localSession.createConsumer(localSession.createQueue("testqueue"));
    }

    private void openRemoteConnection() throws JMSException {
        remoteFactory = new ActiveMQConnectionFactory(remoteBroker.getVmConnectorURI());
        //remoteFactory.setSendAcksAsync(false);
        remoteConnection = remoteFactory.createConnection();
        remoteConnection.start();
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = remoteSession.createProducer(remoteSession.createQueue("testqueue"));
    }

    private void closeConnections() throws JMSException {
        closeLocalConnection();
        closeRemoteConnection();
    }

    private void closeLocalConnection() throws JMSException {
        localConnection.close();
    }

    private void closeRemoteConnection() throws JMSException {
        remoteConnection.close();
    }

    private void stopBrokers() throws Exception {
        stopRemoteBroker();
        stopLocalBroker();
    }

    private void stopLocalBroker() throws Exception {
        localBroker.stop();
        localBroker.waitUntilStopped();
    }

    private void stopRemoteBroker() throws Exception {
        remoteBroker.stop();
        remoteBroker.waitUntilStopped();
    }

    private PersistenceAdapter persistanceAdapterFactory(String path) {
        if (useLevelDB) {
            return persistanceAdapterFactory_LevelDB(path);
        } else {
            return persistanceAdapterFactory_KahaDB(path);
        }
    }

    private PersistenceAdapter persistanceAdapterFactory_KahaDB(String path) {
        KahaDBPersistenceAdapter kahaDBPersistenceAdapter = new KahaDBPersistenceAdapter();
        kahaDBPersistenceAdapter.setDirectory(new File(path));
        kahaDBPersistenceAdapter.setIgnoreMissingJournalfiles(true);
        kahaDBPersistenceAdapter.setCheckForCorruptJournalFiles(true);
        kahaDBPersistenceAdapter.setChecksumJournalFiles(true);
        return kahaDBPersistenceAdapter;
    }

    private PersistenceAdapter persistanceAdapterFactory_LevelDB(String path) {
        LevelDBPersistenceAdapter levelDBPersistenceAdapter = new LevelDBPersistenceAdapter();
        levelDBPersistenceAdapter.setDirectory(new File(path));
        return levelDBPersistenceAdapter;
    }

    private class DebugTransportFactory extends NIOTransportFactory {

        @Override
        protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory)
                throws IOException, URISyntaxException {
            return new DebugTransportServer(this, location, serverSocketFactory);
        }
    }

    private class DebugTransportServer extends TcpTransportServer {

        public DebugTransportServer(TcpTransportFactory transportFactory, URI location,
                                    ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
            super(transportFactory, location, serverSocketFactory);
        }

        @Override
        protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
            Transport transport;
            transport = new NIOTransport(format, socket);
            debugTransportFilter = new DebugTransportFilter(transport);
            return debugTransportFilter;
        }
    }

    private class DebugTransportFilter extends TransportFilter {

        boolean closeOnResponse = false;

        public DebugTransportFilter(Transport next) {
            super(next);
        }

        @Override
        public void oneway(Object command) throws IOException {
            if (closeOnResponse && command instanceof Response) {
                closeOnResponse = false;
                log.warn("\n\nclosing connection before response is sent\n\n");
                try {
                    ((NIOTransport) next).stop();
                } catch (Exception ex) {
                    log.error("couldn't stop niotransport", ex);
                }
                // don't send response
                return;
            }
            super.oneway(command);
        }
    }
}