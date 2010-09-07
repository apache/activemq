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

import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.transport.tcp.SslBrokerServiceTest;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FailoverStaticNetworkTest {
    protected static final Log LOG = LogFactory.getLog(FailoverStaticNetworkTest.class);

	private final static String DESTINATION_NAME = "testQ";
	protected BrokerService brokerA;
    protected BrokerService brokerB;


    private SslContext sslContext;
    
    protected BrokerService createBroker(String scheme, String listenPort, String[] networkToPorts) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        broker.setSslContext(sslContext);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setBrokerName("Broker_" + listenPort);
        broker.addConnector(scheme + "://localhost:" + listenPort);
        if (networkToPorts != null && networkToPorts.length > 0) {
            StringBuilder builder = new StringBuilder("static:(failover:(" + scheme + "://localhost:");
            builder.append(networkToPorts[0]);
            for (int i=1;i<networkToPorts.length; i++) {
                builder.append("," + scheme + "://localhost:" + networkToPorts[i]);
            }
            builder.append(")?randomize=false)");
            broker.addNetworkConnector(builder.toString());
        }
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

        LOG.info("stopping brokerA");
        brokerA.stop();
        brokerA.waitUntilStopped();

        LOG.info("restarting brokerA");
        brokerA = createBroker("tcp", "63617", null);
        brokerA.start();

        doTestNetworkSendReceive();
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

    private void doTestNetworkSendReceive() throws Exception, JMSException {
        LOG.info("Creating Consumer on the networked brokerA ...");
        
        SslContext.setCurrentSslContext(sslContext);
        // Create a consumer on brokerA
        ConnectionFactory consFactory = createConnectionFactory(brokerA);
        Connection consConn = consFactory.createConnection();
        consConn.start();
        Session consSession = consConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQDestination destination = (ActiveMQDestination) consSession.createQueue(DESTINATION_NAME);
        final MessageConsumer consumer = consSession.createConsumer(destination);

        LOG.info("publishing to brokerB");

        sendMessageTo(destination, brokerB);
        
        boolean gotMessage = Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return consumer.receive(1000) != null;
            }      
        });
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
        String url = ((TransportConnector) broker.getTransportConnectors().get(0)).getServer().getConnectURI().toString();
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        connectionFactory.setOptimizedMessageDispatch(true);
        connectionFactory.setDispatchAsync(false);
        connectionFactory.setUseAsyncSend(false);
        connectionFactory.setOptimizeAcknowledge(false);
        connectionFactory.setAlwaysSyncSend(true);
        return connectionFactory;
    }
}