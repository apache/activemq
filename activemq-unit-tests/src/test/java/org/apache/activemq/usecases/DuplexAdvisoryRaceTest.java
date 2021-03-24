/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.network.DemandForwardingBridge;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.TestUtils;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

// https://issues.apache.org/jira/browse/AMQ-6640
public class DuplexAdvisoryRaceTest {
    private static final Logger LOG = LoggerFactory.getLogger(DuplexAdvisoryRaceTest.class);
    private static String hostName;

    final AtomicLong responseReceived = new AtomicLong(0);

    BrokerService brokerA,brokerB;
    String networkConnectorUrlString;

    @BeforeClass
    public static void initIp() throws Exception {
        // attempt to bypass loopback - not vital but it helps to reproduce
        hostName = InetAddress.getLocalHost().getHostAddress();
    }

    @Before
    public void createBrokers() throws Exception {
        networkConnectorUrlString = "tcp://" + hostName + ":" + TestUtils.findOpenPort();

        brokerA = newBroker("A");
        brokerB = newBroker("B");
        responseReceived.set(0);
    }

    @After
    public void stopBrokers() throws Exception {
        brokerA.stop();
        brokerB.stop();
    }


    // to be sure to be sure
    public void repeatTestHang() throws Exception {
        for (int i=0; i<10;i++) {
            testHang();
            stopBrokers();
            createBrokers();
        }
    }

    @Test
    public void testHang() throws Exception {

        brokerA.setPlugins(new BrokerPlugin[] { new BrokerPluginSupport() {
            @Override
            public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
                Subscription subscription = super.addConsumer(context, info);
                // delay return to allow dispatch to interleave
                if (context.isNetworkConnection()) {
                    TimeUnit.MILLISECONDS.sleep(100);
                }
                return subscription;
            };
        }});

        // bridge
        NetworkConnector networkConnector = bridgeBrokers(brokerA, brokerB);

        brokerA.start();
        brokerB.start();

        ActiveMQConnectionFactory brokerAFactory = new ActiveMQConnectionFactory(brokerA.getTransportConnectorByScheme("tcp").getPublishableConnectString()
                + "?jms.watchTopicAdvisories=false");

        ActiveMQConnectionFactory brokerBFactory = new ActiveMQConnectionFactory(brokerB.getTransportConnectorByScheme("tcp").getPublishableConnectString()
                + "?jms.watchTopicAdvisories=false");

        // populate dests
        final int numDests = 400;
        final int numMessagesPerDest = 50;
        final int numConsumersPerDest = 5;
        populate(brokerAFactory, 0, numDests/2, numMessagesPerDest);
        populate(brokerBFactory, numDests/2, numDests, numMessagesPerDest);

        // demand
        List<Connection> connections = new LinkedList<>();
        connections.add(demand(brokerBFactory, 0, numDests/2, numConsumersPerDest));
        connections.add(demand(brokerAFactory, numDests/2, numDests, numConsumersPerDest));


        LOG.info("Allow duplex bridge to connect....");
        // allow bridge to start
        brokerB.startTransportConnector(brokerB.addConnector(networkConnectorUrlString + "?transport.socketBufferSize=1024"));

       if (!Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("received: " + responseReceived.get());
                return responseReceived.get() >= numMessagesPerDest * numDests;
            }
        }, 30*60*1000)) {

           org.apache.activemq.TestSupport.dumpAllThreads("DD");

           // when hung close will also hang!
           for (NetworkBridge networkBridge : networkConnector.activeBridges()) {
               if (networkBridge instanceof DemandForwardingBridge) {
                   DemandForwardingBridge demandForwardingBridge = (DemandForwardingBridge) networkBridge;
                   Socket socket = demandForwardingBridge.getRemoteBroker().narrow(Socket.class);
                   socket.close();
               }
           }
       }

        networkConnector.stop();
        for (Connection connection: connections) {
            try {
                connection.close();
            } catch (Exception ignored) {}
        }
        assertTrue("received all sent: " + responseReceived.get(), responseReceived.get() >= numMessagesPerDest * numDests);
    }


    private void populate(ActiveMQConnectionFactory factory, int minDest, int maxDest, int numMessages) throws JMSException {
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final BytesMessage message = session.createBytesMessage();
        MessageProducer producer = session.createProducer(null);;
        for (int i=minDest; i<maxDest; i++) {
            Destination destination = qFromInt(i);
            for (int j=0; j<numMessages; j++) {
                producer.send(destination, message);
            }
        }
        connection.close();
    }

    private Connection demand(ActiveMQConnectionFactory factory, int minDest, int maxDest, int numConsumers) throws Exception {
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        for (int i=minDest; i<maxDest; i++) {
            Destination destination = qFromInt(i);
            for (int j=0; j<numConsumers; j++) {
                session.createConsumer(destination).setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        responseReceived.incrementAndGet();
                    }
                });
            }
        }
        connection.start();
        return connection;
    }

    private Destination qFromInt(int val) {
        StringBuilder builder = new StringBuilder();
        String digits = String.format("%03d", val);
        for (int i=0; i<3; i++) {
            builder.append(digits.charAt(i));
            if (i < 2) {
                builder.append('.');
            }
        }
        return new ActiveMQQueue("Test." + builder.toString());
    }

    private BrokerService newBroker(String name) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setBrokerName(name);
        brokerService.addConnector("tcp://" + hostName + ":0?transport.socketBufferSize=1024");

        PolicyMap map = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(0);
        map.setDefaultEntry(defaultEntry);
        brokerService.setDestinationPolicy(map);
        return brokerService;
    }


    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker) throws Exception {

        String uri = "static:(failover:(" + networkConnectorUrlString + "?socketBufferSize=1024&trace=false)?maxReconnectAttempts=0)";

        NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
        connector.setName(localBroker.getBrokerName() + "-to-" + remoteBroker.getBrokerName());
        connector.setDuplex(true);
        localBroker.addNetworkConnector(connector);
        return connector;
    }
}