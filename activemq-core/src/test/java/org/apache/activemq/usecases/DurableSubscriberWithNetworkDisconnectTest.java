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
import java.util.List;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class DurableSubscriberWithNetworkDisconnectTest extends JmsMultipleBrokersTestSupport {
    private static final Log LOG = LogFactory.getLog(DurableSubscriberWithNetworkDisconnectTest.class);
    private static final int NETWORK_DOWN_TIME = 10000;
    private static final String HUB = "HubBroker";
    private static final String SPOKE = "SpokeBroker";
    private SocketProxy socketProxy;
    private long networkDownTimeStart;
    private long inactiveDuration = 1000;
    private long receivedMsgs = 0;
    private boolean useSocketProxy = true;
    protected static final int MESSAGE_COUNT = 200;
    public boolean useDuplexNetworkBridge = true;
    public boolean simulateStalledNetwork;
    public boolean dynamicOnly = true;
    public long networkTTL = 3;
    public boolean exponentialBackOff;
    public boolean failover = false;
    public boolean inactivity = true;

    public void initCombosForTestSendOnAReceiveOnBWithTransportDisconnect() {
        addCombinationValues("failover", new Object[]{Boolean.FALSE, Boolean.TRUE});
    }

    public void testSendOnAReceiveOnBWithTransportDisconnect() throws Exception {
        bridgeBrokers(SPOKE, HUB);

        startAllBrokers();

        // Setup connection
        URI hubURI = brokers.get(HUB).broker.getVmConnectorURI();
        URI spokeURI = brokers.get(SPOKE).broker.getVmConnectorURI();
        ActiveMQConnectionFactory facHub = new ActiveMQConnectionFactory(hubURI);
        ActiveMQConnectionFactory facSpoke = new ActiveMQConnectionFactory(spokeURI);
        Connection conHub = facHub.createConnection();
        Connection conSpoke = facSpoke.createConnection();
        conHub.setClientID("clientHUB");
        conSpoke.setClientID("clientSPOKE");
        conHub.start();
        conSpoke.start();
        Session sesHub = conHub.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session sesSpoke = conSpoke.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQTopic topic = new ActiveMQTopic("TEST.FOO");
        String consumerName = "consumerName";

        // Setup consumers
        MessageConsumer remoteConsumer = sesSpoke.createDurableSubscriber(topic, consumerName);
        remoteConsumer.setMessageListener(new MessageListener() {
            public void onMessage(Message msg) {
                try {
                    TextMessage textMsg = (TextMessage) msg;
                    receivedMsgs++;
                    LOG.info("Received messages (" + receivedMsgs + "): " + textMsg.getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        // allow subscription information to flow back to Spoke
        sleep(1000);

        // Setup producer
        MessageProducer localProducer = sesHub.createProducer(topic);
        localProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

        // Send messages
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            sleep(50);
            if (i == 50 || i == 150) {
                if (simulateStalledNetwork) {
                    socketProxy.pause();
                } else {
                    socketProxy.close();
                }
                networkDownTimeStart = System.currentTimeMillis();
            } else if (networkDownTimeStart > 0) {
                // restart after NETWORK_DOWN_TIME seconds
                sleep(NETWORK_DOWN_TIME);
                networkDownTimeStart = 0;
                if (simulateStalledNetwork) {
                    socketProxy.goOn();
                } else {
                    socketProxy.reopen();
                }
            } else {
                // slow message production to allow bridge to recover and limit message duplication
                sleep(500);
            }
            Message test = sesHub.createTextMessage("test-" + i);
            localProducer.send(test);
        }

        LOG.info("waiting for messages to flow");
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return receivedMsgs >= MESSAGE_COUNT;
            }
        });

        assertTrue("At least message " + MESSAGE_COUNT +
                " must be received, count=" + receivedMsgs,
                MESSAGE_COUNT <= receivedMsgs);
        brokers.get(HUB).broker.deleteAllMessages();
        brokers.get(SPOKE).broker.deleteAllMessages();
        conHub.close();
        conSpoke.close();
    }

    @Override
    protected void startAllBrokers() throws Exception {
        // Ensure HUB is started first so bridge will be active from the get go
        BrokerItem brokerItem = brokers.get(HUB);
        brokerItem.broker.start();
        brokerItem = brokers.get(SPOKE);
        brokerItem.broker.start();
        sleep(600);
    }

    public void setUp() throws Exception {
        networkDownTimeStart = 0;
        inactiveDuration = 1000;
        useSocketProxy = true;
        receivedMsgs = 0;
        super.setAutoFail(true);
        super.setUp();
        final String options = "?persistent=true&useJmx=false&deleteAllMessagesOnStartup=true";
        createBroker(new URI("broker:(tcp://localhost:61617)/" + HUB + options));
        createBroker(new URI("broker:(tcp://localhost:61616)/" + SPOKE + options));
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if (socketProxy != null) {
            socketProxy.close();
        }
    }

    public static Test suite() {
        return suite(DurableSubscriberWithNetworkDisconnectTest.class);
    }

    private void sleep(int milliSecondTime) {
        try {
            Thread.sleep(milliSecondTime);
        } catch (InterruptedException igonred) {
        }
    }

    @Override
    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker, boolean l_dynamicOnly, int networkTTL, boolean l_conduit, boolean l_failover) throws Exception {
        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI;
        if (!transportConnectors.isEmpty()) {
            remoteURI = ((TransportConnector) transportConnectors.get(0)).getConnectUri();
            if (useSocketProxy) {
                socketProxy = new SocketProxy(remoteURI);
                remoteURI = socketProxy.getUrl();
            }
            String options = "";
            if (failover) {
                options = "static:(failover:(" + remoteURI;
            } else {
                options = "static:(" + remoteURI;
            }
            if (inactivity) {
                options += "?wireFormat.maxInactivityDuration=" + inactiveDuration + "&wireFormat.maxInactivityDurationInitalDelay=" + inactiveDuration + ")";
            } else {
                options += ")";
            }

             if (failover) {
                options += "?maxReconnectAttempts=0)";
             }

            options += "?useExponentialBackOff=" + exponentialBackOff;
            DiscoveryNetworkConnector connector = new DiscoveryNetworkConnector(new URI(options));
            connector.setDynamicOnly(dynamicOnly);
            connector.setNetworkTTL(networkTTL);
            localBroker.addNetworkConnector(connector);
            maxSetupTime = 2000;
            if (useDuplexNetworkBridge) {
                connector.setDuplex(true);
            }
            return connector;
        } else {
            throw new Exception("Remote broker has no registered connectors.");
        }
    }
}
