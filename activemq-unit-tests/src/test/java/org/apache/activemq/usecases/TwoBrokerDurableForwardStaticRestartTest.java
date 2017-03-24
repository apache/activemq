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

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TopicSubscriber;
import java.io.File;
import java.net.URI;
import java.util.List;

public class TwoBrokerDurableForwardStaticRestartTest extends JmsMultipleBrokersTestSupport {
    protected static final Logger LOG = LoggerFactory.getLogger(TwoBrokerDurableForwardStaticRestartTest.class);
    final ActiveMQTopic dest = new ActiveMQTopic("TEST.FOO");

    public void testNonDurableReceiveThrougRestart() throws Exception {

        bridgeBrokerPair("BrokerA", "BrokerB");
        bridgeBrokerPair("BrokerB", "BrokerC");

        registerDurableForwardSub("BrokerA", dest, "BrokerB");
        registerDurableForwardSub("BrokerB", dest, "BrokerC");

        startAllBrokers();
        waitForBridgeFormation();

        MessageConsumer clientC = createConsumer("BrokerC", dest);
        
        // Send messages
        sendMessages("BrokerA", dest, 100);

        // Get message count
        final MessageIdList messagesFromC = getConsumerMessages("BrokerC", clientC);

        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return messagesFromC.getMessageCount() == 100;
            }});
        
        LOG.info("B got: " +  messagesFromC.getMessageCount());

        assertEquals(100, messagesFromC.getMessageCount());

        destroyBroker("BrokerB");

        // Send messages
        sendMessages("BrokerA", dest, 100);

        BrokerService broker = createBroker(new URI(
                "broker:(tcp://0.0.0.0:61616)/BrokerB"));
        bridgeBrokerPair("BrokerB", "BrokerC");
        broker.start();

        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return messagesFromC.getMessageCount() == 200;
            }});

        LOG.info("B got: " +  messagesFromC.getMessageCount());

        assertEquals(200, messagesFromC.getMessageCount());
    }

    @Override
    protected void configureBroker(BrokerService broker) {
        broker.getManagementContext().setCreateConnector(false);
        broker.setAdvisorySupport(false);
    }

    private void registerDurableForwardSub(String brokerName, ActiveMQTopic dest, String remoteBrokerName) throws Exception {

        // need to match the durable sub that would be created by the bridge in response to a remote durable sub advisory
        String clientId = "NC_" + remoteBrokerName + "_inbound_" + brokerName;
        String subName = "NC-DS_" + brokerName + "_" + dest.getPhysicalName();
        BrokerItem brokerItem = brokers.get(brokerName);
        //brokerItem.broker.getAdminView().createDurableSubscriber(clientId, subName, dest.getPhysicalName(), null);

        Connection c = brokerItem.factory.createConnection();
        c.setClientID(clientId);
        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber topicSubscriber = session.createDurableSubscriber(dest, subName);
        topicSubscriber.close();
        c.close();
    }

    protected NetworkConnector bridgeBrokerPair(String localBrokerName, String remoteBrokerName) throws Exception {
        BrokerService localBroker = brokers.get(localBrokerName).broker;
        BrokerService remoteBroker = brokers.get(remoteBrokerName).broker;

        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI;
        if (!transportConnectors.isEmpty()) {
            remoteURI = transportConnectors.get(0).getConnectUri();
            String uri = "static:(" + remoteURI + ")";
            NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
            connector.setDynamicOnly(false); // so matching durable subs are loaded on start
            connector.setMessageTTL(2);
            connector.setStaticBridge(true);
            localBroker.addNetworkConnector(connector);
            return connector;
        } else {
            throw new Exception("Remote broker has no registered connectors.");
        }
    }

    public void setUp() throws Exception {
        File dataDir = new File(IOHelper.getDefaultDataDirectory());
        LOG.info("Delete dataDir.." + dataDir.getCanonicalPath());
        org.apache.activemq.TestSupport.recursiveDelete(dataDir);
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI(
                "broker:(tcp://0.0.0.0:0)/BrokerA"));
        createBroker(new URI(
                "broker:(tcp://0.0.0.0:61616)/BrokerB"));
        createBroker(new URI(
                "broker:(tcp://0.0.0.0:0)/BrokerC"));

    }
}
