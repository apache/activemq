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


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Map;
import java.util.Vector;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.apache.activemq.xbean.XBeanBrokerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestReplyNoAdvisoryNetworkTest extends JmsMultipleBrokersTestSupport {
    private static final transient Logger LOG = LoggerFactory.getLogger(RequestReplyNoAdvisoryNetworkTest.class);

    Vector<BrokerService> brokers = new Vector<BrokerService>();
    BrokerService a, b;
    ActiveMQQueue sendQ = new ActiveMQQueue("sendQ");
    static final String connectionIdMarker = "ID:marker.";
    ActiveMQTempQueue replyQWildcard = new ActiveMQTempQueue(connectionIdMarker + ">");
    private final long receiveTimeout = 30000;

    public void testNonAdvisoryNetworkRequestReplyXmlConfig() throws Exception {
        final String xmlConfigString = new String(
                "<beans" +
                " xmlns=\"http://www.springframework.org/schema/beans\"" +
                " xmlns:amq=\"http://activemq.apache.org/schema/core\"" +
                " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" +
                " xsi:schemaLocation=\"http://www.springframework.org/schema/beans" +
                " http://www.springframework.org/schema/beans/spring-beans-2.0.xsd" +
                " http://activemq.apache.org/schema/core" +
                " http://activemq.apache.org/schema/core/activemq-core.xsd\">" +
                "  <broker xmlns=\"http://activemq.apache.org/schema/core\" id=\"broker\"" +
                "    allowTempAutoCreationOnSend=\"true\" schedulePeriodForDestinationPurge=\"1000\"" +
                "    brokerName=\"%HOST%\" persistent=\"false\" advisorySupport=\"false\" useJmx=\"false\" >" +
                "   <destinationPolicy>" +
                "    <policyMap>" +
                "     <policyEntries>" +
                "      <policyEntry optimizedDispatch=\"true\"  gcInactiveDestinations=\"true\" gcWithNetworkConsumers=\"true\" inactiveTimoutBeforeGC=\"1000\">"+
                "       <destination>"+
                "        <tempQueue physicalName=\"" + replyQWildcard.getPhysicalName() + "\"/>" +
                "       </destination>" +
                "      </policyEntry>" +
                "     </policyEntries>" +
                "    </policyMap>" +
                "   </destinationPolicy>" +
                "   <networkConnectors>" +
                "    <networkConnector uri=\"multicast://default\">" +
                "     <staticallyIncludedDestinations>" +
                "      <queue physicalName=\"" + sendQ.getPhysicalName() + "\"/>" +
                "      <tempQueue physicalName=\"" + replyQWildcard.getPhysicalName() + "\"/>" +
                "     </staticallyIncludedDestinations>" +
                "    </networkConnector>" +
                "   </networkConnectors>" +
                "   <transportConnectors>" +
                "     <transportConnector uri=\"tcp://0.0.0.0:0\" discoveryUri=\"multicast://default\" />" +
                "   </transportConnectors>" +
                "  </broker>" +
                "</beans>");
        final String localProtocolScheme = "inline";
        URL.setURLStreamHandlerFactory(new URLStreamHandlerFactory() {
            @Override
            public URLStreamHandler createURLStreamHandler(String protocol) {
                if (localProtocolScheme.equalsIgnoreCase(protocol)) {
                    return new URLStreamHandler() {
                        @Override
                        protected URLConnection openConnection(URL u) throws IOException {
                            return new URLConnection(u) {
                                @Override
                                public void connect() throws IOException {
                                }
                                @Override
                                public InputStream getInputStream() throws IOException {
                                    return new ByteArrayInputStream(xmlConfigString.replace("%HOST%", url.getFile()).getBytes("UTF-8"));
                                }
                            };
                        }
                    };
                }
                return null;
            }
        });
        a = new XBeanBrokerFactory().createBroker(new URI("xbean:" + localProtocolScheme + ":A"));
        b = new XBeanBrokerFactory().createBroker(new URI("xbean:" + localProtocolScheme + ":B"));
        brokers.add(a);
        brokers.add(b);

        doTestNonAdvisoryNetworkRequestReply();
    }

    public void testNonAdvisoryNetworkRequestReply() throws Exception {
        createBridgeAndStartBrokers();
        doTestNonAdvisoryNetworkRequestReply();
    }

    public void testNonAdvisoryNetworkRequestReplyWithPIM() throws Exception {
        a = configureBroker("A");
        b = configureBroker("B");
        BrokerService hub = configureBroker("M");
        hub.setAllowTempAutoCreationOnSend(true);
        configureForPiggyInTheMiddle(bridge(a, hub));
        configureForPiggyInTheMiddle(bridge(b, hub));

        startBrokers();

        waitForBridgeFormation(hub, 2, 0);
        doTestNonAdvisoryNetworkRequestReply();
    }

    private void configureForPiggyInTheMiddle(NetworkConnector bridge) {
        bridge.setDuplex(true);
        bridge.setNetworkTTL(2);
    }

    public void doTestNonAdvisoryNetworkRequestReply() throws Exception {

        waitForBridgeFormation(a, 1, 0);
        waitForBridgeFormation(b, 1, 0);

        ActiveMQConnectionFactory sendFactory = createConnectionFactory(a);
        ActiveMQConnection sendConnection = createConnection(sendFactory);

        ActiveMQSession sendSession = (ActiveMQSession)sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = sendSession.createProducer(sendQ);
        ActiveMQTempQueue realReplyQ = (ActiveMQTempQueue) sendSession.createTemporaryQueue();
        TextMessage message = sendSession.createTextMessage("1");
        message.setJMSReplyTo(realReplyQ);
        producer.send(message);
        LOG.info("request sent");

        // responder
        ActiveMQConnectionFactory consumerFactory = createConnectionFactory(b);
        ActiveMQConnection consumerConnection = createConnection(consumerFactory);

        ActiveMQSession consumerSession = (ActiveMQSession)consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(sendQ);
        TextMessage received = (TextMessage) consumer.receive(receiveTimeout);
        assertNotNull("got request from sender ok", received);

        LOG.info("got request, sending reply");

        MessageProducer consumerProducer = consumerSession.createProducer(received.getJMSReplyTo());
        consumerProducer.send(consumerSession.createTextMessage("got " + received.getText()));
        // temp dest on reply broker tied to this connection, setOptimizedDispatch=true ensures
        // message gets delivered before destination is removed
        consumerConnection.close();

        // reply consumer
        MessageConsumer replyConsumer = sendSession.createConsumer(realReplyQ);
        TextMessage reply = (TextMessage) replyConsumer.receive(receiveTimeout);
        assertNotNull("expected reply message", reply);
        assertEquals("text is as expected", "got 1", reply.getText());
        sendConnection.close();

        LOG.info("checking for dangling temp destinations");
        // ensure all temp dests get cleaned up on all brokers
        for (BrokerService brokerService : brokers) {
            final RegionBroker regionBroker = (RegionBroker) brokerService.getRegionBroker();
            assertTrue("all temps are gone on " + regionBroker.getBrokerName(), Wait.waitFor(new Wait.Condition(){
                @Override
                public boolean isSatisified() throws Exception {
                    Map<?,?> tempTopics = regionBroker.getTempTopicRegion().getDestinationMap();
                    LOG.info("temp topics on " + regionBroker.getBrokerName() + ", " + tempTopics);
                    Map<?,?> tempQ = regionBroker.getTempQueueRegion().getDestinationMap();
                    LOG.info("temp queues on " + regionBroker.getBrokerName() + ", " + tempQ);
                    return tempQ.isEmpty() && tempTopics.isEmpty();
                }
            }));
        }
    }

    private ActiveMQConnection createConnection(ActiveMQConnectionFactory factory) throws Exception {
        ActiveMQConnection c =(ActiveMQConnection) factory.createConnection();
        c.start();
        return c;
    }

    private ActiveMQConnectionFactory createConnectionFactory(BrokerService brokerService) throws Exception {
        String target = brokerService.getTransportConnectors().get(0).getPublishableConnectString();
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory(target);
        factory.setWatchTopicAdvisories(false);
        factory.setConnectionIDPrefix(connectionIdMarker + brokerService.getBrokerName());
        return factory;
    }

    public void createBridgeAndStartBrokers() throws Exception {
        a = configureBroker("A");
        b = configureBroker("B");
        bridge(a, b);
        bridge(b, a);
        startBrokers();
    }

    private void startBrokers() throws Exception {
        for (BrokerService broker: brokers) {
            broker.start();
        }
    }

    @Override
    public void tearDown() throws Exception {
        for (BrokerService broker: brokers) {
            broker.stop();
        }
        brokers.clear();
    }


    private NetworkConnector bridge(BrokerService from, BrokerService to) throws Exception {
        TransportConnector toConnector = to.getTransportConnectors().get(0);
        NetworkConnector bridge =
                from.addNetworkConnector("static://" + toConnector.getPublishableConnectString());
        bridge.addStaticallyIncludedDestination(sendQ);
        bridge.addStaticallyIncludedDestination(replyQWildcard);
        return bridge;
    }

    private BrokerService configureBroker(String brokerName) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(brokerName);
        broker.setAdvisorySupport(false);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setSchedulePeriodForDestinationPurge(1000);
        broker.setAllowTempAutoCreationOnSend(true);

        PolicyMap map = new PolicyMap();
        PolicyEntry tempReplyQPolicy = new PolicyEntry();
        tempReplyQPolicy.setOptimizedDispatch(true);
        tempReplyQPolicy.setGcInactiveDestinations(true);
        tempReplyQPolicy.setGcWithNetworkConsumers(true);
        tempReplyQPolicy.setInactiveTimoutBeforeGC(1000);
        map.put(replyQWildcard, tempReplyQPolicy);
        broker.setDestinationPolicy(map);

        broker.addConnector("tcp://localhost:0");
        brokers.add(broker);
        return broker;
    }
}
