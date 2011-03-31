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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.CombinationTestSupport;
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

    BrokerService a, b;
    ActiveMQQueue sendQ = new ActiveMQQueue("sendQ");
    static final String connectionIdMarker = "ID:marker.";
    ActiveMQTempQueue replyQWildcard = new ActiveMQTempQueue(connectionIdMarker + ">");
    private long receiveTimeout = 30000;

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
                "    brokerName=\"%HOST%\" persistent=\"false\" advisorySupport=\"false\" useJmx=\"false\" >" +
                "   <destinationPolicy>" +
                "    <policyMap>" +
                "     <policyEntries>" +
                "      <policyEntry optimizedDispatch=\"true\">"+
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

        doTestNonAdvisoryNetworkRequestReply();
    }

    public void testNonAdvisoryNetworkRequestReply() throws Exception {
        createBridgeAndStartBrokers();
        doTestNonAdvisoryNetworkRequestReply();
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

        // responder
        ActiveMQConnectionFactory consumerFactory = createConnectionFactory(b);
        ActiveMQConnection consumerConnection = createConnection(consumerFactory);

        ActiveMQSession consumerSession = (ActiveMQSession)consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(sendQ);
        TextMessage received = (TextMessage) consumer.receive(receiveTimeout);
        assertNotNull(received);

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

        verifyAllTempQueuesAreGone();
    }

    private void verifyAllTempQueuesAreGone() throws Exception {
        for (BrokerService brokerService : new BrokerService[]{a, b}) {
            RegionBroker regionBroker = (RegionBroker) brokerService.getRegionBroker();
            Map temps = regionBroker.getTempTopicRegion().getDestinationMap();
            assertTrue("no temp topics on " + brokerService + ", " + temps, temps.isEmpty());
            temps = regionBroker.getTempQueueRegion().getDestinationMap();
            assertTrue("no temp queues on " + brokerService + ", " + temps, temps.isEmpty());
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
        a.start();
        b.start();
    }

    public void tearDown() throws Exception {
        stop(a);
        stop(b);
    }

    private void stop(BrokerService broker) throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    private void bridge(BrokerService from, BrokerService to) throws Exception {
        TransportConnector toConnector = to.addConnector("tcp://localhost:0");
        NetworkConnector bridge =
                from.addNetworkConnector("static://" + toConnector.getPublishableConnectString());
        bridge.addStaticallyIncludedDestination(sendQ);
        bridge.addStaticallyIncludedDestination(replyQWildcard);
    }

    private BrokerService configureBroker(String brokerName) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(brokerName);
        broker.setAdvisorySupport(false);
        broker.setPersistent(false);
        broker.setUseJmx(false);

        PolicyMap map = new PolicyMap();
        PolicyEntry tempReplyQPolicy = new PolicyEntry();
        tempReplyQPolicy.setOptimizedDispatch(true);
        map.put(replyQWildcard, tempReplyQPolicy);
        broker.setDestinationPolicy(map);
        return broker;
    }
}
