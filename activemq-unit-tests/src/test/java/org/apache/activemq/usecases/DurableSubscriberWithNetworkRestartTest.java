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

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Set;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import static org.junit.Assume.assumeNotNull;


public class DurableSubscriberWithNetworkRestartTest extends JmsMultipleBrokersTestSupport {
    private static final Log LOG = LogFactory.getLog(DurableSubscriberWithNetworkRestartTest.class);
    private static final String HUB = "HubBroker";
    private static final String SPOKE = "SpokeBroker";
    protected static final int MESSAGE_COUNT = 10;
    public boolean dynamicOnly = false;

    public void testSendOnAReceiveOnBWithTransportDisconnectDynamicOnly() throws Exception {
        dynamicOnly = true;
        try {
            testSendOnAReceiveOnBWithTransportDisconnect();
        } finally {
            dynamicOnly = false;
        }
    }

    public void testSendOnAReceiveOnBWithTransportDisconnect() throws Exception {
        bridge(SPOKE, HUB);
        startAllBrokers();

        verifyDuplexBridgeMbean();

        // Setup connection
        URI hubURI = brokers.get(HUB).broker.getTransportConnectors().get(0).getPublishableConnectURI();
        URI spokeURI = brokers.get(SPOKE).broker.getTransportConnectors().get(0).getPublishableConnectURI();
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
        MessageConsumer remoteConsumer = sesHub.createDurableSubscriber(topic, consumerName);
        sleep(1000);
        remoteConsumer.close();

        // Setup producer
        MessageProducer localProducer = sesSpoke.createProducer(topic);
        localProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

        final String payloadString = new String(new byte[10*1024]);
        // Send messages
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = sesSpoke.createTextMessage("test-" + i);
            test.setStringProperty("payload", payloadString);
            localProducer.send(test);
        }
        localProducer.close();

        final String options = "?persistent=true&useJmx=true&deleteAllMessagesOnStartup=false";
        for (int i=0;i<2;i++) {
            brokers.get(SPOKE).broker.stop();
            sleep(1000);
            createBroker(new URI("broker:(tcp://localhost:61616)/" + SPOKE + options));
            bridge(SPOKE, HUB);
            brokers.get(SPOKE).broker.start();
            LOG.info("restarted spoke..:" + i);

            assertTrue("got mbeans on restart", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return countMbeans( brokers.get(HUB).broker, "networkBridge", 20000) == (dynamicOnly ? 1 : 2);
                }
            }));
        }
    }

    private void verifyDuplexBridgeMbean() throws Exception {
        assertEquals(1, countMbeans( brokers.get(HUB).broker, "networkBridge", 5000));
    }

    private int countMbeans(BrokerService broker, String type, int timeout) throws Exception {
        final long expiryTime = System.currentTimeMillis() + timeout;

        if (!type.contains("=")) {
            type = type + "=*";
        }

        final ObjectName beanName = new ObjectName("org.apache.activemq:type=Broker,brokerName="
                + broker.getBrokerName() + "," + type +",*");
        Set<ObjectName> mbeans = null;
        int count = 0;
        do {
            if (timeout > 0) {
                Thread.sleep(100);
            }

            mbeans = broker.getManagementContext().queryNames(beanName, null);
            if (mbeans != null) {
                count = mbeans.size();
                LOG.info("Found: " + count + ", matching type: " +type);
                for (ObjectName objectName : mbeans) {
                    LOG.info("" + objectName);
                }
                //} else {
                //logAllMbeans(broker);
            }
        } while ((mbeans == null || mbeans.isEmpty()) && expiryTime > System.currentTimeMillis());

        // If port 1099 is in use when the Broker starts, starting the jmx connector
        // will fail.  So, if we have no mbsc to query, skip the test.
        if (timeout > 0) {
            assumeNotNull(mbeans);
        }

        return count;

    }

    private void logAllMbeans(BrokerService broker) throws MalformedURLException {
        try {
            // trace all existing MBeans
            Set<?> all = broker.getManagementContext().queryNames(null, null);
            LOG.info("Total MBean count=" + all.size());
            for (Object o : all) {
                //ObjectInstance bean = (ObjectInstance)o;
                LOG.info(o);
            }
        } catch (Exception ignored) {
            LOG.warn("getMBeanServer ex: " + ignored);
        }
    }

    public NetworkConnector bridge(String from, String to) throws Exception {
        NetworkConnector networkConnector = bridgeBrokers(from, to, dynamicOnly, -1, true);
        networkConnector.setSuppressDuplicateQueueSubscriptions(true);
        networkConnector.setDecreaseNetworkConsumerPriority(true);
        networkConnector.setConsumerTTL(1);
        networkConnector.setDuplex(true);
        return networkConnector;
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
        super.setAutoFail(false);
        super.setUp();
        createBrokers(true);
    }

    private void createBrokers(boolean del) throws Exception {
        final String options = "?persistent=true&useJmx=true&deleteAllMessagesOnStartup=" + del;
        createBroker(new URI("broker:(tcp://localhost:61617)/" + HUB + options));
        createBroker(new URI("broker:(tcp://localhost:61616)/" + SPOKE + options));
    }

    protected void configureBroker(BrokerService broker) {
        broker.setKeepDurableSubsActive(false);
        broker.getManagementContext().setCreateConnector(false);
        PolicyMap defaultPolcyMap = new PolicyMap();
        PolicyEntry defaultPolicy = new PolicyEntry();
        //defaultPolicy.setUseCache(false);
        if (broker.getBrokerName().equals(HUB)) {
            defaultPolicy.setStoreUsageHighWaterMark(2);
            broker.getSystemUsage().getStoreUsage().setLimit(1*1024*1024);
        }
        defaultPolcyMap.setDefaultEntry(defaultPolicy);
        broker.setDestinationPolicy(defaultPolcyMap);
        broker.getSystemUsage().getMemoryUsage().setLimit(100*1024*1024);
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void sleep(int milliSecondTime) {
        try {
            Thread.sleep(milliSecondTime);
        } catch (InterruptedException igonred) {
        }
    }
}
