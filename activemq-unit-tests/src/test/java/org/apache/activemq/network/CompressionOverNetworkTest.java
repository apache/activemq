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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.StreamMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.util.Wait;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class CompressionOverNetworkTest {

    protected static final int RECEIVE_TIMEOUT_MILLS = 10000;
    protected static final int MESSAGE_COUNT = 10;
    private static final Logger LOG = LoggerFactory.getLogger(CompressionOverNetworkTest.class);

    protected AbstractApplicationContext context;
    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;
    protected ActiveMQDestination included;

    @Test
    public void testCompressedOverCompressedNetwork() throws Exception {

        ActiveMQConnection localAmqConnection = (ActiveMQConnection) localConnection;
        localAmqConnection.setUseCompression(true);

        MessageConsumer consumer1 = remoteSession.createConsumer(included);
        MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 1, included);

        StringBuilder payload = new StringBuilder("test-");
        for (int i = 0; i < 100; ++i) {
            payload.append(UUID.randomUUID().toString());
        }

        Message test = localSession.createTextMessage(payload.toString());
        producer.send(test);
        Message msg = consumer1.receive(RECEIVE_TIMEOUT_MILLS);
        assertNotNull(msg);
        ActiveMQTextMessage message = (ActiveMQTextMessage) msg;
        assertTrue(message.isCompressed());
        assertEquals(payload.toString(), message.getText());
    }

    @Test
    public void testTextMessageCompression() throws Exception {

        MessageConsumer consumer1 = remoteSession.createConsumer(included);
        MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 1, included);

        StringBuilder payload = new StringBuilder("test-");
        for (int i = 0; i < 100; ++i) {
            payload.append(UUID.randomUUID().toString());
        }

        Message test = localSession.createTextMessage(payload.toString());
        producer.send(test);
        Message msg = consumer1.receive(RECEIVE_TIMEOUT_MILLS);
        assertNotNull(msg);
        ActiveMQTextMessage message = (ActiveMQTextMessage) msg;
        assertTrue(message.isCompressed());
        assertEquals(payload.toString(), message.getText());
    }

    @Test
    public void testBytesMessageCompression() throws Exception {

        MessageConsumer consumer1 = remoteSession.createConsumer(included);
        MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 1, included);

        StringBuilder payload = new StringBuilder("test-");
        for (int i = 0; i < 100; ++i) {
            payload.append(UUID.randomUUID().toString());
        }

        byte[] bytes = payload.toString().getBytes("UTF-8");

        BytesMessage test = localSession.createBytesMessage();
        test.writeBytes(bytes);
        producer.send(test);
        Message msg = consumer1.receive(RECEIVE_TIMEOUT_MILLS);
        assertNotNull(msg);
        ActiveMQBytesMessage message = (ActiveMQBytesMessage) msg;
        assertTrue(message.isCompressed());
        assertTrue(message.getContent().getLength() < bytes.length);

        byte[] result = new byte[bytes.length];
        assertEquals(bytes.length, message.readBytes(result));
        assertEquals(-1, message.readBytes(result));

        for(int i = 0; i < bytes.length; ++i) {
            assertEquals(bytes[i], result[i]);
        }
    }

    @Test
    public void testStreamMessageCompression() throws Exception {

        MessageConsumer consumer1 = remoteSession.createConsumer(included);
        MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 1, included);

        StreamMessage test = localSession.createStreamMessage();

        for (int i = 0; i < 100; ++i) {
            test.writeString("test string: " + i);
        }

        producer.send(test);
        Message msg = consumer1.receive(RECEIVE_TIMEOUT_MILLS);
        assertNotNull(msg);
        ActiveMQStreamMessage message = (ActiveMQStreamMessage) msg;
        assertTrue(message.isCompressed());

        for (int i = 0; i < 100; ++i) {
            assertEquals("test string: " + i, message.readString());
        }
    }

    @Test
    public void testMapMessageCompression() throws Exception {

        MessageConsumer consumer1 = remoteSession.createConsumer(included);
        MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 1, included);

        MapMessage test = localSession.createMapMessage();

        for (int i = 0; i < 100; ++i) {
            test.setString(Integer.toString(i), "test string: " + i);
        }

        producer.send(test);
        Message msg = consumer1.receive(RECEIVE_TIMEOUT_MILLS);
        assertNotNull(msg);
        ActiveMQMapMessage message = (ActiveMQMapMessage) msg;
        assertTrue(message.isCompressed());

        for (int i = 0; i < 100; ++i) {
            assertEquals("test string: " + i, message.getString(Integer.toString(i)));
        }
    }

    @Test
    public void testObjectMessageCompression() throws Exception {

        MessageConsumer consumer1 = remoteSession.createConsumer(included);
        MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 1, included);

        StringBuilder payload = new StringBuilder("test-");
        for (int i = 0; i < 100; ++i) {
            payload.append(UUID.randomUUID().toString());
        }

        Message test = localSession.createObjectMessage(payload.toString());
        producer.send(test);
        Message msg = consumer1.receive(RECEIVE_TIMEOUT_MILLS);
        assertNotNull(msg);
        ActiveMQObjectMessage message = (ActiveMQObjectMessage) msg;
        assertTrue(message.isCompressed());
        assertEquals(payload.toString(), message.getObject());
    }

    private void waitForConsumerRegistration(final BrokerService brokerService, final int min, final ActiveMQDestination destination) throws Exception {
        assertTrue("Internal bridge consumers registered in time", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Object[] bridges = brokerService.getNetworkConnectors().get(0).bridges.values().toArray();
                if (bridges.length > 0) {
                    LOG.info(brokerService + " bridges "  + Arrays.toString(bridges));
                    DemandForwardingBridgeSupport demandForwardingBridgeSupport = (DemandForwardingBridgeSupport) bridges[0];
                    ConcurrentMap<ConsumerId, DemandSubscription> forwardingBridges = demandForwardingBridgeSupport.getLocalSubscriptionMap();
                    LOG.info(brokerService + " bridge "  + demandForwardingBridgeSupport + ", localSubs: " + forwardingBridges);
                    if (!forwardingBridges.isEmpty()) {
                        for (DemandSubscription demandSubscription : forwardingBridges.values()) {
                            if (demandSubscription.getLocalInfo().getDestination().equals(destination)) {
                                LOG.info(brokerService + " DemandSubscription "  + demandSubscription + ", size: " + demandSubscription.size());
                                return demandSubscription.size() >= min;
                            }
                        }
                    }
                }
                return false;
            }
        }));
    }

    @Before
    public void setUp() throws Exception {
        doSetUp(true);
    }

    @After
    public void tearDown() throws Exception {
        doTearDown();
    }

    protected void doTearDown() throws Exception {
        try {
            localConnection.close();
            remoteConnection.close();
        } catch (Exception ignored) {}
        try {
            localBroker.stop();
        } finally {
            remoteBroker.stop();
        }
    }

    protected void doSetUp(boolean deleteAllMessages) throws Exception {
        localBroker = createLocalBroker();
        localBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        localBroker.start();
        localBroker.waitUntilStarted();
        remoteBroker = createRemoteBroker();
        remoteBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        URI localURI = localBroker.getVmConnectorURI();
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(localURI);
        fac.setAlwaysSyncSend(true);
        fac.setDispatchAsync(false);
        localConnection = fac.createConnection();
        localConnection.setClientID("clientId");
        localConnection.start();
        URI remoteURI = remoteBroker.getVmConnectorURI();
        fac = new ActiveMQConnectionFactory(remoteURI);
        remoteConnection = fac.createConnection();
        remoteConnection.setClientID("clientId");
        remoteConnection.start();
        included = new ActiveMQTopic("include.test.bar");
        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected String getRemoteBrokerURI() {
        return "org/apache/activemq/network/remoteBroker.xml";
    }

    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/localBroker.xml";
    }

    protected BrokerService createBroker(String uri) throws Exception {
        Resource resource = new ClassPathResource(uri);
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        resource = new ClassPathResource(uri);
        factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();
        BrokerService result = factory.getBroker();

        for (NetworkConnector connector : result.getNetworkConnectors()) {
            connector.setUseCompression(true);
        }

        return result;
    }

    protected BrokerService createLocalBroker() throws Exception {
        return createBroker(getLocalBrokerURI());
    }

    protected BrokerService createRemoteBroker() throws Exception {
        return createBroker(getRemoteBrokerURI());
    }
}
