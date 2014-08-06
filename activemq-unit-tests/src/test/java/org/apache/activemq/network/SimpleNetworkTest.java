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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicRequestor;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.util.Wait;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class SimpleNetworkTest {

    protected static final int MESSAGE_COUNT = 10;
    private static final Logger LOG = LoggerFactory.getLogger(SimpleNetworkTest.class);

    protected AbstractApplicationContext context;
    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;
    protected ActiveMQTopic included;
    protected ActiveMQTopic excluded;
    protected String consumerName = "durableSubs";

    // works b/c of non marshaling vm transport, the connection
    // ref from the client is used during the forward
    @Test(timeout = 60 * 1000)
    public void testMessageCompression() throws Exception {

        ActiveMQConnection localAmqConnection = (ActiveMQConnection) localConnection;
        localAmqConnection.setUseCompression(true);

        MessageConsumer consumer1 = remoteSession.createConsumer(included);
        MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 1, included);

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
            Message msg = consumer1.receive(3000);
            assertNotNull(msg);
            ActiveMQMessage amqMessage = (ActiveMQMessage) msg;
            assertTrue(amqMessage.isCompressed());
        }
        // ensure no more messages received
        assertNull(consumer1.receive(1000));
    }

    @Test(timeout = 60 * 1000)
    public void testRequestReply() throws Exception {
        final MessageProducer remoteProducer = remoteSession.createProducer(null);
        MessageConsumer remoteConsumer = remoteSession.createConsumer(included);
        remoteConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message msg) {
                try {
                    TextMessage textMsg = (TextMessage)msg;
                    String payload = "REPLY: " + textMsg.getText();
                    Destination replyTo;
                    replyTo = msg.getJMSReplyTo();
                    textMsg.clearBody();
                    textMsg.setText(payload);
                    remoteProducer.send(replyTo, textMsg);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        TopicRequestor requestor = new TopicRequestor((TopicSession)localSession, included);
        // allow for consumer infos to perculate arround
        Thread.sleep(5000);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            TextMessage msg = localSession.createTextMessage("test msg: " + i);
            TextMessage result = (TextMessage)requestor.request(msg);
            assertNotNull(result);
            LOG.info(result.getText());
        }
    }

    @Test(timeout = 60 * 1000)
    public void testFiltering() throws Exception {
        MessageConsumer includedConsumer = remoteSession.createConsumer(included);
        MessageConsumer excludedConsumer = remoteSession.createConsumer(excluded);
        MessageProducer includedProducer = localSession.createProducer(included);
        MessageProducer excludedProducer = localSession.createProducer(excluded);
        // allow for consumer infos to perculate arround
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");
        includedProducer.send(test);
        excludedProducer.send(test);
        assertNull(excludedConsumer.receive(1000));
        assertNotNull(includedConsumer.receive(1000));
    }

    @Test(timeout = 60 * 1000)
    public void testConduitBridge() throws Exception {
        MessageConsumer consumer1 = remoteSession.createConsumer(included);
        MessageConsumer consumer2 = remoteSession.createConsumer(included);
        MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 2, included);

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
            assertNotNull(consumer1.receive(1000));
            assertNotNull(consumer2.receive(1000));
        }
        // ensure no more messages received
        assertNull(consumer1.receive(1000));
        assertNull(consumer2.receive(1000));
    }

    private void waitForConsumerRegistration(final BrokerService brokerService, final int min, final ActiveMQDestination destination) throws Exception {
        assertTrue("Internal bridge consumers registered in time", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Object[] bridges = brokerService.getNetworkConnectors().get(0).bridges.values().toArray();
                if (bridges.length > 0) {
                    LOG.info(brokerService + " bridges "  + Arrays.toString(bridges));
                    DemandForwardingBridgeSupport demandForwardingBridgeSupport = (DemandForwardingBridgeSupport) bridges[0];
                    ConcurrentHashMap<ConsumerId, DemandSubscription> forwardingBridges = demandForwardingBridgeSupport.getLocalSubscriptionMap();
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

    @Test(timeout = 60 * 1000)
    public void testDurableStoreAndForward() throws Exception {
        // create a remote durable consumer
        MessageConsumer remoteConsumer = remoteSession.createDurableSubscriber(included, consumerName);
        Thread.sleep(1000);
        // now close everything down and restart
        doTearDown();
        doSetUp(false);
        MessageProducer producer = localSession.createProducer(included);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
        }
        Thread.sleep(1000);
        // close everything down and restart
        doTearDown();
        doSetUp(false);
        remoteConsumer = remoteSession.createDurableSubscriber(included, consumerName);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            assertNotNull("message count: " + i, remoteConsumer.receive(2500));
        }
    }

    @Ignore("This seems like a simple use case, but it is problematic to consume an existing topic store, " +
            "it requires a connection per durable to match that connectionId")
    public void testDurableStoreAndForwardReconnect() throws Exception {
        // create a local durable consumer
        MessageConsumer localConsumer = localSession.createDurableSubscriber(included, consumerName);
        Thread.sleep(5000);
        // now close everything down and restart
        doTearDown();
        doSetUp(false);
        // send messages
        MessageProducer producer = localSession.createProducer(included);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
        }
        Thread.sleep(5000);
        // consume some messages locally
        localConsumer = localSession.createDurableSubscriber(included, consumerName);
        LOG.info("Consume from local consumer: " + localConsumer);
        for (int i = 0; i < MESSAGE_COUNT / 2; i++) {
            assertNotNull("message count: " + i, localConsumer.receive(2500));
        }
        Thread.sleep(5000);
        // close everything down and restart
        doTearDown();
        doSetUp(false);
        Thread.sleep(5000);

        LOG.info("Consume from remote");
        // consume the rest remotely
        MessageConsumer remoteConsumer = remoteSession.createDurableSubscriber(included, consumerName);
        LOG.info("Remote consumer: " + remoteConsumer);
        Thread.sleep(5000);
        for (int i = 0; i < MESSAGE_COUNT / 2; i++) {
            assertNotNull("message count: " + i, remoteConsumer.receive(10000));
        }
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
        localConnection.close();
        remoteConnection.close();
        localBroker.stop();
        remoteBroker.stop();
    }

    protected void doSetUp(boolean deleteAllMessages) throws Exception {
        remoteBroker = createRemoteBroker();
        remoteBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        localBroker = createLocalBroker();
        localBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        localBroker.start();
        localBroker.waitUntilStarted();
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
        excluded = new ActiveMQTopic("exclude.test.bar");
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
        return result;
    }

    protected BrokerService createLocalBroker() throws Exception {
        return createBroker(getLocalBrokerURI());
    }

    protected BrokerService createRemoteBroker() throws Exception {
        return createBroker(getRemoteBrokerURI());
    }
}
