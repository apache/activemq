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

import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.TextMessage;
import jakarta.jms.TopicRequestor;
import jakarta.jms.TopicSession;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.concurrent.TimeUnit;

public class SimpleNetworkTest extends BaseNetworkTest {

    protected static final int MESSAGE_COUNT = 10;

    protected AbstractApplicationContext context;
    protected ActiveMQTopic included;
    protected ActiveMQTopic excluded;
    protected String consumerName = "durableSubs";

    @Override
    protected void doSetUp(boolean deleteAllMessages) throws Exception {
        super.doSetUp(deleteAllMessages);

        included = new ActiveMQTopic("include.test.bar");
        excluded = new ActiveMQTopic("exclude.test.bar");
    }

    // works b/c of non marshaling vm transport, the connection
    // ref from the client is used during the forward
    @Test(timeout = 60 * 1000)
    public void testMessageCompression() throws Exception {

        final ActiveMQConnection localAmqConnection = (ActiveMQConnection) localConnection;
        localAmqConnection.setUseCompression(true);

        final MessageConsumer consumer1 = remoteSession.createConsumer(included);
        final MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 1, included);

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
            final Message msg = consumer1.receive(3000);
            assertNotNull("not null? message: " + i, msg);
            final ActiveMQMessage amqMessage = (ActiveMQMessage) msg;
            assertTrue(amqMessage.isCompressed());
        }
        // ensure no more messages received
        assertNull(consumer1.receive(1000));

        assertNetworkBridgeStatistics(MESSAGE_COUNT, 0);
    }

    @Test(timeout = 60 * 1000)
    public void testRequestReply() throws Exception {
        final MessageProducer remoteProducer = remoteSession.createProducer(null);
        final MessageConsumer remoteConsumer = remoteSession.createConsumer(included);
        remoteConsumer.setMessageListener(msg -> {
            try {
                final TextMessage textMsg = (TextMessage) msg;
                final String payload = "REPLY: " + textMsg.getText();
                final Destination replyTo = msg.getJMSReplyTo();
                textMsg.clearBody();
                textMsg.setText(payload);
                remoteProducer.send(replyTo, textMsg);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        final TopicRequestor requestor = new TopicRequestor((TopicSession) localSession, included);
        // Wait for consumer demand to propagate across the network bridge
        waitForConsumerRegistration(localBroker, 1, included);

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final TextMessage msg = localSession.createTextMessage("test msg: " + i);
            final TextMessage result = (TextMessage) requestor.request(msg);
            assertNotNull(result);
            LOG.info(result.getText());
        }

        assertNetworkBridgeStatistics(MESSAGE_COUNT, MESSAGE_COUNT);
    }

    @Test(timeout = 60 * 1000)
    public void testFiltering() throws Exception {
        final MessageConsumer includedConsumer = remoteSession.createConsumer(included);
        final MessageConsumer excludedConsumer = remoteSession.createConsumer(excluded);
        final MessageProducer includedProducer = localSession.createProducer(included);
        final MessageProducer excludedProducer = localSession.createProducer(excluded);
        // Wait for consumer demand to propagate across the network bridge
        waitForConsumerRegistration(localBroker, 1, included);

        final Message test = localSession.createTextMessage("test");
        includedProducer.send(test);
        excludedProducer.send(test);
        assertNull(excludedConsumer.receive(1000));
        assertNotNull(includedConsumer.receive(1000));

        assertNetworkBridgeStatistics(1, 0);
    }

    @Test(timeout = 60 * 1000)
    public void testConduitBridge() throws Exception {
        final MessageConsumer consumer1 = remoteSession.createConsumer(included);
        final MessageConsumer consumer2 = remoteSession.createConsumer(included);
        final MessageProducer producer = localSession.createProducer(included);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        waitForConsumerRegistration(localBroker, 2, included);

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
            assertNotNull(consumer1.receive(1000));
            assertNotNull(consumer2.receive(1000));
        }
        // ensure no more messages received
        assertNull(consumer1.receive(1000));
        assertNull(consumer2.receive(1000));

        assertNetworkBridgeStatistics(MESSAGE_COUNT, 0);

        assertNotNull(localBroker.getManagementContext().getObjectInstance(
                localBroker.createNetworkConnectorObjectName(localBroker.getNetworkConnectors().get(0))));
    }

    private void waitForConsumerRegistration(final BrokerService brokerService, final int min, final ActiveMQDestination destination) throws Exception {
        // Wait for the bridge demand subscriptions to register the expected number of
        // remote consumers. With conduit subscriptions, multiple remote consumers map
        // to a single local subscription, so we check DemandSubscription.size().
        assertTrue("Bridge demand subscription registered for " + destination, Wait.waitFor(() -> {
            if (brokerService.getNetworkConnectors().isEmpty()) {
                return false;
            }
            for (final NetworkBridge bridge : brokerService.getNetworkConnectors().get(0).activeBridges()) {
                if (bridge instanceof DemandForwardingBridgeSupport) {
                    final DemandForwardingBridgeSupport dfBridge = (DemandForwardingBridgeSupport) bridge;
                    for (final DemandSubscription ds : dfBridge.getLocalSubscriptionMap().values()) {
                        if (ds.getLocalInfo().getDestination().equals(destination)) {
                            return ds.size() >= min;
                        }
                    }
                }
            }
            return false;
        }, TimeUnit.SECONDS.toMillis(30), 100));

        // Also verify the consumer is actually dispatching on the broker's destination.
        // The DemandSubscription may exist before the local consumer is fully registered.
        assertTrue("Consumer dispatching on " + destination, Wait.waitFor(
            () -> brokerService.getDestination(destination).getDestinationStatistics().getConsumers().getCount() >= 1,
            TimeUnit.SECONDS.toMillis(10), 100));
    }

    //Added for AMQ-6465 to make sure memory usage decreased back to 0 after messages are forwarded
    //to the other broker
    @Test(timeout = 60 * 1000)
    public void testDurableTopicSubForwardMemoryUsage() throws Exception {
        // create a remote durable consumer to create demand
        final MessageConsumer remoteConsumer = remoteSession.createDurableSubscriber(included, consumerName);
        // Wait for consumer demand to propagate across the network bridge
        waitForConsumerRegistration(localBroker, 1, included);

        final MessageProducer producer = localSession.createProducer(included);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
        }

        //Make sure stats are set - wait for forwards to complete
        assertTrue("Forwards count did not reach expected value", Wait.waitFor(
                () -> {
                    final long count = localBroker.getDestination(included).getDestinationStatistics().getForwards().getCount();
                    LOG.info("testDurableTopicSubForwardMemoryUsage: forwards count = " + count + " (expected " + MESSAGE_COUNT + ")");
                    return count == MESSAGE_COUNT;
                },
                TimeUnit.SECONDS.toMillis(30), 100));

        assertTrue("Memory usage did not return to 0", Wait.waitFor(
                () -> localBroker.getSystemUsage().getMemoryUsage().getUsage() == 0,
                TimeUnit.SECONDS.toMillis(10), 500));
        remoteConsumer.close();
    }

    //Added for AMQ-6465 to make sure memory usage decreased back to 0 after messages are forwarded
    //to the other broker
    @Test(timeout = 60 * 1000)
    public void testTopicSubForwardMemoryUsage() throws Exception {
        // create a remote consumer to create demand
        final MessageConsumer remoteConsumer = remoteSession.createConsumer(included);
        // Wait for consumer demand to propagate across the network bridge
        waitForConsumerRegistration(localBroker, 1, included);

        final MessageProducer producer = localSession.createProducer(included);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
        }

        //Make sure stats are set - wait for forwards to complete
        assertTrue("Forwards count did not reach expected value", Wait.waitFor(
                () -> localBroker.getDestination(included).getDestinationStatistics().getForwards().getCount() == MESSAGE_COUNT,
                TimeUnit.SECONDS.toMillis(30), 100));

        assertTrue("Memory usage did not return to 0", Wait.waitFor(
                () -> localBroker.getSystemUsage().getMemoryUsage().getUsage() == 0,
                TimeUnit.SECONDS.toMillis(10), 500));

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            assertNotNull("message count: " + i, remoteConsumer.receive(2500));
        }
        remoteConsumer.close();
    }

    //Added for AMQ-6465 to make sure memory usage decreased back to 0 after messages are forwarded
    //to the other broker
    @Test(timeout = 60 * 1000)
    public void testQueueSubForwardMemoryUsage() throws Exception {
        final ActiveMQQueue queue = new ActiveMQQueue("include.test.foo");
        final MessageConsumer remoteConsumer = remoteSession.createConsumer(queue);
        // Wait for consumer demand to propagate across the network bridge
        waitForConsumerRegistration(localBroker, 1, queue);

        final MessageProducer producer = localSession.createProducer(queue);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
        }

        //Make sure stats are set - wait for forwards to complete
        assertTrue("Forwards count did not reach expected value", Wait.waitFor(
                () -> localBroker.getDestination(queue).getDestinationStatistics().getForwards().getCount() == MESSAGE_COUNT,
                TimeUnit.SECONDS.toMillis(30), 100));

        assertTrue("Memory usage did not return to 0", Wait.waitFor(
                () -> localBroker.getSystemUsage().getMemoryUsage().getUsage() == 0,
                TimeUnit.SECONDS.toMillis(10), 500));

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            assertNotNull("message count: " + i, remoteConsumer.receive(2500));
        }
        remoteConsumer.close();
    }

    @Test(timeout = 60 * 1000)
    public void testDurableStoreAndForward() throws Exception {
        // create a remote durable consumer
        final MessageConsumer remoteConsumer = remoteSession.createDurableSubscriber(included, consumerName);
        // Wait for consumer demand to propagate across the network bridge
        waitForConsumerRegistration(localBroker, 1, included);

        // now close everything down and restart
        doTearDown();
        doSetUp(false);
        final MessageProducer producer = localSession.createProducer(included);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
        }

        //Make sure stats are set - wait for forwards to complete
        assertTrue("Forwards count did not reach expected value", Wait.waitFor(
                () -> localBroker.getDestination(included).getDestinationStatistics().getForwards().getCount() == MESSAGE_COUNT,
                TimeUnit.SECONDS.toMillis(30), 100));

        // close everything down and restart
        doTearDown();
        doSetUp(false);
        final MessageConsumer remoteConsumer2 = remoteSession.createDurableSubscriber(included, consumerName);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            assertNotNull("message count: " + i, remoteConsumer2.receive(2500));
        }
    }

    @Ignore("This seems like a simple use case, but it is problematic to consume an existing topic store, " +
            "it requires a connection per durable to match that connectionId")
    public void testDurableStoreAndForwardReconnect() throws Exception {
        // create a local durable consumer
        final MessageConsumer localConsumer = localSession.createDurableSubscriber(included, consumerName);
        // Wait for consumer demand to propagate across the network bridge
        waitForConsumerRegistration(localBroker, 1, included);

        // now close everything down and restart
        doTearDown();
        doSetUp(false);
        // send messages
        final MessageProducer producer = localSession.createProducer(included);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
        }

        //Make sure stats are set - wait for forwards to complete
        assertTrue("Forwards count did not reach expected value", Wait.waitFor(
                () -> localBroker.getDestination(included).getDestinationStatistics().getForwards().getCount() == MESSAGE_COUNT,
                TimeUnit.SECONDS.toMillis(30), 100));

        // consume some messages locally
        final MessageConsumer localConsumer2 = localSession.createDurableSubscriber(included, consumerName);
        LOG.info("Consume from local consumer: " + localConsumer2);
        for (int i = 0; i < MESSAGE_COUNT / 2; i++) {
            assertNotNull("message count: " + i, localConsumer2.receive(2500));
        }

        // Wait for local messages to be consumed
        assertTrue("Local messages consumed", Wait.waitFor(
                () -> localBroker.getDestination(included).getDestinationStatistics().getDequeues().getCount() >= MESSAGE_COUNT / 2,
                TimeUnit.SECONDS.toMillis(10), 100));

        // close everything down and restart
        doTearDown();
        doSetUp(false);

        // Wait for network bridge to re-form
        assertTrue("Network bridge re-formed", Wait.waitFor(
                () -> !localBroker.getNetworkConnectors().isEmpty()
                    && !localBroker.getNetworkConnectors().get(0).activeBridges().isEmpty(),
                TimeUnit.SECONDS.toMillis(10), 100));

        LOG.info("Consume from remote");
        // consume the rest remotely
        final MessageConsumer remoteConsumer = remoteSession.createDurableSubscriber(included, consumerName);
        LOG.info("Remote consumer: " + remoteConsumer);

        // Wait for consumer demand to propagate
        waitForConsumerRegistration(localBroker, 1, included);

        for (int i = 0; i < MESSAGE_COUNT / 2; i++) {
            assertNotNull("message count: " + i, remoteConsumer.receive(10000));
        }
    }

    protected void assertNetworkBridgeStatistics(final long expectedLocalSent, final long expectedRemoteSent) throws Exception {

        final NetworkBridge localBridge = localBroker.getNetworkConnectors().get(0).activeBridges().iterator().next();
        final NetworkBridge remoteBridge = remoteBroker.getNetworkConnectors().get(0).activeBridges().iterator().next();

        assertTrue(Wait.waitFor(() ->
                expectedLocalSent == localBridge.getNetworkBridgeStatistics().getDequeues().getCount() &&
                0 == localBridge.getNetworkBridgeStatistics().getReceivedCount().getCount() &&
                expectedRemoteSent == remoteBridge.getNetworkBridgeStatistics().getDequeues().getCount() &&
                0 == remoteBridge.getNetworkBridgeStatistics().getReceivedCount().getCount()
        , TimeUnit.SECONDS.toMillis(30), 100));
    }
}
