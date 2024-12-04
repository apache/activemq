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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.context.support.AbstractApplicationContext;

@RunWith(value = Parameterized.class)
public class NetworkAdvancedStatisticsTest extends BaseNetworkTest {

    @Parameterized.Parameters(name="includedDestination={0}, excludedDestination={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { new ActiveMQTopic("include.test.durable"), new ActiveMQTopic("exclude.test.durable"), true},
                { new ActiveMQTopic("include.test.nondurable"), new ActiveMQTopic("exclude.test.nondurable"), false},
                { new ActiveMQQueue("include.test.foo"), new ActiveMQQueue("exclude.test.foo"), false}});
    }

    protected static final int MESSAGE_COUNT = 10;

    protected AbstractApplicationContext context;
    protected String consumerName = "durableSubs";

    private final ActiveMQDestination includedDestination;
    private final ActiveMQDestination excludedDestination;
    private final boolean durable;

    public NetworkAdvancedStatisticsTest(ActiveMQDestination includedDestionation, ActiveMQDestination excludedDestination, boolean durable) {
        this.includedDestination = includedDestionation;
        this.excludedDestination = excludedDestination;
        this.durable = durable;
    }

    @Override
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
        localConnection.setClientID("localClientId");

        URI remoteURI = remoteBroker.getVmConnectorURI();
        fac = new ActiveMQConnectionFactory(remoteURI);
        remoteConnection = fac.createConnection();
        remoteConnection.setClientID("remoteClientId");

        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    protected String getRemoteBrokerURI() {
        return "org/apache/activemq/network/remoteBroker-advancedNetworkStatistics.xml";
    }

    @Override
    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/localBroker-advancedNetworkStatistics.xml";
    }

    //Added for AMQ-9437 test advancedStatistics for networkEnqueue and networkDequeue
    @Test(timeout = 60 * 1000)
    public void testNetworkAdvancedStatistics() throws Exception {

        // create a remote durable consumer to create demand
        final Map<String, Message> receivedMessages = new ConcurrentHashMap<>();
        final Collection<Exception> receivedExceptions = new ConcurrentLinkedQueue<>();

        MessageConsumer remoteConsumer;
        if(includedDestination.isTopic() && durable) {
            remoteConsumer = remoteSession.createDurableSubscriber(ActiveMQTopic.class.cast(includedDestination), consumerName);
        } else {
            remoteConsumer = remoteSession.createConsumer(includedDestination);
        }
        remoteConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    receivedMessages.put(message.getJMSMessageID(), message);
                } catch (JMSException e) {
                    receivedExceptions.add(e);
                }
            }
        });

        localConnection.start();
        remoteConnection.start();

        MessageProducer producer = localSession.createProducer(includedDestination);
        String lastIncludedSentMessageID = null;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
            lastIncludedSentMessageID = test.getJMSMessageID();
        }

        MessageProducer producerExcluded = localSession.createProducer(excludedDestination);
        String lastExcludedSentMessageID = null;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = localSession.createTextMessage("test-" + i);
            producerExcluded.send(test);
            lastExcludedSentMessageID = test.getJMSMessageID();
        }

        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                // The number of message that remain is due to the exclude queue
                return receivedMessages.size() == MESSAGE_COUNT;
            }
        }, 10000, 500));

        assertTrue(receivedExceptions.isEmpty());
        assertEquals(Integer.valueOf(MESSAGE_COUNT), Integer.valueOf(receivedMessages.size()));

        //Make sure stats are correct for local -> remote
        assertEquals(MESSAGE_COUNT, localBroker.getDestination(includedDestination).getDestinationStatistics().getEnqueues().getCount());
        assertEquals(MESSAGE_COUNT, localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeues().getCount());
        assertEquals(MESSAGE_COUNT, localBroker.getDestination(includedDestination).getDestinationStatistics().getForwards().getCount());
        assertEquals(MESSAGE_COUNT, localBroker.getDestination(includedDestination).getDestinationStatistics().getNetworkDequeues().getCount());
        assertEquals(0, localBroker.getDestination(includedDestination).getDestinationStatistics().getNetworkEnqueues().getCount());
        assertEquals(MESSAGE_COUNT, remoteBroker.getDestination(includedDestination).getDestinationStatistics().getEnqueues().getCount());
        assertEquals(0, remoteBroker.getDestination(includedDestination).getDestinationStatistics().getForwards().getCount());
        assertEquals(MESSAGE_COUNT, remoteBroker.getDestination(includedDestination).getDestinationStatistics().getNetworkEnqueues().getCount());
        assertEquals(0, remoteBroker.getDestination(includedDestination).getDestinationStatistics().getNetworkDequeues().getCount());

        // Advanced Message status - enqueue
        assertEquals(lastIncludedSentMessageID, localBroker.getDestination(includedDestination).getDestinationStatistics().getEnqueuedMessageID().getValue());
        assertNotNull(localBroker.getDestination(includedDestination).getDestinationStatistics().getEnqueuedMessageBrokerInTime().getValue());
        assertTrue(localBroker.getDestination(includedDestination).getDestinationStatistics().getEnqueuedMessageBrokerInTime().getValue() > 0l);
        assertNotNull(localBroker.getDestination(includedDestination).getDestinationStatistics().getEnqueuedMessageTimestamp().getValue());
        assertTrue(localBroker.getDestination(includedDestination).getDestinationStatistics().getEnqueuedMessageTimestamp().getValue() > 0l);
        assertEquals("localClientId", localBroker.getDestination(includedDestination).getDestinationStatistics().getEnqueuedMessageClientID().getValue());

        // Advanced Message status - dequeue
        assertEquals(lastIncludedSentMessageID, localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageID().getValue());
        assertNotNull(localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageBrokerInTime().getValue());
        assertNotNull(localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageBrokerOutTime().getValue());
        assertTrue(localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageClientID().getValue().startsWith("networkConnector"));
        assertNotNull(localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageTimestamp().getValue());

        if(includedDestination.isTopic() && !durable) {
            assertEquals(Long.valueOf(0l), localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageBrokerInTime().getValue());
            assertEquals(Long.valueOf(0l), localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageBrokerOutTime().getValue());
            assertEquals(Long.valueOf(0l), localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageTimestamp().getValue());
        } else {
            assertTrue(localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageBrokerInTime().getValue() > 0l);
            assertTrue(localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageBrokerOutTime().getValue() > 0l);
            assertTrue(localBroker.getDestination(includedDestination).getDestinationStatistics().getDequeuedMessageTimestamp().getValue() > 0l);
        }

        // Make sure stats do not increment for local-only excluded destinations
        assertEquals(MESSAGE_COUNT, localBroker.getDestination(excludedDestination).getDestinationStatistics().getEnqueues().getCount());
        assertEquals(0, localBroker.getDestination(excludedDestination).getDestinationStatistics().getForwards().getCount());
        assertEquals(0, localBroker.getDestination(excludedDestination).getDestinationStatistics().getNetworkDequeues().getCount());
        assertEquals(0, localBroker.getDestination(excludedDestination).getDestinationStatistics().getNetworkEnqueues().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getEnqueues().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getDequeues().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getForwards().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getNetworkEnqueues().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getNetworkDequeues().getCount());
        assertEquals(lastExcludedSentMessageID, localBroker.getDestination(excludedDestination).getDestinationStatistics().getEnqueuedMessageID().getValue());
        assertNull(localBroker.getDestination(excludedDestination).getDestinationStatistics().getDequeuedMessageID().getValue());

        // Advanced Message status - enqueue
        assertNull(remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getEnqueuedMessageID().getValue());
        assertEquals(Long.valueOf(0l), remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getEnqueuedMessageBrokerInTime().getValue());
        assertEquals(Long.valueOf(0l), remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getEnqueuedMessageTimestamp().getValue());
        assertNull(remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getEnqueuedMessageClientID().getValue());

        // Advanced Message status - dequeue
        assertNull(lastIncludedSentMessageID, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getDequeuedMessageID().getValue());
        assertEquals(Long.valueOf(0l), remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getDequeuedMessageBrokerInTime().getValue());
        assertEquals(Long.valueOf(0l), remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getDequeuedMessageBrokerOutTime().getValue());
        assertEquals(Long.valueOf(0l), remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getDequeuedMessageTimestamp().getValue());
        assertNull(remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getDequeuedMessageClientID().getValue());

        if(includedDestination.isTopic()) {
            assertTrue(Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return localBroker.getSystemUsage().getMemoryUsage().getUsage() == 0;
                }
            }, 10000, 500));
        } else {
            assertTrue(Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    // The number of message that remain is due to the exclude queue
                    return localBroker.getAdminView().getTotalMessageCount() == MESSAGE_COUNT;
                }
            }, 10000, 500));
        }
        remoteConsumer.close();
    }

    protected void assertNetworkBridgeStatistics(final long expectedLocalSent, final long expectedRemoteSent) throws Exception {

        final NetworkBridge localBridge = localBroker.getNetworkConnectors().get(0).activeBridges().iterator().next();
        final NetworkBridge remoteBridge = remoteBroker.getNetworkConnectors().get(0).activeBridges().iterator().next();

        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return expectedLocalSent == localBridge.getNetworkBridgeStatistics().getDequeues().getCount() &&
                       0 == localBridge.getNetworkBridgeStatistics().getReceivedCount().getCount() &&
                       expectedRemoteSent == remoteBridge.getNetworkBridgeStatistics().getDequeues().getCount() &&
                       0 == remoteBridge.getNetworkBridgeStatistics().getReceivedCount().getCount();
            }
        }));
    }

}
