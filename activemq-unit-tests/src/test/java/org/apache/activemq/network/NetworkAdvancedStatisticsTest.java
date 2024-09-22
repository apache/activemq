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
import static org.junit.Assert.assertTrue;


import java.util.Arrays;
import java.util.Collection;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;

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
                { new ActiveMQTopic("include.test.bar"), new ActiveMQTopic("exclude.test.bar")},
                { new ActiveMQQueue("include.test.foo"), new ActiveMQQueue("exclude.test.foo")}});
    }

    protected static final int MESSAGE_COUNT = 10;

    protected AbstractApplicationContext context;
    protected String consumerName = "durableSubs";

    private final ActiveMQDestination includedDestination;
    private final ActiveMQDestination excludedDestination;

    public NetworkAdvancedStatisticsTest(ActiveMQDestination includedDestionation, ActiveMQDestination excludedDestination) {
        this.includedDestination = includedDestionation;
        this.excludedDestination = excludedDestination;
    }

    @Override
    protected void doSetUp(boolean deleteAllMessages) throws Exception {
        super.doSetUp(deleteAllMessages);
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
        MessageConsumer remoteConsumer;
        if(includedDestination.isTopic()) {
            remoteConsumer = remoteSession.createDurableSubscriber(ActiveMQTopic.class.cast(includedDestination), consumerName);
        } else {
            remoteConsumer = remoteSession.createConsumer(includedDestination);
            remoteConsumer.setMessageListener(new MessageListener() {                
                @Override
                public void onMessage(Message message) {
                }
            });
        }
        Thread.sleep(1000);

        MessageProducer producer = localSession.createProducer(includedDestination);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
        }
        Thread.sleep(1000);

        MessageProducer producerExcluded = localSession.createProducer(excludedDestination);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = localSession.createTextMessage("test-" + i);
            producerExcluded.send(test);
        }
        Thread.sleep(1000);

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

        // Make sure stats do not increment for local-only
        assertEquals(MESSAGE_COUNT, localBroker.getDestination(excludedDestination).getDestinationStatistics().getEnqueues().getCount());
        assertEquals(0, localBroker.getDestination(excludedDestination).getDestinationStatistics().getForwards().getCount());
        assertEquals(0, localBroker.getDestination(excludedDestination).getDestinationStatistics().getNetworkDequeues().getCount());
        assertEquals(0, localBroker.getDestination(excludedDestination).getDestinationStatistics().getNetworkEnqueues().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getEnqueues().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getDequeues().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getForwards().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getNetworkEnqueues().getCount());
        assertEquals(0, remoteBroker.getDestination(excludedDestination).getDestinationStatistics().getNetworkDequeues().getCount());

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
