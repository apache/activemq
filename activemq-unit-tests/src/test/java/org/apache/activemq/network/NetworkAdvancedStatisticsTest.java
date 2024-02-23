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

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;

import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;

public class NetworkAdvancedStatisticsTest extends BaseNetworkTest {

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

    @Override
    protected String getRemoteBrokerURI() {
        return "org/apache/activemq/network/remoteBroker-advancedStatistics.xml";
    }

    @Override
    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/localBroker-advancedStatistics.xml";
    }

    //Added for AMQ-9437 test advancedStatistics for networkEnqueue and networkDequeue
    @Test(timeout = 60 * 1000)
    public void testNetworkAdvancedStatistics() throws Exception {

        // create a remote durable consumer to create demand
        MessageConsumer remoteConsumer = remoteSession.createDurableSubscriber(included, consumerName);
        Thread.sleep(1000);

        MessageProducer producer = localSession.createProducer(included);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message test = localSession.createTextMessage("test-" + i);
            producer.send(test);
        }
        Thread.sleep(1000);

        //Make sure stats are correct for local -> remote
        assertEquals(MESSAGE_COUNT, localBroker.getDestination(included).getDestinationStatistics().getForwards().getCount());
        assertEquals(MESSAGE_COUNT, localBroker.getDestination(included).getDestinationStatistics().getNetworkDequeues().getCount());
        assertEquals(0, localBroker.getDestination(included).getDestinationStatistics().getNetworkEnqueues().getCount());
        assertEquals(MESSAGE_COUNT, remoteBroker.getDestination(included).getDestinationStatistics().getNetworkEnqueues().getCount());
        assertEquals(0, remoteBroker.getDestination(included).getDestinationStatistics().getNetworkDequeues().getCount());

        assertTrue(Wait.waitFor(new Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return localBroker.getSystemUsage().getMemoryUsage().getUsage() == 0;
            }
        }, 10000, 500));
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
