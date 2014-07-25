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
package org.apache.activemq.broker.policy;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.QueueSubscriptionTest;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

@RunWith(BlockJUnit4ClassRunner.class)
public class RoundRobinDispatchPolicyTest extends QueueSubscriptionTest {

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();

        PolicyEntry policy = new PolicyEntry();
        policy.setDispatchPolicy(new RoundRobinDispatchPolicy());

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);

        return broker;
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerTwoConsumersSmallMessagesOnePrefetch() throws Exception {
        super.testOneProducerTwoConsumersSmallMessagesOnePrefetch();

        // Ensure that each consumer should have received at least one message
        // We cannot guarantee that messages will be equally divided, since
        // prefetch is one
        assertEachConsumerReceivedAtLeastXMessages(1);
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerTwoConsumersSmallMessagesLargePrefetch() throws Exception {
        super.testOneProducerTwoConsumersSmallMessagesLargePrefetch();
        assertMessagesDividedAmongConsumers();
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerTwoConsumersLargeMessagesOnePrefetch() throws Exception {
        super.testOneProducerTwoConsumersLargeMessagesOnePrefetch();

        // Ensure that each consumer should have received at least one message
        // We cannot guarantee that messages will be equally divided, since
        // prefetch is one
        assertEachConsumerReceivedAtLeastXMessages(1);
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerTwoConsumersLargeMessagesLargePrefetch() throws Exception {
        super.testOneProducerTwoConsumersLargeMessagesLargePrefetch();
        assertMessagesDividedAmongConsumers();
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerManyConsumersFewMessages() throws Exception {
        super.testOneProducerManyConsumersFewMessages();

        // Since there are more consumers, each consumer should have received at
        // most one message only
        assertMessagesDividedAmongConsumers();
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerManyConsumersManyMessages() throws Exception {
        super.testOneProducerManyConsumersManyMessages();
        assertMessagesDividedAmongConsumers();
    }

    @Test(timeout = 60 * 1000)
    public void testManyProducersManyConsumers() throws Exception {
        super.testManyProducersManyConsumers();
        assertMessagesDividedAmongConsumers();
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerTwoMatchingConsumersOneNotMatchingConsumer() throws Exception {
        // Create consumer that won't consume any message
        createMessageConsumer(createConnectionFactory().createConnection(), createDestination(), "JMSPriority<1");
        super.testOneProducerTwoConsumersSmallMessagesLargePrefetch();
        assertMessagesDividedAmongConsumers();
    }

    protected MessageConsumer createMessageConsumer(Connection conn, Destination dest, String selector) throws Exception {
        connections.add(conn);

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageConsumer consumer = sess.createConsumer(dest, selector);
        conn.start();

        return consumer;
    }

    public void assertMessagesDividedAmongConsumers() {
        assertEachConsumerReceivedAtLeastXMessages((messageCount * producerCount) / consumerCount);
        assertEachConsumerReceivedAtMostXMessages(((messageCount * producerCount) / consumerCount) + 1);
    }
}
