/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.policy;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TopicSubscriptionTest;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.StrictOrderDispatchPolicy;

import java.util.List;
import java.util.Iterator;

public class StrictOrderDispatchPolicyTest extends TopicSubscriptionTest {

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();

        PolicyEntry policy = new PolicyEntry();
        policy.setDispatchPolicy(new StrictOrderDispatchPolicy());

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);

        return broker;
    }

    public void testOneProducerTwoConsumersSmallMessagesOnePrefetch() throws Exception {
        super.testOneProducerTwoConsumersSmallMessagesOnePrefetch();

        assertReceivedMessagesAreOrdered();
    }

    public void testOneProducerTwoConsumersSmallMessagesLargePrefetch() throws Exception {
        super.testOneProducerTwoConsumersSmallMessagesLargePrefetch();

        assertReceivedMessagesAreOrdered();
    }

    public void testOneProducerTwoConsumersLargeMessagesOnePrefetch() throws Exception {
        super.testOneProducerTwoConsumersLargeMessagesOnePrefetch();

        assertReceivedMessagesAreOrdered();
    }

    public void testOneProducerTwoConsumersLargeMessagesLargePrefetch() throws Exception {
        super.testOneProducerTwoConsumersLargeMessagesLargePrefetch();

        assertReceivedMessagesAreOrdered();
    }

    public void testOneProducerManyConsumersFewMessages() throws Exception {
        super.testOneProducerManyConsumersFewMessages();

        assertReceivedMessagesAreOrdered();
    }

    public void testOneProducerManyConsumersManyMessages() throws Exception {
        super.testOneProducerManyConsumersManyMessages();

        assertReceivedMessagesAreOrdered();
    }

    public void testManyProducersOneConsumer() throws Exception {
        super.testManyProducersOneConsumer();

        assertReceivedMessagesAreOrdered();
    }

    public void testManyProducersManyConsumers() throws Exception {
        super.testManyProducersManyConsumers();

        assertReceivedMessagesAreOrdered();
    }

    public void assertReceivedMessagesAreOrdered() throws Exception {
        // If there is only one consumer, messages is definitely ordered
        if (consumers.size() <= 1) {
            return;
        }

        // Get basis of order
        Iterator i = consumers.keySet().iterator();
        List messageOrder = (List)consumers.get(i.next());

        for (;i.hasNext();) {
            List messageList = (List)consumers.get(i.next());
            assertTrue("Messages are not ordered.", messageOrder.equals(messageList));
        }
    }
}
