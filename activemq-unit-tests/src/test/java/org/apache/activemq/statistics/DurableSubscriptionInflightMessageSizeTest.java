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
package org.apache.activemq.statistics;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;

import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.SubscriptionKey;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This test shows Inflight Message sizes are correct for various acknowledgement modes
 * using a DurableSubscription
 */
@RunWith(Parameterized.class)
public class DurableSubscriptionInflightMessageSizeTest extends AbstractInflightMessageSizeTest {

    public DurableSubscriptionInflightMessageSizeTest(int ackType, boolean optimizeAcknowledge) {
        super(ackType, optimizeAcknowledge);
    }

    @Override
    protected MessageConsumer getMessageConsumer() throws JMSException {
        return session.createDurableSubscriber((javax.jms.Topic)dest, "sub1");
    }

    @Override
    protected Subscription getSubscription() {
        return ((Topic)amqDestination).getDurableTopicSubs().get(new SubscriptionKey("client1", "sub1"));
    }

    @Override
    protected javax.jms.Topic getDestination() throws JMSException {
        return session.createTopic(destName);
    }

    @Override
    protected ActiveMQDestination getActiveMQDestination() {
        return new ActiveMQTopic(destName);
    }

}
