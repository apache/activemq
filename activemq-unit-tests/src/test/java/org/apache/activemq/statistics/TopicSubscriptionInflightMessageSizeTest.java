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

import static org.junit.Assert.assertTrue;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;

import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This test shows Inflight Message sizes are correct for various acknowledgement modes
 * using a TopicSubscription
 */
@RunWith(Parameterized.class)
public class TopicSubscriptionInflightMessageSizeTest extends AbstractInflightMessageSizeTest {

    public TopicSubscriptionInflightMessageSizeTest(int ackType, boolean optimizeAcknowledge, boolean useTopicSubscriptionInflightStats) {
        super(ackType, optimizeAcknowledge, useTopicSubscriptionInflightStats);
    }

    @Override
    protected MessageConsumer getMessageConsumer() throws JMSException {
        return session.createConsumer(dest);
    }

    @Override
    protected Subscription getSubscription() {
        return amqDestination.getConsumers().get(0);
    }

    @Override
    protected Destination getDestination() throws JMSException {
        return session.createTopic(destName);
    }

    @Override
    protected ActiveMQDestination getActiveMQDestination() {
        return new ActiveMQTopic(destName);
    }

    @Test(timeout=15000)
    public void testInflightMessageSizeDisabled() throws Exception {
        Assume.assumeFalse(useTopicSubscriptionInflightStats);
        sendMessages(10);

        Thread.sleep(1000);

        assertTrue("Inflight message size should be 0", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() == 0;
            }
        }));

        receiveMessages(10);

        Thread.sleep(1000);
        assertTrue("Inflight message size should still be 0", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() == 0;
            }
        }));
    }

}
