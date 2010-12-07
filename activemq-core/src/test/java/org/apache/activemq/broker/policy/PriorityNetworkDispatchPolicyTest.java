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

import java.util.ArrayList;
import java.util.List;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicSubscription;
import org.apache.activemq.broker.region.policy.PriorityNetworkDispatchPolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.usage.SystemUsage;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertEquals;

public class PriorityNetworkDispatchPolicyTest {

    PriorityNetworkDispatchPolicy underTest = new PriorityNetworkDispatchPolicy();
    SystemUsage usageManager = new SystemUsage();
    ConsumerInfo info = new ConsumerInfo();
    ActiveMQMessage node = new ActiveMQMessage();
    ConsumerId id = new ConsumerId();
    ConnectionContext context = new ConnectionContext();

    @Before
    public void init() throws Exception {
        info.setDestination(ActiveMQDestination.createDestination("test", ActiveMQDestination.TOPIC_TYPE));
        info.setConsumerId(id);
        info.setNetworkSubscription(true);
        info.setNetworkConsumerPath(new ConsumerId[]{id});
    }

    @Test
    public void testRemoveLowerPriorityDup() throws Exception {
        List<Subscription> consumers = new ArrayList<Subscription>();

        for (int i=0; i<3; i++) {
            ConsumerInfo instance = info.copy();
            instance.setPriority((byte)i);
            consumers.add(new TopicSubscription(null, context, instance, usageManager));
        }
        underTest.dispatch(node, null, consumers);

        long count = 0;
        for (Subscription consumer : consumers) {
            count += consumer.getEnqueueCounter();
        }
        assertEquals("only one sub got message", 1, count);
    }
}
