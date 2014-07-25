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

import javax.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;
import org.apache.activemq.broker.region.policy.AbortSlowConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class AbortSlowAckConsumer1Test extends AbortSlowConsumer1Test {

    protected long maxTimeSinceLastAck = 5 * 1000;

    public AbortSlowAckConsumer1Test(Boolean abortConnection, Boolean topic) {
        super(abortConnection, topic);
    }

    @Override
    protected AbortSlowConsumerStrategy createSlowConsumerStrategy() {
        return new AbortSlowConsumerStrategy();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        PolicyEntry policy = new PolicyEntry();

        AbortSlowAckConsumerStrategy strategy = new AbortSlowAckConsumerStrategy();
        strategy.setAbortConnection(abortConnection);
        strategy.setCheckPeriod(checkPeriod);
        strategy.setMaxSlowDuration(maxSlowDuration);
        strategy.setMaxTimeSinceLastAck(maxTimeSinceLastAck);

        policy.setSlowConsumerStrategy(strategy);
        policy.setQueuePrefetch(10);
        policy.setTopicPrefetch(10);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);
        return broker;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.getPrefetchPolicy().setAll(1);
        return factory;
    }


}
