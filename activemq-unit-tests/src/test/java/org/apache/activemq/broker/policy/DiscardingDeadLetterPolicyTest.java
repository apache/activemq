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

import javax.jms.Message;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.DiscardingDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DiscardingDeadLetterPolicyTest extends DeadLetterTest {
    private static final Logger LOG = LoggerFactory.getLogger(DiscardingDeadLetterPolicyTest.class);

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();

        PolicyEntry policy = new PolicyEntry();
        DeadLetterStrategy strategy = new DiscardingDeadLetterStrategy();
        strategy.setProcessNonPersistent(true);
        policy.setDeadLetterStrategy(strategy);

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);

        return broker;
    }

    @Override
    protected void doTest() throws Exception {
        connection.start();

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;
        LOG.info("Will redeliver messages: " + rollbackCount + " times");

        makeConsumer();
        makeDlqConsumer();

        sendMessages();

        // now lets receive and rollback N times
        for (int i = 0; i < messageCount; i++) {
            consumeAndRollback(i);
        }

        for (int i = 0; i < messageCount; i++) {
            Message msg = dlqConsumer.receive(1000);
            assertNull("Should not be a DLQ message for loop: " + i, msg);
        }
        session.commit();
    }
}
