/*
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

import java.util.Enumeration;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndividualDeadLetterTest extends DeadLetterTest {

    private static final Logger LOG = LoggerFactory.getLogger(IndividualDeadLetterTest.class);

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();

        PolicyEntry policy = new PolicyEntry();
        DeadLetterStrategy strategy = new IndividualDeadLetterStrategy();
        strategy.setProcessNonPersistent(true);
        policy.setDeadLetterStrategy(strategy);

        PolicyMap pMap = new PolicyMap();
        pMap.put(new ActiveMQQueue(getDestinationString()), policy);
        pMap.put(new ActiveMQTopic(getDestinationString()), policy);

        broker.setDestinationPolicy(pMap);

        return broker;
    }

    @Override
    protected Destination createDlqDestination() {
        String prefix = topic ? "ActiveMQ.DLQ.Topic." : "ActiveMQ.DLQ.Queue.";
        return new ActiveMQQueue(prefix + getClass().getName() + "." + getName());
    }

    public void testDLQBrowsing() throws Exception {
        super.topic = false;
        deliveryMode = DeliveryMode.PERSISTENT;
        durableSubscriber = false;
        messageCount = 1;

        connection.start();

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;
        LOG.info("Will redeliver messages: " + rollbackCount + " times");

        sendMessages();

        // now lets receive and rollback N times
        for (int i = 0; i < rollbackCount; i++) {
            makeConsumer();
            Message message = consumer.receive(5000);
            assertNotNull("No message received: ", message);

            session.rollback();
            LOG.info("Rolled back: " + rollbackCount + " times");
            consumer.close();
        }

        makeDlqBrowser();
        browseDlq();
        dlqBrowser.close();
        session.close();
        Thread.sleep(1000);
        session = connection.createSession(transactedMode, acknowledgeMode);
        Queue testQueue = new ActiveMQQueue("ActiveMQ.DLQ.Queue.ActiveMQ.DLQ.Queue." + getClass().getName() + "." + getName());
        MessageConsumer testConsumer = session.createConsumer(testQueue);
        assertNull("The message shouldn't be sent to another DLQ", testConsumer.receive(1000));
    }

    protected void browseDlq() throws Exception {
        Enumeration<?> messages = dlqBrowser.getEnumeration();
        while (messages.hasMoreElements()) {
            LOG.info("Browsing: " + messages.nextElement());
        }
    }
}
