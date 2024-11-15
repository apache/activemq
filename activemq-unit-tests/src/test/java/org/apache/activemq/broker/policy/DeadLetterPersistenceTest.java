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

import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.*;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetterPersistenceTest extends DeadLetterTest {
    private static final Logger LOG = LoggerFactory.getLogger(DiscardingDeadLetterPolicyTest.class);
    private static final String CLIENT_ID = "clientID";
    private static final String NON_PERSISTENT_DEST = "nonPersistentDest";
    private static final String PRESERVE_DELIVERY_DEST = "preserveDeliveryDest";

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();

        PolicyEntry policy = new PolicyEntry();
        IndividualDeadLetterStrategy strategy = new IndividualDeadLetterStrategy();
        strategy.setProcessNonPersistent(true);
        strategy.setDestinationPerDurableSubscriber(true);
        policy.setDeadLetterStrategy(strategy);

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        SharedDeadLetterStrategy processNonPersistent = new SharedDeadLetterStrategy();
        processNonPersistent.setDeadLetterQueue(new ActiveMQQueue("DLQ." + NON_PERSISTENT_DEST));
        processNonPersistent.setProcessNonPersistent(true);
        PolicyEntry processNonPersistentDlqPolicy = new PolicyEntry();
        processNonPersistentDlqPolicy.setDeadLetterStrategy(processNonPersistent);

        pMap.put(new ActiveMQQueue(NON_PERSISTENT_DEST), processNonPersistentDlqPolicy);

        SharedDeadLetterStrategy processPreserveDelivery = new SharedDeadLetterStrategy();
        processPreserveDelivery.setDeadLetterQueue(new ActiveMQQueue("DLQ." + PRESERVE_DELIVERY_DEST));
        processPreserveDelivery.setProcessNonPersistent(true);
        processPreserveDelivery.setPreserveDeliveryMode(true);
        PolicyEntry processPreserveDeliveryDlqPolicy = new PolicyEntry();
        processPreserveDeliveryDlqPolicy.setDeadLetterStrategy(processPreserveDelivery);

        pMap.put(new ActiveMQQueue(PRESERVE_DELIVERY_DEST), processPreserveDeliveryDlqPolicy);

        broker.setDestinationPolicy(pMap);

        return broker;
    }

    @Override
    protected String createClientId() {
        return CLIENT_ID;
    }

    @Override
    protected Destination createDlqDestination() {
        String prefix = topic ? "ActiveMQ.DLQ.Topic." : "ActiveMQ.DLQ.Queue.";
        String destinationName = prefix + getClass().getName() + "." + getName();
        if (durableSubscriber) {
            String subName = // connectionId:SubName
                    CLIENT_ID + ":" + getDestination().toString();
            destinationName += "." + subName ;
        }
        return new ActiveMQQueue(destinationName);
    }

    @Override
    protected void doTest() throws Exception {
        validateMessagePersistentSetToTrueWhenProducerIsPersistent();
        validateMessagePersistentSetToTrueWhenProducerIsNonPeristent();
        validateMessagePersitentNotSetWhenPreserveDeliveryModeIsTrue();
    }

    public void validateMessagePersistentSetToTrueWhenProducerIsPersistent() throws Exception {
        messageCount = 1;
        connection.start();

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;

        makeConsumer();
        makeDlqConsumer();
        sendMessages();

        for (int i = 0; i < messageCount; i++) {
            consumeAndRollback(i);
        }

        for (int i = 0; i < messageCount; i++) {
            Message msg = dlqConsumer.receive(1000);
            assertNotNull("Should be a DLQ message for loop: " + i, msg);
            org.apache.activemq.command.Message commandMsg = (org.apache.activemq.command.Message ) msg;
            assertTrue(commandMsg.isPersistent());
        }

        session.commit();
    }

    public void validateMessagePersistentSetToTrueWhenProducerIsNonPeristent() throws Exception {
        messageCount = 1;
        destination = new ActiveMQQueue(NON_PERSISTENT_DEST);
        durableSubscriber = false;
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        connection.start();

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;

        makeConsumer();
        makeDlqConsumer();
        sendMessages();

        for (int i = 0; i < messageCount; i++) {
            consumeAndRollback(i);
        }

        dlqDestination = new ActiveMQQueue("DLQ." + NON_PERSISTENT_DEST);
        dlqConsumer = session.createConsumer(dlqDestination);

        for (int i = 0; i < messageCount; i++) {
            Message msg = dlqConsumer.receive(1000);
            assertNotNull("Should be a DLQ message for loop: " + i, msg);
            assertEquals("NON_PERSISTENT", msg.getStringProperty("originalDeliveryMode"));
            org.apache.activemq.command.Message commandMsg = (org.apache.activemq.command.Message ) msg;
            assertTrue(commandMsg.isPersistent());
        }

        session.commit();
    }

    public void validateMessagePersitentNotSetWhenPreserveDeliveryModeIsTrue() throws Exception {
        messageCount = 1;
        destination = new ActiveMQQueue(PRESERVE_DELIVERY_DEST);
        durableSubscriber = false;
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        connection.start();

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;

        makeConsumer();
        makeDlqConsumer();
        sendMessages();

        for (int i = 0; i < messageCount; i++) {
            consumeAndRollback(i);
        }

        dlqDestination = new ActiveMQQueue("DLQ." + PRESERVE_DELIVERY_DEST);
        dlqConsumer = session.createConsumer(dlqDestination);

        for (int i = 0; i < messageCount; i++) {
            Message msg = dlqConsumer.receive(1000);
            assertNotNull("Should be a DLQ message for loop: " + i, msg);
            org.apache.activemq.command.Message commandMsg = (org.apache.activemq.command.Message ) msg;
            assertFalse(commandMsg.isPersistent());
        }

        session.commit();
    }
}
