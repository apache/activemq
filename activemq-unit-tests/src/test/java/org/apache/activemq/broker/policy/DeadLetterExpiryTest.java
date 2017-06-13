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

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Queue;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetterExpiryTest extends DeadLetterTest {
    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterExpiryTest.class);

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        PolicyMap pMap = broker.getDestinationPolicy();

        PolicyEntry policy = new PolicyEntry();
        DeadLetterStrategy strategy = new IndividualDeadLetterStrategy();
        strategy.setExpiration(4000);
        strategy.setProcessNonPersistent(true);
        policy.setDeadLetterStrategy(strategy);

        pMap.put(new ActiveMQQueue(getDestinationString()), policy);
        pMap.put(new ActiveMQTopic(getDestinationString()), policy);

        SharedDeadLetterStrategy sharedLoopStrategy = new SharedDeadLetterStrategy();
        strategy.setProcessNonPersistent(true);
        sharedLoopStrategy.setExpiration(1000);
        sharedLoopStrategy.setDeadLetterQueue(new ActiveMQQueue("DLQ.loop"));

        PolicyEntry buggyLoopingDLQPolicy = new PolicyEntry();
        buggyLoopingDLQPolicy.setDeadLetterStrategy(sharedLoopStrategy);

        pMap.put(new ActiveMQQueue("loop"), buggyLoopingDLQPolicy);
        pMap.put(new ActiveMQQueue("DLQ.loop"), buggyLoopingDLQPolicy);

        SharedDeadLetterStrategy auditConfigured = new SharedDeadLetterStrategy();
        auditConfigured.setDeadLetterQueue(new ActiveMQQueue("DLQ.auditConfigured"));
        auditConfigured.setProcessNonPersistent(true);
        auditConfigured.setProcessExpired(true);
        auditConfigured.setMaxProducersToAudit(1);
        auditConfigured.setMaxAuditDepth(10);
        PolicyEntry auditConfiguredDlqPolicy = new PolicyEntry();
        auditConfiguredDlqPolicy.setDeadLetterStrategy(auditConfigured);
        auditConfiguredDlqPolicy.setExpireMessagesPeriod(1000);

        pMap.put(new ActiveMQQueue("Comp.One"), auditConfiguredDlqPolicy);
        pMap.put(new ActiveMQQueue("Comp.Two"), auditConfiguredDlqPolicy);

        PolicyEntry auditConfiguredPolicy = new PolicyEntry();
        auditConfiguredPolicy.setEnableAudit(false); // allow duplicates through the cursor
        pMap.put(new ActiveMQQueue("DLQ.auditConfigured"), auditConfiguredPolicy);

        PolicyEntry policyWithExpiryProcessing = pMap.getDefaultEntry();
        policyWithExpiryProcessing.setExpireMessagesPeriod(1000);
        pMap.setDefaultEntry(policyWithExpiryProcessing);

        broker.setDestinationPolicy(pMap);

        return broker;
    }

    @Override
    protected Destination createDlqDestination() {
        String prefix = topic ? "ActiveMQ.DLQ.Topic." : "ActiveMQ.DLQ.Queue.";
        return new ActiveMQQueue(prefix + getClass().getName() + "." + getName());
    }

    protected void doTest() throws Exception {
        connection.start();
        messageCount = 4;

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;
        LOG.info("Will redeliver messages: " + rollbackCount + " times");

        makeConsumer();
        sendMessages();

        // now lets receive and rollback N times
        for (int i = 0; i < messageCount; i++) {
            consumeAndRollback(i);
        }

        Queue dlqQueue = (Queue) createDlqDestination();
        verifyIsDlq(dlqQueue);

        // they should expire
        final QueueViewMBean queueViewMBean = getProxyToQueue(dlqQueue.getQueueName());

        assertTrue("all dlq messages expired", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Queue size:" + queueViewMBean.getQueueSize());
                return queueViewMBean.getExpiredCount() == messageCount;
            }
        }));

        makeDlqConsumer();
        assertNull("no message available", dlqConsumer.receive(1000));

        final QueueViewMBean sharedDlqViewMBean = getProxyToQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME);
        assertTrue("messages stay on shared dlq which has default expiration=0", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Q " + sharedDlqViewMBean.getName() + " size:" + sharedDlqViewMBean.getQueueSize());
                return sharedDlqViewMBean.getQueueSize() == messageCount;
            }
        }));

    }

    public void testAuditConfigured() throws Exception {
        destination = new ActiveMQQueue("Comp.One,Comp.Two");
        connection.start();

        messageCount = 1;
        timeToLive = 2000;
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        sendMessages();
        sendMessages();

        assertTrue("all messages expired even duplicates!", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                try {
                    QueueViewMBean queueViewMBean = getProxyToQueue("DLQ.auditConfigured");
                    LOG.info("Queue " + queueViewMBean.getName() + ", size:" + queueViewMBean.getQueueSize());
                    return queueViewMBean.getQueueSize() == 4;
                } catch (Exception expectedTillExpiry) {}
                return false;
            }
        }));
    }

    public void testNoDLQLoop() throws Exception {
        destination = new ActiveMQQueue("loop");
        messageCount = 2;

        connection.start();

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;
        LOG.info("Will redeliver messages: " + rollbackCount + " times");

        makeConsumer();
        sendMessages();

        // now lets receive and rollback N times
        for (int i = 0; i < messageCount; i++) {
            consumeAndRollback(i);
        }

        // they should expire
        final QueueViewMBean queueViewMBean = getProxyToQueue("DLQ.loop");

        assertTrue("all dlq messages expired", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Queue size:" + queueViewMBean.getQueueSize());
                return queueViewMBean.getExpiredCount() == messageCount;
            }
        }));


        // strategy audit suppresses resend
        assertEquals("it should be empty", 0, queueViewMBean.getQueueSize());

    }

    protected void consumeAndRollback(int messageCounter) throws Exception {
        for (int i = 0; i < rollbackCount; i++) {
            Message message = consumer.receive(5000);
            assertNotNull("No message received for message: " + messageCounter + " and rollback loop: " + i, message);
            assertMessage(message, messageCounter);

            session.rollback();
        }
        LOG.info("Rolled back: " + rollbackCount + " times");
    }

    protected void setUp() throws Exception {
        transactedMode = true;
        deliveryMode = DeliveryMode.PERSISTENT;
        timeToLive = 0;
        super.setUp();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory answer = super.createConnectionFactory();
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setMaximumRedeliveries(3);
        policy.setBackOffMultiplier((short) 1);
        policy.setInitialRedeliveryDelay(10);
        policy.setUseExponentialBackOff(false);
        answer.setRedeliveryPolicy(policy);
        return answer;
    }

}
