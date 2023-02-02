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

import java.util.Arrays;
import java.util.Enumeration;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.broker.region.virtual.CompositeQueue;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
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

        PolicyEntry indvAuditPolicy = new PolicyEntry();
        IndividualDeadLetterStrategy indvAuditDlqStrategy = new IndividualDeadLetterStrategy();
        indvAuditDlqStrategy.setEnableAudit(true);
        indvAuditPolicy.setDeadLetterStrategy(indvAuditDlqStrategy);

        PolicyEntry shrAuditPolicy = new PolicyEntry();
        SharedDeadLetterStrategy shrAuditDlqStrategy = new SharedDeadLetterStrategy();
        shrAuditDlqStrategy.setEnableAudit(true);
        shrAuditPolicy.setDeadLetterStrategy(shrAuditDlqStrategy);

        PolicyMap pMap = new PolicyMap();
        pMap.put(new ActiveMQQueue(getDestinationString()), policy);
        pMap.put(new ActiveMQTopic(getDestinationString()), policy);
        pMap.put(new ActiveMQQueue(getDestinationString() + ".INDV.>"), indvAuditPolicy);
        pMap.put(new ActiveMQQueue(getDestinationString() + ".SHR.>"), shrAuditPolicy);

        broker.setDestinationPolicy(pMap);

        CompositeQueue indvAuditCompQueue = new CompositeQueue();
        indvAuditCompQueue.setName(getDestinationString() + ".INDV.A");
        indvAuditCompQueue.setForwardOnly(true);
        indvAuditCompQueue.setForwardTo(Arrays.asList(new ActiveMQQueue(getDestinationString() + ".INDV.B"), new ActiveMQQueue(getDestinationString() + ".INDV.C")));

        CompositeQueue sharedAuditCompQueue = new CompositeQueue();
        sharedAuditCompQueue.setName(getDestinationString() + ".SHR.A");
        sharedAuditCompQueue.setForwardOnly(true);
        sharedAuditCompQueue.setForwardTo(Arrays.asList(new ActiveMQQueue(getDestinationString() + ".SHR.B"), new ActiveMQQueue(getDestinationString() + ".SHR.C")));

        VirtualDestinationInterceptor vdi = new VirtualDestinationInterceptor();
        vdi.setVirtualDestinations(new VirtualDestination[] { indvAuditCompQueue, sharedAuditCompQueue });
        broker.setDestinationInterceptors(new VirtualDestinationInterceptor[] {vdi});
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

    // AMQ-9217
    public void testPerDestinationAuditDefault() throws Exception {
        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;

        connection.start();
        session = connection.createSession(transactedMode, acknowledgeMode);
        MessageProducer messageProducerA = session.createProducer(session.createQueue(getDestinationString() + ".INDV.A"));
        messageProducerA.send(session.createTextMessage("testPerDestinationAuditEnabled"));
        session.commit();

        for(String destName : Set.of(getDestinationString() + ".INDV.B", getDestinationString() + ".INDV.C")) {
            for (int i = 0; i < rollbackCount; i++) {
                MessageConsumer indvConsumer = session.createConsumer(session.createQueue(destName));
                Message message = indvConsumer.receive(5000);
                assertNotNull("No message received: ", message);

                session.rollback();
                LOG.info("Rolled back: " + rollbackCount + " times");
                indvConsumer.close();
            }
        }

        QueueViewMBean a = getProxyToQueue(getDestinationString() + ".INDV.A");
        assertNotNull(a);
        assertTrue(Wait.waitFor(() -> a.getEnqueueCount() == 0l, 3000, 250));
        assertTrue(Wait.waitFor(() -> a.getQueueSize() == 0l, 3000, 250));

        QueueViewMBean b = getProxyToQueue(getDestinationString() + ".INDV.B");
        assertNotNull(b);
        assertTrue(Wait.waitFor(() -> b.getEnqueueCount() == 1l, 3000, 250));
        assertTrue(Wait.waitFor(() -> b.getQueueSize() == 0l, 3000, 250));

        QueueViewMBean c = getProxyToQueue(getDestinationString() + ".INDV.C");
        assertNotNull(c);
        assertTrue(Wait.waitFor(() -> c.getEnqueueCount() == 1l, 3000, 250));
        assertTrue(Wait.waitFor(() -> c.getQueueSize() == 0l, 3000, 250));

        QueueViewMBean bDlq = getProxyToQueue("ActiveMQ.DLQ.Queue." + getDestinationString() + ".INDV.B");
        assertNotNull(bDlq);
        assertTrue(Wait.waitFor(() -> bDlq.getEnqueueCount() == 1l, 3000, 250));
        assertTrue(Wait.waitFor(() -> bDlq.getQueueSize() == 1l, 3000, 250));

        QueueViewMBean cDlq = getProxyToQueue("ActiveMQ.DLQ.Queue." + getDestinationString() + ".INDV.C");
        assertNotNull(cDlq);
        assertTrue(Wait.waitFor(() -> cDlq.getEnqueueCount() == 1, 3000, 250));
        assertTrue(Wait.waitFor(() -> cDlq.getQueueSize() == 1, 3000, 250));
    }

    public void testSharedDestinationAuditDropsMessages() throws Exception {
        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;

        connection.start();
        session = connection.createSession(transactedMode, acknowledgeMode);
        MessageProducer messageProducerA = session.createProducer(session.createQueue(getDestinationString() + ".SHR.A"));
        messageProducerA.send(session.createTextMessage("testSharedDestinationAuditDropsMessages"));
        session.commit();

        for(String destName : Set.of(getDestinationString() + ".SHR.B", getDestinationString() + ".SHR.C")) {
            for (int i = 0; i < rollbackCount; i++) {
                MessageConsumer shrConsumer = session.createConsumer(session.createQueue(destName));
                Message message = shrConsumer.receive(5000);
                assertNotNull("No message received: ", message);

                session.rollback();
                LOG.info("Rolled back: " + rollbackCount + " times");
                shrConsumer.close();
            }
        }

        QueueViewMBean a = getProxyToQueue(getDestinationString() + ".SHR.A");
        assertNotNull(a);
        assertTrue(Wait.waitFor(() -> a.getEnqueueCount() == 0l, 3000, 250));
        assertTrue(Wait.waitFor(() -> a.getQueueSize() == 0l, 3000, 250));

        QueueViewMBean b = getProxyToQueue(getDestinationString() + ".SHR.B");
        assertNotNull(b);
        assertTrue(Wait.waitFor(() -> b.getEnqueueCount() == 1l, 3000, 250));
        assertTrue(Wait.waitFor(() -> b.getQueueSize() == 0l, 3000, 250));

        QueueViewMBean c = getProxyToQueue(getDestinationString() + ".SHR.C");
        assertNotNull(c);
        assertTrue(Wait.waitFor(() -> c.getEnqueueCount() == 1l, 3000, 250));
        assertTrue(Wait.waitFor(() -> c.getQueueSize() == 0l, 3000, 250));

        // Only 1 message in 1 DLQ means the a message was dropped due to shared message audit
        QueueViewMBean sharedDlq = getProxyToQueue("ActiveMQ.DLQ");
        assertNotNull(sharedDlq);
        assertTrue(Wait.waitFor(() -> sharedDlq.getEnqueueCount() == 1, 3000, 250));
        assertTrue(Wait.waitFor(() -> sharedDlq.getQueueSize() == 1, 3000, 250));
    }

    protected void browseDlq() throws Exception {
        Enumeration<?> messages = dlqBrowser.getEnumeration();
        while (messages.hasMoreElements()) {
            LOG.info("Browsing: " + messages.nextElement());
        }
    }
}
