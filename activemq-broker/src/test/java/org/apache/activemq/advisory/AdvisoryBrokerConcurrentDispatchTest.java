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
package org.apache.activemq.advisory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.EmptyBroker;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.SimpleDispatchPolicy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.network.ConditionalNetworkBridgeFilterFactory;
import org.apache.activemq.security.SecurityContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AdvisoryBrokerConcurrentDispatchTest {

    @Test
    public void advisoryExchangesRetainBrokerContextWithIndependentEvaluationState() {
        EmptyBroker next = new EmptyBroker();
        AdvisoryBroker advisoryBroker = new AdvisoryBroker(next);
        ConnectionContext adminContext = new ConnectionContext();
        adminContext.setBroker(next);
        adminContext.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
        adminContext.setClientId("broker-admin");
        advisoryBroker.setAdminConnectionContext(adminContext);

        ConnectionContext first = advisoryBroker.newAdvisoryProducerExchange().getConnectionContext();
        ConnectionContext second = advisoryBroker.newAdvisoryProducerExchange().getConnectionContext();

        assertNotSame(first, second);
        assertNotSame(adminContext.getMessageEvaluationContext(), first.getMessageEvaluationContext());
        assertNotSame(first.getMessageEvaluationContext(), second.getMessageEvaluationContext());
        for (ConnectionContext context : List.of(first, second)) {
            assertSame(next, context.getBroker());
            assertSame(SecurityContext.BROKER_SECURITY_CONTEXT, context.getSecurityContext());
            assertEquals(adminContext.getClientId(), context.getClientId());
            assertFalse(context.isProducerFlowControl());
        }
        assertTrue("Advisory flow control must not change the admin context", adminContext.isProducerFlowControl());
        assertSame("Existing copy callers must retain their evaluation context",
                adminContext.getMessageEvaluationContext(), adminContext.copy().getMessageEvaluationContext());
    }

    @Test(timeout = 30000)
    public void concurrentConsumerRemovalKeepsAdvisoryEvaluationContextsIsolated() throws Exception {
        ActiveMQQueue firstQueue = new ActiveMQQueue("first");
        ActiveMQQueue secondQueue = new ActiveMQQueue("second");
        ActiveMQTopic firstAdvisory = AdvisorySupport.getConsumerAdvisoryTopic(firstQueue);
        ActiveMQTopic advisoryTopics = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Queue.>");
        CountDownLatch firstDispatchEntered = new CountDownLatch(1);
        CountDownLatch resumeFirstDispatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        BrokerService broker = new BrokerService();
        broker.setBrokerName("advisory-context-isolation");
        broker.setPersistent(false);
        broker.setUseJmx(false);

        PolicyEntry policy = new PolicyEntry();
        policy.setTopic(advisoryTopics.getPhysicalName());
        policy.setDispatchPolicy(new SimpleDispatchPolicy() {
            @Override
            public boolean dispatch(MessageReference node, MessageEvaluationContext context,
                    List<Subscription> consumers) throws Exception {
                Message message = node.getMessage();
                if (firstAdvisory.equals(message.getDestination())
                        && message.getDataStructure() instanceof RemoveInfo) {
                    firstDispatchEntered.countDown();
                    assertTrue("Second removal must complete while the first dispatch is paused",
                            resumeFirstDispatch.await(10, TimeUnit.SECONDS));
                }
                boolean dispatched = super.dispatch(node, context, consumers);
                assertSame("Another advisory must not replace the message being evaluated",
                        node, context.getMessageReference());
                assertEquals(message.getDestination(), context.getDestination());
                return dispatched;
            }
        });
        PolicyMap policies = new PolicyMap();
        policies.setPolicyEntries(List.of(policy));
        broker.setDestinationPolicy(policies);

        // Install the same additional predicate used by a conditional network bridge,
        // while keeping the test local and the dispatch interleaving deterministic.
        broker.setPlugins(new BrokerPlugin[] { next -> new BrokerFilter(next) {
            @Override
            public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
                if (advisoryTopics.equals(info.getDestination())) {
                    ConditionalNetworkBridgeFilterFactory factory = new ConditionalNetworkBridgeFilterFactory();
                    factory.setReplayWhenNoConsumers(true);
                    info.setAdditionalPredicate(factory.create(info, new BrokerId[] {new BrokerId("remote")}, 1, 1));
                }
                return super.addConsumer(context, info);
            }
        }});

        try {
            broker.start();
            broker.waitUntilStarted();
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                    "vm://advisory-context-isolation?create=false");
            factory.setWatchTopicAdvisories(false);
            try (Connection observerConnection = factory.createConnection();
                    Connection firstConnection = factory.createConnection();
                    Connection secondConnection = factory.createConnection()) {
                observerConnection.start();
                Session observerSession = observerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer observer = observerSession.createConsumer(advisoryTopics);
                MessageConsumer first = firstConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
                        .createConsumer(firstQueue);
                MessageConsumer second = secondConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
                        .createConsumer(secondQueue);

                // Request a response so failures in broker-side consumer cleanup reach the test.
                Future<?> firstRemoval = executor.submit(() -> {
                    ((ActiveMQConnection) firstConnection).syncSendPacket(
                            new RemoveInfo(((ActiveMQMessageConsumer) first).getConsumerId()));
                    return null;
                });
                assertTrue("First consumer removal must reach advisory dispatch",
                        firstDispatchEntered.await(10, TimeUnit.SECONDS));
                Future<?> secondRemoval = executor.submit(() -> {
                    ((ActiveMQConnection) secondConnection).syncSendPacket(
                            new RemoveInfo(((ActiveMQMessageConsumer) second).getConsumerId()));
                    return null;
                });
                try {
                    secondRemoval.get(10, TimeUnit.SECONDS);
                } finally {
                    resumeFirstDispatch.countDown();
                }
                firstRemoval.get(10, TimeUnit.SECONDS);

                int removals = 0;
                for (int i = 0; i < 4; i++) {
                    Message advisory = (Message) observer.receive(2000);
                    assertNotNull("Both consumer creation and removal advisories must be delivered", advisory);
                    if (advisory.getDataStructure() instanceof RemoveInfo) {
                        removals++;
                    }
                }
                assertEquals("Both removals must reach the advisory subscriber", 2, removals);
                assertTrue(broker.getDestination(firstQueue).getConsumers().isEmpty());
                assertTrue(broker.getDestination(secondQueue).getConsumers().isEmpty());
            }
        } finally {
            resumeFirstDispatch.countDown();
            executor.shutdownNow();
            broker.stop();
            broker.waitUntilStopped();
        }
    }
}
