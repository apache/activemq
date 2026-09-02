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
package org.apache.activemq.broker.region;

import static org.junit.Assert.*;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * [AMQ-9692] Support garbage collecting destinations
 * that have a wildcard-only subscription.
 *
 * This test suite confirms the logic in the
 * BaseDestination.isActive() method to ensure
 * destinations are not accidentally deleted due
 * to incorrect logic combination of PolicyEntr
 * config flag and status of current subscriptions.
 *
 * Every row runs against both a queue and a (non-durable) topic destination -
 * the isActive() contract is destination-type agnostic. Note the durable rows
 * exercise different paths per type: on a queue the durable sub appears in the
 * consumers list (predicate path); on a topic an inactive durable sub is
 * counted but not listed (count/list mismatch path).
 *
 * prod = attached producer
 * appC = normal application consumer
 * netC = network consumer
 * wildC = wildcard consumer (non-durable)
 * durWildC = durable wildcard consumer
 */
@RunWith(Parameterized.class)
public class DestinationIsActiveTest {

    private static BrokerService brokerService;
    private static final AtomicInteger counter = new AtomicInteger(0);

    @BeforeClass
    public static void beforeClass() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Parameterized.Parameters(name = "dest={0} gcNC={1} gcWC={2} prod={3} appC={4} netC={5} wildC={6} durWildC={7} exp={8}") // Optional name attribute for better test reporting
    public static Collection<Object[]> data() {
        // The truth table is destination-type agnostic - run every row against
        // both a queue and a topic destination
        var truthTable = new Object[][] {
                // Simple app consumer
                { false, false, false, false, false, false, false, false },
                { false, true, false, false, false, false, false, false },
                { true, false, false, false, false, false, false, false },
                { true, true, false, false, false, false, false, false },
                { false, false, false, true, false, false, false, true },
                { false, true, false, true, false, false, false, true },
                { true, false, false, true, false, false, false, true },
                { true, true, false, true, false, false, false, true },

                // Network consumer
                { false, false, false, false, true, false, false, true },
                { false, true, false, false, true, false, false, true },
                { true, false, false, false, true, false, false, false },
                { true, true, false, false, true, false, false, false },
                { false, false, false, true, true, false, false, true },
                { false, true, false, true, true, false, false, true },
                { true, false, false, true, true, false, false, true },
                { true, true, false, true, true, false, false, true },

                // Wildcard consumer
                { false, false, false, false, false, true, false, true },
                { false, true, false, false, false, true, false, false },
                { true, false, false, false, false, true, false, true },
                { true, true, false, false, false, true, false, false },
                { false, false, false, true, false, true, false, true },
                { false, true, false, true, false, true, false, true },
                { true, false, false, true, false, true, false, true },
                { true, true, false, true, false, true, false, true },

                // Mixed network + wildcard consumers - gc only allowed when
                // BOTH flags permit ignoring their respective consumer type
                { false, false, false, false, true, true, false, true },
                { false, true, false, false, true, true, false, true },
                { true, false, false, false, true, true, false, true },
                { true, true, false, false, true, true, false, false },
                { false, false, false, true, true, true, false, true },
                { false, true, false, true, true, true, false, true },
                { true, false, false, true, true, true, false, true },
                { true, true, false, true, true, true, false, true },

                // Durable wildcard consumer - never gc-eligible, its registration
                // and pending messages live in the destination's store
                { false, false, false, false, false, false, true, true },
                { false, true, false, false, false, false, true, true },
                { true, false, false, false, false, false, true, true },
                { true, true, false, false, false, false, true, true },
                { false, true, false, false, false, true, true, true },
                { true, true, false, false, true, true, true, true },

                // Attached producer - always active, even when every consumer
                // present is gc-eligible under the enabled flags
                { false, false, true, false, false, false, false, true },
                { true, true, true, false, false, false, false, true },
                { true, false, true, false, true, false, false, true },
                { false, true, true, false, false, true, false, true },
                { true, true, true, false, true, true, false, true }
        };

        var params = new ArrayList<Object[]>();
        for (var destinationType : List.of("queue", "topic")) {
            for (var row : truthTable) {
                var param = new Object[row.length + 1];
                param[0] = destinationType;
                System.arraycopy(row, 0, param, 1, row.length);
                params.add(param);
            }
        }
        return params;
    }

    private final String destinationType;
    private final boolean gcWithNetworkConsumersEnabled;
    private final boolean gcWithOnlyWildcardConsumersEnabled;
    private final boolean producerActive;
    private final boolean appConsumerActive;
    private final boolean networkConsumerActive;
    private final boolean wildcardConsumerActive;
    private final boolean durableWildcardConsumerActive;
    private final boolean activeExpected;

    public DestinationIsActiveTest(String destinationType, boolean gcWithNetworkConsumersEnabled, boolean gcWithOnlyWildcardConsumersEnabled, boolean producerActive, boolean appConsumerActive, boolean networkConsumerActive, boolean wildcardConsumerActive, boolean durableWildcardConsumerActive, boolean activeExpected) {
        this.destinationType = destinationType;
        this.gcWithNetworkConsumersEnabled = gcWithNetworkConsumersEnabled;
        this.gcWithOnlyWildcardConsumersEnabled = gcWithOnlyWildcardConsumersEnabled;
        this.producerActive = producerActive;
        this.appConsumerActive = appConsumerActive;
        this.networkConsumerActive = networkConsumerActive;
        this.wildcardConsumerActive = wildcardConsumerActive;
        this.durableWildcardConsumerActive = durableWildcardConsumerActive;
        this.activeExpected = activeExpected;
    }

    @Test
    public void testDestinationIsActive() throws Exception {
        var destinationName = "amq.gc." + counter.incrementAndGet();
        final var isTopic = "topic".equals(destinationType);

        var policyEntry = new PolicyEntry();
        policyEntry.setGcInactiveDestinations(true);
        policyEntry.setGcWithOnlyWildcardConsumers(gcWithOnlyWildcardConsumersEnabled);
        policyEntry.setGcWithNetworkConsumers(gcWithNetworkConsumersEnabled);
        policyEntry.setInactiveTimeoutBeforeGC(3000L);
        if (isTopic) {
            policyEntry.setTopic(destinationName);
        } else {
            policyEntry.setQueue(destinationName);
        }
        brokerService.getDestinationPolicy().setPolicyEntries(List.of(policyEntry));

        ActiveMQDestination activemqDestination;
        if (isTopic) {
            brokerService.getAdminView().addTopic(destinationName);
            activemqDestination = new ActiveMQTopic(destinationName);
        } else {
            brokerService.getAdminView().addQueue(destinationName);
            activemqDestination = new ActiveMQQueue(destinationName);
        }
        var destination = brokerService.getDestination(activemqDestination);

        assertFalse(destination.isActive());

        if(producerActive) {
            destination.addProducer(null, new ProducerInfo());
        }
        if(appConsumerActive) {
            destination.addSubscription(null, new MockQueueSubscription(activemqDestination, false, false, false));
        }
        if(networkConsumerActive) {
            destination.addSubscription(null, new MockQueueSubscription(activemqDestination, true, false, false));
        }
        if(wildcardConsumerActive) {
            destination.addSubscription(null, new MockQueueSubscription(activemqDestination, false, true, false));
        }
        if(durableWildcardConsumerActive) {
            // Topic.addSubscription casts durable subscriptions, so topics need
            // a DurableTopicSubscription-based mock (inactive, like one recovered
            // from the store at broker start)
            if (isTopic) {
                destination.addSubscription(null, new MockDurableTopicSubscription(activemqDestination, true));
            } else {
                destination.addSubscription(null, new MockQueueSubscription(activemqDestination, false, true, true));
            }
        }

        assertEquals(activeExpected, destination.isActive());

        // Test parameter config safety checks
        // if an appConsumer is active, destination must *always* be active
        if(appConsumerActive) {
            assertTrue(destination.isActive());
        }
        // if a producer is attached, destination must *always* be active
        if(producerActive) {
            assertTrue(destination.isActive());
        }
        // a durable subscription must *always* keep the destination active
        if(durableWildcardConsumerActive) {
            assertTrue(destination.isActive());
        }

        if (isTopic) {
            brokerService.getAdminView().removeTopic(destinationName);
        } else {
            brokerService.getAdminView().removeQueue(destinationName);
        }
    }

    protected static BrokerService createBroker() throws Exception {
        var map = new PolicyMap();
        map.setDefaultEntry(new PolicyEntry());

        var broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setSchedulePeriodForDestinationPurge(100_000_000);
        broker.setSchedulerSupport(true);
        broker.setMaxPurgedDestinationsPerSweep(1);
        broker.setDestinationPolicy(map);
        return broker;
    }

    static class MockConsumerInfo extends ConsumerInfo {

        private final boolean networkSubscription;
        private final boolean durableSubscription;

        public MockConsumerInfo(ActiveMQDestination activeMQDestination, boolean networkSubscription, boolean durableSubscription) {
            setDestination(activeMQDestination);
            this.networkSubscription = networkSubscription;
            this.durableSubscription = durableSubscription;
        }

        @Override
        public boolean isNetworkSubscription() {
            return this.networkSubscription;
        }

        @Override
        public boolean isDurable() {
            return this.durableSubscription;
        }
    }

    static class MockQueueSubscription extends QueueSubscription {

        private final boolean wildCardSubscription;

        public MockQueueSubscription(ActiveMQDestination activemqDestination, boolean networkSubscription, boolean wildCardSubscription, boolean durableSubscription) throws Exception {
            super(brokerService.getBroker(), null, null, new MockConsumerInfo(activemqDestination, networkSubscription, durableSubscription));
            this.wildCardSubscription = wildCardSubscription;
        }

        @Override
        public boolean isWildcard() {
            return this.wildCardSubscription;
        }
    }

    // An inactive durable subscription, as recovered from the store at broker start
    static class MockDurableTopicSubscription extends DurableTopicSubscription {

        private final boolean wildCardSubscription;

        public MockDurableTopicSubscription(ActiveMQDestination activemqDestination, boolean wildCardSubscription) throws Exception {
            super(brokerService.getBroker(), brokerService.getSystemUsage(),
                    durableContext(), durableConsumerInfo(activemqDestination), true);
            this.wildCardSubscription = wildCardSubscription;
        }

        @Override
        public boolean isWildcard() {
            return this.wildCardSubscription;
        }

        private static ConnectionContext durableContext() throws Exception {
            var context = new ConnectionContext();
            context.setClientId("mock-durable-client-" + counter.get());
            context.setBroker(brokerService.getBroker());
            return context;
        }

        private static ConsumerInfo durableConsumerInfo(ActiveMQDestination activemqDestination) {
            var info = new MockConsumerInfo(activemqDestination, false, true);
            info.setSubscriptionName("mock-durable-sub");
            info.setConsumerId(new ConsumerId(new SessionId(new ConnectionId("mock-durable-connection"), 1), counter.get()));
            return info;
        }
    }
}
