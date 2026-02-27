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
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
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
 * appC = normal application consumer
 * netC = network consumer
 * wildC = wildcard consumer
 */
@Category(ParallelTest.class)
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

    @Parameterized.Parameters(name = "gcNC={0} gcWC={1} appC={2} netC={3} wildC={4} exp={5}") // Optional name attribute for better test reporting
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // Simple app consumer
                { false, false, false, false, false, false },
                { false, true, false, false, false, false },
                { true, false, false, false, false, false },
                { true, true, false, false, false, false },
                { false, false, true, false, false, true },
                { false, true, true, false, false, true },
                { true, false, true, false, false, true },
                { true, true, true, false, false, true },

                // Network consumer
                { false, false, false, true, false, true },
                { false, true, false, true, false, true },
                { true, false, false, true, false, false },
                { true, true, false, true, false, false },
                { false, false, true, true, false, true },
                { false, true, true, true, false, true },
                { true, false, true, true, false, true },
                { true, true, true, true, false, true },

                // Wildcard consumer
                { false, false, false, false, true, true },
                { false, true, false, false, true, false },
                { true, false, false, false, true, true },
                { true, true, false, false, true, false },
                { false, false, true, false, true, true },
                { false, true, true, false, true, true },
                { true, false, true, false, true, true },
                { true, true, true, false, true, true }
        });
    }

    private final boolean gcWithNetworkConsumersEnabled;
    private final boolean gcWithOnlyWildcardConsumersEnabled;
    private final boolean appConsumerActive;
    private final boolean networkConsumerActive;
    private final boolean wildcardConsumerActive;
    private final boolean activeExpected;

    public DestinationIsActiveTest(boolean gcWithNetworkConsumersEnabled, boolean gcWithOnlyWildcardConsumersEnabled, boolean appConsumerActive, boolean networkConsumerActive, boolean wildcardConsumerActive, boolean activeExpected) {
        this.gcWithNetworkConsumersEnabled = gcWithNetworkConsumersEnabled;
        this.gcWithOnlyWildcardConsumersEnabled = gcWithOnlyWildcardConsumersEnabled;
        this.appConsumerActive = appConsumerActive;
        this.networkConsumerActive = networkConsumerActive;
        this.wildcardConsumerActive = wildcardConsumerActive;
        this.activeExpected = activeExpected;
    }

    @Test
    public void testDestinationIsActive() throws Exception {
        var queueName = "amq.gc." + counter.incrementAndGet();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setGcInactiveDestinations(true);
        policyEntry.setGcWithOnlyWildcardConsumers(gcWithOnlyWildcardConsumersEnabled);
        policyEntry.setGcWithNetworkConsumers(gcWithNetworkConsumersEnabled);
        policyEntry.setInactiveTimeoutBeforeGC(3000L);
        policyEntry.setQueue(queueName);
        brokerService.getDestinationPolicy().setPolicyEntries(List.of(policyEntry));

        brokerService.getAdminView().addQueue(queueName);
        var activemqDestination = new ActiveMQQueue(queueName);
        var queue = (Queue)brokerService.getDestination(activemqDestination);

        assertFalse(queue.isActive());

        if(appConsumerActive) {
            queue.addSubscription(null, new MockQueueSubscription(activemqDestination, false, false));
        }
        if(networkConsumerActive) {
            queue.addSubscription(null, new MockQueueSubscription(activemqDestination, true, false));
        }
        if(wildcardConsumerActive) {
            queue.addSubscription(null, new MockQueueSubscription(activemqDestination,false, true));
        }

        assertEquals(activeExpected, queue.isActive());

        // Test parameter config safety check
        // if an appConsumer is active, queue must *always* be active
        if(appConsumerActive) {
            assertTrue(queue.isActive());
        }
        brokerService.getAdminView().removeQueue(queueName);
    }

    protected static BrokerService createBroker() throws Exception {
        PolicyMap map = new PolicyMap();
        map.setDefaultEntry(new PolicyEntry());

        BrokerService broker = new BrokerService();
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

        public MockConsumerInfo(ActiveMQDestination activeMQDestination, boolean networkSubscription) {
            setDestination(activeMQDestination);
            this.networkSubscription = networkSubscription;
        }

        @Override
        public boolean isNetworkSubscription() {
            return this.networkSubscription;
        }
    }

    static class MockQueueSubscription extends QueueSubscription {

        private final boolean wildCardSubscription;

        public MockQueueSubscription(ActiveMQDestination activemqDestination, boolean networkSubscription, boolean wildCardSubscription) throws Exception {
            super(brokerService.getBroker(), null, null, new MockConsumerInfo(activemqDestination, networkSubscription));
            this.wildCardSubscription = wildCardSubscription;
        }

        @Override
        public boolean isWildcard() {
            return this.wildCardSubscription;
        }
    }
}
