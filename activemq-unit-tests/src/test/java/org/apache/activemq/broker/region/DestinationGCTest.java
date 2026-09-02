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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class DestinationGCTest {

    protected static final Logger logger = LoggerFactory.getLogger(DestinationGCTest.class);

    private final ActiveMQQueue queue = new ActiveMQQueue("TEST");
    private final ActiveMQQueue otherQueue = new ActiveMQQueue("TEST-OTHER");
    private final ActiveMQQueue wildcardQueueA = new ActiveMQQueue("TEST.FOO.A");
    private final ActiveMQQueue wildcardQueueB = new ActiveMQQueue("TEST.FOO.B");

    private BrokerService brokerService;

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    protected BrokerService createBroker() throws Exception {
        var entry = new PolicyEntry();
        entry.setGcInactiveDestinations(true);
        entry.setGcWithOnlyWildcardConsumers(true);
        entry.setInactiveTimeoutBeforeGC(3000);
        var map = new PolicyMap();
        map.setDefaultEntry(entry);

        // GUARD.> queues allow wildcard-only removal but are excluded from the
        // gc sweep, so the removeDestination guard tests are not raced by gc
        var guardEntry = new PolicyEntry();
        guardEntry.setQueue("GUARD.>");
        guardEntry.setGcInactiveDestinations(false);
        guardEntry.setGcWithOnlyWildcardConsumers(true);
        map.put(new ActiveMQQueue("GUARD.>"), guardEntry);

        // NOGC.> queues have the wildcard flag off - wildcard consumers keep
        // these destinations active
        var noGcEntry = new PolicyEntry();
        noGcEntry.setQueue("NOGC.>");
        noGcEntry.setGcInactiveDestinations(false);
        noGcEntry.setGcWithOnlyWildcardConsumers(false);
        map.put(new ActiveMQQueue("NOGC.>"), noGcEntry);

        var broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setDestinations(new ActiveMQDestination[] {queue});
        broker.setSchedulePeriodForDestinationPurge(1000);
        broker.setMaxPurgedDestinationsPerSweep(1);
        broker.setDestinationPolicy(map);

        return broker;
    }

    @Test(timeout = 60000)
    public void testDestinationGCWithActiveConsumers() throws Exception {
        assertEquals(1, brokerService.getAdminView().getQueues().length);

        var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        try (var connection = factory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createConsumer(queue)) {

            session.createProducer(otherQueue).close();
            consumer.setMessageListener(message -> {});

            connection.start();
            assertTrue("After GC runs there should be one Queue.",
                Wait.waitFor(() -> brokerService.getAdminView().getQueues().length == 1));
        }
    }

    @Test
    public void testDestinationGCWithOnlyWildcardConsumers() throws Exception {
        assertEquals(1, brokerService.getAdminView().getQueues().length);

        var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");

        final var receivedCount = new AtomicInteger(0);

        // Anonymous producer - does not register on the destinations, so the gc
        // assertions exercise only the wildcard consumer
        try (var connection = factory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(null)) {

            producer.send(wildcardQueueA, session.createTextMessage("Test first step queueA"));
            producer.send(wildcardQueueB, session.createTextMessage("Test first step queueB"));

            var consumer = session.createConsumer(session.createQueue("TEST.FOO.*"));
            consumer.setMessageListener(message -> receivedCount.incrementAndGet());

            connection.start();

            // Confirm queues are gc'd
            assertTrue("After GC runs there should be one Queue (count=" + brokerService.getAdminView().getQueues().length + ")",
                Wait.waitFor(() -> brokerService.getAdminView().getQueues().length == 1, 30000, 1000));

            assertEquals(Integer.valueOf(2), Integer.valueOf(receivedCount.get()));

            // Confirm wild-card consumer is able to stay active after zero matching destinations
            producer.send(wildcardQueueA, session.createTextMessage("Test second step queueA"));

            // Confirm queues are gc'd
            assertTrue("After GC runs there should be one Queue (count=" + brokerService.getAdminView().getQueues().length + ")",
                Wait.waitFor(() -> brokerService.getAdminView().getQueues().length == 1, 30000, 1000));
            assertEquals(Integer.valueOf(3), Integer.valueOf(receivedCount.get()));
        }
    }

    private int countMatchingTopics(String prefix) throws Exception {
        var count = 0;
        for (var name : brokerService.getAdminView().getTopics()) {
            var destinationName = name.getKeyProperty("destinationName");
            if (destinationName != null && destinationName.startsWith(prefix)) {
                count++;
            }
        }
        return count;
    }

    // [AMQ-9692] non-durable topic flavor of the wildcard-consumer gc lifecycle:
    // topics are gc'd while the wildcard consumer stays connected, and delivery
    // continues when a producer recreates them
    @Test(timeout = 60000)
    public void testTopicDestinationGCWithOnlyWildcardConsumers() throws Exception {
        final var receivedCount = new AtomicInteger(0);
        var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");

        // Subscribe before sending - non-durable topics do not retain messages.
        // Anonymous producer - does not register on the destinations
        try (var connection = factory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createConsumer(session.createTopic("TEST.BAR.*"));
             var producer = session.createProducer(null)) {

            consumer.setMessageListener(message -> receivedCount.incrementAndGet());
            connection.start();

            producer.send(session.createTopic("TEST.BAR.A"), session.createTextMessage("first-a"));
            producer.send(session.createTopic("TEST.BAR.B"), session.createTextMessage("first-b"));

            assertTrue("Wildcard topic consumer should receive both messages",
                    Wait.waitFor(() -> receivedCount.get() == 2));

            // Both topics should gc while the wildcard consumer stays connected
            assertTrue("After GC runs there should be no TEST.BAR. topics",
                    Wait.waitFor(() -> countMatchingTopics("TEST.BAR.") == 0, 30000, 1000));

            // A new send recreates the topic and the consumer must still receive
            producer.send(session.createTopic("TEST.BAR.A"), session.createTextMessage("second-a"));
            assertTrue("Wildcard topic consumer should receive after gc + recreate",
                    Wait.waitFor(() -> receivedCount.get() == 3));

            assertTrue("Recreated topic should gc again",
                    Wait.waitFor(() -> countMatchingTopics("TEST.BAR.") == 0, 30000, 1000));
        }
    }

    // [AMQ-9692] removeDestination(timeout=0) guard: an app consumer must block removal
    @Test(timeout = 60000)
    public void testRemoveDestinationWithAppConsumerThrows() throws Exception {
        var guardQueue = new ActiveMQQueue("GUARD.APP");
        var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        try (var connection = factory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createConsumer(guardQueue)) {

            connection.start();

            try {
                brokerService.getBroker().removeDestination(brokerService.getAdminConnectionContext(), guardQueue, 0);
                fail("Expected JMSException removing a destination with an active app consumer");
            } catch (JMSException expected) {
                assertTrue(expected.getMessage().contains("still has an active subscription"));
            }
        }
    }

    // [AMQ-9692] removeDestination(timeout=0) guard: a wildcard-only consumer permits
    // removal when gcWithOnlyWildcardConsumers is enabled, and keeps working afterwards
    @Test(timeout = 60000)
    public void testRemoveDestinationWithOnlyWildcardConsumerSucceeds() throws Exception {
        var guardQueue = new ActiveMQQueue("GUARD.WILD.A");
        var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        // Anonymous producer - does not register on the destination
        try (var connection = factory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createConsumer(session.createQueue("GUARD.WILD.*"));
             var producer = session.createProducer(null)) {

            connection.start();

            producer.send(guardQueue, session.createTextMessage("before-remove"));
            assertNotNull("Wildcard consumer should receive", consumer.receive(5000));

            // Only the wildcard consumer remains - removal is allowed
            brokerService.getBroker().removeDestination(brokerService.getAdminConnectionContext(), guardQueue, 0);

            // The wildcard consumer stays connected - a new send recreates the queue
            producer.send(guardQueue, session.createTextMessage("after-remove"));
            assertNotNull("Wildcard consumer should receive after remove + recreate", consumer.receive(5000));
        }
    }

    // [AMQ-9692] removeDestination(timeout=0) guard: with gcWithOnlyWildcardConsumers
    // disabled a wildcard consumer blocks removal (default behavior unchanged)
    @Test(timeout = 60000)
    public void testRemoveDestinationWithWildcardConsumerFlagOffThrows() throws Exception {
        var noGcQueue = new ActiveMQQueue("NOGC.WILD.A");
        var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        try (var connection = factory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createConsumer(session.createQueue("NOGC.WILD.*"));
             var producer = session.createProducer(null)) {

            connection.start();

            producer.send(noGcQueue, session.createTextMessage("test"));
            assertNotNull("Wildcard consumer should receive", consumer.receive(5000));

            try {
                brokerService.getBroker().removeDestination(brokerService.getAdminConnectionContext(), noGcQueue, 0);
                fail("Expected JMSException removing a wildcard-consumed destination with the flag off");
            } catch (JMSException expected) {
                assertTrue(expected.getMessage().contains("still has an active subscription"));
            }
        }
    }

    // [AMQ-9692] removeDestination(timeout=0) guard: removing a nonexistent destination
    // is a silent no-op even when a wildcard subscription matches the name. RegionBroker
    // filters nonexistent destinations before delegating, so this contract is only
    // reachable at the region level - call the queue region directly.
    @Test(timeout = 60000)
    public void testRemoveNonExistentDestinationMatchingWildcardIsNoOp() throws Exception {
        var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        try (var connection = factory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createConsumer(session.createQueue("GUARD.MISSING.*"));
             var producer = session.createProducer(null)) {

            connection.start();

            var queueCountBefore = brokerService.getAdminView().getQueues().length;
            var queueRegion = ((RegionBroker) brokerService.getRegionBroker()).getQueueRegion();
            queueRegion.removeDestination(brokerService.getAdminConnectionContext(),
                    new ActiveMQQueue("GUARD.MISSING.A"), 0);

            assertEquals(queueCountBefore, brokerService.getAdminView().getQueues().length);

            // The wildcard subscription is unaffected by the no-op removal
            producer.send(new ActiveMQQueue("GUARD.MISSING.A"), session.createTextMessage("after-noop"));
            assertNotNull("Wildcard consumer should still receive", consumer.receive(5000));
        }
    }

    @Test(timeout = 60000)
    public void testDestinationGc() throws Exception {
        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertTrue("After GC runs the Queue should be empty.",
            Wait.waitFor(() -> brokerService.getAdminView().getQueues().length == 0));
    }

    @Test(timeout = 60000)
    public void testDestinationGcLimit() throws Exception {

        brokerService.getAdminView().addQueue("TEST1");
        brokerService.getAdminView().addQueue("TEST2");
        brokerService.getAdminView().addQueue("TEST3");
        brokerService.getAdminView().addQueue("TEST4");

        assertEquals(5, brokerService.getAdminView().getQueues().length);

        // With maxPurgedDestinationsPerSweep=1, wait until at least one queue has been GC'd
        // but not all (verifying the sweep limit works)
        assertTrue("GC should have removed some but not all queues",
            Wait.waitFor(() -> {
                final var count = brokerService.getAdminView().getQueues().length;
                return count > 0 && count < 5;
            }, 15000, 500));

        assertTrue("After GC runs the Queue should be empty.", Wait.waitFor(() ->
            brokerService.getAdminView().getQueues().length == 0
        , 30000, 500));
    }

    @Test(timeout = 60000)
    public void testDestinationGcAnonymousProducer() throws Exception {

        final var q = new ActiveMQQueue("Q.TEST.ANONYMOUS.PRODUCER");

        brokerService.getAdminView().addQueue(q.getPhysicalName());
        assertEquals(2, brokerService.getAdminView().getQueues().length);

        final var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        try (var connection = factory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(null)) {

            // wait for the queue to be marked for GC
            logger.info("Waiting for '{}' to be marked for GC...", q);
            Wait.waitFor(() -> brokerService.getDestination(q).canGC(), Wait.MAX_WAIT_MILLIS, 500L);

            // send a message via the anonymous producer
            logger.info("Sending PERSISTENT message to QUEUE '{}'", q.getPhysicalName());
            producer.send(q, session.createTextMessage());

            assertFalse(brokerService.getDestination(q).canGC());
        }
    }
}
