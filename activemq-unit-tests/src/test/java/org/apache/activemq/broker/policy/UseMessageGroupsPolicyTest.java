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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * PolicyEntry.useMessageGroups=false ignores JMSXGroupID for dispatch: no consumer is ever
 * assigned a group, no ownership is recorded, JMSXGroupFirstForConsumer is never set, and a
 * group's messages are dispatched across consumers like any other messages. The default (true)
 * keeps the standard contract: a group is delivered in order to its single owning consumer.
 *
 * Motivation: a group's owner that can never be dispatched a later member - its selector, or
 * the network bridge filter on a network subscription, rejects the message - strands that
 * message for everyone (the owner will not take it, no other consumer may). Enough of those fill
 * dispatchPendingList to maxPageSize and stall the whole queue until the owning subscription is
 * removed. Disabling groups on the policy is the configuration-level way out when the group
 * header is unintended.
 */
@Category(ParallelTest.class)
public class UseMessageGroupsPolicyTest {

    private static final String QUEUE_NAME = "USE.MESSAGE.GROUPS.TEST";

    private BrokerService broker;
    private String brokerUri;

    private void startBroker(boolean useMessageGroups) throws Exception {
        startBroker(useMessageGroups, false);
    }

    private void startBroker(boolean useMessageGroups, boolean useJmx) throws Exception {
        var entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setUseMessageGroups(useMessageGroups);
        var policyMap = new PolicyMap();
        policyMap.setDefaultEntry(entry);

        broker = new BrokerService();
        broker.setBrokerName("useMessageGroups-" + useMessageGroups);
        broker.setPersistent(false);
        broker.setUseJmx(useJmx);
        if (useJmx) {
            broker.getManagementContext().setCreateConnector(false);
        }
        broker.setDestinationPolicy(policyMap);
        broker.addConnector("tcp://127.0.0.1:0");
        broker.start();
        broker.waitUntilStarted();
        brokerUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    /** Default: a group goes, in order, to the single consumer that owns it. */
    @Test(timeout = 60_000)
    public void testGroupsEnabledByDefaultKeepAGroupOnOneConsumer() throws Exception {
        startBroker(true);
        var first = new AtomicInteger();
        var second = new AtomicInteger();
        var firstFlags = new AtomicInteger();
        try (var c1 = consumer(first, firstFlags); var c2 = consumer(second, null)) {
            produce(40, "group-A");
            assertTrue(Wait.waitFor(() -> first.get() + second.get() == 40, 20_000, 10));
            assertTrue("with groups enabled one consumer must own the whole group (got "
                    + first.get() + "/" + second.get() + ")", first.get() == 40 || second.get() == 40);
            assertEquals("the owner is recorded", 1, queue().getMessageGroupOwners().getGroups().size());
        }
    }

    /** useMessageGroups=false: the same group is spread across consumers; nothing is recorded. */
    @Test(timeout = 60_000)
    public void testUseMessageGroupsFalseDispatchesAcrossConsumersAndRecordsNoOwnership() throws Exception {
        startBroker(false);
        assertFalse("policy must reach the queue", queue(true).isUseMessageGroups());
        var first = new AtomicInteger();
        var second = new AtomicInteger();
        var firstFlags = new AtomicInteger();
        try (var c1 = consumer(first, firstFlags); var c2 = consumer(second, firstFlags)) {
            produce(40, "group-A");
            assertTrue(Wait.waitFor(() -> first.get() + second.get() == 40, 20_000, 10));
            assertTrue("with groups disabled both consumers must receive (got "
                    + first.get() + "/" + second.get() + ")", first.get() > 0 && second.get() > 0);
            assertTrue("no ownership may be recorded", queue().getMessageGroupOwners().getGroups().isEmpty());
            assertEquals("JMSXGroupFirstForConsumer must never be set", 0, firstFlags.get());
        }
    }

    /**
     * The stall the option exists for: the group's owner carries a selector that matched the
     * first message but rejects later members. With groups enabled those members are stuck for
     * everyone; with useMessageGroups=false the unselective consumer simply takes them.
     */
    @Test(timeout = 60_000)
    public void testUseMessageGroupsFalseAvoidsStrandingMembersTheOwnerCannotTake() throws Exception {
        startBroker(false);
        var selective = new AtomicInteger();
        var unselective = new AtomicInteger();
        try (var owner = consumer(selective, null, "color = 'red'");
             var other = consumer(unselective, null)) {
            produce(1, "group-B", "red");     // would make the selective consumer the owner
            assertTrue(Wait.waitFor(() -> selective.get() == 1, 20_000, 10));
            produce(20, "group-B", "blue");   // the owner can never take these
            assertTrue("members the would-be owner cannot take must be dispatched to the consumer that can "
                    + "(unselective got " + unselective.get() + ")",
                    Wait.waitFor(() -> unselective.get() == 20, 20_000, 10));
            assertEquals("nothing left pending", 0, queue().getDestinationStatistics().getMessages().getCount());
        }
    }

    /** The option is readable and settable at runtime through the queue MBean. */
    @Test(timeout = 60_000)
    public void testUseMessageGroupsReadableAndSettableOnQueueMBean() throws Exception {
        startBroker(false, true);
        var queue = queue(true);
        assertFalse("policy must reach the queue", queue.isUseMessageGroups());

        var name = new ObjectName(broker.getManagementContext().getJmxDomainName()
                + ":type=Broker,brokerName=" + broker.getBrokerName()
                + ",destinationType=Queue,destinationName=" + QUEUE_NAME);
        var view = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(name, QueueViewMBean.class, true);
        assertFalse("MBean must report the queue value", view.isUseMessageGroups());

        view.setUseMessageGroups(true);
        assertTrue("MBean set must reach queue dispatch state", queue.isUseMessageGroups());
        assertTrue(view.isUseMessageGroups());
    }

    /** The same scenario with the default proves the stall the option prevents. */
    @Test(timeout = 60_000)
    public void testGroupsEnabledStrandMembersTheOwnerCannotTake() throws Exception {
        startBroker(true);
        var selective = new AtomicInteger();
        var unselective = new AtomicInteger();
        try (var owner = consumer(selective, null, "color = 'red'");
             var other = consumer(unselective, null)) {
            produce(1, "group-B", "red");
            assertTrue(Wait.waitFor(() -> selective.get() == 1, 20_000, 10));
            produce(20, "group-B", "blue");
            Thread.sleep(3000);
            assertEquals("with groups enabled the blue members are stranded: owner rejects them, others may not take them",
                    0, unselective.get());
            assertEquals(20, queue().getDestinationStatistics().getMessages().getCount());
        }
    }

    // ---- helpers ----

    private AutoCloseableConsumer consumer(AtomicInteger counter, AtomicInteger groupFirstFlags) throws Exception {
        return consumer(counter, groupFirstFlags, null);
    }

    private AutoCloseableConsumer consumer(AtomicInteger counter, AtomicInteger groupFirstFlags, String selector) throws Exception {
        var factory = new ActiveMQConnectionFactory(brokerUri);
        var prefetch = new ActiveMQPrefetchPolicy();
        prefetch.setQueuePrefetch(5);   // small prefetch so a spread across consumers is visible
        factory.setPrefetchPolicy(prefetch);
        var connection = factory.createConnection();
        connection.start();
        var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var mc = selector == null ? session.createConsumer(new ActiveMQQueue(QUEUE_NAME))
                : session.createConsumer(new ActiveMQQueue(QUEUE_NAME), selector);
        mc.setMessageListener(m -> {
            try {
                if (groupFirstFlags != null && m.propertyExists("JMSXGroupFirstForConsumer")
                        && m.getBooleanProperty("JMSXGroupFirstForConsumer")) {
                    groupFirstFlags.incrementAndGet();
                }
                Thread.sleep(5);   // let the other consumer get a turn
            } catch (Exception ignored) {
            }
            counter.incrementAndGet();
        });
        return new AutoCloseableConsumer(connection, mc);
    }

    private void produce(int n, String groupId) throws Exception {
        produce(n, groupId, null);
    }

    private void produce(int n, String groupId, String color) throws Exception {
        try (var c = new ActiveMQConnectionFactory(brokerUri).createConnection()) {
            c.start();
            var session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            var producer = session.createProducer(new ActiveMQQueue(QUEUE_NAME));
            for (var i = 0; i < n; i++) {
                var m = session.createTextMessage("m-" + i);
                m.setStringProperty("JMSXGroupID", groupId);
                if (color != null) {
                    m.setStringProperty("color", color);
                }
                producer.send(m);
            }
        }
    }

    private Queue queue() throws Exception {
        return queue(false);
    }

    private Queue queue(boolean create) throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);
        if (create) {
            broker.getBroker().addDestination(broker.getAdminConnectionContext(), dest, true);
        }
        return (Queue) ((RegionBroker) broker.getRegionBroker()).getQueueRegion().getDestinationMap().get(dest);
    }

    private record AutoCloseableConsumer(Connection connection, MessageConsumer consumer) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            connection.close();
        }
    }
}
