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
package org.apache.activemq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.region.AbstractRegion;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Connection suspend and resume: a connection that is not started sends a ConnectionControl
 * with suspend set before its first consumer registers and when it stops, and one with resume
 * set when it starts. A consumer that registers while its connection is suspended is created
 * stopped, and a suspend or resume stops or starts the consumers already registered. A broker
 * plugin records each registration, with whether the subscription was created stopped, and the
 * stops and starts the broker applies to existing consumers.
 */
@Category(ParallelTest.class)
public class ConsumerStartStopTest {

    private BrokerService broker;
    private String connectionUri;
    private final List<String> events = new CopyOnWriteArrayList<>();
    private final List<Connection> connections = new ArrayList<>();

    private void startBroker(boolean useConsumerStartStop) throws Exception {
        startBroker(useConsumerStartStop, "tcp://localhost:0");
    }

    private void startBroker(boolean useConsumerStartStop, String connectorUri) throws Exception {
        var entry = new PolicyEntry();
        entry.setUseConsumerStartStop(useConsumerStartStop);
        var policyMap = new PolicyMap();
        policyMap.setDefaultEntry(entry);

        broker = new BrokerService();
        broker.setBrokerName("startstop");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(false);
        broker.setDestinationPolicy(policyMap);
        broker.setPlugins(new BrokerPlugin[] { next -> new BrokerFilter(next) {
            @Override
            public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
                var subscription = super.addConsumer(context, info);
                events.add(info.getConsumerId() + " add prefetch=" + info.getPrefetchSize()
                        + " stopped=" + subscription.isDispatchStopped());
                return subscription;
            }

            @Override
            public void processConsumerControl(ConsumerBrokerExchange exchange, ConsumerControl control) {
                var kind = control.isStop() ? "stop" : control.isStart() ? "start" : "prefetch=" + control.getPrefetch();
                events.add(control.getConsumerId() + " " + kind);
                super.processConsumerControl(exchange, control);
            }
        } });
        broker.addConnector(connectorUri);
        broker.start();
        broker.waitUntilStarted();
        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        for (var connection : connections) {
            try {
                connection.close();
            } catch (Exception ignored) {
            }
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private ActiveMQConnectionFactory factory(String uri) {
        var factory = new ActiveMQConnectionFactory(uri);
        factory.setWatchTopicAdvisories(false);
        return factory;
    }

    private ActiveMQConnection connect(boolean start) throws Exception {
        return connect(factory(connectionUri), start);
    }

    private ActiveMQConnection connect(ActiveMQConnectionFactory factory, boolean start) throws Exception {
        var connection = (ActiveMQConnection) factory.createConnection();
        connections.add(connection);
        if (start) {
            connection.start();
        }
        return connection;
    }

    private List<String> events(MessageConsumer consumer) {
        return events(((ActiveMQMessageConsumer) consumer).getConsumerId());
    }

    private List<String> events(ConsumerId consumerId) {
        var prefix = consumerId + " ";
        var result = new ArrayList<String>();
        for (var event : events) {
            if (event.startsWith(prefix)) {
                result.add(event.substring(prefix.length()));
            }
        }
        return result;
    }

    private void assertEvents(MessageConsumer consumer, String... expected) throws Exception {
        var wanted = List.of(expected);
        if (!Wait.waitFor(() -> events(consumer).equals(wanted), 5000, 20)) {
            assertEquals(wanted, events(consumer));
        }
    }

    private Subscription subscription(MessageConsumer consumer) throws Exception {
        var consumerId = ((ActiveMQMessageConsumer) consumer).getConsumerId();
        var regionBroker = (RegionBroker) broker.getRegionBroker();
        var subscription = ((AbstractRegion) regionBroker.getQueueRegion()).getSubscriptions().get(consumerId);
        if (subscription == null) {
            subscription = ((AbstractRegion) regionBroker.getTopicRegion()).getSubscriptions().get(consumerId);
        }
        return subscription;
    }

    private void send(Destination destination, int count) throws Exception {
        var connection = connect(true);
        var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var producer = session.createProducer(destination);
        for (int i = 0; i < count; i++) {
            producer.send(session.createTextMessage("message-" + i));
        }
        connections.remove(connection);
        connection.close();
    }

    private static int drain(MessageConsumer consumer, long waitMillis) throws Exception {
        var count = 0;
        while (consumer.receive(waitMillis) != null) {
            count++;
        }
        return count;
    }

    @Test(timeout = 60_000)
    public void testConsumerIsCreatedStoppedUntilConnectionStarts() throws Exception {
        startBroker(true);
        var connection = connect(false);
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQQueue("startstop.order"));

        assertEvents(consumer, "add prefetch=1000 stopped=true");
        assertTrue(subscription(consumer).isDispatchStopped());
        assertEquals("the prefetch is not changed", 1000, subscription(consumer).getConsumerInfo().getPrefetchSize());

        connection.start();
        assertEvents(consumer, "add prefetch=1000 stopped=true", "start");
        assertFalse(subscription(consumer).isDispatchStopped());
        assertEquals(1000, subscription(consumer).getConsumerInfo().getPrefetchSize());
    }

    @Test(timeout = 60_000)
    public void testConsumerOnStartedConnectionIsNotStopped() throws Exception {
        startBroker(true);
        var consumer = connect(true).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQQueue("startstop.started"));
        assertEvents(consumer, "add prefetch=1000 stopped=false");
        TimeUnit.MILLISECONDS.sleep(200);
        assertEquals(List.of("add prefetch=1000 stopped=false"), events(consumer));
    }

    @Test(timeout = 60_000)
    public void testNeverStartedCompetitorTakesNoMessages() throws Exception {
        startBroker(true);
        assertNeverStartedCompetitorTakesNoMessages();
    }

    @Test(timeout = 60_000)
    public void testNeverStartedCompetitorTakesNoMessagesOverVm() throws Exception {
        startBroker(true, "vm://startstop");
        assertNeverStartedCompetitorTakesNoMessages();
    }

    private void assertNeverStartedCompetitorTakesNoMessages() throws Exception {
        var queue = new ActiveMQQueue("startstop.competing");
        var idle = connect(false).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        var running = connect(true).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        assertTrue(subscription(idle).isDispatchStopped());

        send(queue, 10);
        assertEquals("the started consumer receives every message", 10, drain(running, 2000));
        assertEquals(0, subscription(idle).getDispatchedQueueSize());
    }

    @Test(timeout = 60_000)
    public void testNeverStartedCompetitorTakesMessagesWithoutPolicy() throws Exception {
        startBroker(false);
        var queue = new ActiveMQQueue("startstop.competing.nopolicy");
        var idle = connect(false).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        var running = connect(true).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        assertEvents(idle, "add prefetch=1000 stopped=false");

        send(queue, 10);
        assertTrue("without the policy the broker dispatches to the idle consumer as before",
                Wait.waitFor(() -> subscription(idle).getDispatchedQueueSize() > 0, 5000, 20));
        assertTrue(drain(running, 1000) < 10);
    }

    @Test(timeout = 60_000)
    public void testWaitingMessagesAreNotTakenAtRegistration() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.backlog");
        send(queue, 5);

        var idle = connect(false).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        assertEvents(idle, "add prefetch=1000 stopped=true");
        var running = connect(true).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);

        assertEquals("the waiting messages go to the started consumer", 5, drain(running, 2000));
        assertEquals(0, subscription(idle).getDispatchedQueueSize());
    }

    @Test(timeout = 60_000)
    public void testStartDeliversWaitingMessages() throws Exception {
        startBroker(true);
        assertStartDeliversWaitingMessages(new ActiveMQQueue("startstop.plain"),
                new ActiveMQQueue("startstop.plain"), new ActiveMQQueue("startstop.plain"));
    }

    @Test(timeout = 60_000)
    public void testStartDeliversWaitingMessagesToWildcardConsumer() throws Exception {
        startBroker(true);
        assertStartDeliversWaitingMessages(new ActiveMQQueue("startstop.wild.>"),
                new ActiveMQQueue("startstop.wild.a"), new ActiveMQQueue("startstop.wild.b"));
    }

    @Test(timeout = 60_000)
    public void testStartDeliversWaitingMessagesToCompositeConsumer() throws Exception {
        startBroker(true);
        assertStartDeliversWaitingMessages(new ActiveMQQueue("startstop.comp.a,startstop.comp.b"),
                new ActiveMQQueue("startstop.comp.a"), new ActiveMQQueue("startstop.comp.b"));
    }

    private void assertStartDeliversWaitingMessages(ActiveMQQueue consumerDestination, ActiveMQQueue first,
            ActiveMQQueue second) throws Exception {
        // half of the messages wait before the consumer registers and half arrive after
        send(first, 1);
        send(second, 1);
        var connection = connect(false);
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(consumerDestination);
        assertEvents(consumer, "add prefetch=1000 stopped=true");

        send(first, 1);
        send(second, 1);
        assertNull(consumer.receive(500));
        assertEquals(0, subscription(consumer).getDispatchedQueueSize());

        connection.start();
        assertEquals("start delivers the waiting messages", 4, drain(consumer, 2000));
    }

    @Test(timeout = 60_000)
    public void testDurableSubscriberIsCreatedStopped() throws Exception {
        startBroker(true);
        var topic = new ActiveMQTopic("startstop.durable");
        var connection = connect(false);
        connection.setClientID("startstop-durable");
        var subscriber = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createDurableSubscriber(topic, "sub");
        assertTrue(subscription(subscriber).isDispatchStopped());

        send(topic, 3);
        assertNull(subscriber.receive(500));
        assertEquals(0, subscription(subscriber).getDispatchedQueueSize());

        connection.start();
        assertEquals(3, drain(subscriber, 2000));
    }

    @Test(timeout = 60_000)
    public void testReactivatedDurableSubscriptionIsStoppedWhileSuspended() throws Exception {
        startBroker(true);
        var topic = new ActiveMQTopic("startstop.durable.reactivate");
        var first = connect(false);
        first.setClientID("startstop-reactivate");
        first.start();
        first.createSession(false, Session.AUTO_ACKNOWLEDGE).createDurableSubscriber(topic, "sub").close();
        connections.remove(first);
        first.close();
        send(topic, 3);

        var second = connect(false);
        second.setClientID("startstop-reactivate");
        var subscriber = second.createSession(false, Session.AUTO_ACKNOWLEDGE).createDurableSubscriber(topic, "sub");
        assertTrue("the reactivated subscription is stopped", subscription(subscriber).isDispatchStopped());
        assertNull(subscriber.receive(500));
        assertEquals(0, subscription(subscriber).getDispatchedQueueSize());

        second.start();
        assertEquals(3, drain(subscriber, 2000));
    }

    @Test(timeout = 60_000)
    public void testTopicSubscriberAndBrowserAreNotStopped() throws Exception {
        startBroker(true);
        var session = connect(false).createSession(false, Session.AUTO_ACKNOWLEDGE);
        var subscriber = session.createConsumer(new ActiveMQTopic("startstop.topic"));
        assertEvents(subscriber, "add prefetch=" + ActiveMQPrefetchPolicy.DEFAULT_TOPIC_PREFETCH + " stopped=false");

        var queue = new ActiveMQQueue("startstop.browse");
        send(queue, 1);
        events.clear();
        session.createBrowser(queue).getEnumeration();
        assertTrue(Wait.waitFor(() -> !events.isEmpty(), 5000, 20));
        for (var event : events) {
            assertTrue("browser is not stopped: " + event, event.endsWith(" stopped=false"));
        }
    }

    @Test(timeout = 60_000)
    public void testJmsContextAutoStartIsNotStopped() throws Exception {
        startBroker(true);
        try (var context = factory(connectionUri).createContext()) {
            var consumer = context.createConsumer(new ActiveMQQueue("startstop.context"));
            assertTrue(Wait.waitFor(() -> events.size() == 1, 5000, 20));
            assertTrue(events.get(0), events.get(0).endsWith(" add prefetch=1000 stopped=false"));
            assertNull(consumer.receiveNoWait());
        }
    }

    @Test(timeout = 60_000)
    public void testJmsContextWithoutAutoStartStartsOnContextStart() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.context.manual");
        try (var context = factory(connectionUri).createContext()) {
            context.setAutoStart(false);
            var consumer = context.createConsumer(queue);
            assertTrue(Wait.waitFor(() -> events.size() == 1, 5000, 20));
            assertTrue(events.get(0), events.get(0).endsWith(" add prefetch=1000 stopped=true"));

            send(queue, 1);
            assertNull(consumer.receive(500));

            context.start();
            assertTrue(Wait.waitFor(() -> events.size() == 2, 5000, 20));
            assertTrue(events.get(1), events.get(1).endsWith(" start"));
            assertNotNull(consumer.receive(2000));
        }
    }

    @Test(timeout = 60_000)
    public void testConnectionStopSuspendsItsConsumers() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.restart");
        var connection = connect(true);
        var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var before = session.createConsumer(queue);
        var running = connect(true).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);

        connection.stop();
        assertEvents(before, "add prefetch=1000 stopped=false", "stop");
        var during = session.createConsumer(queue);
        assertEvents(during, "add prefetch=1000 stopped=true");

        send(queue, 5);
        assertEquals("a stopped connection takes no new messages", 5, drain(running, 2000));

        connection.start();
        assertEvents(before, "add prefetch=1000 stopped=false", "stop", "start");
        assertEvents(during, "add prefetch=1000 stopped=true", "start");
        assertFalse(subscription(before).isDispatchStopped());
        assertFalse(subscription(during).isDispatchStopped());
    }

    @Test(timeout = 60_000)
    public void testConsumerOnCleanedUpConnectionIsCreatedStopped() throws Exception {
        startBroker(true);
        var connection = connect(true);
        connection.cleanup();
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQQueue("startstop.cleanup"));
        assertEvents(consumer, "add prefetch=1000 stopped=true");
    }

    @Test(timeout = 60_000)
    public void testReceiveBeforeStartReturnsWithoutMessage() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.receive");
        var consumer = (ActiveMQMessageConsumer) connect(false).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        assertEvents(consumer, "add prefetch=1000 stopped=true");
        send(queue, 1);

        assertNull(consumer.receive(300));
        assertNull(consumer.receiveNoWait());
        assertNull(consumer.receiveBody(String.class, 300));
        assertNull(consumer.receiveBodyNoWait(String.class));
    }

    @Test(timeout = 60_000)
    public void testMessageListenerBeforeStart() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.listener");
        var connection = connect(false);
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        var delivered = new CountDownLatch(5);
        consumer.setMessageListener(message -> delivered.countDown());

        send(queue, 5);
        assertFalse("no delivery before start", delivered.await(500, TimeUnit.MILLISECONDS));
        assertEquals(0, subscription(consumer).getDispatchedQueueSize());

        connection.start();
        assertTrue(delivered.await(5, TimeUnit.SECONDS));
    }

    @Test(timeout = 120_000)
    public void testConnectionStartRacingConsumerCreation() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.race");
        var executor = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 25; round++) {
                var connection = connect(false);
                var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                var go = new CountDownLatch(1);
                var created = executor.submit(() -> {
                    go.await();
                    return session.createConsumer(queue);
                });
                var started = executor.submit(() -> {
                    go.await();
                    connection.start();
                    return null;
                });
                go.countDown();
                var consumer = created.get(10, TimeUnit.SECONDS);
                started.get(10, TimeUnit.SECONDS);

                final int attempt = round;
                assertTrue("round " + attempt + " left the consumer stopped: " + events(consumer),
                        Wait.waitFor(() -> !subscription(consumer).isDispatchStopped(), 5000, 10));
                connections.remove(connection);
                connection.close();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeout = 60_000)
    public void testOptOutLeavesConsumerStarted() throws Exception {
        startBroker(true);
        var factory = factory(connectionUri);
        factory.setSuspendDispatchWhenStopped(false);
        var consumer = connect(factory, false).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQQueue("startstop.optout"));
        assertEvents(consumer, "add prefetch=1000 stopped=false");
    }

    @Test
    public void testSuspendDispatchWhenStoppedSurvivesSerializationAndProperties() throws Exception {
        var factory = new ActiveMQConnectionFactory("vm://localhost");
        assertTrue(factory.isSuspendDispatchWhenStopped());
        factory.setSuspendDispatchWhenStopped(false);

        var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) {
            out.writeObject(factory);
        }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertFalse(((ActiveMQConnectionFactory) in.readObject()).isSuspendDispatchWhenStopped());
        }

        var copy = new ActiveMQConnectionFactory();
        copy.setProperties(factory.getProperties());
        assertFalse(copy.isSuspendDispatchWhenStopped());

        assertFalse(new ActiveMQConnectionFactory("vm://localhost?jms.suspendDispatchWhenStopped=false").isSuspendDispatchWhenStopped());
    }

    private void dropClientConnections() throws Exception {
        for (TransportConnection connection : broker.getTransportConnectors().get(0).getConnections()) {
            connection.serviceException(new IOException("simulated connection failure"));
        }
    }

    @Test(timeout = 60_000)
    public void testSuspendKeepsFailoverConnectionFaultTolerant() throws Exception {
        startBroker(true);
        var connection = connect(factory("failover:(" + connectionUri + ")"), false);
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQQueue("startstop.faulttolerant"));
        connection.start();
        assertTrue(Wait.waitFor(() -> broker.getTransportConnectors().get(0).getConnections().size() == 1, 5000, 20));
        var brokerSide = broker.getTransportConnectors().get(0).getConnections().get(0);
        assertTrue("suspend and resume keep the fault tolerance the connection registered with",
                brokerSide.isFaultTolerantConnection());
    }

    @Test(timeout = 60_000)
    public void testReconnectWhileSuspendedCreatesConsumersStoppedAgain() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.failover.stopped");
        var connection = connect(factory("failover:(" + connectionUri + ")?maxReconnectDelay=100"), false);
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        assertEvents(consumer, "add prefetch=1000 stopped=true");

        events.clear();
        dropClientConnections();
        // the replay suspends the connection again before it registers the consumer
        assertTrue("re-registered after the failure", Wait.waitFor(() -> !events(consumer).isEmpty(), 10000, 20));
        assertTrue(events(consumer).get(0), events(consumer).get(0).endsWith(" stopped=true"));
        assertTrue(Wait.waitFor(() -> subscription(consumer) != null && subscription(consumer).isDispatchStopped(), 5000, 20));

        var running = connect(true).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        send(queue, 5);
        assertEquals(5, drain(running, 2000));

        connection.start();
        assertTrue(Wait.waitFor(() -> !subscription(consumer).isDispatchStopped(), 5000, 20));
    }

    @Test(timeout = 60_000)
    public void testReconnectAfterStartLeavesConsumersStarted() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.failover.started");
        var connection = connect(factory("failover:(" + connectionUri + ")?maxReconnectDelay=100"), false);
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        connection.start();
        assertEvents(consumer, "add prefetch=1000 stopped=true", "start");

        events.clear();
        dropClientConnections();
        assertTrue("re-registered after the failure", Wait.waitFor(() -> !events(consumer).isEmpty(), 10000, 20));
        assertTrue(events(consumer).get(0), events(consumer).get(0).endsWith(" stopped=false"));
        assertTrue(Wait.waitFor(() -> subscription(consumer) != null
                && subscription(consumer).getConsumerInfo().getPrefetchSize() == 1000, 10000, 20));

        send(queue, 3);
        assertEquals(3, drain(consumer, 2000));
    }
}
