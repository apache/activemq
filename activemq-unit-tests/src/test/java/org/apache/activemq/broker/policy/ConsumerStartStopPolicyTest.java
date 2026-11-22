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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.activemq.broker.region.AbstractRegion;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Broker side of {@link PolicyEntry#setUseConsumerStartStop(boolean)}: a ConsumerControl with
 * stop set keeps the broker from dispatching to the consumer whatever its prefetch, and one with
 * start set resumes dispatch. The controls are sent directly so the client's own use of them is
 * not involved.
 */
@Category(ParallelTest.class)
public class ConsumerStartStopPolicyTest {

    private BrokerService broker;
    private String connectionUri;
    private final List<Connection> connections = new ArrayList<>();

    private interface PolicyCustomizer {
        void customize(PolicyEntry entry);
    }

    private void startBroker(boolean useConsumerStartStop) throws Exception {
        startBroker(useConsumerStartStop, false, entry -> { });
    }

    private void startBroker(boolean useConsumerStartStop, boolean useJmx, PolicyCustomizer customizer) throws Exception {
        var entry = new PolicyEntry();
        entry.setUseConsumerStartStop(useConsumerStartStop);
        customizer.customize(entry);
        var policyMap = new PolicyMap();
        policyMap.setDefaultEntry(entry);

        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(useJmx);
        if (useJmx) {
            broker.getManagementContext().setCreateConnector(false);
        }
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(false);
        broker.setDestinationPolicy(policyMap);
        broker.addConnector("tcp://localhost:0");
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

    private ActiveMQConnection connect(String clientId) throws Exception {
        var factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setWatchTopicAdvisories(false);
        var connection = (ActiveMQConnection) factory.createConnection();
        if (clientId != null) {
            connection.setClientID(clientId);
        }
        connections.add(connection);
        connection.start();
        return connection;
    }

    private void control(ActiveMQConnection connection, MessageConsumer consumer, boolean start) throws Exception {
        var activemqConsumer = (ActiveMQMessageConsumer) consumer;
        var control = new ConsumerControl();
        control.setConsumerId(activemqConsumer.getConsumerId());
        control.setDestination(subscription(consumer).getConsumerInfo().getDestination());
        control.setPrefetch(activemqConsumer.getPrefetchNumber());
        control.setStart(start);
        control.setStop(!start);
        connection.syncSendPacket(control);
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
        send(destination, count, null);
    }

    private void send(Destination destination, int count, String groupId) throws Exception {
        var connection = connect(null);
        var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 0; i < count; i++) {
            var message = session.createTextMessage("message-" + i);
            if (groupId != null) {
                message.setStringProperty("JMSXGroupID", groupId);
            }
            producer.send(message);
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
    public void testStoppedConsumerTakesNoMessages() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.competing");
        var stoppedConnection = connect(null);
        var stopped = stoppedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        control(stoppedConnection, stopped, false);
        var running = connect(null).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);

        send(queue, 10);

        assertEquals("the running consumer receives every message", 10, drain(running, 2000));
        assertTrue(subscription(stopped).isDispatchStopped());
        assertEquals(0, subscription(stopped).getDispatchedQueueSize());
        assertNull(stopped.receiveNoWait());
    }

    @Test(timeout = 60_000)
    public void testStopIgnoredWithoutPolicy() throws Exception {
        startBroker(false);
        var queue = new ActiveMQQueue("startstop.ignored");
        var stoppedConnection = connect(null);
        var stopped = stoppedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        control(stoppedConnection, stopped, false);
        var running = connect(null).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);

        send(queue, 10);

        assertFalse(subscription(stopped).isDispatchStopped());
        var receivedByRunning = drain(running, 1000);
        var receivedByStopped = drain(stopped, 1000);
        assertEquals(10, receivedByRunning + receivedByStopped);
        assertTrue("without the policy the stop is ignored and dispatch is shared", receivedByStopped > 0);
    }

    @Test(timeout = 60_000)
    public void testStartDispatchesWaitingMessages() throws Exception {
        startBroker(true);
        assertStartDispatchesWaitingMessages(new ActiveMQQueue("startstop.plain"), "startstop.plain", "startstop.plain");
    }

    @Test(timeout = 60_000)
    public void testStartDispatchesWaitingMessagesToWildcardConsumer() throws Exception {
        startBroker(true);
        assertStartDispatchesWaitingMessages(new ActiveMQQueue("startstop.wild.>"), "startstop.wild.a", "startstop.wild.b");
    }

    @Test(timeout = 60_000)
    public void testStartDispatchesWaitingMessagesToCompositeConsumer() throws Exception {
        startBroker(true);
        assertStartDispatchesWaitingMessages(new ActiveMQQueue("startstop.comp.a,startstop.comp.b"), "startstop.comp.a", "startstop.comp.b");
    }

    private void assertStartDispatchesWaitingMessages(ActiveMQQueue consumerDestination, String first, String second) throws Exception {
        var connection = connect(null);
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(consumerDestination);
        control(connection, consumer, false);

        send(new ActiveMQQueue(first), 2);
        send(new ActiveMQQueue(second), 2);

        assertNull("nothing is dispatched while stopped", consumer.receive(500));
        assertEquals(0, subscription(consumer).getDispatchedQueueSize());

        control(connection, consumer, true);
        assertFalse(subscription(consumer).isDispatchStopped());
        assertEquals("start dispatches the waiting messages", 4, drain(consumer, 2000));
    }

    @Test(timeout = 60_000)
    public void testPullConsumerTimesOutWhileStopped() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.pull?consumer.prefetchSize=0");
        var connection = connect(null);
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        control(connection, consumer, false);
        send(queue, 1);

        var begin = System.nanoTime();
        assertNull("a pull while stopped takes no message", consumer.receive(1000));
        assertTrue("the pull timeout is still answered", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin) < 5000);
        assertNull(consumer.receiveNoWait());

        control(connection, consumer, true);
        assertNotNull("the message is pulled after start", consumer.receive(2000));
    }

    @Test(timeout = 60_000)
    public void testStoppedConsumerNeverOwnsMessageGroup() throws Exception {
        startBroker(true);
        var queue = new ActiveMQQueue("startstop.groups");
        var stoppedConnection = connect(null);
        var stopped = stoppedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        control(stoppedConnection, stopped, false);
        var running = connect(null).createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);

        send(queue, 5, "group-1");
        assertEquals(5, drain(running, 2000));

        var owners = ((Queue) broker.getDestination(queue)).getMessageGroupOwners().getGroups();
        assertEquals(((ActiveMQMessageConsumer) running).getConsumerId().toString(), owners.get("group-1"));

        control(stoppedConnection, stopped, true);
        send(queue, 5, "group-1");
        assertEquals("the group stays with its owner after the other consumer starts", 5, drain(running, 2000));
        assertNull(stopped.receive(500));
    }

    @Test(timeout = 60_000)
    public void testStoppedConsumerIsNotAbortedAsSlow() throws Exception {
        startBroker(true, false, entry -> {
            var strategy = new AbortSlowAckConsumerStrategy();
            strategy.setIgnoreIdleConsumers(false);
            strategy.setMaxTimeSinceLastAck(100);
            strategy.setMaxSlowDuration(100);
            strategy.setCheckPeriod(50);
            entry.setSlowConsumerStrategy(strategy);
        });
        var queue = new ActiveMQQueue("startstop.slow");
        var connection = connect(null);
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);
        control(connection, consumer, false);
        send(queue, 1);

        TimeUnit.SECONDS.sleep(1);
        var subscription = subscription(consumer);
        assertNotNull("the stopped consumer is not aborted", subscription);
        assertFalse(subscription.isSlowConsumer());

        control(connection, consumer, true);
        assertNotNull(consumer.receive(2000));
    }

    @Test(timeout = 60_000)
    public void testStoppedDurableSubscriberKeepsMessages() throws Exception {
        startBroker(true);
        var topic = new ActiveMQTopic("startstop.durable");
        var connection = connect("durable-client");
        var subscriber = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createDurableSubscriber(topic, "sub");
        control(connection, subscriber, false);

        send(topic, 3);
        assertNull("nothing is dispatched while stopped", subscriber.receive(500));
        assertEquals(0, subscription(subscriber).getDispatchedQueueSize());

        control(connection, subscriber, true);
        assertEquals(3, drain(subscriber, 2000));
    }

    @Test(timeout = 60_000)
    public void testNewDurableConsumerIsNotLeftStopped() throws Exception {
        startBroker(true);
        var topic = new ActiveMQTopic("startstop.durable.reuse");
        var connection = connect("durable-reuse");
        var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var first = session.createDurableSubscriber(topic, "sub");
        control(connection, first, false);
        first.close();

        send(topic, 3);
        var second = session.createDurableSubscriber(topic, "sub");
        assertEquals("a consumer that never stopped receives the messages", 3, drain(second, 2000));
    }

    @Test(timeout = 60_000)
    public void testStopIgnoredForNonDurableTopicSubscriber() throws Exception {
        startBroker(true);
        var topic = new ActiveMQTopic("startstop.topic");
        var connection = connect(null);
        var subscriber = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(topic);
        control(connection, subscriber, false);

        send(topic, 3);
        assertFalse(subscription(subscriber).isDispatchStopped());
        assertEquals(3, drain(subscriber, 2000));
    }

    @Test(timeout = 60_000)
    public void testDispatchStoppedOnSubscriptionView() throws Exception {
        startBroker(true, true, entry -> { });
        var queue = new ActiveMQQueue("startstop.jmx");
        var connection = connect(null);
        var consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue);

        assertTrue(Wait.waitFor(() -> broker.getAdminView().getQueueSubscribers().length == 1, 5000, 50));
        var name = broker.getAdminView().getQueueSubscribers()[0];
        var view = (SubscriptionViewMBean) broker.getManagementContext()
                .newProxyInstance(name, SubscriptionViewMBean.class, true);
        assertFalse(view.isDispatchStopped());

        control(connection, consumer, false);
        assertTrue(view.isDispatchStopped());

        control(connection, consumer, true);
        assertFalse(view.isDispatchStopped());
    }
}
