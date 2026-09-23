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

import jakarta.jms.JMSException;

import org.apache.activemq.broker.SharedTopicBrokerService;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SharedConsumerInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SharedTopicRegionTest {

    private BrokerService brokerService;

    @Before
    public void setUp() throws Exception {
        brokerService = new SharedTopicBrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setBrokerName("shared-test");
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

    private ConnectionContext createContext(String clientId) throws Exception {
        ConnectionContext ctx = new ConnectionContext();
        ctx.setClientId(clientId);
        ctx.setBroker(brokerService.getBroker());
        return ctx;
    }

    private SharedConsumerInfo createSharedConsumerInfo(String connId, int session, int consumer,
            String subName, boolean durable) {
        ConnectionId cid = new ConnectionId(connId);
        SessionId sid = new SessionId(cid, session);
        ConsumerId consumerId = new ConsumerId(sid, consumer);
        SharedConsumerInfo info = new SharedConsumerInfo(consumerId);
        info.setDestination(new ActiveMQTopic("test.shared.topic"));
        info.setPrefetchSize(10);
        info.setSubscriptionName(subName);
        info.setShared(true);
        info.setDurable(durable);
        return info;
    }

    private ConsumerInfo createNormalConsumerInfo(String connId, int session, int consumer) {
        ConnectionId cid = new ConnectionId(connId);
        SessionId sid = new SessionId(cid, session);
        ConsumerId consumerId = new ConsumerId(sid, consumer);
        ConsumerInfo info = new ConsumerInfo(consumerId);
        info.setDestination(new ActiveMQTopic("test.normal.topic"));
        info.setPrefetchSize(10);
        return info;
    }

    @Test
    public void testSharedDurableCreatesCorrectType() throws Exception {
        ConnectionContext ctx = createContext("client-1");
        SharedConsumerInfo info = createSharedConsumerInfo("conn-1", 1, 1, "durSub", true);

        Subscription sub = brokerService.getBroker().addConsumer(ctx, info);
        assertNotNull(sub);
        assertTrue("Should create SharedDurableTopicSubscription",
                sub instanceof SharedDurableTopicSubscription);
    }

    @Test
    public void testSharedDurableSecondConsumerJoins() throws Exception {
        ConnectionContext ctx1 = createContext(null);
        SharedConsumerInfo info1 = createSharedConsumerInfo("conn-1", 1, 1, "durSub", true);
        Subscription sub1 = brokerService.getBroker().addConsumer(ctx1, info1);

        ConnectionContext ctx2 = createContext(null);
        SharedConsumerInfo info2 = createSharedConsumerInfo("conn-2", 1, 2, "durSub", true);
        Subscription sub2 = brokerService.getBroker().addConsumer(ctx2, info2);

        assertSame("Second consumer should join same subscription", sub1, sub2);
        assertEquals(2, ((SharedDurableTopicSubscription) sub1).getConsumerCount());
    }

    @Test
    public void testSharedDurableRemoveOneConsumer() throws Exception {
        ConnectionContext ctx1 = createContext(null);
        SharedConsumerInfo info1 = createSharedConsumerInfo("conn-1", 1, 1, "durSub", true);
        Subscription sub = brokerService.getBroker().addConsumer(ctx1, info1);

        ConnectionContext ctx2 = createContext(null);
        SharedConsumerInfo info2 = createSharedConsumerInfo("conn-2", 1, 2, "durSub", true);
        brokerService.getBroker().addConsumer(ctx2, info2);

        brokerService.getBroker().removeConsumer(ctx2, info2);

        SharedDurableTopicSubscription shared = (SharedDurableTopicSubscription) sub;
        assertEquals("One consumer should remain", 1, shared.getConsumerCount());
        assertTrue(shared.hasConsumers());
    }

    @Test
    public void testSharedDurableRemoveAllConsumersPreserves() throws Exception {
        ConnectionContext ctx = createContext("client-1");
        SharedConsumerInfo info = createSharedConsumerInfo("conn-1", 1, 1, "durSub", true);
        brokerService.getBroker().addConsumer(ctx, info);

        brokerService.getBroker().removeConsumer(ctx, info);

        // Durable subscription should still exist (deactivated, not destroyed)
        // A new consumer with the same name should be able to join
        ConnectionContext ctx2 = createContext("client-2");
        SharedConsumerInfo info2 = createSharedConsumerInfo("conn-2", 1, 2, "durSub", true);
        Subscription sub2 = brokerService.getBroker().addConsumer(ctx2, info2);
        assertNotNull(sub2);
        assertTrue(sub2 instanceof SharedDurableTopicSubscription);
    }

    @Test
    public void testSharedNonDurableCreatesCorrectType() throws Exception {
        ConnectionContext ctx = createContext("client-1");
        SharedConsumerInfo info = createSharedConsumerInfo("conn-1", 1, 1, "nonDurSub", false);

        Subscription sub = brokerService.getBroker().addConsumer(ctx, info);
        assertNotNull(sub);
        assertTrue("Should create SharedTopicSubscription",
                sub instanceof SharedTopicSubscription);
    }

    @Test
    public void testSharedNonDurableSecondConsumerJoins() throws Exception {
        ConnectionContext ctx1 = createContext(null);
        SharedConsumerInfo info1 = createSharedConsumerInfo("conn-1", 1, 1, "nonDurSub", false);
        Subscription sub1 = brokerService.getBroker().addConsumer(ctx1, info1);

        ConnectionContext ctx2 = createContext(null);
        SharedConsumerInfo info2 = createSharedConsumerInfo("conn-2", 1, 2, "nonDurSub", false);
        Subscription sub2 = brokerService.getBroker().addConsumer(ctx2, info2);

        assertSame("Second consumer should join same subscription", sub1, sub2);
        assertEquals(2, ((SharedTopicSubscription) sub1).getConsumerCount());
    }

    @Test
    public void testSharedNonDurableRemoveLastConsumerDestroys() throws Exception {
        ConnectionContext ctx = createContext("client-1");
        SharedConsumerInfo info = createSharedConsumerInfo("conn-1", 1, 1, "nonDurSub", false);
        brokerService.getBroker().addConsumer(ctx, info);

        brokerService.getBroker().removeConsumer(ctx, info);

        // Non-durable should be destroyed — a new consumer creates a fresh subscription
        ConnectionContext ctx2 = createContext("client-2");
        SharedConsumerInfo info2 = createSharedConsumerInfo("conn-2", 1, 2, "nonDurSub", false);
        Subscription sub2 = brokerService.getBroker().addConsumer(ctx2, info2);
        assertNotNull(sub2);
        assertTrue(sub2 instanceof SharedTopicSubscription);
    }

    @Test
    public void testNormalConsumerPassesThrough() throws Exception {
        ConnectionContext ctx = createContext("client-1");
        ConsumerInfo info = createNormalConsumerInfo("conn-1", 1, 1);

        Subscription sub = brokerService.getBroker().addConsumer(ctx, info);
        assertNotNull(sub);
        assertFalse("Normal consumer should NOT create shared subscription",
                sub instanceof SharedTopicSubscription);
        assertFalse(sub instanceof SharedDurableTopicSubscription);
    }

    @Test(expected = JMSException.class)
    public void testSelectorMismatchOnJoinThrows() throws Exception {
        ConnectionContext ctx1 = createContext(null);
        SharedConsumerInfo info1 = createSharedConsumerInfo("conn-1", 1, 1, "selSub", true);
        info1.setSelector("color = 'red'");
        brokerService.getBroker().addConsumer(ctx1, info1);

        ConnectionContext ctx2 = createContext(null);
        SharedConsumerInfo info2 = createSharedConsumerInfo("conn-2", 1, 2, "selSub", true);
        info2.setSelector("color = 'blue'");
        brokerService.getBroker().addConsumer(ctx2, info2);
    }

    @Test
    public void testMatchingSelectorAllowsJoin() throws Exception {
        ConnectionContext ctx1 = createContext(null);
        SharedConsumerInfo info1 = createSharedConsumerInfo("conn-1", 1, 1, "selSub", true);
        info1.setSelector("color = 'red'");
        Subscription sub1 = brokerService.getBroker().addConsumer(ctx1, info1);

        ConnectionContext ctx2 = createContext(null);
        SharedConsumerInfo info2 = createSharedConsumerInfo("conn-2", 1, 2, "selSub", true);
        info2.setSelector("color = 'red'");
        Subscription sub2 = brokerService.getBroker().addConsumer(ctx2, info2);

        assertSame(sub1, sub2);
    }

    @Test
    public void testSharedDurableWithNullClientId() throws Exception {
        ConnectionContext ctx = createContext(null);
        SharedConsumerInfo info = createSharedConsumerInfo("conn-1", 1, 1, "noClientSub", true);

        Subscription sub = brokerService.getBroker().addConsumer(ctx, info);
        assertNotNull(sub);
        assertTrue(sub instanceof SharedDurableTopicSubscription);
    }

    @Test
    public void testSharedNonDurableWithNullClientId() throws Exception {
        ConnectionContext ctx = createContext(null);
        SharedConsumerInfo info = createSharedConsumerInfo("conn-1", 1, 1, "noClientSub", false);

        Subscription sub = brokerService.getBroker().addConsumer(ctx, info);
        assertNotNull(sub);
        assertTrue(sub instanceof SharedTopicSubscription);
    }

    @Test(expected = JMSException.class)
    public void testSharedToUnsharedConflictThrows() throws Exception {
        ConnectionContext ctx1 = createContext("client-1");
        SharedConsumerInfo sharedInfo = createSharedConsumerInfo("conn-1", 1, 1, "conflictSub", true);
        brokerService.getBroker().addConsumer(ctx1, sharedInfo);

        ConnectionContext ctx2 = createContext("client-1");
        ConsumerInfo unsharedInfo = createDurableConsumerInfo("conn-2", 1, 2, "conflictSub");
        brokerService.getBroker().addConsumer(ctx2, unsharedInfo);
    }

    @Test(expected = JMSException.class)
    public void testUnsharedToSharedConflictThrows() throws Exception {
        ConnectionContext ctx1 = createContext("client-1");
        ConsumerInfo unsharedInfo = createDurableConsumerInfo("conn-1", 1, 1, "conflictSub");
        brokerService.getBroker().addConsumer(ctx1, unsharedInfo);

        ConnectionContext ctx2 = createContext("client-1");
        SharedConsumerInfo sharedInfo = createSharedConsumerInfo("conn-2", 1, 2, "conflictSub", true);
        brokerService.getBroker().addConsumer(ctx2, sharedInfo);
    }

    @Test
    public void testConversionEnabledAllowsSharedToUnshared() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();

        SharedTopicBrokerService convBroker = new SharedTopicBrokerService();
        convBroker.setTopicSubscriptionConversionEnabled(true);
        convBroker.setPersistent(false);
        convBroker.setUseJmx(false);
        convBroker.setBrokerName("conv-test-1");
        convBroker.start();
        convBroker.waitUntilStarted();
        try {
            ConnectionContext ctx1 = new ConnectionContext();
            ctx1.setClientId("client-1");
            ctx1.setBroker(convBroker.getBroker());
            SharedConsumerInfo sharedInfo = createSharedConsumerInfo("conn-1", 1, 1, "convertSub", true);
            convBroker.getBroker().addConsumer(ctx1, sharedInfo);
            convBroker.getBroker().removeConsumer(ctx1, sharedInfo);

            ConnectionContext ctx2 = new ConnectionContext();
            ctx2.setClientId("client-1");
            ctx2.setBroker(convBroker.getBroker());
            ConsumerInfo unsharedInfo = createDurableConsumerInfo("conn-2", 1, 2, "convertSub");
            Subscription sub = convBroker.getBroker().addConsumer(ctx2, unsharedInfo);
            assertNotNull(sub);
            assertFalse("Should be unshared after conversion",
                    sub instanceof SharedDurableTopicSubscription);
        } finally {
            convBroker.stop();
            convBroker.waitUntilStopped();
        }
    }

    @Test
    public void testConversionEnabledAllowsUnsharedToShared() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();

        SharedTopicBrokerService convBroker = new SharedTopicBrokerService();
        convBroker.setTopicSubscriptionConversionEnabled(true);
        convBroker.setPersistent(false);
        convBroker.setUseJmx(false);
        convBroker.setBrokerName("conv-test-2");
        convBroker.start();
        convBroker.waitUntilStarted();
        try {
            ConnectionContext ctx1 = new ConnectionContext();
            ctx1.setClientId("client-1");
            ctx1.setBroker(convBroker.getBroker());
            ConsumerInfo unsharedInfo = createDurableConsumerInfo("conn-1", 1, 1, "convertSub");
            convBroker.getBroker().addConsumer(ctx1, unsharedInfo);
            convBroker.getBroker().removeConsumer(ctx1, unsharedInfo);

            ConnectionContext ctx2 = new ConnectionContext();
            ctx2.setClientId("client-1");
            ctx2.setBroker(convBroker.getBroker());
            SharedConsumerInfo sharedInfo = createSharedConsumerInfo("conn-2", 1, 2, "convertSub", true);
            Subscription sub = convBroker.getBroker().addConsumer(ctx2, sharedInfo);
            assertNotNull(sub);
            assertTrue("Should be shared after conversion",
                    sub instanceof SharedDurableTopicSubscription);
        } finally {
            convBroker.stop();
            convBroker.waitUntilStopped();
        }
    }

    private ConsumerInfo createDurableConsumerInfo(String connId, int session, int consumer,
            String subName) {
        ConnectionId cid = new ConnectionId(connId);
        SessionId sid = new SessionId(cid, session);
        ConsumerId consumerId = new ConsumerId(sid, consumer);
        ConsumerInfo info = new ConsumerInfo(consumerId);
        info.setDestination(new ActiveMQTopic("test.shared.topic"));
        info.setPrefetchSize(10);
        info.setSubscriptionName(subName);
        return info;
    }
}
