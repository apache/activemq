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
import org.apache.activemq.broker.EmptyBroker;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.usage.SystemUsage;
import org.junit.Before;
import org.junit.Test;

public class SharedTopicSubscriptionTest {

    private EmptyBroker broker;
    private SystemUsage usage;

    @Before
    public void setUp() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        broker = new EmptyBroker() {
            @Override
            public BrokerService getBrokerService() {
                return brokerService;
            }
        };
        usage = new SystemUsage();
    }

    private ConnectionContext createContext(String connId) {
        ConnectionContext ctx = new ConnectionContext();
        ctx.setClientId(connId);
        return ctx;
    }

    private ConsumerInfo createConsumerInfo(String connId, int sessionNum, int consumerNum,
            int prefetch) {
        ConnectionId cid = new ConnectionId(connId);
        SessionId sid = new SessionId(cid, sessionNum);
        ConsumerId consumerId = new ConsumerId(sid, consumerNum);
        ConsumerInfo info = new ConsumerInfo(consumerId);
        info.setDestination(new ActiveMQTopic("test.topic"));
        info.setPrefetchSize(prefetch);
        info.setSubscriptionName("sharedSub");
        return info;
    }

    @Test
    public void testConstructorAddsSingleConsumer() throws Exception {
        ConnectionContext ctx = createContext("conn-1");
        ConsumerInfo info = createConsumerInfo("conn-1", 1, 1, 10);

        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx, info);
        assertEquals(1, sub.getConsumerCount());
        assertTrue(sub.hasConsumers());
    }

    @Test
    public void testAddConsumerIncrementsCount() throws Exception {
        ConnectionContext ctx1 = createContext("conn-1");
        ConsumerInfo info1 = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx1, info1);

        ConnectionContext ctx2 = createContext("conn-2");
        ConsumerInfo info2 = createConsumerInfo("conn-2", 1, 2, 10);
        sub.addConsumer(ctx2, info2);

        assertEquals(2, sub.getConsumerCount());
    }

    @Test
    public void testRemoveConsumerDecrementsCount() throws Exception {
        ConnectionContext ctx1 = createContext("conn-1");
        ConsumerInfo info1 = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx1, info1);

        ConnectionContext ctx2 = createContext("conn-2");
        ConsumerInfo info2 = createConsumerInfo("conn-2", 1, 2, 10);
        sub.addConsumer(ctx2, info2);
        assertEquals(2, sub.getConsumerCount());

        sub.removeConsumer(info2.getConsumerId());
        assertEquals(1, sub.getConsumerCount());
        assertTrue(sub.hasConsumers());
    }

    @Test
    public void testRemoveLastConsumerLeavesEmpty() throws Exception {
        ConnectionContext ctx = createContext("conn-1");
        ConsumerInfo info = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx, info);

        sub.removeConsumer(info.getConsumerId());
        assertEquals(0, sub.getConsumerCount());
        assertFalse(sub.hasConsumers());
    }

    @Test
    public void testIsFullWhenNoConsumers() throws Exception {
        ConnectionContext ctx = createContext("conn-1");
        ConsumerInfo info = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx, info);
        sub.removeConsumer(info.getConsumerId());

        assertTrue("Should be full when no consumers", sub.isFull());
    }

    @Test
    public void testIsNotFullWithFreshConsumer() throws Exception {
        ConnectionContext ctx = createContext("conn-1");
        ConsumerInfo info = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx, info);

        assertFalse("Fresh consumer should not be full", sub.isFull());
    }

    @Test
    public void testCountBeforeFullSingleConsumer() throws Exception {
        ConnectionContext ctx = createContext("conn-1");
        ConsumerInfo info = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx, info);

        assertEquals(10, sub.countBeforeFull());
    }

    @Test
    public void testCountBeforeFullMultipleConsumers() throws Exception {
        ConnectionContext ctx1 = createContext("conn-1");
        ConsumerInfo info1 = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx1, info1);

        ConnectionContext ctx2 = createContext("conn-2");
        ConsumerInfo info2 = createConsumerInfo("conn-2", 1, 2, 5);
        sub.addConsumer(ctx2, info2);

        assertEquals(15, sub.countBeforeFull());
    }

    @Test
    public void testConsumerStateIsFullWhenDispatchedEqualsPrefetch() {
        ConsumerInfo info = createConsumerInfo("c1", 1, 1, 5);
        SharedTopicSubscription.ConsumerState cs =
                new SharedTopicSubscription.ConsumerState(createContext("c1"), info);

        assertFalse(cs.isFull());

        cs.dispatched = 5;
        assertTrue(cs.isFull());
    }

    @Test
    public void testConsumerStateNeverFullWithZeroPrefetch() {
        ConsumerInfo info = createConsumerInfo("c1", 1, 1, 0);
        SharedTopicSubscription.ConsumerState cs =
                new SharedTopicSubscription.ConsumerState(createContext("c1"), info);

        cs.dispatched = 100;
        assertFalse("Zero prefetch means unlimited", cs.isFull());
    }

    @Test
    public void testConsumerStateCountBeforeFull() {
        ConsumerInfo info = createConsumerInfo("c1", 1, 1, 10);
        SharedTopicSubscription.ConsumerState cs =
                new SharedTopicSubscription.ConsumerState(createContext("c1"), info);

        assertEquals(10, cs.countBeforeFull());

        cs.dispatched = 7;
        assertEquals(3, cs.countBeforeFull());

        cs.dispatched = 10;
        assertEquals(0, cs.countBeforeFull());
    }

    @Test
    public void testSelectConsumerSingleConsumer() throws Exception {
        ConnectionContext ctx = createContext("conn-1");
        ConsumerInfo info = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx, info);

        SharedTopicSubscription.ConsumerState selected = sub.selectConsumer();
        assertNotNull(selected);
        assertEquals(info.getConsumerId(), selected.info.getConsumerId());
    }

    @Test
    public void testSelectConsumerReturnsNullWhenEmpty() throws Exception {
        ConnectionContext ctx = createContext("conn-1");
        ConsumerInfo info = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx, info);
        sub.removeConsumer(info.getConsumerId());

        assertNull(sub.selectConsumer());
    }

    @Test
    public void testSelectConsumerRoundRobin() throws Exception {
        ConnectionContext ctx1 = createContext("conn-1");
        ConsumerInfo info1 = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx1, info1);

        ConnectionContext ctx2 = createContext("conn-2");
        ConsumerInfo info2 = createConsumerInfo("conn-2", 1, 2, 10);
        sub.addConsumer(ctx2, info2);

        SharedTopicSubscription.ConsumerState first = sub.selectConsumer();
        SharedTopicSubscription.ConsumerState second = sub.selectConsumer();
        SharedTopicSubscription.ConsumerState third = sub.selectConsumer();

        assertNotEquals("Should round-robin between consumers",
                first.info.getConsumerId(), second.info.getConsumerId());
        assertEquals("Should wrap around to first consumer",
                first.info.getConsumerId(), third.info.getConsumerId());
    }

    @Test
    public void testSelectConsumerSkipsFull() throws Exception {
        ConnectionContext ctx1 = createContext("conn-1");
        ConsumerInfo info1 = createConsumerInfo("conn-1", 1, 1, 2);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx1, info1);

        ConnectionContext ctx2 = createContext("conn-2");
        ConsumerInfo info2 = createConsumerInfo("conn-2", 1, 2, 10);
        sub.addConsumer(ctx2, info2);

        // Fill consumer 1
        SharedTopicSubscription.ConsumerState s1 = sub.selectConsumer();
        s1.dispatched = 2;

        // Next select should skip full consumer 1 and return consumer 2
        SharedTopicSubscription.ConsumerState selected = sub.selectConsumer();
        assertNotNull(selected);
        assertEquals(info2.getConsumerId(), selected.info.getConsumerId());
    }

    @Test
    public void testSelectConsumerReturnsNullWhenAllFull() throws Exception {
        ConnectionContext ctx1 = createContext("conn-1");
        ConsumerInfo info1 = createConsumerInfo("conn-1", 1, 1, 1);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx1, info1);

        ConnectionContext ctx2 = createContext("conn-2");
        ConsumerInfo info2 = createConsumerInfo("conn-2", 1, 2, 1);
        sub.addConsumer(ctx2, info2);

        // Fill both consumers
        SharedTopicSubscription.ConsumerState s1 = sub.selectConsumer();
        s1.dispatched = 1;
        SharedTopicSubscription.ConsumerState s2 = sub.selectConsumer();
        s2.dispatched = 1;

        assertNull("All full: should return null", sub.selectConsumer());
    }

    @Test
    public void testRemoveNonExistentConsumerIsNoOp() throws Exception {
        ConnectionContext ctx = createContext("conn-1");
        ConsumerInfo info = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx, info);

        ConsumerId bogus = new ConsumerId(new SessionId(new ConnectionId("fake"), 1), 99);
        sub.removeConsumer(bogus);

        assertEquals("Count unchanged", 1, sub.getConsumerCount());
    }

    @Test
    public void testDestroyClears() throws Exception {
        ConnectionContext ctx = createContext("conn-1");
        ConsumerInfo info = createConsumerInfo("conn-1", 1, 1, 10);
        SharedTopicSubscription sub = new SharedTopicSubscription(broker, usage, ctx, info);

        sub.addConsumer(createContext("conn-2"), createConsumerInfo("conn-2", 1, 2, 10));
        assertEquals(2, sub.getConsumerCount());

        sub.destroy();
        assertEquals(0, sub.getConsumerCount());
    }
}
