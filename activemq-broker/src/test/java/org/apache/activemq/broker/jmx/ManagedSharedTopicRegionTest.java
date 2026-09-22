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
package org.apache.activemq.broker.jmx;

import static org.junit.Assert.*;

import javax.management.ObjectName;

import org.apache.activemq.broker.SharedTopicBrokerService;
import org.apache.activemq.broker.region.SharedDurableTopicSubscription;
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

public class ManagedSharedTopicRegionTest {

    private SharedTopicBrokerService brokerService;

    @Before
    public void setUp() throws Exception {
        brokerService = new SharedTopicBrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.setBrokerName("jmx-shared-test");
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

    @Test
    public void testSharedDurableSubscriptionRegisteredInJmx() throws Exception {
        ConnectionContext ctx = createContext("client-1");
        SharedConsumerInfo info = createSharedConsumerInfo("conn-1", 1, 1, "jmxDurSub", true);

        Subscription sub = brokerService.getBroker().addConsumer(ctx, info);
        assertNotNull(sub);
        assertTrue(sub instanceof SharedDurableTopicSubscription);
        assertNotNull("Subscription should have JMX ObjectName", sub.getObjectName());
    }

    @Test
    public void testSharedNonDurableSubscriptionRegisteredInJmx() throws Exception {
        ConnectionContext ctx = createContext("client-1");
        SharedConsumerInfo info = createSharedConsumerInfo("conn-1", 1, 1, "jmxNonDurSub", false);

        Subscription sub = brokerService.getBroker().addConsumer(ctx, info);
        assertNotNull(sub);
        assertNotNull("Subscription should have JMX ObjectName", sub.getObjectName());
    }

    @Test
    public void testJoiningConsumerReusesMBean() throws Exception {
        ConnectionContext ctx1 = createContext(null);
        SharedConsumerInfo info1 = createSharedConsumerInfo("conn-1", 1, 1, "jmxJoinSub", true);
        Subscription sub1 = brokerService.getBroker().addConsumer(ctx1, info1);
        ObjectName name1 = sub1.getObjectName();
        assertNotNull(name1);

        ConnectionContext ctx2 = createContext(null);
        SharedConsumerInfo info2 = createSharedConsumerInfo("conn-2", 1, 2, "jmxJoinSub", true);
        Subscription sub2 = brokerService.getBroker().addConsumer(ctx2, info2);

        assertSame("Second consumer should join same subscription", sub1, sub2);
        assertEquals("ObjectName should be the same MBean", name1, sub2.getObjectName());
    }

    @Test
    public void testNonSharedDurableRegisteredInJmx() throws Exception {
        ConnectionContext ctx = createContext("client-1");
        ConsumerInfo info = createDurableConsumerInfo("conn-1", 1, 1, "plainDurSub");

        Subscription sub = brokerService.getBroker().addConsumer(ctx, info);
        assertNotNull(sub);
        assertNotNull("Non-shared durable should also have JMX ObjectName",
                sub.getObjectName());
    }

    private ConnectionContext createContext(String clientId) throws Exception {
        ConnectionContext ctx = new ConnectionContext();
        ctx.setClientId(clientId);
        ctx.setBroker(brokerService.getBroker());
        return ctx;
    }

    private SharedConsumerInfo createSharedConsumerInfo(String connId, int session,
            int consumer, String subName, boolean durable) {
        ConnectionId cid = new ConnectionId(connId);
        SessionId sid = new SessionId(cid, session);
        ConsumerId consumerId = new ConsumerId(sid, consumer);
        SharedConsumerInfo info = new SharedConsumerInfo(consumerId);
        info.setDestination(new ActiveMQTopic("test.jmx.topic"));
        info.setPrefetchSize(10);
        info.setSubscriptionName(subName);
        info.setShared(true);
        info.setDurable(durable);
        return info;
    }

    private ConsumerInfo createDurableConsumerInfo(String connId, int session,
            int consumer, String subName) {
        ConnectionId cid = new ConnectionId(connId);
        SessionId sid = new SessionId(cid, session);
        ConsumerId consumerId = new ConsumerId(sid, consumer);
        ConsumerInfo info = new ConsumerInfo(consumerId);
        info.setDestination(new ActiveMQTopic("test.jmx.topic"));
        info.setPrefetchSize(10);
        info.setSubscriptionName(subName);
        return info;
    }
}
