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
package org.apache.activemq.network;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;


public abstract class DynamicNetworkTestSupport {

    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;
    protected ActiveMQTopic included;
    protected ActiveMQTopic excluded;
    protected String testTopicName = "include.test.bar";
    protected String excludeTopicName = "exclude.test.bar";
    protected String clientId = "clientId";
    protected String subName = "subId";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    protected void doTearDown() throws Exception {
        stopLocalBroker();
        stopRemoteBroker();
    }

    protected void stopLocalBroker() throws Exception {
        if (localConnection != null) {
            localConnection.close();
        }
        if (localBroker != null) {
            localBroker.stop();
            localBroker.waitUntilStopped();
        }
    }

    protected void stopRemoteBroker() throws Exception {
        if (remoteConnection != null) {
            remoteConnection.close();
        }
        if (remoteBroker != null) {
            remoteBroker.stop();
            remoteBroker.waitUntilStopped();
        }
    }


    protected void assertBridgeStarted() throws Exception {
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return localBroker.getNetworkConnectors().get(0).activeBridges().size() == 1;
            }
        }, 10000, 500));
    }

    protected RemoveSubscriptionInfo getRemoveSubscriptionInfo(final ConnectionContext context,
            final BrokerService brokerService) throws Exception {
        RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
        info.setClientId(clientId);
        info.setSubcriptionName(subName);
        context.setBroker(brokerService.getBroker());
        context.setClientId(clientId);
        return info;
    }

    protected void waitForConsumerCount(final DestinationStatistics destinationStatistics, final int count) throws Exception {
        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                //should only be 1 for the composite destination creation
                return count == destinationStatistics.getConsumers().getCount();
            }
        }));
    }

    protected void waitForDispatchFromLocalBroker(final DestinationStatistics destinationStatistics, final int count) throws Exception {
        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return count == destinationStatistics.getDequeues().getCount() &&
                       count == destinationStatistics.getDispatched().getCount() &&
                       count == destinationStatistics.getForwards().getCount();
            }
        }));
    }

    protected void assertLocalBrokerStatistics(final DestinationStatistics localStatistics, final int count) {
        assertEquals("local broker dest stat dispatched", count, localStatistics.getDispatched().getCount());
        assertEquals("local broker dest stat dequeues", count, localStatistics.getDequeues().getCount());
        assertEquals("local broker dest stat forwards", count, localStatistics.getForwards().getCount());
    }

    protected interface ConsumerCreator {
        MessageConsumer createConsumer() throws JMSException;
    }

    protected void assertNCDurableSubsCount(final BrokerService brokerService,
            final ActiveMQTopic dest, final int count) throws Exception {
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return count == getNCDurableSubs(brokerService, dest).size();
            }
        }, 10000, 500));
    }

    protected void assertConsumersCount(final BrokerService brokerService,
            final ActiveMQTopic dest, final int count) throws Exception {
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return count == getConsumers(brokerService, dest).size();
            }
        }, 10000, 500));
    }

    protected List<Subscription> getConsumers(final BrokerService brokerService,
            final ActiveMQTopic dest) throws Exception {
        Topic destination = (Topic) brokerService.getDestination(dest);
        return destination.getConsumers();
    }

    protected List<DurableTopicSubscription> getSubscriptions(final BrokerService brokerService,
            final ActiveMQTopic dest) throws Exception {
        List<DurableTopicSubscription> subs = new ArrayList<>();
        Topic destination = (Topic) brokerService.getDestination(dest);
        for (SubscriptionKey key : destination.getDurableTopicSubs().keySet()) {
            if (!key.getSubscriptionName().startsWith(DemandForwardingBridge.DURABLE_SUB_PREFIX)) {
                DurableTopicSubscription sub = destination.getDurableTopicSubs().get(key);
                if (sub != null) {
                    subs.add(sub);
                }
            }
        }
        return subs;
    }

    protected List<DurableTopicSubscription> getNCDurableSubs(final BrokerService brokerService,
            final ActiveMQTopic dest) throws Exception {
        List<DurableTopicSubscription> subs = new ArrayList<>();
        Destination d = brokerService.getDestination(dest);
        Topic destination = null;
        if (d instanceof DestinationFilter){
            destination = ((DestinationFilter) d).getAdaptor(Topic.class);
        } else {
            destination = (Topic) d;
        }


        for (SubscriptionKey key : destination.getDurableTopicSubs().keySet()) {
            if (key.getSubscriptionName().startsWith(DemandForwardingBridge.DURABLE_SUB_PREFIX)) {
                DurableTopicSubscription sub = destination.getDurableTopicSubs().get(key);
                if (sub != null) {
                    subs.add(sub);
                }
            }
        }

        return subs;
    }

    protected void removeSubscription(final BrokerService brokerService, final ActiveMQTopic topic,
            final String subName) throws Exception {
        final RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
        info.setClientId(clientId);
        info.setSubscriptionName(subName);

        final ConnectionContext context = new ConnectionContext();
        context.setBroker(brokerService.getBroker());
        context.setClientId(clientId);

        brokerService.getBroker().removeSubscription(context, info);
    }

    protected void assertSubscriptionsCount(final BrokerService brokerService,
            final ActiveMQTopic dest, final int count) throws Exception {
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return count == getSubscriptions(brokerService, dest).size();
            }
        }, 10000, 500));
    }

    protected void assertSubscriptionMapCounts(NetworkBridge networkBridge, final int count) {
        assertNotNull(networkBridge);
        DemandForwardingBridgeSupport bridge = (DemandForwardingBridgeSupport) networkBridge;
        assertEquals(count, bridge.subscriptionMapByLocalId.size());
        assertEquals(count, bridge.subscriptionMapByRemoteId.size());
    }

    protected DemandForwardingBridge findDuplexBridge(final TransportConnector connector) throws Exception {
        assertNotNull(connector);

        for (TransportConnection tc : connector.getConnections()) {
            if (tc.getConnectionId().startsWith("networkConnector_")) {
                final Field bridgeField = TransportConnection.class.getDeclaredField("duplexBridge");
                bridgeField.setAccessible(true);
                return (DemandForwardingBridge) bridgeField.get(tc);
            }
        }

        return null;
    }

}
