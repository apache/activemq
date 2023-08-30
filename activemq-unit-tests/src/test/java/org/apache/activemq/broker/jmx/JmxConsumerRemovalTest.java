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

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import junit.framework.Test;
import junit.textui.TestRunner;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JmxConsumerRemovalTest extends EmbeddedBrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(JmxConsumerRemovalTest.class);

    protected MBeanServer mbeanServer;
    protected ManagedRegionBroker regionBroker;
    protected Session session;
    protected String clientID = "foo";

    protected Connection connection;
    protected boolean transacted;

    public static void main(String[] args) {
        TestRunner.run(JmxConsumerRemovalTest.class);
    }

    public static Test suite() {
        return suite(JmxConsumerRemovalTest.class);
    }

    public void testCompositeDestConsumerRemoval() throws Exception {
        Map<Subscription, ObjectName> subscriptionMap = getSubscriptionMap();
        int consumersToAdd = 1000;
        Set<MessageConsumer> consumers = new HashSet<>();

        final ActiveMQDestination dest = new ActiveMQQueue("test");
        dest.setCompositeDestinations(new ActiveMQDestination[]{new ActiveMQQueue("test1"),
            new ActiveMQQueue("test2"), new ActiveMQQueue("test3")});

        for (int i = 0; i < consumersToAdd; i++) {
            consumers.add(session.createConsumer(dest));
        }

        //Create a lot of consumers and make sure they are all tracked in ManagedRegionBroker map
        assertTrue(Wait.waitFor(() -> consumersToAdd == subscriptionMap.size(), 5000, 500));

        for (MessageConsumer consumer : consumers) {
            consumer.close();
        }

        //Make sure map removed all consumers after close
        assertTrue(Wait.waitFor(() -> 0 == subscriptionMap.size(), 5000, 500));
        assertTrue(Wait.waitFor(() -> 0 == regionBroker.getQueueSubscribers().length +
            regionBroker.getTopicSubscribers().length, 5000, 500));
    }

    public void testDurableConsumerRemoval() throws Exception {
        testDurableConsumerRemoval(new ActiveMQTopic("wildcard.topic.1"));
    }

    public void testDurableConsumerWildcardRemoval() throws Exception {
        testDurableConsumerRemoval(new ActiveMQTopic("wildcard.topic.>"));

    }
    public void testDurableConsumerRemoval(ActiveMQDestination dest) throws Exception {
        int consumersToAdd = 1000;
        Set<MessageConsumer> durables = new HashSet<>();

        //Create a lot of durables and then
        for (int i = 0; i < consumersToAdd; i++) {
            durables.add(session.createDurableSubscriber((Topic) dest, "sub" + i));
        }

        //Create a lot of consumers and make sure they are all tracked in ManagedRegionBroker map
        assertTrue(Wait.waitFor(() -> consumersToAdd == getSubscriptionMap().size(), 5000, 500));

        for (MessageConsumer consumer : durables) {
            consumer.close();
        }

        //Make sure map removed all consumers after close
        assertTrue(Wait.waitFor(() -> 0 == regionBroker.getDurableTopicSubscribers().length, 5000, 500));
        //Note we can't check the subscription map as the durables still exist, just offline
    }

    public void testQueueConsumerRemoval() throws Exception {
        testConsumerRemoval(new ActiveMQQueue("wildcard.queue.1"));
    }

    public void testQueueConsumerRemovalWildcard() throws Exception {
        testConsumerRemoval(new ActiveMQQueue("wildcard.queue.>"));
    }

    public void testTopicConsumerRemoval() throws Exception {
        testConsumerRemoval(new ActiveMQTopic("wildcard.topic.1"));
    }

    public void testTopicConsumerRemovalWildcard() throws Exception {
        testConsumerRemoval(new ActiveMQTopic("wildcard.topic.>"));
    }

    private void testConsumerRemoval(ActiveMQDestination dest) throws Exception {
        Map<Subscription, ObjectName> subscriptionMap = getSubscriptionMap();
        int consumersToAdd = 1000;
        Set<MessageConsumer> consumers = new HashSet<>();

        for (int i = 0; i < consumersToAdd; i++) {
            consumers.add(session.createConsumer(dest));
        }

        //Create a lot of consumers and make sure they are all tracked in ManagedRegionBroker map
        assertTrue(Wait.waitFor(() ->  consumersToAdd == subscriptionMap.size(), 5000, 500));

        for (MessageConsumer consumer : consumers) {
            consumer.close();
        }

        //Make sure map removed all consumers after close
        assertTrue(Wait.waitFor(() -> 0 == subscriptionMap.size(), 5000, 500));
        assertTrue(Wait.waitFor(() -> 0 == regionBroker.getQueueSubscribers().length &&
            0 == regionBroker.getTopicSubscribers().length, 5000, 500));
    }

    private Map<Subscription, ObjectName> getSubscriptionMap() throws Exception {
        ManagedRegionBroker regionBroker = (ManagedRegionBroker) broker.getBroker().getAdaptor(ManagedRegionBroker.class);
        Field subMapField = ManagedRegionBroker.class.getDeclaredField("subscriptionMap");
        subMapField.setAccessible(true);
        return (Map<Subscription, ObjectName>) subMapField.get(regionBroker);
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:0";
        useTopic = true;
        super.setUp();
        mbeanServer = broker.getManagementContext().getMBeanServer();
        regionBroker = (ManagedRegionBroker) broker.getBroker().getAdaptor(ManagedRegionBroker.class);
        ((ActiveMQConnectionFactory)connectionFactory).setWatchTopicAdvisories(false);
        connection = connectionFactory.createConnection();
        connection.setClientID(clientID);
        connection.start();
        session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.addConnector(bindAddress);
        answer.deleteAllMessages();
        return answer;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
    }

    protected void echo(String text) {
        LOG.info(text);
    }

    /**
     * Returns the name of the destination used in this test case
     */
    protected String getDestinationString() {
        return getClass().getName() + "." + getName(true);
    }
}
