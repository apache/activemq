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
package org.apache.activemq.usecases;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.*;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

public class DurableSubscriptionUnsubscribeTest extends TestSupport {

    BrokerService broker = null;
    Connection connection = null;
    ActiveMQTopic topic;

    public void testJMXSubscriptionUnsubscribe() throws Exception {
        doJMXUnsubscribe(false);
    }

    public void testJMXSubscriptionUnsubscribeWithRestart() throws Exception {
        doJMXUnsubscribe(true);
    }

    public void testConnectionSubscriptionUnsubscribe() throws Exception {
        doConnectionUnsubscribe(false);
    }

    public void testConnectionSubscriptionUnsubscribeWithRestart() throws Exception {
        doConnectionUnsubscribe(true);
    }

    public void testDirectSubscriptionUnsubscribe() throws Exception {
        doDirectUnsubscribe(false);
    }

    public void testDirectubscriptionUnsubscribeWithRestart() throws Exception {
        doDirectUnsubscribe(true);
    }

    public void doJMXUnsubscribe(boolean restart) throws Exception {
        createSubscriptions();
        createAdvisorySubscription();

        Thread.sleep(1000);
        assertCount(100, 0);

        if (restart) {
            restartBroker();
            createAdvisorySubscription();
            assertCount(100, 0);
        }

        ObjectName[] subs = broker.getAdminView().getInactiveDurableTopicSubscribers();

        for (int i = 0; i < subs.length; i++) {
            ObjectName subName = subs[i];
            DurableSubscriptionViewMBean sub = (DurableSubscriptionViewMBean)broker.getManagementContext().newProxyInstance(subName, DurableSubscriptionViewMBean.class, true);
            sub.destroy();

            if (i % 20 == 0) {
                Thread.sleep(1000);
                assertCount(100 - i - 1, 0);
            }
        }

        Thread.sleep(1000);
        assertCount(0, 0);

        if (restart) {
            restartBroker();
            createAdvisorySubscription();
            assertCount(0, 0);
        }
    }

    public void doConnectionUnsubscribe(boolean restart) throws Exception {
        createSubscriptions();
        createAdvisorySubscription();

        Thread.sleep(1000);
        assertCount(100, 0);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId1");

        Thread.sleep(1000);
        assertCount(100, 1);

        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2.createDurableSubscriber(topic, "SubsId2");

        Thread.sleep(1000);
        assertCount(100, 2);

        session.close();

        Thread.sleep(1000);
        assertCount(100, 1);

        session2.close();

        Thread.sleep(1000);
        assertCount(100, 0);

        if (restart) {
            restartBroker();
            createAdvisorySubscription();
            assertCount(100, 0);
        }

        for (int i = 0; i < 100; i++) {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.unsubscribe("SubsId" + i);
            session.close();

            if (i % 20 == 0) {
                Thread.sleep(1000);
                assertCount(100 - i - 1, 0);
            }
        }

        Thread.sleep(1000);
        assertCount(0, 0);

        if (restart) {
            restartBroker();
            createAdvisorySubscription();
            assertCount(0, 0);
        }
    }

    public void doDirectUnsubscribe(boolean restart) throws Exception {
        createSubscriptions();
        createAdvisorySubscription();

        Thread.sleep(1000);
        assertCount(100, 0);

        if (restart) {
            restartBroker();
            createAdvisorySubscription();
            assertCount(100, 0);
        }

        for (int i = 0; i < 100; i++) {
            RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
            info.setClientId(getName());
            info.setSubscriptionName("SubsId" + i);
            ConnectionContext context = new ConnectionContext();
            context.setBroker(broker.getRegionBroker());
            context.setClientId(getName());
            broker.getBroker().removeSubscription(context, info);

            if (i % 20 == 0) {
                Thread.sleep(1000);
                assertCount(100 - i - 1, 0);
            }
        }

        assertCount(0, 0);

        if (restart) {
            restartBroker();
            createAdvisorySubscription();
            assertCount(0, 0);
        }
    }

    private void createSubscriptions() throws Exception {
        for (int i = 0; i < 100; i++) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId" + i);
            session.close();
        }
    }

    private final AtomicInteger advisories = new AtomicInteger(0);

    private void createAdvisorySubscription() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer advisoryConsumer = session.createConsumer(AdvisorySupport.getConsumerAdvisoryTopic(topic));
        advisoryConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (((ActiveMQMessage)message).getDataStructure() instanceof RemoveSubscriptionInfo) {
                    advisories.incrementAndGet();
                }
            }
        });
    }

    private void assertCount(int all, int active) throws Exception {
        int inactive = all - active;

        // broker check
        Destination destination = broker.getDestination(topic);
        List<Subscription> subs = destination.getConsumers();
        int cActive = 0, cInactive = 0;
        for (Subscription sub: subs) {
            if (sub instanceof DurableTopicSubscription) {
                DurableTopicSubscription durable = (DurableTopicSubscription) sub;
                if (durable.isActive())
                    cActive++;
                else
                    cInactive++;
            }
        }
        assertEquals(active, cActive);
        assertEquals(inactive, cInactive);

        // admin view
        ObjectName[] subscriptions = broker.getAdminView().getDurableTopicSubscribers();
        assertEquals(active, subscriptions.length);
        subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
        assertEquals(inactive, subscriptions.length);

        // check the strange false MBean
        if (all == 0)
            assertEquals(0, countMBean());

        // check if we got all advisories
        assertEquals(100, all + advisories.get());
    }

    private int countMBean() throws MalformedObjectNameException, InstanceNotFoundException {
        int count = 0;
        for (int i = 0; i < 100; i++) {
            String name = "org.apache.activemq:BrokerName=" + getName() + ",Type=Subscription,active=false,name=" + getName() + "_SubsId" + i;
            ObjectName sub = new ObjectName(name);
            try {
                broker.getManagementContext().getObjectInstance(sub);
                count++;
            }
            catch (InstanceNotFoundException ignore) {
                // this should happen
            }
        }
        return count;
    }

    private void startBroker(boolean deleteMessages) throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://" + getName() + ")");
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        broker.setBrokerName(getName());

        broker.setPersistent(true);
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(new File("activemq-data/" + getName()));
        broker.setPersistenceAdapter(persistenceAdapter);
        if (deleteMessages) {
            broker.setDeleteAllMessagesOnStartup(true);
        }


        broker.setKeepDurableSubsActive(true);

        broker.start();
        broker.waitUntilStarted();

        connection = createConnection();
    }

    private void stopBroker() throws Exception {
        if (connection != null)
            connection.close();
        connection = null;

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        broker = null;
    }

    private void restartBroker() throws Exception {
        stopBroker();
        startBroker(false);
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://" + getName() + "?waitForStart=5000&create=false");
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        topic = (ActiveMQTopic) createDestination();
        startBroker(true);
    }

    @Override
    protected void tearDown() throws Exception {
        stopBroker();
        super.tearDown();
    }

    @Override
    protected Connection createConnection() throws Exception {
        Connection rc = super.createConnection();
        rc.setClientID(getName());
        rc.start();
        return rc;
    }
}