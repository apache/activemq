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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

import javax.jms.Connection;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;

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
        for (int i = 0; i < 100; i++) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId" + i);
            session.close();
        }

        Thread.sleep(2 * 1000);

        if (restart) {
            stopBroker();
            startBroker(false);
        }

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName[] subscriptions = broker.getAdminView().getDurableTopicSubscribers();
        ObjectName[] inactive = broker.getAdminView().getInactiveDurableTopicSubscribers();

        for (ObjectName subscription: subscriptions) {
            mbs.invoke(subscription, "destroy", null, null);
        }
        for (ObjectName subscription: inactive) {
            mbs.invoke(subscription, "destroy", null, null);
        }

        Thread.sleep(2 * 1000);

        subscriptions = broker.getAdminView().getDurableTopicSubscribers();
        assertEquals(0, subscriptions.length);

        subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
        assertEquals(0, subscriptions.length);
    }

    public void testInactiveSubscriptions() throws Exception {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId");

            ObjectName[] subscriptions = broker.getAdminView().getDurableTopicSubscribers();
            assertEquals(1, subscriptions.length);

            subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
            assertEquals(0, subscriptions.length);

            session.close();

            Thread.sleep(1000);

            subscriptions = broker.getAdminView().getDurableTopicSubscribers();
            assertEquals(0, subscriptions.length);

            subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
            assertEquals(1, subscriptions.length);

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId");

            Thread.sleep(1000);

            subscriptions = broker.getAdminView().getDurableTopicSubscribers();
            assertEquals(1, subscriptions.length);

            subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
            assertEquals(0, subscriptions.length);

            session.close();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Thread.sleep(1000);

            subscriptions = broker.getAdminView().getDurableTopicSubscribers();
            assertEquals(0, subscriptions.length);

            subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
            assertEquals(1, subscriptions.length);

            session.unsubscribe("SubsId");

            Thread.sleep(1000);

            subscriptions = broker.getAdminView().getDurableTopicSubscribers();
            assertEquals(0, subscriptions.length);

            subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
            assertEquals(0, subscriptions.length);

            session.close();

    }

    public void doConnectionUnsubscribe(boolean restart) throws Exception {
        for (int i = 0; i < 100; i++) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId" + i);
            session.close();
        }

        Thread.sleep(2 * 1000);

        if (restart) {
            stopBroker();
            startBroker(false);
        }

        ObjectName[] subscriptions = broker.getAdminView().getDurableTopicSubscribers();
        assertEquals(0, subscriptions.length);

        subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
        assertEquals(100, subscriptions.length);

        for (int i = 0; i < 100; i++) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.unsubscribe("SubsId" + i);
            session.close();
        }

        Thread.sleep(2 * 1000);

        subscriptions = broker.getAdminView().getDurableTopicSubscribers();
        assertEquals(0, subscriptions.length);

        subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
        assertEquals(0, subscriptions.length);
    }

    public void doDirectUnsubscribe(boolean restart) throws Exception {
        for (int i = 0; i < 100; i++) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId" + i);
            session.close();
        }

        Thread.sleep(2 * 1000);

        if (restart) {
            stopBroker();
            startBroker(false);
        }

        for (int i = 0; i < 100; i++) {
            RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
            info.setClientId(getName());
            info.setSubscriptionName("SubsId" + i);
            ConnectionContext context = new ConnectionContext();
            context.setBroker(broker.getRegionBroker());
            context.setClientId(getName());
            broker.getRegionBroker().removeSubscription(context, info);
        }

        Thread.sleep(2 * 1000);

        ObjectName[] subscriptions = broker.getAdminView().getDurableTopicSubscribers();
        assertEquals(0, subscriptions.length);

        subscriptions = broker.getAdminView().getInactiveDurableTopicSubscribers();
        assertEquals(0, subscriptions.length);
    }

    private void startBroker(boolean deleteMessages) throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://localhost)");
        broker.setUseJmx(true);
        broker.setBrokerName(getName());

        broker.setPersistent(true);
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(new File("activemq-data/" + getName()));
        broker.setPersistenceAdapter(persistenceAdapter);
        if (deleteMessages) {
            broker.setDeleteAllMessagesOnStartup(true);
        }

        broker.start();

        connection = createConnection();
    }

    private void stopBroker() throws Exception {
        if (connection != null)
            connection.close();
        connection = null;

        if (broker != null)
            broker.stop();
        broker = null;
    }

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
