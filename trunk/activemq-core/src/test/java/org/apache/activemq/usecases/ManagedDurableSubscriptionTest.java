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

import javax.jms.Connection;
import javax.jms.Session;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadaptor.KahaPersistenceAdapter;

public class ManagedDurableSubscriptionTest extends org.apache.activemq.TestSupport {

    BrokerService broker = null;
    Connection connection = null;
    ActiveMQTopic topic;

    public void testJMXSubscriptions() throws Exception {
        // create durable subscription
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId");
        session.close();

        // restart the broker
        stopBroker();
        startBroker();

        ObjectName inactiveSubscriptionObjectName = broker.getAdminView().getInactiveDurableTopicSubscribers()[0];

        Object inactive = broker.getManagementContext().getAttribute(inactiveSubscriptionObjectName, "Active");
        assertTrue("Subscription is active.", Boolean.FALSE.equals(inactive));

        // activate
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId");

        ObjectName activeSubscriptionObjectName = broker.getAdminView().getDurableTopicSubscribers()[0];

        Object active = broker.getManagementContext().getAttribute(activeSubscriptionObjectName, "Active");
        assertTrue("Subscription is INactive.", Boolean.TRUE.equals(active));

        // deactivate
        connection.close();
        connection = null;

        inactive = broker.getManagementContext().getAttribute(inactiveSubscriptionObjectName, "Active");
        assertTrue("Subscription is active.", Boolean.FALSE.equals(inactive));

    }

    private void startBroker() throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://localhost)");
        broker.setKeepDurableSubsActive(false);
        broker.setPersistent(true);
        KahaPersistenceAdapter persistenceAdapter = new KahaPersistenceAdapter();
        persistenceAdapter.setDirectory(new File("activemq-data/" + getName()));
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        broker.setBrokerName(getName());
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
        startBroker();
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
