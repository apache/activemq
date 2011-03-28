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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.amq.AMQPersistenceAdapter;

public class DurableSubscriptionActivationTest extends org.apache.activemq.TestSupport {

    private BrokerService broker;
    private Connection connection;
    private ActiveMQTopic topic;

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://" + getName());
    }

    protected Connection createConnection() throws Exception {
        Connection rc = super.createConnection();
        rc.setClientID(getName());
        return rc;
    }

    protected void setUp() throws Exception {
        topic = (ActiveMQTopic) createDestination();
        createBroker();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    protected void restartBroker() throws Exception {
        destroyBroker();
        createBroker();
    }

    private void createBroker() throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://localhost)");
        broker.setKeepDurableSubsActive(true);
        broker.setPersistent(true);
        AMQPersistenceAdapter persistenceAdapter = new AMQPersistenceAdapter();
        persistenceAdapter.setDirectory(new File("activemq-data/" + getName()));
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.setBrokerName(getName());

        // only if we pre-create the destinations
        broker.setDestinations(new ActiveMQDestination[]{topic});

        broker.start();

        connection = createConnection();
    }

    private void destroyBroker() throws Exception {
        if (connection != null)
            connection.close();
        if (broker != null)
            broker.stop();
    }

    public void testActivateWithExistingTopic() throws Exception {
        // create durable subscription
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId");

        Destination d = broker.getDestination(topic);
        assertTrue("More than one consumer.", d.getConsumers().size() == 1);
        
        // restart the broker
        restartBroker();

        // activate
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId");

        assertTrue("More than one consumer.", d.getConsumers().size() == 1);

        // re-activate
        connection.close();
        connection = createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId");

        assertTrue("More than one consumer.", d.getConsumers().size() == 1);
    }
}
