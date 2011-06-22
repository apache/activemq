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

import java.lang.management.ManagementFactory;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQTopic;

public class DurableUnsubscribeTest extends org.apache.activemq.TestSupport {

    private BrokerService broker;
    private Connection connection;
    private ActiveMQTopic topic;

    public void testUnsubscribe() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId");
        session.close();

        Destination d = broker.getDestination(topic);
        assertEquals("Subscription is missing.", 1, d.getConsumers().size());


        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(topic);
        for (int i = 0; i < 1000; i++) {
            producer.send(session.createTextMessage("text"));
        }

        Thread.sleep(1000);

        session.unsubscribe("SubsId");
        session.close();

        assertEquals("Subscription exists.", 0, d.getConsumers().size());
    }

    public void testDestroy() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId2");
        session.close();

        connection.close();
        connection = null;
        Thread.sleep(1000);

        Destination d = broker.getDestination(topic);
        assertEquals("Subscription is missing.", 1, d.getConsumers().size());

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName[] subNames = broker.getAdminView().getInactiveDurableTopicSubscribers();
        mbs.invoke(subNames[0], "destroy", new Object[0], new String[0]);

        assertEquals("Subscription exists.", 0, d.getConsumers().size());
    }

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

    private void createBroker() throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://localhost)");
        //broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setBrokerName(getName());
        broker.start();

        connection = createConnection();
    }

    private void destroyBroker() throws Exception {
        if (connection != null)
            connection.close();
        if (broker != null)
            broker.stop();
    }
}
