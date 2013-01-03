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
package org.apache.activemq;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.ObjectName;

import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RemoveDestinationTest {

    private static final String VM_BROKER_URL = "vm://localhost?create=false";
    private static final String BROKER_URL = "broker:vm://localhost?broker.persistent=false&broker.useJmx=true";

    BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = BrokerFactory.createBroker(new URI(BROKER_URL));
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        broker = null;
    }

    private Connection createConnection(final boolean start) throws JMSException {
        ConnectionFactory cf = new ActiveMQConnectionFactory(VM_BROKER_URL);
        Connection conn = cf.createConnection();
        if (start) {
            conn.start();
        }
        return conn;
    }

    @Test
    public void testRemoveDestinationWithoutSubscriber() throws Exception {

        ActiveMQConnection amqConnection = (ActiveMQConnection) createConnection(true);
        DestinationSource destinationSource = amqConnection.getDestinationSource();
        Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("TEST.FOO");
        MessageProducer producer = session.createProducer(topic);
        MessageConsumer consumer = session.createConsumer(topic);

        TextMessage msg = session.createTextMessage("Hellow World");
        producer.send(msg);
        assertNotNull(consumer.receive(5000));
        Thread.sleep(1000);

        ActiveMQTopic amqTopic = (ActiveMQTopic) topic;
        assertTrue(destinationSource.getTopics().contains(amqTopic));

        consumer.close();
        producer.close();
        session.close();

        Thread.sleep(3000);
        amqConnection.destroyDestination((ActiveMQDestination) topic);
        Thread.sleep(3000);
        assertFalse(destinationSource.getTopics().contains(amqTopic));
    }

    @Test
    public void testRemoveDestinationWithSubscriber() throws Exception {
        ActiveMQConnection amqConnection = (ActiveMQConnection) createConnection(true);
        DestinationSource destinationSource = amqConnection.getDestinationSource();

        Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("TEST.FOO");
        MessageProducer producer = session.createProducer(topic);
        MessageConsumer consumer = session.createConsumer(topic);

        TextMessage msg = session.createTextMessage("Hellow World");
        producer.send(msg);
        assertNotNull(consumer.receive(5000));
        Thread.sleep(1000);

        ActiveMQTopic amqTopic = (ActiveMQTopic) topic;

        assertTrue(destinationPresentInAdminView(broker, amqTopic));
        assertTrue(destinationSource.getTopics().contains(amqTopic));

        // This line generates a broker error since the consumer is still active.
        try {
            amqConnection.destroyDestination((ActiveMQDestination) topic);
            fail("expect exception on destroy if comsumer present");
        } catch (JMSException expected) {
            assertTrue(expected.getMessage().indexOf(amqTopic.getTopicName()) != -1);
        }

        Thread.sleep(3000);

        assertTrue(destinationSource.getTopics().contains(amqTopic));
        assertTrue(destinationPresentInAdminView(broker, amqTopic));

        consumer.close();
        producer.close();
        session.close();

        Thread.sleep(3000);

        // The destination will not be removed with this call, but if you remove
        // the call above that generates the error it will.
        amqConnection.destroyDestination(amqTopic);
        Thread.sleep(3000);
        assertFalse(destinationSource.getTopics().contains(amqTopic));
        assertFalse(destinationPresentInAdminView(broker, amqTopic));
    }

    private boolean destinationPresentInAdminView(BrokerService broker2, ActiveMQTopic amqTopic) throws Exception {
        boolean found = false;
        for (ObjectName name : broker.getAdminView().getTopics()) {

            DestinationViewMBean proxy = (DestinationViewMBean)
                broker.getManagementContext().newProxyInstance(name, DestinationViewMBean.class, true);

            if (proxy.getName().equals(amqTopic.getPhysicalName())) {
                found = true;
                break;
            }
        }
        return found;
    }
}
