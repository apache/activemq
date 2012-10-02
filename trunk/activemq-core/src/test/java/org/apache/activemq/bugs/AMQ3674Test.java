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

package org.apache.activemq.bugs;

import static org.junit.Assert.*;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ3674Test {

    private static Logger LOG = LoggerFactory.getLogger(AMQ3674Test.class);

    private final static int deliveryMode = DeliveryMode.NON_PERSISTENT;
    private final static ActiveMQTopic destination = new ActiveMQTopic("XYZ");

    private ActiveMQConnectionFactory factory;
    private BrokerService broker;

    @Test
    public void removeSubscription() throws Exception {

        final Connection producerConnection = factory.createConnection();
        producerConnection.start();
        final Connection consumerConnection = factory.createConnection();

        consumerConnection.setClientID("subscriber1");
        Session consumerMQSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        TopicSubscriber activeConsumer = (TopicSubscriber) consumerMQSession.createDurableSubscriber(destination, "myTopic");
        consumerConnection.start();

        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        final BrokerView brokerView = broker.getAdminView();

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);

        LOG.info("Current Durable Topic Subscriptions: " + brokerView.getDurableTopicSubscribers().length);

        try {
            brokerView.destroyDurableSubscriber("subscriber1", "myTopic");
            fail("Expected Exception for Durable consumer is in use");
        } catch(Exception e) {
            LOG.info("Recieved expected exception: " + e.getMessage());
        }

        LOG.info("Current Durable Topic Subscriptions: " + brokerView.getDurableTopicSubscribers().length);

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);

        activeConsumer.close();
        consumerConnection.stop();

        assertTrue("The subscription should be in the inactive state.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        try {
            brokerView.destroyDurableSubscriber("subscriber1", "myTopic");
        } finally {
            producer.close();
            producerConnection.close();
        }
    }

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        TransportConnector connector = broker.addConnector("tcp://localhost:0");
        broker.start();

        factory = new ActiveMQConnectionFactory(connector.getPublishableConnectString());
        factory.setAlwaysSyncSend(true);
        factory.setDispatchAsync(false);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }
}
