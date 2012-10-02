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
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ3675Test {

    private static Logger LOG = LoggerFactory.getLogger(AMQ3675Test.class);

    private final static int deliveryMode = DeliveryMode.NON_PERSISTENT;
    private final static ActiveMQTopic destination = new ActiveMQTopic("XYZ");

    private ActiveMQConnectionFactory factory;
    private BrokerService broker;

    public TopicViewMBean getTopicView() throws Exception {
        ObjectName destinationName = broker.getAdminView().getTopics()[0];
        TopicViewMBean topicView = (TopicViewMBean) broker.getManagementContext().newProxyInstance(destinationName, TopicViewMBean.class, true);
        return topicView;
    }

    @Test
    public void countConsumers() throws Exception {

        final Connection producerConnection = factory.createConnection();
        producerConnection.start();
        final Connection consumerConnection = factory.createConnection();

        consumerConnection.setClientID("subscriber1");
        Session consumerMQSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        TopicSubscriber consumer = consumerMQSession.createDurableSubscriber(destination, "myTopic");
        consumerConnection.start();

        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        final BrokerView brokerView = broker.getAdminView();
        final TopicViewMBean topicView = getTopicView();

        assertTrue("Should have one consumer on topic: ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return topicView.getConsumerCount() == 1;
            }
        }));

        consumer.close();

        assertTrue("Durable consumer should now be inactive.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        try {
            brokerView.removeTopic(destination.getTopicName());
        } catch (Exception e1) {
            fail("Unable to remove destination:" + destination.getPhysicalName());
        }

        assertTrue("Should have no topics on the broker", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getTopics().length == 0;
            }
        }));

        try {
            brokerView.destroyDurableSubscriber("subscriber1", "myTopic");
        } catch(Exception e) {
            fail("Exception not expected when attempting to delete Durable consumer.");
        }

        assertTrue("Should be no durable consumers active or inactive.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getInactiveDurableTopicSubscribers().length == 0 &&
                       brokerView.getDurableTopicSubscribers().length == 0;
            }
        }));

        consumer = consumerMQSession.createDurableSubscriber(destination, "myTopic");

        consumer.close();

        assertTrue("Should be one consumer on the Topic.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Number of inactive consumers: " + brokerView.getInactiveDurableTopicSubscribers().length);
                return brokerView.getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        final TopicViewMBean recreatedTopicView = getTopicView();

        assertTrue("Should have one consumer on topic: ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return recreatedTopicView.getConsumerCount() == 1;
            }
        }));
    }

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
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
