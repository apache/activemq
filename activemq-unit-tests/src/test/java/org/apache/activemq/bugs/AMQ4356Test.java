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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ4356Test {

    private static BrokerService brokerService;
    private static String BROKER_ADDRESS = "tcp://localhost:0";

    private String connectionUri;
    private ActiveMQConnectionFactory cf;
    private final String CLIENT_ID = "AMQ4356Test";
    private final String SUBSCRIPTION_NAME = "AMQ4356Test";

    private void createBroker(boolean deleteOnStart) throws Exception {
        brokerService = new BrokerService();
        brokerService.setUseJmx(true);
        brokerService.setDeleteAllMessagesOnStartup(deleteOnStart);
        connectionUri = brokerService.addConnector(BROKER_ADDRESS).getPublishableConnectString();
        brokerService.start();
        brokerService.waitUntilStarted();

    }

    private void startBroker() throws Exception {
        createBroker(true);
    }

    private void restartBroker() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
        createBroker(false);
    }

    @Before
    public void setUp() throws Exception {
        startBroker();
        cf = new ActiveMQConnectionFactory(connectionUri);
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test
    public void testVirtualTopicUnsubDurable() throws Exception {
        Connection connection = cf.createConnection();
        connection.setClientID(CLIENT_ID);
        connection.start();

        // create consumer 'cluster'
        ActiveMQQueue queue1 = new ActiveMQQueue(getVirtualTopicConsumerName());
        ActiveMQQueue queue2 = new ActiveMQQueue(getVirtualTopicConsumerName());

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer c1 = session.createConsumer(queue1);
        c1.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
            }
        });
        MessageConsumer c2 = session.createConsumer(queue2);
        c2.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
            }
        });

        ActiveMQTopic topic = new ActiveMQTopic(getVirtualTopicName());
        MessageConsumer c3 = session.createDurableSubscriber(topic, SUBSCRIPTION_NAME);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        c3.close();

        // create topic producer
        MessageProducer producer = session.createProducer(topic);
        assertNotNull(producer);

        int total = 10;
        for (int i = 0; i < total; i++) {
            producer.send(session.createTextMessage("message: " + i));
        }

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        session.unsubscribe(SUBSCRIPTION_NAME);
        connection.close();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        restartBroker();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);
    }

    protected String getVirtualTopicName() {
        return "VirtualTopic.TEST";
    }

    protected String getVirtualTopicConsumerName() {
        return "Consumer.A.VirtualTopic.TEST";
    }
}
