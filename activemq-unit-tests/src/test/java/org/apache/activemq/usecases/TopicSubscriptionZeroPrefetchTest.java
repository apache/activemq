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

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TopicSubscriptionZeroPrefetchTest {

    private static final String TOPIC_NAME = "slow.consumer";
    private Connection connection;
    private Session session;
    private ActiveMQTopic destination;
    private MessageProducer producer;
    private MessageConsumer consumer;
    private BrokerService brokerService;

    @Before
    public void setUp() throws Exception {

        brokerService = createBroker();

        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");

        activeMQConnectionFactory.setWatchTopicAdvisories(true);
        connection = activeMQConnectionFactory.createConnection();
        connection.setClientID("ClientID-1");
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = new ActiveMQTopic(TOPIC_NAME);
        producer = session.createProducer(destination);

        connection.start();
    }

    /*
     * test non durable topic subscription with prefetch set to zero
     */
    @Test(timeout=60000)
    public void testTopicConsumerPrefetchZero() throws Exception {

        ActiveMQTopic consumerDestination = new ActiveMQTopic(TOPIC_NAME + "?consumer.retroactive=true&consumer.prefetchSize=0");
        consumer = session.createConsumer(consumerDestination);

        // publish messages
        Message txtMessage = session.createTextMessage("M");
        producer.send(txtMessage);

        Message consumedMessage = consumer.receiveNoWait();

        Assert.assertNotNull("should have received a message the published message", consumedMessage);
    }

    /*
     * test durable topic subscription with prefetch zero
     */
    @Test(timeout=60000)
    public void testDurableTopicConsumerPrefetchZero() throws Exception {

        ActiveMQTopic consumerDestination = new ActiveMQTopic(TOPIC_NAME + "?consumer.prefetchSize=0");
        consumer = session.createDurableSubscriber(consumerDestination, "mysub1");

        // publish messages
        Message txtMessage = session.createTextMessage("M");
        producer.send(txtMessage);

        Message consumedMessage = consumer.receive(100);

        Assert.assertNotNull("should have received a message the published message", consumedMessage);
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
        producer.close();
        session.close();
        connection.close();
        brokerService.stop();
    }

    // helper method to create a broker with slow consumer advisory turned on
    private BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setUseJmx(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("vm://localhost");
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }
}
