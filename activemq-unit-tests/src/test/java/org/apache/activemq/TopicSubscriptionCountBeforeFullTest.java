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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Test;

/**
 * This test shows that the countBeforeFull statistic that is part of a Subscription is correct
 * for TopicSubscriptions.
 */
public class TopicSubscriptionCountBeforeFullTest extends TestSupport {

    protected BrokerService brokerService;
    private Connection connection;
    private String brokerUrlString;
    private Session session;
    private Topic topic;
    private Destination amqDestination;
    private int prefetch = 10;

    protected void setUp() throws Exception {
        super.setUp();
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        TransportConnector tcp = brokerService
                .addConnector("tcp://localhost:0");
        brokerService.start();
        brokerUrlString = tcp.getPublishableConnectString();
        connection = createConnection();
        connection.start();
        session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic("test");
        session.createConsumer(topic);
        amqDestination = TestSupport.getDestination(brokerService,new ActiveMQTopic("test"));
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory()
            throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrlString);
        ActiveMQPrefetchPolicy prefecthPolicy = new ActiveMQPrefetchPolicy();
        prefecthPolicy.setTopicPrefetch(prefetch);
        factory.setPrefetchPolicy(prefecthPolicy);
        return factory;
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        brokerService.stop();
        super.tearDown();
    }

    /**
     * Tests that countBeforeFull is 0 if prefetch is filled
     *
     * @throws javax.jms.JMSException
     */
    @Test
    public void testCountBeforeFullPrefetchFilled() throws JMSException {
        sendMessages(10);
        assertEquals(getSubscription().countBeforeFull(), 0);
    }

    /**
     * Tests that countBeforeFull is a positive number when no messages have been sent
     * and prefetch is greater than 0
     *
     * @throws javax.jms.JMSException
     */
    @Test
    public void testCountBeforeFullNotNull() throws JMSException {
        assertTrue(getSubscription().countBeforeFull() == prefetch);
    }

    protected void sendMessages(int count) throws JMSException {
        MessageProducer producer = session.createProducer(topic);
        for (int i = 0; i < count; i++) {
            producer.send(session.createTextMessage("testMessage"));
        }
    }

    protected Subscription getSubscription() {
        return amqDestination.getConsumers().get(0);
    }

}
