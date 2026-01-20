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

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.TopicSubscriber;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.IOHelper;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DurableSubscriptionPartialAckTest {

    private BrokerService createBroker() throws Exception {
        BrokerService broker = BrokerFactory.createBroker("broker:(vm://" + getClass().getName() + ")");
        broker.setBrokerName("broker");
        broker.setAdvisorySupport(false);
        File dir = broker.getBrokerDataDirectory();
        if (dir != null) {
            IOHelper.deleteChildren(dir);
        }
        return broker;
    }

    @Test
    public void test() throws Exception {
        BrokerService broker = createBroker();
        broker.start();
        broker.waitUntilStarted();

        ActiveMQTopic topic = new ActiveMQTopic("TOPIC.TEST");
        String subName1 = "SUB1";
        String subName2 = "SUB2";
        int numberOfMessages = 10_000;

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        try (Connection connection = connectionFactory.createConnection()) {
            connection.setClientID("CLIENT_ID");
            connection.start();

            Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);

            TopicSubscriber subscriber = session.createDurableSubscriber(topic, subName1);
            session.createDurableSubscriber(topic, subName2).close();

            MessageProducer producer = session.createProducer(topic);

            for (int i = 0; i < numberOfMessages; i++) {
                ActiveMQTextMessage message = new ActiveMQTextMessage();
                message.setText(Integer.toString(i));
                producer.send(message);
            }

            for (int i = 0; i < numberOfMessages; i++) {
                Message receivedMessage = subscriber.receive(1000);
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(Integer.toString(i), ((TextMessage) receivedMessage).getText());
                if (i % 2 == 0) {
                    receivedMessage.acknowledge();
                }
            }
        }
        broker.stop();
        broker.waitUntilStopped();
        broker.start();
        broker.waitUntilStarted();
        try (Connection connection = connectionFactory.createConnection()) {
            connection.setClientID("CLIENT_ID");
            connection.start();

            Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, subName1);

            for (int i = 1; i < numberOfMessages; i += 2) {
                Message receivedMessage = subscriber.receive(10000);
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(Integer.toString(i), ((TextMessage) receivedMessage).getText());

                receivedMessage.acknowledge();
            }
        }
    }

}
