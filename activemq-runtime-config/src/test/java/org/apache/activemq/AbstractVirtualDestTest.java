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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.Map;

import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 *
 *
 */
public abstract class AbstractVirtualDestTest extends RuntimeConfigTestSupport {

    protected void forceAddDestination(String dest) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createConsumer(session.createQueue("Consumer.A." + dest));
        connection.close();
    }

    protected void exerciseVirtualTopic(String topic) throws Exception {
        exerciseVirtualTopic("Consumer.A.", topic);
    }

    protected void exerciseVirtualTopic(String prefix, String topic) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(session.createQueue(prefix + topic));
        LOG.info("new consumer for: " + consumer.getDestination());
        MessageProducer producer = session.createProducer(session.createTopic(topic));
        final String body = "To vt:" + topic;
        Message message = sendAndReceiveMessage(session, consumer, producer, body);
        assertNotNull("got message", message);
        assertEquals("got expected message", body, ((TextMessage) message).getText());
        connection.close();
    }

    protected void exerciseCompositeQueue(String dest, String consumerQ) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(session.createQueue(consumerQ));
        LOG.info("new consumer for: " + consumer.getDestination());
        MessageProducer producer = session.createProducer(session.createQueue(dest));
        final String body = "To cq:" + dest;
        Message message = sendAndReceiveMessage(session, consumer, producer, body);
        assertNotNull("got message", message);
        assertEquals("got expected message", body, ((TextMessage) message).getText());
        connection.close();
    }

    protected void exerciseFilteredCompositeQueue(String dest, String consumerDestination, String acceptedHeaderValue) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(session.createQueue(consumerDestination));
        LOG.info("new consumer for: " + consumer.getDestination());
        MessageProducer producer = session.createProducer(session.createQueue(dest));

        // positive test
        String body = "To filtered cq:" + dest;

        Message message = sendAndReceiveMessage(session, consumer, producer, body, Collections.singletonMap("odd", acceptedHeaderValue));
        assertNotNull("The message did not reach the destination even though it should pass through the filter.", message);
        assertEquals("Did not get expected message", body, ((TextMessage) message).getText());

        // negative test
        message = sendAndReceiveMessage(session, consumer, producer, "Not to filtered cq:" + dest, Collections.singletonMap("odd", "somethingElse"));
        assertNull("The message reached the destination, but it should have been removed by the filter.", message);

        connection.close();
    }

    protected Message sendAndReceiveMessage(Session session,
            ActiveMQMessageConsumer consumer, MessageProducer producer,
            final String messageBody) throws Exception {
        return sendAndReceiveMessage(session, consumer, producer, messageBody,
                null);
    }

    protected Message sendAndReceiveMessage(Session session,
            ActiveMQMessageConsumer consumer, MessageProducer producer,
            final String messageBody, Map<String, String> propertiesMap)
            throws Exception {
        TextMessage messageToSend = session.createTextMessage(messageBody);
        if (propertiesMap != null) {
            for (String headerKey : propertiesMap.keySet()) {
                messageToSend.setStringProperty(headerKey,
                        propertiesMap.get(headerKey));
            }
        }
        producer.send(messageToSend);
        LOG.info("sent to: " + producer.getDestination());

        Message message = null;
        for (int i = 0; i < 10 && message == null; i++) {
            message = consumer.receive(1000);
        }
        return message;
    }
}
