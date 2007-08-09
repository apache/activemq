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
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.test.TestSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Paul Smith
 * @version $Revision: 1.1.1.1 $
 */
public class SubscribeClosePublishThenConsumeTest extends TestSupport {
    private static final Log LOG = LogFactory.getLog(SubscribeClosePublishThenConsumeTest.class);

    public void testDurableTopic() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://locahost");

        String topicName = "TestTopic";
        String clientID = getName();
        String subscriberName = "MySubscriber:" + System.currentTimeMillis();

        Connection connection = connectionFactory.createConnection();
        connection.setClientID(clientID);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(topicName);

        // this should register a durable subscriber, we then close it to
        // test that we get messages from the producer later on
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriberName);
        connection.start();

        topic = null;
        subscriber.close();
        subscriber = null;
        session.close();
        session = null;

        // Create the new connection before closing to avoid the broker shutting
        // down.
        // now create a new Connection, Session & Producer, send some messages &
        // then close
        Connection t = connectionFactory.createConnection();
        connection.close();
        connection = t;

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(topicName);
        MessageProducer producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        TextMessage textMessage = session.createTextMessage("Hello World");
        producer.send(textMessage);
        textMessage = null;

        topic = null;
        session.close();
        session = null;

        // Now (re)register the Durable subscriber, setup a listener and wait
        // for messages that should
        // have been published by the previous producer
        t = connectionFactory.createConnection();
        connection.close();
        connection = t;

        connection.setClientID(clientID);
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(topicName);

        subscriber = session.createDurableSubscriber(topic, subscriberName);
        connection.start();

        LOG.info("Started connection - now about to try receive the textMessage");

        long time = System.currentTimeMillis();
        Message message = subscriber.receive(15000L);
        long elapsed = System.currentTimeMillis() - time;

        LOG.info("Waited for: " + elapsed + " millis");

        assertNotNull("Should have received the message we published by now", message);
        assertTrue("should be text textMessage", message instanceof TextMessage);
        textMessage = (TextMessage)message;
        assertEquals("Hello World", textMessage.getText());
    }
}
