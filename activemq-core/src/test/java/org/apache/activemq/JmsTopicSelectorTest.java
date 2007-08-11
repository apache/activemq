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
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.2 $
 */
public class JmsTopicSelectorTest extends TestSupport {
    private static final Log LOG = LogFactory.getLog(JmsTopicSelectorTest.class);

    protected Connection connection;
    protected Session session;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected Destination consumerDestination;
    protected Destination producerDestination;
    protected boolean topic = true;
    protected boolean durable;
    protected int deliveryMode = DeliveryMode.PERSISTENT;

    public JmsTopicSelectorTest() {
        super();
    }

    public JmsTopicSelectorTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();

        connectionFactory = createConnectionFactory();
        connection = createConnection();
        if (durable) {
            connection.setClientID(getClass().getName());
        }

        LOG.info("Created connection: " + connection);

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        LOG.info("Created session: " + session);

        if (topic) {
            consumerDestination = session.createTopic(getConsumerSubject());
            producerDestination = session.createTopic(getProducerSubject());
        } else {
            consumerDestination = session.createQueue(getConsumerSubject());
            producerDestination = session.createQueue(getProducerSubject());
        }

        LOG.info("Created  consumer destination: " + consumerDestination + " of type: " + consumerDestination.getClass());
        LOG.info("Created  producer destination: " + producerDestination + " of type: " + producerDestination.getClass());
        producer = session.createProducer(producerDestination);
        producer.setDeliveryMode(deliveryMode);

        LOG.info("Created producer: " + producer + " delivery mode = " + (deliveryMode == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT"));
        connection.start();
    }

    public void tearDown() throws Exception {
        session.close();
        connection.close();
    }

    protected MessageConsumer createConsumer(String selector) throws JMSException {
        if (durable) {
            LOG.info("Creating durable consumer");
            return session.createDurableSubscriber((Topic)consumerDestination, getName(), selector, false);
        }
        return session.createConsumer(consumerDestination, selector);
    }

    public void sendMessages() throws Exception {
        TextMessage message = session.createTextMessage("1");
        message.setIntProperty("id", 1);
        message.setJMSType("a");
        message.setStringProperty("stringProperty", "a");
        message.setLongProperty("longProperty", 1);
        message.setBooleanProperty("booleanProperty", true);
        producer.send(message);

        message = session.createTextMessage("2");
        message.setIntProperty("id", 2);
        message.setJMSType("a");
        message.setStringProperty("stringProperty", "a");
        message.setLongProperty("longProperty", 1);
        message.setBooleanProperty("booleanProperty", false);
        producer.send(message);

        message = session.createTextMessage("3");
        message.setIntProperty("id", 3);
        message.setJMSType("a");
        message.setStringProperty("stringProperty", "a");
        message.setLongProperty("longProperty", 1);
        message.setBooleanProperty("booleanProperty", true);
        producer.send(message);

        message = session.createTextMessage("4");
        message.setIntProperty("id", 4);
        message.setJMSType("b");
        message.setStringProperty("stringProperty", "b");
        message.setLongProperty("longProperty", 2);
        message.setBooleanProperty("booleanProperty", false);
        producer.send(message);

        message = session.createTextMessage("5");
        message.setIntProperty("id", 5);
        message.setJMSType("c");
        message.setStringProperty("stringProperty", "c");
        message.setLongProperty("longProperty", 3);
        message.setBooleanProperty("booleanProperty", true);
        producer.send(message);
    }

    public void consumeMessages(int remaining) throws Exception {
        consumer = createConsumer(null);
        for (int i = 0; i < remaining; i++) {
            consumer.receive(1000);
        }
        consumer.close();

    }

    public void testPropertySelector() throws Exception {
        int remaining = 5;
        Message message = null;
        consumer = createConsumer("stringProperty = 'a' and longProperty = 1 and booleanProperty = true");
        sendMessages();
        while (true) {
            message = consumer.receive(1000);
            if (message == null) {
                break;
            }
            String text = ((TextMessage)message).getText();
            if (!text.equals("1") && !text.equals("3")) {
                fail("unexpected message: " + text);
            }
            remaining--;
        }
        assertEquals(remaining, 3);
        consumer.close();
        consumeMessages(remaining);

    }

    public void testJMSPropertySelector() throws Exception {
        int remaining = 5;
        Message message = null;
        consumer = createConsumer("JMSType = 'a' and stringProperty = 'a'");
        sendMessages();
        while (true) {
            message = consumer.receive(1000);
            if (message == null) {
                break;
            }
            String text = ((TextMessage)message).getText();
            if (!text.equals("1") && !text.equals("2") && !text.equals("3")) {
                fail("unexpected message: " + text);
            }
            remaining--;
        }
        assertEquals(remaining, 2);
        consumer.close();
        consumeMessages(remaining);

    }

}
