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
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;

/**
 * @version $Revision: 1.1.1.1 $
 */
public abstract class DurableSubscriptionTestSupport extends TestSupport {

    private Connection connection;
    private Session session;
    private TopicSubscriber consumer;
    private MessageProducer producer;
    private BrokerService broker;

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://durable-broker");
    }

    protected Connection createConnection() throws Exception {
        Connection rc = super.createConnection();
        rc.setClientID(getName());
        return rc;
    }

    protected void setUp() throws Exception {
        createBroker();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    protected void restartBroker() throws Exception {
        destroyBroker();
        createRestartedBroker(); // retain stored messages
    }

    private void createBroker() throws Exception {
        try {
            broker = new BrokerService();
            broker.setBrokerName("durable-broker");
            broker.setDeleteAllMessagesOnStartup(true);
            broker.setPersistenceAdapter(createPersistenceAdapter());
            broker.setPersistent(true);
            broker.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        connection = createConnection();
    }

    private void createRestartedBroker() throws Exception {
        try {
            broker = new BrokerService();
            broker.setBrokerName("durable-broker");
            broker.setDeleteAllMessagesOnStartup(false);
            broker.setPersistenceAdapter(createPersistenceAdapter());
            broker.setPersistent(true);
            broker.start();

        } catch (Exception e) {
            e.printStackTrace();
        }

        connection = createConnection();
    }

    private void destroyBroker() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    protected abstract PersistenceAdapter createPersistenceAdapter() throws Exception;

    public void testUnsubscribeSubscription() throws Exception {
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("TestTopic");
        consumer = session.createDurableSubscriber(topic, "sub1");
        producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();

        // Make sure it works when the durable sub is active.
        producer.send(session.createTextMessage("Msg:1"));
        assertTextMessageEquals("Msg:1", consumer.receive(5000));

        // Deactivate the sub.
        consumer.close();
        // Send a new message.
        producer.send(session.createTextMessage("Msg:2"));
        session.unsubscribe("sub1");

        // Reopen the connection.
        connection.close();
        connection = createConnection();
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(topic);
        connection.start();

        // Activate the sub.
        consumer = session.createDurableSubscriber(topic, "sub1");
        producer.send(session.createTextMessage("Msg:3"));

        // Try to get the message.
        assertTextMessageEquals("Msg:3", consumer.receive(5000));
    }

    public void testInactiveDurableSubscriptionTwoConnections() throws Exception {
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("TestTopic");
        consumer = session.createDurableSubscriber(topic, "sub1");
        producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();

        // Make sure it works when the durable sub is active.
        producer.send(session.createTextMessage("Msg:1"));
        assertTextMessageEquals("Msg:1", consumer.receive(5000));

        // Deactivate the sub.
        consumer.close();

        // Send a new message.
        producer.send(session.createTextMessage("Msg:2"));

        // Reopen the connection.
        connection.close();
        connection = createConnection();
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        connection.start();

        // Activate the sub.
        consumer = session.createDurableSubscriber(topic, "sub1");

        // Try to get the message.
        assertTextMessageEquals("Msg:2", consumer.receive(5000));
    }

    public void testInactiveDurableSubscriptionBrokerRestart() throws Exception {
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("TestTopic");
        consumer = session.createDurableSubscriber(topic, "sub1");
        producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();

        // Make sure it works when the durable sub is active.
        producer.send(session.createTextMessage("Msg:1"));
        assertTextMessageEquals("Msg:1", consumer.receive(5000));

        // Deactivate the sub.
        consumer.close();

        // Send a new message.
        producer.send(session.createTextMessage("Msg:2"));

        // Reopen the connection.
        restartBroker();
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        connection.start();

        // Activate the sub.
        consumer = session.createDurableSubscriber(topic, "sub1");

        // Try to get the message.
        assertTextMessageEquals("Msg:2", consumer.receive(5000));
        assertNull(consumer.receive(5000));
    }

    public void testDurableSubscriptionPersistsPastBrokerRestart() throws Exception {

        // Create the durable sub.
        connection.start();
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);

        // Ensure that consumer will receive messages sent before it was created
        Topic topic = session.createTopic("TestTopic?consumer.retroactive=true");
        consumer = session.createDurableSubscriber(topic, "sub1");

        // Restart the broker.
        restartBroker();

        // Reconnection
        connection.start();
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        // Make sure it works when the durable sub is active.
        producer.send(session.createTextMessage("Msg:1"));

        // Activate the sub.
        consumer = session.createDurableSubscriber(topic, "sub1");

        // Send a new message.
        producer.send(session.createTextMessage("Msg:2"));

        // Try to get the message.
        assertTextMessageEquals("Msg:1", consumer.receive(5000));
        assertTextMessageEquals("Msg:2", consumer.receive(5000));

        assertNull(consumer.receive(5000));
    }

    public void xtestInactiveDurableSubscriptionOneConnection() throws Exception {
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("TestTopic");
        consumer = session.createDurableSubscriber(topic, "sub1");
        producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();

        // Make sure it works when the durable sub is active.
        producer.send(session.createTextMessage("Msg:1"));
        assertTextMessageEquals("Msg:1", consumer.receive(5000));

        // Deactivate the sub.
        consumer.close();

        // Send a new message.
        producer.send(session.createTextMessage("Msg:2"));

        // Activate the sub.
        consumer = session.createDurableSubscriber(topic, "sub1");

        // Try to get the message.
        assertTextMessageEquals("Msg:2", consumer.receive(5000));
    }

    public void testSelectorChange() throws Exception {
        session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("TestTopic");
        consumer = session.createDurableSubscriber(topic, "sub1", "color='red'", false);
        producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();

        // Make sure it works when the durable sub is active.
        TextMessage msg = session.createTextMessage();
        msg.setText("Msg:1");
        msg.setStringProperty("color", "blue");
        producer.send(msg);
        msg.setText("Msg:2");
        msg.setStringProperty("color", "red");
        producer.send(msg);

        assertTextMessageEquals("Msg:2", consumer.receive(5000));

        // Change the subscription
        consumer.close();
        consumer = session.createDurableSubscriber(topic, "sub1", "color='blue'", false);

        // Send a new message.
        msg.setText("Msg:3");
        msg.setStringProperty("color", "red");
        producer.send(msg);
        msg.setText("Msg:4");
        msg.setStringProperty("color", "blue");
        producer.send(msg);

        // Try to get the message.
        assertTextMessageEquals("Msg:4", consumer.receive(5000));
    }

    public void testDurableSubWorksInNewSession() throws JMSException {

        // Create the consumer.
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createDurableSubscriber(topic, "sub1");
        // Drain any messages that may allready be in the sub
        while (consumer.receive(1000) != null) {
        }

        // See if the durable sub works in a new session.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Send a Message that should be added to the durable sub.
        MessageProducer producer = createProducer(session, topic);
        producer.send(session.createTextMessage("Message 1"));

        // Activate the durable sub now. And receive the message.
        consumer = session.createDurableSubscriber(topic, "sub1");
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertEquals("Message 1", ((TextMessage)msg).getText());

    }

    public void testDurableSubWorksInNewConnection() throws Exception {

        // Create the consumer.
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createDurableSubscriber(topic, "sub1");
        // Drain any messages that may allready be in the sub
        while (consumer.receive(1000) != null) {
        }

        // See if the durable sub works in a new connection.
        // The embeded broker shutsdown when his connections are closed.
        // So we open the new connection before the old one is closed.
        connection.close();
        connection = createConnection();
        connection.start();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Send a Message that should be added to the durable sub.
        MessageProducer producer = createProducer(session, topic);
        producer.send(session.createTextMessage("Message 1"));

        // Activate the durable sub now. And receive the message.
        consumer = session.createDurableSubscriber(topic, "sub1");
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertEquals("Message 1", ((TextMessage)msg).getText());

    }

    private MessageProducer createProducer(Session session, Destination queue) throws JMSException {
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(getDeliveryMode());
        return producer;
    }

    protected int getDeliveryMode() {
        return DeliveryMode.PERSISTENT;
    }

    private void assertTextMessageEquals(String string, Message message) throws JMSException {
        assertNotNull("Message was null", message);
        assertTrue("Message is not a TextMessage", message instanceof TextMessage);
        assertEquals(string, ((TextMessage)message).getText());
    }

}
