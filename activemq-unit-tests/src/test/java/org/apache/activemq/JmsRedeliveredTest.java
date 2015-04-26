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

import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.transport.vm.VMTransport;
import org.apache.activemq.util.Wait;

/**
 * 
 */
public class JmsRedeliveredTest extends TestCase {

    private Connection connection;

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        connection = createConnection();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    /**
     * Creates a connection.
     * 
     * @return connection
     * @throws Exception
     */
    protected Connection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                                                                          "vm://localhost?broker.persistent=false");
        return factory.createConnection();
    }

    /**
     * Tests if a message unacknowledged message gets to be resent when the
     * session is closed and then a new consumer session is created.
     * 
     */
    public void testQueueSessionCloseMarksMessageRedelivered() throws JMSException {
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        producer.send(createTextMessage(session));

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        // Don't ack the message.

        // Reset the session. This should cause the Unacked message to be
        // redelivered.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createConsumer(queue);
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }

    

    public void testQueueSessionCloseMarksUnAckedMessageRedelivered() throws JMSException {
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        producer.send(createTextMessage(session, "1"));
        producer.send(createTextMessage(session, "2"));

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        assertEquals("1", ((TextMessage)msg).getText());
        msg.acknowledge();
        
        // Don't ack the message.
        msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        assertEquals("2", ((TextMessage)msg).getText());
        
        // Reset the session. This should cause the Unacked message to be
        // redelivered.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createConsumer(queue);
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertEquals("2", ((TextMessage)msg).getText());
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }

    /**
     * Tests session recovery and that the redelivered message is marked as
     * such. Session uses client acknowledgement, the destination is a queue.
     * 
     * @throws JMSException
     */
    public void testQueueRecoverMarksMessageRedelivered() throws JMSException {
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        producer.send(createTextMessage(session));

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        // Don't ack the message.

        // Reset the session. This should cause the Unacked message to be
        // redelivered.
        session.recover();

        // Attempt to Consume the message...
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }

    /**
     * Tests rollback message to be marked as redelivered. Session uses client
     * acknowledgement and the destination is a queue.
     * 
     * @throws JMSException
     */
    public void testQueueRollbackMarksMessageRedelivered() throws JMSException {
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        producer.send(createTextMessage(session));
        session.commit();

        // Get the message... Should not be redelivered.
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());

        // Rollback.. should cause redelivery.
        session.rollback();

        // Attempt to Consume the message...
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());

        session.commit();
        session.close();
    }

    /**
     * Tests if the message gets to be re-delivered when the session closes and
     * that the re-delivered message is marked as such. Session uses client
     * acknowledgment, the destination is a topic and the consumer is a durable
     * subscriber.
     * 
     * @throws JMSException
     */
    public void testDurableTopicSessionCloseMarksMessageRedelivered() throws JMSException {
        connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createDurableSubscriber(topic, "sub1");

        // This case only works with persistent messages since transient
        // messages
        // are dropped when the consumer goes offline.
        MessageProducer producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(createTextMessage(session));

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be re-delivered.", msg.getJMSRedelivered());
        // Don't ack the message.

        // Reset the session. This should cause the Unacked message to be
        // re-delivered.
        session.close();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createDurableSubscriber(topic, "sub1");
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }

    /**
     * Tests session recovery and that the redelivered message is marked as
     * such. Session uses client acknowledgement, the destination is a topic and
     * the consumer is a durable suscriber.
     * 
     * @throws JMSException
     */
    public void testDurableTopicRecoverMarksMessageRedelivered() throws JMSException {
        connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createDurableSubscriber(topic, "sub1");

        MessageProducer producer = createProducer(session, topic);
        producer.send(createTextMessage(session));

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        // Don't ack the message.

        // Reset the session. This should cause the Unacked message to be
        // redelivered.
        session.recover();

        // Attempt to Consume the message...
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }

    /**
     * Tests rollback message to be marked as redelivered. Session uses client
     * acknowledgement and the destination is a topic.
     * 
     * @throws JMSException
     */
    public void testDurableTopicRollbackMarksMessageRedelivered() throws JMSException {
        connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createDurableSubscriber(topic, "sub1");

        MessageProducer producer = createProducer(session, topic);
        producer.send(createTextMessage(session));
        session.commit();

        // Get the message... Should not be redelivered.
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());

        // Rollback.. should cause redelivery.
        session.rollback();

        // Attempt to Consume the message...
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());

        session.commit();
        session.close();
    }

    /**
     * 
     * 
     * @throws JMSException
     */
    public void testTopicRecoverMarksMessageRedelivered() throws JMSException {

        connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createConsumer(topic);

        MessageProducer producer = createProducer(session, topic);
        producer.send(createTextMessage(session));

        // Consume the message...
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        // Don't ack the message.

        // Reset the session. This should cause the Unacked message to be
        // redelivered.
        session.recover();

        // Attempt to Consume the message...
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        msg.acknowledge();

        session.close();
    }

    /**
     * Tests rollback message to be marked as redelivered. Session uses client
     * acknowledgement and the destination is a topic.
     * 
     * @throws JMSException
     */
    public void testTopicRollbackMarksMessageRedelivered() throws JMSException {
        connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createConsumer(topic);

        MessageProducer producer = createProducer(session, topic);
        producer.send(createTextMessage(session));
        session.commit();

        // Get the message... Should not be redelivered.
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());

        // Rollback.. should cause redelivery.
        session.rollback();

        // Attempt to Consume the message...
        msg = consumer.receive(2000);
        assertNotNull(msg);
        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());

        session.commit();
        session.close();
    }

    public void testNoReceiveConsumerDisconnectDoesIncrementRedelivery() throws Exception {
        connection.setClientID(getName());
        connection.start();

        Connection keepBrokerAliveConnection = createConnection();
        keepBrokerAliveConnection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        final MessageConsumer consumer = session.createConsumer(queue);

        MessageProducer producer = createProducer(session, queue);
        producer.send(createTextMessage(session));
        session.commit();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return ((ActiveMQMessageConsumer)consumer).getMessageSize() == 1;
            }
        });

        // whack the connection - like a rebalance or tcp drop - consumer does not get to communicate
        // a close and delivered sequence info to broker. So broker is in the dark and must increment
        // redelivery to be safe
        ((ActiveMQConnection)connection).getTransport().narrow(VMTransport.class).stop();

        session = keepBrokerAliveConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer messageConsumer = session.createConsumer(queue);
        Message msg = messageConsumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue("Message should be redelivered.", msg.getJMSRedelivered());
        session.close();
        keepBrokerAliveConnection.close();
    }

    public void testNoReceiveConsumerAbortDoesNotIncrementRedelivery() throws Exception {
        connection.setClientID(getName());
        connection.start();

        Connection keepBrokerAliveConnection = createConnection();
        keepBrokerAliveConnection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        final MessageConsumer consumer = session.createConsumer(queue);

        MessageProducer producer = createProducer(session, queue);
        producer.send(createTextMessage(session));
        session.commit();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return ((ActiveMQMessageConsumer)consumer).getMessageSize() == 1;
            }
        });

        // on abort via something like slowConsumerPolicy
        ConsumerControl consumerControl = new ConsumerControl();
        consumerControl.setConsumerId(((ActiveMQMessageConsumer)consumer).getConsumerId());
        consumerControl.setClose(true);
        ((ActiveMQConnection) connection).getTransport().narrow(VMTransport.class).getTransportListener().onCommand(consumerControl);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return ((ActiveMQMessageConsumer)consumer).getMessageSize() == 0;
            }
        });

        session = keepBrokerAliveConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer messageConsumer = session.createConsumer(queue);
        Message msg = messageConsumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        session.close();
        keepBrokerAliveConnection.close();
    }

    public void testNoReceiveConsumerDoesNotIncrementRedelivery() throws Exception {
        connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageConsumer consumer = session.createConsumer(queue);

        MessageProducer producer = createProducer(session, queue);
        producer.send(createTextMessage(session));
        session.commit();

        TimeUnit.SECONDS.sleep(1);
        consumer.close();

        consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);

        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        session.close();
    }

    public void testNoReceiveDurableConsumerDoesNotIncrementRedelivery() throws Exception {
        connection.setClientID(getName());
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic("topic-" + getName());
        MessageConsumer consumer = session.createDurableSubscriber(topic, "sub");

        MessageProducer producer = createProducer(session, topic);
        producer.send(createTextMessage(session));
        session.commit();

        TimeUnit.SECONDS.sleep(1);
        consumer.close();

        consumer = session.createDurableSubscriber(topic, "sub");
        Message msg = consumer.receive(1000);
        assertNotNull(msg);

        assertFalse("Message should not be redelivered.", msg.getJMSRedelivered());
        session.close();
    }

    /**
     * Creates a text message.
     * 
     * @param session
     * @return TextMessage.
     * @throws JMSException
     */
    private TextMessage createTextMessage(Session session) throws JMSException {
        return createTextMessage(session, "Hello");
    }

    private TextMessage createTextMessage(Session session, String txt) throws JMSException {
        return session.createTextMessage(txt);
    }
    
    /**
     * Creates a producer.
     * 
     * @param session
     * @param queue - destination.
     * @return MessageProducer
     * @throws JMSException
     */
    private MessageProducer createProducer(Session session, Destination queue) throws JMSException {
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(getDeliveryMode());
        return producer;
    }

    /**
     * Returns delivery mode.
     * 
     * @return int - persistent delivery mode.
     */
    protected int getDeliveryMode() {
        return DeliveryMode.PERSISTENT;
    }

    /**
     * Run the JmsRedeliverTest with the delivery mode set as persistent.
     */
    public static final class PersistentCase extends JmsRedeliveredTest {

        /**
         * Returns delivery mode.
         * 
         * @return int - persistent delivery mode.
         */
        protected int getDeliveryMode() {
            return DeliveryMode.PERSISTENT;
        }
    }

    /**
     * Run the JmsRedeliverTest with the delivery mode set as non-persistent.
     */
    public static final class TransientCase extends JmsRedeliveredTest {

        /**
         * Returns delivery mode.
         * 
         * @return int - non-persistent delivery mode.
         */
        protected int getDeliveryMode() {
            return DeliveryMode.NON_PERSISTENT;
        }
    }

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(PersistentCase.class);
        suite.addTestSuite(TransientCase.class);
        return suite;
    }
}
