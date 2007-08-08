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

import junit.framework.TestCase;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import java.util.ArrayList;

/**
 * @version
 */
public class JmsTempDestinationTest extends TestCase {

    private Connection connection;
    private ActiveMQConnectionFactory factory;

    protected void setUp() throws Exception {
        factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        factory.setUseAsyncSend(false);
        connection = factory.createConnection();
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
     * Make sure Temp destination can only be consumed by local connection
     * 
     * @throws JMSException
     */
    public void testTempDestOnlyConsumedByLocalConn() throws JMSException {
        connection.start();

        Session tempSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = tempSession.createTemporaryQueue();
        MessageProducer producer = tempSession.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = tempSession.createTextMessage("First");
        producer.send(message);

        // temp destination should not be consume when using another connection
        Connection otherConnection = factory.createConnection();
        Session otherSession = otherConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue otherQueue = otherSession.createTemporaryQueue();
        MessageConsumer consumer = otherSession.createConsumer(otherQueue);
        Message msg = consumer.receive(3000);
        assertNull(msg);

        // should throw InvalidDestinationException when consuming a temp
        // destination from another connection
        try {
            consumer = otherSession.createConsumer(queue);
            fail("Send should fail since temp destination should be used from another connection");
        } catch (InvalidDestinationException e) {
            assertTrue("failed to throw an exception", true);
        }

        // should be able to consume temp destination from the same connection
        consumer = tempSession.createConsumer(queue);
        msg = consumer.receive(3000);
        assertNotNull(msg);

    }

    /**
     * Make sure that a temp queue does not drop message if there is an active
     * consumers.
     * 
     * @throws JMSException
     */
    public void testTempQueueHoldsMessagesWithConsumers() throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = session.createTextMessage("Hello");
        producer.send(message);

        Message message2 = consumer.receive(1000);
        assertNotNull(message2);
        assertTrue("Expected message to be a TextMessage", message2 instanceof TextMessage);
        assertTrue("Expected message to be a '" + message.getText() + "'", ((TextMessage)message2).getText().equals(message.getText()));
    }

    /**
     * Make sure that a temp queue does not drop message if there are no active
     * consumers.
     * 
     * @throws JMSException
     */
    public void testTempQueueHoldsMessagesWithoutConsumers() throws JMSException {

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = session.createTextMessage("Hello");
        producer.send(message);

        connection.start();
        MessageConsumer consumer = session.createConsumer(queue);
        Message message2 = consumer.receive(3000);
        assertNotNull(message2);
        assertTrue("Expected message to be a TextMessage", message2 instanceof TextMessage);
        assertTrue("Expected message to be a '" + message.getText() + "'", ((TextMessage)message2).getText().equals(message.getText()));

    }

    /**
     * Test temp queue works under load
     * 
     * @throws JMSException
     */
    public void testTmpQueueWorksUnderLoad() throws JMSException {
        int count = 500;
        int dataSize = 1024;

        ArrayList list = new ArrayList(count);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        byte[] data = new byte[dataSize];
        for (int i = 0; i < count; i++) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(data);
            producer.send(message);
            list.add(message);
        }

        connection.start();
        MessageConsumer consumer = session.createConsumer(queue);
        for (int i = 0; i < count; i++) {
            Message message2 = consumer.receive(2000);

            assertTrue(message2 != null);
            assertTrue(message2.equals(list.get(i)));
        }
    }

    /**
     * Make sure you cannot publish to a temp destination that does not exist
     * anymore.
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void testPublishFailsForClosedConnection() throws JMSException, InterruptedException {

        Connection tempConnection = factory.createConnection();
        Session tempSession = tempConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = tempSession.createTemporaryQueue();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        // This message delivery should work since the temp connection is still
        // open.
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = session.createTextMessage("First");
        producer.send(message);

        // Closing the connection should destroy the temp queue that was
        // created.
        tempConnection.close();
        Thread.sleep(1000); // Wait a little bit to let the delete take effect.

        // This message delivery NOT should work since the temp connection is
        // now closed.
        try {
            message = session.createTextMessage("Hello");
            producer.send(message);
            fail("Send should fail since temp destination should not exist anymore.");
        } catch (JMSException e) {
            assertTrue("failed to throw an exception", true);
        }
    }

    /**
     * Make sure you cannot publish to a temp destination that does not exist
     * anymore.
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void testPublishFailsForDestoryedTempDestination() throws JMSException, InterruptedException {

        Connection tempConnection = factory.createConnection();
        Session tempSession = tempConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = tempSession.createTemporaryQueue();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        // This message delivery should work since the temp connection is still
        // open.
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = session.createTextMessage("First");
        producer.send(message);

        // deleting the Queue will cause sends to fail
        queue.delete();
        Thread.sleep(1000); // Wait a little bit to let the delete take effect.

        // This message delivery NOT should work since the temp connection is
        // now closed.
        try {
            message = session.createTextMessage("Hello");
            producer.send(message);
            fail("Send should fail since temp destination should not exist anymore.");
        } catch (JMSException e) {
            assertTrue("failed to throw an exception", true);
        }
    }

    /**
     * Test you can't delete a Destination with Active Subscribers
     * 
     * @throws JMSException
     */
    public void testDeleteDestinationWithSubscribersFails() throws JMSException {
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = session.createTemporaryQueue();

        connection.start();

        session.createConsumer(queue);

        // This message delivery should NOT work since the temp connection is
        // now closed.
        try {
            queue.delete();
            fail("Should fail as Subscribers are active");
        } catch (JMSException e) {
            assertTrue("failed to throw an exception", true);
        }
    }
}
