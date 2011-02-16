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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.JmsResourceProvider;
import org.apache.activemq.test.TestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public abstract class JmsTransactionTestSupport extends TestSupport implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(JmsTransactionTestSupport.class);
    private static final int MESSAGE_COUNT = 5;
    private static final String MESSAGE_TEXT = "message";

    protected ConnectionFactory connectionFactory;
    protected Connection connection;
    protected Session session;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected JmsResourceProvider resourceProvider;
    protected Destination destination;
    protected int batchCount = 10;
    protected int batchSize = 20;
    protected BrokerService broker;

    // for message listener test
    private List<Message> unackMessages = new ArrayList<Message>(MESSAGE_COUNT);
    private List<Message> ackMessages = new ArrayList<Message>(MESSAGE_COUNT);
    private boolean resendPhase;

    public JmsTransactionTestSupport() {
        super();
    }

    public JmsTransactionTestSupport(String name) {
        super(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();

        resourceProvider = getJmsResourceProvider();
        topic = resourceProvider.isTopic();
        // We will be using transacted sessions.
        setSessionTransacted();
        connectionFactory = newConnectionFactory();
        reconnect();
    }

    protected void setSessionTransacted() {
        resourceProvider.setTransacted(true);
    }

    protected ConnectionFactory newConnectionFactory() throws Exception {
        return resourceProvider.createConnectionFactory();
    }

    protected void beginTx() throws Exception {
        //no-op for local tx
    }

    protected void commitTx() throws Exception {
        session.commit();
    }

    protected void rollbackTx() throws Exception {
        session.rollback();
    }

    /**
     */
    protected BrokerService createBroker() throws Exception, URISyntaxException {
        return BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false"));
    }

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        LOG.info("Closing down connection");

        session.close();
        session = null;
        connection.close();
        connection = null;
        broker.stop();
        broker.waitUntilStopped();
        broker = null;

        LOG.info("Connection closed.");
    }

    protected abstract JmsResourceProvider getJmsResourceProvider();

    /**
     * Sends a batch of messages and validates that the messages are received.
     * 
     * @throws Exception
     */
    public void testSendReceiveTransactedBatches() throws Exception {

        TextMessage message = session.createTextMessage("Batch Message");
        for (int j = 0; j < batchCount; j++) {
            LOG.info("Producing bacth " + j + " of " + batchSize + " messages");

            beginTx();
            for (int i = 0; i < batchSize; i++) {
                producer.send(message);
            }
            messageSent();
            commitTx();
            LOG.info("Consuming bacth " + j + " of " + batchSize + " messages");

            beginTx();
            for (int i = 0; i < batchSize; i++) {
                message = (TextMessage)consumer.receive(1000 * 5);
                assertNotNull("Received only " + i + " messages in batch " + j, message);
                assertEquals("Batch Message", message.getText());
            }

            commitTx();
        }
    }

    protected void messageSent() throws Exception {
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was
     * not consumed.
     * 
     * @throws Exception
     */
    public void testSendRollback() throws Exception {
        Message[] outbound = new Message[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message")};

        // sends a message
        beginTx();
        producer.send(outbound[0]);
        commitTx();

        // sends a message that gets rollbacked
        beginTx();
        producer.send(session.createTextMessage("I'm going to get rolled back."));
        rollbackTx();

        // sends a message
        beginTx();
        producer.send(outbound[1]);
        commitTx();

        // receives the first message
        beginTx();
        ArrayList<Message> messages = new ArrayList<Message>();
        LOG.info("About to consume message 1");
        Message message = consumer.receive(1000);
        messages.add(message);
        LOG.info("Received: " + message);

        // receives the second message
        LOG.info("About to consume message 2");
        message = consumer.receive(4000);
        messages.add(message);
        LOG.info("Received: " + message);

        // validates that the rollbacked was not consumed
        commitTx();
        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work.", outbound, inbound);
    }

    /**
     * spec section 3.6 acking a message with automation acks has no effect.
     * @throws Exception
     */
    public void testAckMessageInTx() throws Exception {
        Message[] outbound = new Message[] {session.createTextMessage("First Message")};

        // sends a message
        beginTx();
        producer.send(outbound[0]);
        outbound[0].acknowledge();
        commitTx();
        outbound[0].acknowledge();

        // receives the first message
        beginTx();
        ArrayList<Message> messages = new ArrayList<Message>();
        LOG.info("About to consume message 1");
        Message message = consumer.receive(1000);
        messages.add(message);
        LOG.info("Received: " + message);

        // validates that the rollbacked was not consumed
        commitTx();
        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Message not delivered.", outbound, inbound);
    }

    /**
     * Sends a batch of messages and validates that the message sent before
     * session close is not consumed.
     *
     * This test only works with local transactions, not xa.
     * @throws Exception
     */
    public void testSendSessionClose() throws Exception {
        Message[] outbound = new Message[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message")};

        // sends a message
        beginTx();
        producer.send(outbound[0]);
        commitTx();

        // sends a message that gets rollbacked
        beginTx();
        producer.send(session.createTextMessage("I'm going to get rolled back."));
        consumer.close();

        reconnectSession();

        // sends a message
        producer.send(outbound[1]);
        commitTx();

        // receives the first message
        ArrayList<Message> messages = new ArrayList<Message>();
        LOG.info("About to consume message 1");
        beginTx();
        Message message = consumer.receive(1000);
        messages.add(message);
        LOG.info("Received: " + message);

        // receives the second message
        LOG.info("About to consume message 2");
        message = consumer.receive(4000);
        messages.add(message);
        LOG.info("Received: " + message);

        // validates that the rollbacked was not consumed
        commitTx();
        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work.", outbound, inbound);
    }

    /**
     * Sends a batch of messages and validates that the message sent before
     * session close is not consumed.
     * 
     * @throws Exception
     */
    public void testSendSessionAndConnectionClose() throws Exception {
        Message[] outbound = new Message[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message")};

        // sends a message
        beginTx();
        producer.send(outbound[0]);
        commitTx();

        // sends a message that gets rollbacked
        beginTx();
        producer.send(session.createTextMessage("I'm going to get rolled back."));
        consumer.close();
        session.close();

        reconnect();

        // sends a message
        beginTx();
        producer.send(outbound[1]);
        commitTx();

        // receives the first message
        ArrayList<Message> messages = new ArrayList<Message>();
        LOG.info("About to consume message 1");
        beginTx();
        Message message = consumer.receive(1000);
        messages.add(message);
        LOG.info("Received: " + message);

        // receives the second message
        LOG.info("About to consume message 2");
        message = consumer.receive(4000);
        messages.add(message);
        LOG.info("Received: " + message);

        // validates that the rollbacked was not consumed
        commitTx();
        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work.", outbound, inbound);
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was
     * redelivered.
     * 
     * @throws Exception
     */
    public void testReceiveRollback() throws Exception {
        Message[] outbound = new Message[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message")};

        // lets consume any outstanding messages from prev test runs
        beginTx();
            while (consumer.receive(1000) != null) {
        }
        commitTx();

        // sent both messages
        beginTx();
        producer.send(outbound[0]);
        producer.send(outbound[1]);
        commitTx();

        LOG.info("Sent 0: " + outbound[0]);
        LOG.info("Sent 1: " + outbound[1]);

        ArrayList<Message> messages = new ArrayList<Message>();
        beginTx();
        Message message = consumer.receive(1000);
        messages.add(message);
        assertEquals(outbound[0], message);
        commitTx();

        // rollback so we can get that last message again.
        beginTx();
        message = consumer.receive(1000);
        assertNotNull(message);
        assertEquals(outbound[1], message);
        rollbackTx();

        // Consume again.. the prev message should
        // get redelivered.
        beginTx();
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the message again!", message);
        messages.add(message);
        commitTx();

        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work", outbound, inbound);
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was
     * redelivered.
     * 
     * @throws Exception
     */
    public void testReceiveTwoThenRollback() throws Exception {
        Message[] outbound = new Message[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message")};

        // lets consume any outstanding messages from prev test runs
        beginTx();
        while (consumer.receive(1000) != null) {
        }
        commitTx();

        //
        beginTx();
        producer.send(outbound[0]);
        producer.send(outbound[1]);
        commitTx();

        LOG.info("Sent 0: " + outbound[0]);
        LOG.info("Sent 1: " + outbound[1]);

        ArrayList<Message> messages = new ArrayList<Message>();
        beginTx();
        Message message = consumer.receive(1000);
        assertEquals(outbound[0], message);

        message = consumer.receive(1000);
        assertNotNull(message);
        assertEquals(outbound[1], message);
        rollbackTx();

        // Consume again.. the prev message should
        // get redelivered.
        beginTx();
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the first message again!", message);
        messages.add(message);
        assertEquals(outbound[0], message);
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the second message again!", message);
        messages.add(message);
        assertEquals(outbound[1], message);

        assertNull(consumer.receiveNoWait());
        commitTx();

        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work", outbound, inbound);
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was
     * not consumed.
     * 
     * @throws Exception
     */
    public void testSendReceiveWithPrefetchOne() throws Exception {
        setPrefetchToOne();
        Message[] outbound = new Message[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message"), session.createTextMessage("Third Message"),
                                            session.createTextMessage("Fourth Message")};

        beginTx();
        for (int i = 0; i < outbound.length; i++) {
            // sends a message
            producer.send(outbound[i]);
        }
        commitTx();

        // receives the first message
        beginTx();
        for (int i = 0; i < outbound.length; i++) {
            LOG.info("About to consume message 1");
            Message message = consumer.receive(1000);
            assertNotNull(message);
            LOG.info("Received: " + message);
        }

        // validates that the rollbacked was not consumed
        commitTx();
    }

    /**
     * Perform the test that validates if the rollbacked message was redelivered
     * multiple times.
     * 
     * @throws Exception
     */
    public void testReceiveTwoThenRollbackManyTimes() throws Exception {
        for (int i = 0; i < 5; i++) {
            testReceiveTwoThenRollback();
        }
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was
     * not consumed. This test differs by setting the message prefetch to one.
     * 
     * @throws Exception
     */
    public void testSendRollbackWithPrefetchOfOne() throws Exception {
        setPrefetchToOne();
        testSendRollback();
    }

    /**
     * Sends a batch of messages and and validates that the rollbacked message
     * was redelivered. This test differs by setting the message prefetch to
     * one.
     * 
     * @throws Exception
     */
    public void testReceiveRollbackWithPrefetchOfOne() throws Exception {
        setPrefetchToOne();
        testReceiveRollback();
    }

    /**
     * Tests if the messages can still be received if the consumer is closed
     * (session is not closed).
     * 
     * @throws Exception see http://jira.codehaus.org/browse/AMQ-143
     */
    public void testCloseConsumerBeforeCommit() throws Exception {
        TextMessage[] outbound = new TextMessage[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message")};

        // lets consume any outstanding messages from prev test runs
        beginTx();
        while (consumer.receiveNoWait() != null) {
        }

        commitTx();

        // sends the messages
        beginTx();
        producer.send(outbound[0]);
        producer.send(outbound[1]);
        commitTx();
        LOG.info("Sent 0: " + outbound[0]);
        LOG.info("Sent 1: " + outbound[1]);

        beginTx();
        TextMessage message = (TextMessage)consumer.receive(1000);
        assertEquals(outbound[0].getText(), message.getText());
        // Close the consumer before the commit. This should not cause the
        // received message
        // to rollback.
        consumer.close();
        commitTx();

        // Create a new consumer
        consumer = resourceProvider.createConsumer(session, destination);
        LOG.info("Created consumer: " + consumer);

        beginTx();
        message = (TextMessage)consumer.receive(1000);
        assertEquals(outbound[1].getText(), message.getText());
        commitTx();
    }

    public void testChangeMutableObjectInObjectMessageThenRollback() throws Exception {
        ArrayList<String> list = new ArrayList<String>();
        list.add("First");
        Message outbound = session.createObjectMessage(list);
        outbound.setStringProperty("foo", "abc");

        beginTx();
        producer.send(outbound);
        commitTx();

        LOG.info("About to consume message 1");
        beginTx();
        Message message = consumer.receive(5000);

        List<String> body = assertReceivedObjectMessageWithListBody(message);

        // now lets try mutate it
        try {
            message.setStringProperty("foo", "def");
            fail("Cannot change properties of the object!");
        } catch (JMSException e) {
            LOG.info("Caught expected exception: " + e, e);
        }
        body.clear();
        body.add("This should never be seen!");
        rollbackTx();

        beginTx();
        message = consumer.receive(5000);
        List<String> secondBody = assertReceivedObjectMessageWithListBody(message);
        assertNotSame("Second call should return a different body", secondBody, body);
        commitTx();
    }

    @SuppressWarnings("unchecked")
    protected List<String> assertReceivedObjectMessageWithListBody(Message message) throws JMSException {
        assertNotNull("Should have received a message!", message);
        assertEquals("foo header", "abc", message.getStringProperty("foo"));

        assertTrue("Should be an object message but was: " + message, message instanceof ObjectMessage);
        ObjectMessage objectMessage = (ObjectMessage)message;
        List<String> body = (List<String>)objectMessage.getObject();
        LOG.info("Received body: " + body);

        assertEquals("Size of list should be 1", 1, body.size());
        assertEquals("element 0 of list", "First", body.get(0));
        return body;
    }

    /**
     * Recreates the connection.
     * 
     * @throws JMSException
     */
    protected void reconnect() throws Exception {

        if (connection != null) {
            // Close the prev connection.
            connection.close();
        }
        session = null;
        connection = resourceProvider.createConnection(connectionFactory);
        reconnectSession();
        connection.start();
    }

    /**
     * Recreates the connection.
     * 
     * @throws JMSException
     */
    protected void reconnectSession() throws JMSException {
        if (session != null) {
            session.close();
        }

        session = resourceProvider.createSession(connection);
        destination = resourceProvider.createDestination(session, getSubject());
        producer = resourceProvider.createProducer(session, destination);
        consumer = resourceProvider.createConsumer(session, destination);
    }

    /**
     * Sets the prefeftch policy to one.
     */
    protected void setPrefetchToOne() {
        ActiveMQPrefetchPolicy prefetchPolicy = getPrefetchPolicy();
        prefetchPolicy.setQueuePrefetch(1);
        prefetchPolicy.setTopicPrefetch(1);
        prefetchPolicy.setDurableTopicPrefetch(1);
        prefetchPolicy.setOptimizeDurableTopicPrefetch(1);
    }

    protected ActiveMQPrefetchPolicy getPrefetchPolicy() {
        return ((ActiveMQConnection)connection).getPrefetchPolicy();
    }

    //This test won't work with xa tx so no beginTx() has been added.
    public void testMessageListener() throws Exception {
        // send messages
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            producer.send(session.createTextMessage(MESSAGE_TEXT + i));
        }
        commitTx();
        consumer.setMessageListener(this);
        // wait receive
        waitReceiveUnack();
        assertEquals(unackMessages.size(), MESSAGE_COUNT);
        // resend phase
        waitReceiveAck();
        assertEquals(ackMessages.size(), MESSAGE_COUNT);
        // should no longer re-receive
        consumer.setMessageListener(null);
        assertNull(consumer.receive(500));
        reconnect();
    }

    public void onMessage(Message message) {
        if (!resendPhase) {
            unackMessages.add(message);
            if (unackMessages.size() == MESSAGE_COUNT) {
                try {
                    rollbackTx();
                    resendPhase = true;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            ackMessages.add(message);
            if (ackMessages.size() == MESSAGE_COUNT) {
                try {
                    commitTx();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void waitReceiveUnack() throws Exception {
        for (int i = 0; i < 100 && !resendPhase; i++) {
            Thread.sleep(100);
        }
        assertTrue(resendPhase);
    }

    private void waitReceiveAck() throws Exception {
        for (int i = 0; i < 100 && ackMessages.size() < MESSAGE_COUNT; i++) {
            Thread.sleep(100);
        }
        assertFalse(ackMessages.size() < MESSAGE_COUNT);
    }
}
