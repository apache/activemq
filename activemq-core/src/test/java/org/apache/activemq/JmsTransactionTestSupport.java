/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.test.JmsResourceProvider;
import org.apache.activemq.test.TestSupport;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.MessageListener;
import java.util.ArrayList;
import java.util.List;

/**
 * @version $Revision: 1.9 $
 */
abstract public class JmsTransactionTestSupport extends TestSupport implements MessageListener {
    
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(JmsTransactionTestSupport.class);

    protected ConnectionFactory connectionFactory;
    protected Connection connection;
    protected Session session;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected JmsResourceProvider resourceProvider;
    protected Destination destination;
    
    // for message listener test
    private final int messageCount = 5;
    private final String messageText = "message";
    private List unackMessages = new ArrayList(messageCount);
    private List ackMessages = new ArrayList(messageCount);
    private boolean resendPhase = false;

    public JmsTransactionTestSupport() {
        super();
    }

    public JmsTransactionTestSupport(String name) {
        super(name);
    }


    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        resourceProvider = getJmsResourceProvider();
        topic = resourceProvider.isTopic();
        // We will be using transacted sessions.
        resourceProvider.setTransacted(true);
        connectionFactory = resourceProvider.createConnectionFactory();
        reconnect();
    }

    /* (non-Javadoc)
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        //TODO
        //log.info("Test Done.  Stats");
        //((ActiveMQConnectionFactory) connectionFactory).getFactoryStats().dump(new IndentPrinter());
        log.info("Closing down connection");

        session.close();
        connection.close();
        log.info("Connection closed.");
    }

    protected abstract JmsResourceProvider getJmsResourceProvider();

    /**
     * Sends a batch of messages and validates that the messages are received.
     *
     * @throws Exception
     */
    public void testSendReceiveTransactedBatches() throws Exception {
        int batchCount = 10;
        int batchSize = 20;
        TextMessage message = session.createTextMessage("Batch Message");

        for (int j = 0; j < batchCount; j++) {
            log.info("Producing bacth " + j + " of " + batchSize + " messages");

            for (int i = 0; i < batchSize; i++) {
                producer.send(message);
            }

            session.commit();
            log.info("Consuming bacth " + j + " of " + batchSize + " messages");

            for (int i = 0; i < batchSize; i++) {
                message = (TextMessage) consumer.receive(1000 * 5);
                assertNotNull("Received only " + i + " messages in batch " + j, message);
                assertEquals("Batch Message", message.getText());
            }

            session.commit();
        }
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was not consumed.
     *
     * @throws Exception
     */
    public void testSendRollback() throws Exception {
        Message[] outbound = new Message[]{
            session.createTextMessage("First Message"),
            session.createTextMessage("Second Message")
        };

        //sends a message
        producer.send(outbound[0]);
        session.commit();

        //sends a message that gets rollbacked
        producer.send(session.createTextMessage("I'm going to get rolled back."));
        session.rollback();
        
        //sends a message
        producer.send(outbound[1]);
        session.commit();

        //receives the first message
        ArrayList messages = new ArrayList();
        log.info("About to consume message 1");
        Message message = consumer.receive(1000);
        messages.add(message);
        log.info("Received: " + message);

        //receives the second message
        log.info("About to consume message 2");
        message = consumer.receive(4000);
        messages.add(message);
        log.info("Received: " + message);

        //validates that the rollbacked was not consumed
        session.commit();
        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work.", outbound, inbound);
    }

    /**
     * Sends a batch of messages and validates that the message sent before session close is not consumed.
     *
     * @throws Exception
     */
    public void testSendSessionClose() throws Exception {
        Message[] outbound = new Message[]{
            session.createTextMessage("First Message"),
            session.createTextMessage("Second Message")
        };

        //sends a message
        producer.send(outbound[0]);
        session.commit();

        //sends a message that gets rollbacked
        producer.send(session.createTextMessage("I'm going to get rolled back."));
        consumer.close();
        session.close();

        reconnect();

        //sends a message
        producer.send(outbound[1]);
        session.commit();

        //receives the first message
        ArrayList messages = new ArrayList();
        log.info("About to consume message 1");
        Message message = consumer.receive(1000);
        messages.add(message);
        log.info("Received: " + message);

        //receives the second message
        log.info("About to consume message 2");
        message = consumer.receive(4000);
        messages.add(message);
        log.info("Received: " + message);

        //validates that the rollbacked was not consumed
        session.commit();
        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work.", outbound, inbound);
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was redelivered.
     *
     * @throws Exception
     */
    public void testReceiveRollback() throws Exception {
        Message[] outbound = new Message[]{
            session.createTextMessage("First Message"),
            session.createTextMessage("Second Message")
        };

        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }
        session.commit();

        //sent both messages
        producer.send(outbound[0]);
        producer.send(outbound[1]);
        session.commit();

        log.info("Sent 0: " + outbound[0]);
        log.info("Sent 1: " + outbound[1]);

        ArrayList messages = new ArrayList();
        Message message = consumer.receive(1000);
        messages.add(message);
        assertEquals(outbound[0], message);
        session.commit();

        // rollback so we can get that last message again.
        message = consumer.receive(1000);
        assertNotNull(message);
        assertEquals(outbound[1], message);
        session.rollback();

        // Consume again.. the previous message should
        // get redelivered.
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the message again!", message);
        messages.add(message);
        session.commit();

        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work", outbound, inbound);
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was redelivered.
     *
     * @throws Exception
     */
    public void testReceiveTwoThenRollback() throws Exception {
        Message[] outbound = new Message[]{
            session.createTextMessage("First Message"),
            session.createTextMessage("Second Message")
        };

        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }
        session.commit();

        //
        producer.send(outbound[0]);
        producer.send(outbound[1]);
        session.commit();

        log.info("Sent 0: " + outbound[0]);
        log.info("Sent 1: " + outbound[1]);

        ArrayList messages = new ArrayList();
        Message message = consumer.receive(1000);
        assertEquals(outbound[0], message);

        message = consumer.receive(1000);
        assertNotNull(message);
        assertEquals(outbound[1], message);
        session.rollback();

        // Consume again.. the previous message should
        // get redelivered.
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the first message again!", message);
        messages.add(message);
        assertEquals(outbound[0], message);
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the second message again!", message);
        messages.add(message);
        assertEquals(outbound[1], message);
        
        assertNull(consumer.receiveNoWait());        
        session.commit();

        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work", outbound, inbound);
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was not consumed.
     *
     * @throws Exception
     */
    public void testSendReceiveWithPrefetchOne() throws Exception {
        setPrefetchToOne();
        Message[] outbound = new Message[]{
            session.createTextMessage("First Message"),
            session.createTextMessage("Second Message"),
            session.createTextMessage("Third Message"),
            session.createTextMessage("Fourth Message")
        };

        for (int i = 0; i < outbound.length; i++) {
            //sends a message
            producer.send(outbound[i]);
        }
        session.commit();

        //receives the first message
        for (int i = 0; i < outbound.length; i++) {
            log.info("About to consume message 1");
            Message message = consumer.receive(1000);
            assertNotNull(message);
            log.info("Received: " + message);
        }

        //validates that the rollbacked was not consumed
        session.commit();
    }

    /**
     * Perform the test that validates if the rollbacked message was redelivered multiple times.
     *
     * @throws Exception
     */
    public void testReceiveTwoThenRollbackManyTimes() throws Exception {
        for (int i = 0; i < 5; i++)
            testReceiveTwoThenRollback();
    }

    /**
     * Sends a batch of messages and validates that the rollbacked message was not consumed. This test differs by
     * setting the message prefetch to one.
     *
     * @throws Exception
     */
    public void testSendRollbackWithPrefetchOfOne() throws Exception {
        setPrefetchToOne();
        testSendRollback();
    }

    /**
     * Sends a batch of messages and  and validates that the rollbacked message was redelivered. This test differs by
     * setting the message prefetch to one.
     *
     * @throws Exception
     */
    public void testReceiveRollbackWithPrefetchOfOne() throws Exception {
        setPrefetchToOne();
        testReceiveRollback();
    }

    /**
     * Tests if the messages can still be received if the consumer is closed (session is not closed).
     *
     * @throws Exception see http://jira.codehaus.org/browse/AMQ-143
     */
    public void testCloseConsumerBeforeCommit() throws Exception {
        TextMessage[] outbound = new TextMessage[]{
            session.createTextMessage("First Message"),
            session.createTextMessage("Second Message")
        };

        // lets consume any outstanding messages from previous test runs
        while (consumer.receiveNoWait() != null) {
        }

        session.commit();

        //sends the messages
        producer.send(outbound[0]);
        producer.send(outbound[1]);
        session.commit();
        log.info("Sent 0: " + outbound[0]);
        log.info("Sent 1: " + outbound[1]);

        TextMessage message = (TextMessage) consumer.receive(1000);
        assertEquals(outbound[0].getText(), message.getText());        
        // Close the consumer before the commit.  This should not cause the received message
        // to rollback.
        consumer.close();
        session.commit();

        // Create a new consumer
        consumer = resourceProvider.createConsumer(session, destination);
        log.info("Created consumer: " + consumer);

        message = (TextMessage) consumer.receive(1000);
        assertEquals(outbound[1].getText(), message.getText());
        session.commit();
    }


    /**
     * Recreates the connection.
     *
     * @throws JMSException
     */
    protected void reconnect() throws JMSException {

        if (connection != null) {
            // Close the previous connection.
            connection.close();
        }
        connection = resourceProvider.createConnection(connectionFactory);

        session = resourceProvider.createSession(connection);
        destination = resourceProvider.createDestination(session, getSubject());
        producer = resourceProvider.createProducer(session, destination);
        consumer = resourceProvider.createConsumer(session, destination);
        connection.start();
    }

    /**
     * Sets the prefeftch policy to one.
     */
    protected void setPrefetchToOne() {
        ActiveMQPrefetchPolicy prefetchPolicy = ((ActiveMQConnection) connection).getPrefetchPolicy();
        prefetchPolicy.setQueuePrefetch(1);
        prefetchPolicy.setTopicPrefetch(1);
        prefetchPolicy.setDurableTopicPrefetch(1);
    }

    public void testMessageListener() throws Exception {
        //send messages
        for(int i = 0;i<messageCount;i++) {
            producer.send(session.createTextMessage(messageText+i));
        }
        session.commit();
        consumer.setMessageListener(this);
        //wait receive
        waitReceiveUnack();
        assertEquals(unackMessages.size(),messageCount);
        //resend phase
        waitReceiveAck();
        assertEquals(ackMessages.size(),messageCount);
        //should no longer re-receive
        consumer.setMessageListener(null);
        assertNull(consumer.receive(500));
        reconnect();
    }

    public void onMessage(Message message) {
        if(!resendPhase) {
            unackMessages.add(message);
            if(unackMessages.size() == messageCount) {
                try {
                    session.rollback();
                    resendPhase = true;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            ackMessages.add(message);
            if(ackMessages.size() == messageCount) {
                try {
                    session.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void waitReceiveUnack() throws Exception {
        for(int i=0; i < 100 && !resendPhase; i++) {
            Thread.sleep(100);
        }
        assertTrue(resendPhase); 
    }

    private void waitReceiveAck() throws Exception {
        for(int i=0; i < 100 && ackMessages.size() < messageCount; i++) {
            Thread.sleep(100);
        }
        assertFalse(ackMessages.size() < messageCount); 
    }
}
