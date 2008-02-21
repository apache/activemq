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
package org.apache.activemq.broker;

import java.util.ArrayList;

import javax.jms.DeliveryMode;

import junit.framework.Test;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;

/**
 * Used to simulate the recovery that occurs when a broker shuts down.
 * 
 * @version $Revision$
 */
public class RecoveryBrokerTest extends BrokerRestartTestSupport {

    /**
     * Used to verify that after a broker restart durable subscriptions that use
     * wild cards are still wild card subscription after broker restart.
     * 
     * @throws Exception
     */
    //need to revist!!!
    public void XtestWildCardSubscriptionPreservedOnRestart() throws Exception {
        ActiveMQDestination dest1 = new ActiveMQTopic("TEST.A");
        ActiveMQDestination dest2 = new ActiveMQTopic("TEST.B");
        ActiveMQDestination dest3 = new ActiveMQTopic("TEST.C");
        ActiveMQDestination wildDest = new ActiveMQTopic("TEST.>");

        ArrayList<MessageId> sentBeforeRestart = new ArrayList<MessageId>();
        ArrayList<MessageId> sentBeforeCreateConsumer = new ArrayList<MessageId>();
        ArrayList<MessageId> sentAfterCreateConsumer = new ArrayList<MessageId>();

        // Setup a first connection
        {
            StubConnection connection1 = createConnection();
            ConnectionInfo connectionInfo1 = createConnectionInfo();
            connectionInfo1.setClientId("A");
            SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
            ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
            connection1.send(connectionInfo1);
            connection1.send(sessionInfo1);
            connection1.send(producerInfo1);

            // Create the durable subscription.
            ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, wildDest);
            consumerInfo1.setSubscriptionName("test");
            consumerInfo1.setPrefetchSize(100);
            connection1.send(consumerInfo1);

            // Close the subscription.
            connection1.send(closeConsumerInfo(consumerInfo1));

            // Send the messages
            for (int i = 0; i < 4; i++) {
                Message m = createMessage(producerInfo1, dest1, DeliveryMode.PERSISTENT);
                connection1.send(m);
                sentBeforeRestart.add(m.getMessageId());
            }
            connection1.request(closeConnectionInfo(connectionInfo1));
            connection1.stop();
        }

        // Restart the broker.
        restartBroker();

        // Get a connection to the new broker.
        {
            StubConnection connection2 = createConnection();
            ConnectionInfo connectionInfo2 = createConnectionInfo();
            connectionInfo2.setClientId("A");
            SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
            connection2.send(connectionInfo2);
            connection2.send(sessionInfo2);

            ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
            connection2.send(producerInfo2);

            // Send messages before the durable subscription is re-activated.
            for (int i = 0; i < 4; i++) {
                Message m = createMessage(producerInfo2, dest2, DeliveryMode.PERSISTENT);
                connection2.send(m);
                sentBeforeCreateConsumer.add(m.getMessageId());
            }

            // Re-open the subscription.
            ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, wildDest);
            consumerInfo2.setSubscriptionName("test");
            consumerInfo2.setPrefetchSize(100);
            connection2.send(consumerInfo2);

            // Send messages after the subscription is activated.
            for (int i = 0; i < 4; i++) {
                Message m = createMessage(producerInfo2, dest3, DeliveryMode.PERSISTENT);
                connection2.send(m);
                sentAfterCreateConsumer.add(m.getMessageId());
            }

            // We should get the recovered messages...
            for (int i = 0; i < 4; i++) {
                Message m2 = receiveMessage(connection2);
                assertNotNull("Recovered message missing: " + i, m2);
                assertEquals(sentBeforeRestart.get(i), m2.getMessageId());
            }

            // We should get get the messages that were sent before the sub was
            // reactivated.
            for (int i = 0; i < 4; i++) {
                Message m2 = receiveMessage(connection2);
                assertNotNull("Before activated message missing: " + i, m2);
                assertEquals(sentBeforeCreateConsumer.get(i), m2.getMessageId());
            }

            // We should get get the messages that were sent after the sub was
            // reactivated.
            for (int i = 0; i < 4; i++) {
                Message m2 = receiveMessage(connection2);
                assertNotNull("After activated message missing: " + i, m2);
                assertEquals("" + i, sentAfterCreateConsumer.get(i), m2.getMessageId());
            }

            assertNoMessagesLeft(connection2);
        }

    }

    public void testConsumedQueuePersistentMessagesLostOnRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // The we should get the messages.
        for (int i = 0; i < 4; i++) {
            Message m2 = receiveMessage(connection);
            assertNotNull(m2);
        }

        // restart the broker.
        restartBroker();

        // No messages should be delivered.
        Message m = receiveMessage(connection);
        assertNull(m);
    }

    public void testQueuePersistentUncommitedMessagesLostOnRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // Begin the transaction.
        LocalTransactionId txid = createLocalTransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);
        }

        // Don't commit

        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // No messages should be delivered.
        Message m = receiveMessage(connection);
        assertNull(m);
    }

    public void testTopicDurableConsumerHoldsPersistentMessageAfterRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        connectionInfo1.setClientId("A");
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        // Create the durable subscription.
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setSubscriptionName("test");
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Close the subscription.
        connection1.send(closeConsumerInfo(consumerInfo1));

        // Send the messages
        connection1.send(createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.send(createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.send(createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.send(createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.request(closeConnectionInfo(connectionInfo1));
        // Restart the broker.
        restartBroker();

        // Get a connection to the new broker.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        connectionInfo2.setClientId("A");
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);

        // Re-open the subscription.
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setSubscriptionName("test");
        consumerInfo2.setPrefetchSize(100);
        connection2.send(consumerInfo2);

        // The we should get the messages.
        for (int i = 0; i < 4; i++) {
            Message m2 = receiveMessage(connection2);
            assertNotNull("Did not get message "+i, m2);
        }
        assertNoMessagesLeft(connection2);
    }

    public void testQueuePersistentMessagesNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        Message message = createMessage(producerInfo, destination);
        message.setPersistent(true);
        connection.send(message);
        connection.request(closeConnectionInfo(connectionInfo));

        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Message should have been dropped due to broker restart.
        Message m = receiveMessage(connection);
        assertNotNull("Should have received a message by now!", m);
        assertEquals(m.getMessageId(), message.getMessageId());
    }

    public void testQueueNonPersistentMessagesLostOnRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        Message message = createMessage(producerInfo, destination);
        message.setPersistent(false);
        connection.send(message);

        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Message should have been dropped due to broker restart.
        assertNoMessagesLeft(connection);
    }

    public void testQueuePersistentCommitedMessagesNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // Begin the transaction.
        LocalTransactionId txid = createLocalTransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);
        }

        // Commit
        connection.send(createCommitTransaction1Phase(connectionInfo, txid));
        connection.request(closeConnectionInfo(connectionInfo));
        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        for (int i = 0; i < 4; i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
        }

        assertNoMessagesLeft(connection);
    }

    public void testQueuePersistentCommitedAcksNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Setup the consumer and receive the message.
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Begin the transaction.
        LocalTransactionId txid = createLocalTransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));
        for (int i = 0; i < 4; i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }
        // Commit
        connection.send(createCommitTransaction1Phase(connectionInfo, txid));
        connection.request(closeConnectionInfo(connectionInfo));
        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // No messages should be delivered.
        Message m = receiveMessage(connection);
        assertNull(m);
    }

    public void testQueuePersistentUncommitedAcksLostOnRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Setup the consumer and receive the message.
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Begin the transaction.
        LocalTransactionId txid = createLocalTransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));
        for (int i = 0; i < 4; i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }
        // Don't commit

        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // All messages should be re-delivered.
        for (int i = 0; i < 4; i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
        }

        assertNoMessagesLeft(connection);
    }

    public static Test suite() {
        return suite(RecoveryBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
