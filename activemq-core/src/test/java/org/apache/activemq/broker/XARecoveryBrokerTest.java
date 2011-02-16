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

import junit.framework.Test;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;

/**
 * Used to simulate the recovery that occurs when a broker shuts down.
 * 
 * 
 */
public class XARecoveryBrokerTest extends BrokerRestartTestSupport {

    public void testPreparedTransactionRecoveredOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Prepare 4 message sends.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            XATransactionId txid = createXATransaction(sessionInfo);
            connection.send(createBeginTransaction(connectionInfo, txid));

            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);

            // Prepare
            connection.send(createPrepareTransaction(connectionInfo, txid));
        }

        // Since prepared but not committed.. they should not get delivered.
        assertNoMessagesLeft(connection);
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

        // Since prepared but not committed.. they should not get delivered.
        assertNoMessagesLeft(connection);

        Response response = connection.request(new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER));
        assertNotNull(response);
        DataArrayResponse dar = (DataArrayResponse)response;
        assertEquals(4, dar.getData().length);

        // Commit the prepared transactions.
        for (int i = 0; i < dar.getData().length; i++) {
            connection.send(createCommitTransaction2Phase(connectionInfo, (TransactionId)dar.getData()[i]));
        }

        // We should not get the committed transactions.
        for (int i = 0; i < 4; i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
        }

        assertNoMessagesLeft(connection);
    }

    public void testQueuePersistentCommitedMessagesNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
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

        ActiveMQDestination destination = createDestination();

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
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));
        for (int i = 0; i < 4; i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }
        // Commit
        connection.request(createCommitTransaction1Phase(connectionInfo, txid));

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
        assertNoMessagesLeft(connection);

        Message m = receiveMessage(connection);
        assertNull(m);
    }
    
    public void testQueuePersistentPreparedAcksNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

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
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));
        for (int i = 0; i < 4; i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }
        
        connection.request(createPrepareTransaction(connectionInfo, txid));

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

    public void testQueuePersistentUncommittedAcksLostOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

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
        XATransactionId txid = createXATransaction(sessionInfo);
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
        return suite(XARecoveryBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected ActiveMQDestination createDestination() {
        return new ActiveMQQueue(getClass().getName() + "." + getName());
    }

}
