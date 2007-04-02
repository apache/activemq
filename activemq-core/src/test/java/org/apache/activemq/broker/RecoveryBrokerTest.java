/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker;

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
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;

/**
 * Used to simulate the recovery that occurs when a broker shuts down.
 * 
 * @version $Revision$
 */
public class RecoveryBrokerTest extends BrokerRestartTestSupport {
        
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
        
        for( int i=0; i < 4; i++) {
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
        for( int i=0; i < 4 ; i++ ) {
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

        for( int i=0; i < 4; i++) {
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
        for( int i=0; i < 4 ; i++ ) {
            Message m2 = receiveMessage(connection2);
            assertNotNull("Message "+(i+1)+" is missing.", m2);
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
        assertEquals( m.getMessageId(), message.getMessageId() );
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

        for( int i=0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);
        }
        
        // Commit
        connection.send(createCommitTransaction1Phase(connectionInfo, txid));

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

        for( int i=0; i < 4 ;i ++ ) {
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
        
        for( int i=0; i < 4; i++) {
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
        for( int i=0; i < 4 ;i ++ ) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }        
        // Commit
        connection.send(createCommitTransaction1Phase(connectionInfo, txid));
        
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
        
        for( int i=0; i < 4; i++) {
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
        for( int i=0; i < 4 ;i ++ ) {
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
        for( int i=0; i < 4 ;i ++ ) {
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
