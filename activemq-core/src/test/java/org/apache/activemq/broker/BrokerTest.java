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
import java.util.List;
import java.util.concurrent.TimeUnit;

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

public class BrokerTest extends BrokerTestSupport {

    public ActiveMQDestination destination;
    public int deliveryMode;
    public int prefetch;
    public byte destinationType;
    public boolean durableConsumer;
    protected static final int MAX_NULL_WAIT=500;

    public void initCombosForTestQueueOnlyOnceDeliveryWith2Consumers() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    public void testQueueOnlyOnceDeliveryWith2Consumers() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(1);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        // Send the messages
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.request(createMessage(producerInfo, destination, deliveryMode));

        for (int i = 0; i < 2; i++) {
            Message m1 = receiveMessage(connection1);
            Message m2 = receiveMessage(connection2);

            assertNotNull("m1 is null for index: " + i, m1);
            assertNotNull("m2 is null for index: " + i, m2);

            assertNotSame(m1.getMessageId(), m2.getMessageId());
            connection1.send(createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
            connection2.send(createAck(consumerInfo2, m2, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        assertNoMessagesLeft(connection1);
        assertNoMessagesLeft(connection2);
    }

    public void initCombosForTestQueueBrowserWith2Consumers() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    public void testQueueBrowserWith2Consumers() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(10);
        connection1.request(consumerInfo1);

        // Send the messages
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        //as the messages are sent async - need to synchronize the last
        //one to ensure they arrive in the order we want
        connection1.request(createMessage(producerInfo, destination, deliveryMode));

        // Setup a second connection with a queue browser.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        consumerInfo2.setBrowser(true);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        List<Message> messages = new ArrayList<Message>();

        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull("m1 is null for index: " + i, m1);
            messages.add(m1);
        }

        for (int i = 0; i < 4; i++) {
            Message m1 = messages.get(i);
            Message m2 = receiveMessage(connection2);
            assertNotNull("m2 is null for index: " + i, m2);
            assertEquals(m1.getMessageId(), m2.getMessageId());
            connection2.send(createAck(consumerInfo2, m2, 1, MessageAck.DELIVERED_ACK_TYPE));
        }

        assertNoMessagesLeft(connection1);
        assertNoMessagesLeft(connection2);
    }

    
    /*
     * change the order of the above test
     */
    public void testQueueBrowserWith2ConsumersBrowseFirst() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        
        
        // Setup a second connection with a queue browser.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(10);
        consumerInfo2.setBrowser(true);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(10);
        connection1.request(consumerInfo1);

        // Send the messages
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        //as the messages are sent async - need to synchronize the last
        //one to ensure they arrive in the order we want
        connection1.request(createMessage(producerInfo, destination, deliveryMode));


        List<Message> messages = new ArrayList<Message>();

        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull("m1 is null for index: " + i, m1);
            messages.add(m1);
        }

        // no messages present in queue browser as there were no messages when it
        // was created
        assertNoMessagesLeft(connection1);
        assertNoMessagesLeft(connection2);
    }

    public void testQueueBrowserWith2ConsumersInterleaved() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        
        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(10);
        connection1.request(consumerInfo1);

        // Send the messages
        connection1.request(createMessage(producerInfo, destination, deliveryMode));
        
        // Setup a second connection with a queue browser.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        consumerInfo2.setBrowser(true);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        //as the messages are sent async - need to synchronize the last
        //one to ensure they arrive in the order we want
        connection1.request(createMessage(producerInfo, destination, deliveryMode));

        
        List<Message> messages = new ArrayList<Message>();

        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull("m1 is null for index: " + i, m1);
            messages.add(m1);
        }

        for (int i = 0; i < 1; i++) {
            Message m1 = messages.get(i);
            Message m2 = receiveMessage(connection2);
            assertNotNull("m2 is null for index: " + i, m2);
            assertEquals(m1.getMessageId(), m2.getMessageId());
            connection2.send(createAck(consumerInfo2, m2, 1, MessageAck.DELIVERED_ACK_TYPE));
        }

        assertNoMessagesLeft(connection1);
        assertNoMessagesLeft(connection2);
    }

    
    public void initCombosForTestConsumerPrefetchAndStandardAck() {
        addCombinationValues("deliveryMode", new Object[] {
        // Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                             Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testConsumerPrefetchAndStandardAck() throws Exception {

        // Start a producer and consumer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, destinationType);

        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setPrefetchSize(1);
        connection.send(consumerInfo);

        // Send 3 messages to the broker.
        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.request(createMessage(producerInfo, destination, deliveryMode));

        // Make sure only 1 message was delivered.
        Message m1 = receiveMessage(connection);
        assertNotNull(m1);
        assertNoMessagesLeft(connection);

        // Acknowledge the first message. This should cause the next message to
        // get dispatched.
        connection.send(createAck(consumerInfo, m1, 1, MessageAck.STANDARD_ACK_TYPE));

        Message m2 = receiveMessage(connection);
        assertNotNull(m2);
        connection.send(createAck(consumerInfo, m2, 1, MessageAck.STANDARD_ACK_TYPE));

        Message m3 = receiveMessage(connection);
        assertNotNull(m3);
        connection.send(createAck(consumerInfo, m3, 1, MessageAck.STANDARD_ACK_TYPE));

        connection.send(closeConnectionInfo(connectionInfo));
    }

    public void initCombosForTestTransactedAckWithPrefetchOfOne() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testTransactedAckWithPrefetchOfOne() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(1);
        connection1.send(consumerInfo1);

        // Send the messages
        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo1, destination, deliveryMode);
            connection1.send(message);
        }

       

        // Now get the messages.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            LocalTransactionId txid = createLocalTransaction(sessionInfo1);
            connection1.send(createBeginTransaction(connectionInfo1, txid));
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
            MessageAck ack = createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection1.send(ack);
         // Commit the transaction.
            connection1.send(createCommitTransaction1Phase(connectionInfo1, txid));
        }
        assertNoMessagesLeft(connection1);
    }

    public void initCombosForTestTransactedSend() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testTransactedSend() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Begin the transaction.
        LocalTransactionId txid = createLocalTransaction(sessionInfo1);
        connection1.send(createBeginTransaction(connectionInfo1, txid));

        // Send the messages
        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo1, destination, deliveryMode);
            message.setTransactionId(txid);
            connection1.request(message);
        }

        // The point of this test is that message should not be delivered until
        // send is committed.
        assertNull(receiveMessage(connection1,MAX_NULL_WAIT));

        // Commit the transaction.
        connection1.send(createCommitTransaction1Phase(connectionInfo1, txid));

        // Now get the messages.
        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
        }

        assertNoMessagesLeft(connection1);
    }

    public void initCombosForTestQueueTransactedAck() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE)});
    }

    public void testQueueTransactedAck() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Send the messages
        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo1, destination, deliveryMode);
            connection1.send(message);
        }

        // Begin the transaction.
        LocalTransactionId txid = createLocalTransaction(sessionInfo1);
        connection1.send(createBeginTransaction(connectionInfo1, txid));

        // Acknowledge the first 2 messages.
        for (int i = 0; i < 2; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull("m1 is null for index: " + i, m1);
            MessageAck ack = createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection1.request(ack);
        }

        // Commit the transaction.
        connection1.send(createCommitTransaction1Phase(connectionInfo1, txid));

        // The queue should now only have the remaining 2 messages
        assertEquals(2, countMessagesInQueue(connection1, connectionInfo1, destination));
    }

    public void initCombosForTestConsumerCloseCausesRedelivery() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST")});
    }

    public void testConsumerCloseCausesRedelivery() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.request(consumerInfo1);

        // Send the messages
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));

        // Receive the messages.
        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull("m1 is null for index: " + i, m1);
            assertFalse(m1.isRedelivered());
        }

        // Close the consumer without acking.. this should cause re-delivery of
        // the messages.
        connection1.send(consumerInfo1.createRemoveCommand());

        // Create another consumer that should get the messages again.
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo2.setPrefetchSize(100);
        connection1.request(consumerInfo2);

        // Receive the messages.
        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull("m1 is null for index: " + i, m1);
            assertTrue(m1.isRedelivered());
        }
        assertNoMessagesLeft(connection1);

    }

    public void testTopicDurableSubscriptionCanBeRestored() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        connectionInfo1.setClientId("clientid1");
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setSubscriptionName("test");
        connection1.send(consumerInfo1);

        // Send the messages
        connection1.send(createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.send(createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.send(createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.request(createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));

        // Get the messages
        Message m = null;
        for (int i = 0; i < 2; i++) {
            m = receiveMessage(connection1);
            assertNotNull(m);
        }
        // Ack the last message.
        connection1.send(createAck(consumerInfo1, m, 2, MessageAck.STANDARD_ACK_TYPE));
        // Close the connection.
        connection1.request(closeConnectionInfo(connectionInfo1));
        connection1.stop();

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        connectionInfo2.setClientId("clientid1");
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(100);
        consumerInfo2.setSubscriptionName("test");

        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(consumerInfo2);

        // Get the rest of the messages
        for (int i = 0; i < 2; i++) {
            Message m1 = receiveMessage(connection2);
            assertNotNull("m1 is null for index: " + i, m1);
        }
        assertNoMessagesLeft(connection2);
    }

    public void initCombosForTestGroupedMessagesDeliveredToOnlyOneConsumer() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    public void testGroupedMessagesDeliveredToOnlyOneConsumer() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(1);
        connection1.send(consumerInfo1);

        // Send the messages.
        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination, deliveryMode);
            message.setGroupID("TEST-GROUP");
            message.setGroupSequence(i + 1);
            connection1.request(message);
        }

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);

        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        connection2.send(consumerInfo2);

        // All the messages should have been sent down connection 1.. just get
        // the first 3
        for (int i = 0; i < 3; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull("m1 is null for index: " + i, m1);
            connection1.send(createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // Close the first consumer.
        connection1.request(closeConsumerInfo(consumerInfo1));

        // The last messages should now go the the second consumer.
        for (int i = 0; i < 1; i++) {
            Message m1 = receiveMessage(connection2);
            assertNotNull("m1 is null for index: " + i, m1);
            connection2.request(createAck(consumerInfo2, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        assertNoMessagesLeft(connection2);
    }

    public void initCombosForTestTopicConsumerOnlySeeMessagesAfterCreation() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("durableConsumer", new Object[] {Boolean.TRUE, Boolean.FALSE});
    }

    public void testTopicConsumerOnlySeeMessagesAfterCreation() throws Exception {

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

        // Send the 1st message
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));

        // Create the durable subscription.
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        if (durableConsumer) {
            consumerInfo1.setSubscriptionName("test");
        }
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        Message m = createMessage(producerInfo1, destination, deliveryMode);
        connection1.send(m);
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));

        // Subscription should skip over the first message
        Message m2 = receiveMessage(connection1);
        assertNotNull(m2);
        assertEquals(m.getMessageId(), m2.getMessageId());
        m2 = receiveMessage(connection1);
        assertNotNull(m2);

        assertNoMessagesLeft(connection1);
    }

    public void initCombosForTestTopicRetroactiveConsumerSeeMessagesBeforeCreation() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("durableConsumer", new Object[] {Boolean.TRUE, Boolean.FALSE});
    }

    public void testTopicRetroactiveConsumerSeeMessagesBeforeCreation() throws Exception {

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

        // Send the messages
        Message m = createMessage(producerInfo1, destination, deliveryMode);
        connection1.send(m);

        // Create the durable subscription.
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        if (durableConsumer) {
            consumerInfo1.setSubscriptionName("test");
        }
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setRetroactive(true);
        connection1.send(consumerInfo1);

        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.request(createMessage(producerInfo1, destination, deliveryMode));

        // the behavior is VERY dependent on the recovery policy used.
        // But the default broker settings try to make it as consistent as
        // possible

        // Subscription should see all messages sent.
        Message m2 = receiveMessage(connection1);
        assertNotNull(m2);
        assertEquals(m.getMessageId(), m2.getMessageId());
        for (int i = 0; i < 2; i++) {
            m2 = receiveMessage(connection1);
            assertNotNull(m2);
        }

        assertNoMessagesLeft(connection1);
    }

    //
    // TODO: need to reimplement this since we don't fail when we send to a
    // non-existant
    // destination. But if we can access the Region directly then we should be
    // able to
    // check that if the destination was removed.
    // 
    // public void initCombosForTestTempDestinationsRemovedOnConnectionClose() {
    // addCombinationValues( "deliveryMode", new Object[]{
    // Integer.valueOf(DeliveryMode.NON_PERSISTENT),
    // Integer.valueOf(DeliveryMode.PERSISTENT)} );
    // addCombinationValues( "destinationType", new Object[]{
    // Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
    // Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)} );
    // }
    //    
    // public void testTempDestinationsRemovedOnConnectionClose() throws
    // Exception {
    //        
    // // Setup a first connection
    // StubConnection connection1 = createConnection();
    // ConnectionInfo connectionInfo1 = createConnectionInfo();
    // SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
    // ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
    // connection1.send(connectionInfo1);
    // connection1.send(sessionInfo1);
    // connection1.send(producerInfo1);
    //
    // destination = createDestinationInfo(connection1, connectionInfo1,
    // destinationType);
    //        
    // StubConnection connection2 = createConnection();
    // ConnectionInfo connectionInfo2 = createConnectionInfo();
    // SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
    // ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
    // connection2.send(connectionInfo2);
    // connection2.send(sessionInfo2);
    // connection2.send(producerInfo2);
    //
    // // Send from connection2 to connection1's temp destination. Should
    // succeed.
    // connection2.send(createMessage(producerInfo2, destination,
    // deliveryMode));
    //        
    // // Close connection 1
    // connection1.request(closeConnectionInfo(connectionInfo1));
    //        
    // try {
    // // Send from connection2 to connection1's temp destination. Should not
    // succeed.
    // connection2.request(createMessage(producerInfo2, destination,
    // deliveryMode));
    // fail("Expected JMSException.");
    // } catch ( JMSException success ) {
    // }
    //        
    // }

    // public void initCombosForTestTempDestinationsAreNotAutoCreated() {
    // addCombinationValues( "deliveryMode", new Object[]{
    // Integer.valueOf(DeliveryMode.NON_PERSISTENT),
    // Integer.valueOf(DeliveryMode.PERSISTENT)} );
    // addCombinationValues( "destinationType", new Object[]{
    // Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
    // Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)} );
    // }
    //    
    //   

    // We create temp destination on demand now so this test case is no longer
    // valid.
    //    
    // public void testTempDestinationsAreNotAutoCreated() throws Exception {
    //        
    // // Setup a first connection
    // StubConnection connection1 = createConnection();
    // ConnectionInfo connectionInfo1 = createConnectionInfo();
    // SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
    // ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
    // connection1.send(connectionInfo1);
    // connection1.send(sessionInfo1);
    // connection1.send(producerInfo1);
    //
    // destination =
    // ActiveMQDestination.createDestination(connectionInfo1.getConnectionId()+":1",
    // destinationType);
    //            
    // // Should not be able to send to a non-existant temp destination.
    // try {
    // connection1.request(createMessage(producerInfo1, destination,
    // deliveryMode));
    // fail("Expected JMSException.");
    // } catch ( JMSException success ) {
    // }
    //        
    // }

    
    public void initCombosForTestExclusiveQueueDeliversToOnlyOneConsumer() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    public void testExclusiveQueueDeliversToOnlyOneConsumer() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(1);
        consumerInfo1.setExclusive(true);
        connection1.send(consumerInfo1);

        // Send a message.. this should make consumer 1 the exclusive owner.
        connection1.request(createMessage(producerInfo, destination, deliveryMode));

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        consumerInfo2.setExclusive(true);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        // Second message should go to consumer 1 even though consumer 2 is
        // ready
        // for dispatch.
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));

        // Acknowledge the first 2 messages
        for (int i = 0; i < 2; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
            connection1.send(createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // Close the first consumer.
        connection1.send(closeConsumerInfo(consumerInfo1));

        // The last two messages should now go the the second consumer.
        connection1.send(createMessage(producerInfo, destination, deliveryMode));

        for (int i = 0; i < 2; i++) {
            Message m1 = receiveMessage(connection2);
            assertNotNull(m1);
            connection2.send(createAck(consumerInfo2, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        assertNoMessagesLeft(connection2);
    }

    public void initCombosForTestWildcardConsume() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testWildcardConsume() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        // setup the wildcard consumer.
        ActiveMQDestination compositeDestination = ActiveMQDestination.createDestination("WILD.*.TEST",
                                                                                         destinationType);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, compositeDestination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // These two message should NOT match the wild card.
        connection1.send(createMessage(producerInfo1, ActiveMQDestination.createDestination("WILD.CARD",
                                                                                            destinationType),
                                       deliveryMode));
        connection1.send(createMessage(producerInfo1, ActiveMQDestination.createDestination("WILD.TEST",
                                                                                            destinationType),
                                       deliveryMode));

        // These two message should match the wild card.
        ActiveMQDestination d1 = ActiveMQDestination.createDestination("WILD.CARD.TEST", destinationType);
        connection1.send(createMessage(producerInfo1, d1, deliveryMode));
        
        Message m = receiveMessage(connection1);
        assertNotNull(m);
        assertEquals(d1, m.getDestination());

        ActiveMQDestination d2 = ActiveMQDestination.createDestination("WILD.FOO.TEST", destinationType);
        connection1.request(createMessage(producerInfo1, d2, deliveryMode));
        m = receiveMessage(connection1);
        assertNotNull(m);
        assertEquals(d2, m.getDestination());

        assertNoMessagesLeft(connection1);
        connection1.send(closeConnectionInfo(connectionInfo1));
    }

    public void initCombosForTestCompositeConsume() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testCompositeConsume() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        // setup the composite consumer.
        ActiveMQDestination compositeDestination = ActiveMQDestination.createDestination("A,B",
                                                                                         destinationType);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, compositeDestination);
        consumerInfo1.setRetroactive(true);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Publish to the two destinations
        ActiveMQDestination destinationA = ActiveMQDestination.createDestination("A", destinationType);
        ActiveMQDestination destinationB = ActiveMQDestination.createDestination("B", destinationType);

        // Send a message to each destination .
        connection1.send(createMessage(producerInfo1, destinationA, deliveryMode));
        connection1.send(createMessage(producerInfo1, destinationB, deliveryMode));

        // The consumer should get both messages.
        for (int i = 0; i < 2; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
        }

        assertNoMessagesLeft(connection1);
        connection1.send(closeConnectionInfo(connectionInfo1));
    }

    public void initCombosForTestCompositeSend() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testCompositeSend() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ActiveMQDestination destinationA = ActiveMQDestination.createDestination("A", destinationType);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destinationA);
        consumerInfo1.setRetroactive(true);
        consumerInfo1.setPrefetchSize(100);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);

        ActiveMQDestination destinationB = ActiveMQDestination.createDestination("B", destinationType);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destinationB);
        consumerInfo2.setRetroactive(true);
        consumerInfo2.setPrefetchSize(100);
        connection2.request(consumerInfo2);

        // Send the messages to the composite destination.
        ActiveMQDestination compositeDestination = ActiveMQDestination.createDestination("A,B",
                                                                                         destinationType);
        for (int i = 0; i < 4; i++) {
            connection1.request(createMessage(producerInfo1, compositeDestination, deliveryMode));
        }

        // The messages should have been delivered to both the A and B
        // destination.
        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            Message m2 = receiveMessage(connection2);

            assertNotNull(m1);
            assertNotNull(m2);

            assertEquals(m1.getMessageId(), m2.getMessageId());
            assertEquals(compositeDestination, m1.getOriginalDestination());
            assertEquals(compositeDestination, m2.getOriginalDestination());

            connection1.request(createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
            connection2.request(createAck(consumerInfo2, m2, 1, MessageAck.STANDARD_ACK_TYPE));

        }

        assertNoMessagesLeft(connection1);
        assertNoMessagesLeft(connection2);

        connection1.send(closeConnectionInfo(connectionInfo1));
        connection2.send(closeConnectionInfo(connectionInfo2));
    }

    public void initCombosForTestConnectionCloseCascades() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destination", new Object[] {new ActiveMQTopic("TEST"),
                                                          new ActiveMQQueue("TEST")});
    }

    public void testConnectionCloseCascades() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setNoLocal(true);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        // Send the messages
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));

        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
            connection1.send(createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // give the async ack a chance to perculate and validate all are currently consumed
        assertNull(connection1.getDispatchQueue().poll(MAX_NULL_WAIT, TimeUnit.MILLISECONDS));

        // Close the connection, this should in turn close the consumer.
        connection1.request(closeConnectionInfo(connectionInfo1));

        // Send another message, connection1 should not get the message.
        connection2.request(createMessage(producerInfo2, destination, deliveryMode));

        assertNull(connection1.getDispatchQueue().poll(MAX_NULL_WAIT, TimeUnit.MILLISECONDS));
    }

    public void initCombosForTestSessionCloseCascades() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destination", new Object[] {new ActiveMQTopic("TEST"),
                                                          new ActiveMQQueue("TEST")});
    }

    public void testSessionCloseCascades() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setNoLocal(true);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        // Send the messages
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));

        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
            connection1.send(createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // Close the session, this should in turn close the consumer.
        connection1.request(closeSessionInfo(sessionInfo1));

        // Send another message, connection1 should not get the message.
        connection2.request(createMessage(producerInfo2, destination, deliveryMode));

        assertNull(connection1.getDispatchQueue().poll(MAX_NULL_WAIT, TimeUnit.MILLISECONDS));
    }

    public void initCombosForTestConsumerClose() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destination", new Object[] {new ActiveMQTopic("TEST"),
                                                          new ActiveMQQueue("TEST")});
    }

    public void testConsumerClose() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setNoLocal(true);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        // Send the messages
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));
        connection2.send(createMessage(producerInfo2, destination, deliveryMode));

        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
            connection1.send(createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // give the async ack a chance to perculate and validate all are currently consumed
        Object result = connection1.getDispatchQueue().poll(MAX_NULL_WAIT, TimeUnit.MILLISECONDS);
        assertNull("no more messages " + result, result);
 
        // Close the consumer.
        connection1.request(closeConsumerInfo(consumerInfo1));

        // Send another message, connection1 should not get the message.
        connection2.request(createMessage(producerInfo2, destination, deliveryMode));

        assertNull(connection1.getDispatchQueue().poll(MAX_NULL_WAIT, TimeUnit.MILLISECONDS));
    }

    public void initCombosForTestTopicNoLocal() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    public void testTopicNoLocal() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setRetroactive(true);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setNoLocal(true);
        connection1.send(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setRetroactive(true);
        consumerInfo2.setPrefetchSize(100);
        consumerInfo2.setNoLocal(true);
        connection2.send(consumerInfo2);

        // Send the messages
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));

        // The 2nd connection should get the messages.
        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection2);
            assertNotNull(m1);
        }

        // Send a message with the 2nd connection
        Message message = createMessage(producerInfo2, destination, deliveryMode);
        connection2.send(message);

        // The first connection should not see the initial 4 local messages sent
        // but should
        // see the messages from connection 2.
        Message m = receiveMessage(connection1);
        assertNotNull(m);
        assertEquals(message.getMessageId(), m.getMessageId());

        assertNoMessagesLeft(connection1);
        assertNoMessagesLeft(connection2);
    }

    public void initCombosForTopicDispatchIsBroadcast() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    public void testTopicDispatchIsBroadcast() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setRetroactive(true);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setRetroactive(true);
        consumerInfo2.setPrefetchSize(100);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(consumerInfo2);

        // Send the messages
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.send(createMessage(producerInfo1, destination, deliveryMode));

        // Get the messages
        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
            m1 = receiveMessage(connection2);
            assertNotNull(m1);
        }
    }

    public void initCombosForTestQueueDispatchedAreRedeliveredOnConsumerClose() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE)});
    }

    public void testQueueDispatchedAreRedeliveredOnConsumerClose() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Send the messages
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));
        connection1.send(createMessage(producerInfo, destination, deliveryMode));

        // Get the messages
        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
            assertFalse(m1.isRedelivered());
        }
        // Close the consumer without sending any ACKS.
        connection1.send(closeConsumerInfo(consumerInfo1));

        // Drain any in flight messages..
        while (connection1.getDispatchQueue().poll(0, TimeUnit.MILLISECONDS) != null) {
        }

        // Add the second consumer
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo2.setPrefetchSize(100);
        connection1.send(consumerInfo2);

        // Make sure the messages were re delivered to the 2nd consumer.
        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
            assertTrue(m1.isRedelivered());
        }
    }

    public void initCombosForTestQueueBrowseMessages() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE)});
    }

    public void testQueueBrowseMessages() throws Exception {

        // Start a producer and consumer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, destinationType);

        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.send(createMessage(producerInfo, destination, deliveryMode));

        // Use selector to skip first message.
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setBrowser(true);
        connection.send(consumerInfo);

        for (int i = 0; i < 4; i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
            connection.send(createAck(consumerInfo, m, 1, MessageAck.DELIVERED_ACK_TYPE));
        }

        assertNoMessagesLeft(connection);
    }

    public void initCombosForTestQueueSendThenAddConsumer() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE)});
    }

    public void testQueueSendThenAddConsumer() throws Exception {

        // Start a producer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, destinationType);

        // Send a message to the broker.
        connection.send(createMessage(producerInfo, destination, deliveryMode));

        // Start the consumer
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Make sure the message was delivered.
        Message m = receiveMessage(connection);
        assertNotNull(m);

    }

    public void initCombosForTestQueueAckRemovesMessage() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE)});
    }

    public void testQueueAckRemovesMessage() throws Exception {

        // Start a producer and consumer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, destinationType);

        Message message1 = createMessage(producerInfo, destination, deliveryMode);
        Message message2 = createMessage(producerInfo, destination, deliveryMode);
        connection.send(message1);
        connection.send(message2);

        // Make sure the message was delivered.
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.request(consumerInfo);
        Message m = receiveMessage(connection);
        assertNotNull(m);
        assertEquals(m.getMessageId(), message1.getMessageId());

        assertTrue(countMessagesInQueue(connection, connectionInfo, destination) == 2);
        connection.send(createAck(consumerInfo, m, 1, MessageAck.DELIVERED_ACK_TYPE));
        assertTrue(countMessagesInQueue(connection, connectionInfo, destination) == 2);
        connection.send(createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));
        assertTrue(countMessagesInQueue(connection, connectionInfo, destination) == 1);

    }

    public void initCombosForTestSelectorSkipsMessages() {
        addCombinationValues("destination", new Object[] {new ActiveMQTopic("TEST_TOPIC"),
                                                          new ActiveMQQueue("TEST_QUEUE")});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testSelectorSkipsMessages() throws Exception {

        // Start a producer and consumer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, destinationType);

        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSelector("JMSType='last'");
        connection.send(consumerInfo);

        Message message1 = createMessage(producerInfo, destination, deliveryMode);
        message1.setType("first");
        Message message2 = createMessage(producerInfo, destination, deliveryMode);
        message2.setType("last");
        connection.send(message1);
        connection.send(message2);

        // Use selector to skip first message.
        Message m = receiveMessage(connection);
        assertNotNull(m);
        assertEquals(m.getMessageId(), message2.getMessageId());
        connection.send(createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));
        connection.send(closeConsumerInfo(consumerInfo));

        assertNoMessagesLeft(connection);
    }

    public void initCombosForTestAddConsumerThenSend() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testAddConsumerThenSend() throws Exception {

        // Start a producer and consumer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, destinationType);

        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        connection.send(createMessage(producerInfo, destination, deliveryMode));

        // Make sure the message was delivered.
        Message m = receiveMessage(connection);
        assertNotNull(m);
    }

    public void initCombosForTestConsumerPrefetchAtOne() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testConsumerPrefetchAtOne() throws Exception {

        // Start a producer and consumer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, destinationType);

        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setPrefetchSize(1);
        connection.send(consumerInfo);

        // Send 2 messages to the broker.
        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.send(createMessage(producerInfo, destination, deliveryMode));

        // Make sure only 1 message was delivered.
        Message m = receiveMessage(connection);
        assertNotNull(m);
        assertNoMessagesLeft(connection);

    }

    public void initCombosForTestConsumerPrefetchAtTwo() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testConsumerPrefetchAtTwo() throws Exception {

        // Start a producer and consumer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, destinationType);

        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setPrefetchSize(2);
        connection.send(consumerInfo);

        // Send 3 messages to the broker.
        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.send(createMessage(producerInfo, destination, deliveryMode));

        // Make sure only 1 message was delivered.
        Message m = receiveMessage(connection);
        assertNotNull(m);
        m = receiveMessage(connection);
        assertNotNull(m);
        assertNoMessagesLeft(connection);

    }

    public void initCombosForTestConsumerPrefetchAndDeliveredAck() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testConsumerPrefetchAndDeliveredAck() throws Exception {

        // Start a producer and consumer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, destinationType);

        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setPrefetchSize(1);
        connection.request(consumerInfo);

        // Send 3 messages to the broker.
        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.send(createMessage(producerInfo, destination, deliveryMode));
        connection.request(createMessage(producerInfo, destination, deliveryMode));

        // Make sure only 1 message was delivered.
        Message m1 = receiveMessage(connection);
        assertNotNull(m1);

        assertNoMessagesLeft(connection);

        // Acknowledge the first message. This should cause the next message to
        // get dispatched.
        connection.request(createAck(consumerInfo, m1, 1, MessageAck.DELIVERED_ACK_TYPE));

        Message m2 = receiveMessage(connection);
        assertNotNull(m2);
        connection.request(createAck(consumerInfo, m2, 1, MessageAck.DELIVERED_ACK_TYPE));

        Message m3 = receiveMessage(connection);
        assertNotNull(m3);
        connection.request(createAck(consumerInfo, m3, 1, MessageAck.DELIVERED_ACK_TYPE));
    }

    public void testGetServices() throws Exception {
        assertTrue(broker.getServices().length != 0);
    }
    
    public static Test suite() {
        return suite(BrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
