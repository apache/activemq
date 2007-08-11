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

import javax.jms.DeliveryMode;

import junit.framework.Test;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;

public class MessageExpirationTest extends BrokerTestSupport {

    public ActiveMQDestination destination;
    public int deliveryMode;
    public int prefetch;
    public byte destinationType;
    public boolean durableConsumer;

    protected Message createMessage(ProducerInfo producerInfo, ActiveMQDestination destination, int deliveryMode, int timeToLive) {
        Message message = createMessage(producerInfo, destination, deliveryMode);
        long now = System.currentTimeMillis();
        message.setTimestamp(now);
        message.setExpiration(now + timeToLive);
        return message;
    }

    public void initCombosForTestMessagesWaitingForUssageDecreaseExpire() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        return broker;
    }

    protected PolicyEntry getDefaultPolicy() {
        PolicyEntry policy = super.getDefaultPolicy();
        // disable spooling
        policy.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());
        return policy;
    }

    public void testMessagesWaitingForUssageDecreaseExpire() throws Exception {

        // Start a producer
        final StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        final ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // Start a consumer..
        final StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);

        destination = createDestinationInfo(connection2, connectionInfo2, destinationType);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        connection2.request(consumerInfo2);

        // Reduce the limit so that only 1 message can flow through the broker
        // at a time.
        broker.getMemoryManager().setLimit(1);

        final Message m1 = createMessage(producerInfo, destination, deliveryMode);
        final Message m2 = createMessage(producerInfo, destination, deliveryMode, 1000);
        final Message m3 = createMessage(producerInfo, destination, deliveryMode);
        final Message m4 = createMessage(producerInfo, destination, deliveryMode, 1000);

        // Produce in an async thread since the producer will be getting blocked
        // by the usage manager..
        new Thread() {
            public void run() {
                // m1 and m3 should not expire.. but the others should.
                try {
                    connection.send(m1);
                    connection.send(m2);
                    connection.send(m3);
                    connection.send(m4);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        // Make sure only 1 message was delivered due to prefetch == 1
        Message m = receiveMessage(connection2);
        assertNotNull(m);
        assertEquals(m1.getMessageId(), m.getMessageId());
        assertNoMessagesLeft(connection);

        // Sleep before we ack so that the messages expire on the usage manager
        Thread.sleep(1500);
        connection2.send(createAck(consumerInfo2, m, 1, MessageAck.STANDARD_ACK_TYPE));

        // 2nd message received should be m3.. it should have expired 2nd
        // message sent.
        m = receiveMessage(connection2);
        assertNotNull(m);
        assertEquals(m3.getMessageId(), m.getMessageId());

        // Sleep before we ack so that the messages expire on the usage manager
        Thread.sleep(1500);
        connection2.send(createAck(consumerInfo2, m, 1, MessageAck.STANDARD_ACK_TYPE));

        // And there should be no messages left now..
        assertNoMessagesLeft(connection2);

        connection.send(closeConnectionInfo(connectionInfo));
        connection.send(closeConnectionInfo(connectionInfo2));
    }

    /**
     * Small regression. Looks like persistent messages to a queue are not being
     * timedout when in a long transaction. See:
     * http://issues.apache.org/activemq/browse/AMQ-1269 Commenting out the
     * DeliveryMode.PERSISTENT test combination for now.
     */
    public void initCombosForTestMessagesInLongTransactionExpire() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testMessagesInLongTransactionExpire() throws Exception {

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
        consumerInfo.setPrefetchSize(1000);
        connection.send(consumerInfo);

        // Start the tx..
        LocalTransactionId txid = createLocalTransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        // m1 and m3 should not expire.. but the others should.
        Message m1 = createMessage(producerInfo, destination, deliveryMode);
        m1.setTransactionId(txid);
        connection.send(m1);
        Message m = createMessage(producerInfo, destination, deliveryMode, 1000);
        m.setTransactionId(txid);
        connection.send(m);
        Message m3 = createMessage(producerInfo, destination, deliveryMode);
        m3.setTransactionId(txid);
        connection.send(m3);
        m = createMessage(producerInfo, destination, deliveryMode, 1000);
        m.setTransactionId(txid);
        connection.send(m);

        // Sleep before we commit so that the messages expire on the commit
        // list..
        Thread.sleep(1500);
        connection.send(createCommitTransaction1Phase(connectionInfo, txid));

        m = receiveMessage(connection);
        assertNotNull(m);
        assertEquals(m1.getMessageId(), m.getMessageId());
        connection.send(createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));

        // 2nd message received should be m3.. it should have expired 2nd
        // message sent.
        m = receiveMessage(connection);
        assertNotNull(m);
        assertEquals(m3.getMessageId(), m.getMessageId());
        connection.send(createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));

        // And there should be no messages left now..
        assertNoMessagesLeft(connection);

        connection.send(closeConnectionInfo(connectionInfo));
    }

    public void xinitCombosForTestMessagesInSubscriptionPendingListExpire() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void initCombosForTestMessagesInSubscriptionPendingListExpire() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                                              Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE), Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testMessagesInSubscriptionPendingListExpire() throws Exception {

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

        // m1 and m3 should not expire.. but the others should.
        Message m1 = createMessage(producerInfo, destination, deliveryMode);
        connection.send(m1);
        connection.send(createMessage(producerInfo, destination, deliveryMode, 1000));
        Message m3 = createMessage(producerInfo, destination, deliveryMode);
        connection.send(m3);
        connection.send(createMessage(producerInfo, destination, deliveryMode, 1000));

        // Make sure only 1 message was delivered due to prefetch == 1
        Message m = receiveMessage(connection);
        assertNotNull(m);
        assertEquals(m1.getMessageId(), m.getMessageId());
        assertNoMessagesLeft(connection);

        // Sleep before we ack so that the messages expire on the pending list..
        Thread.sleep(1500);
        connection.send(createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));

        // 2nd message received should be m3.. it should have expired 2nd
        // message sent.
        m = receiveMessage(connection);
        assertNotNull(m);
        assertEquals(m3.getMessageId(), m.getMessageId());
        connection.send(createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));

        // And there should be no messages left now..
        assertNoMessagesLeft(connection);

        connection.send(closeConnectionInfo(connectionInfo));
    }

    public static Test suite() {
        return suite(MessageExpirationTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
