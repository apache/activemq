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
package org.apache.activemq.broker.replica;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageAck;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class ReplicaMessagePropertyTest extends ReplicaPluginTestSupport {

    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();
    }

    @Override
    protected void tearDown() throws Exception {
        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }

        super.tearDown();
    }

    @Test
    public void testNonPersistentMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination);
        firstBrokerProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message secondBrokerReceivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(secondBrokerReceivedMessage);

        Message firstBrokerReceivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(firstBrokerReceivedMessage);
        assertTrue(firstBrokerReceivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) firstBrokerReceivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testMessagePriority() throws Exception {
        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }

        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress + "?jms.messagePrioritySupported=true");
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress + "?jms.messagePrioritySupported=true");
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        for (int i = 1; i <= 3; i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName() + "No." + i);
            firstBrokerProducer.send(message, DeliveryMode.PERSISTENT, i, 0);
        }

        Thread.sleep(LONG_TIMEOUT);

        for (int i = 3; i >=1; i--) {
            Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals(getName() + "No." + i, ((TextMessage) receivedMessage).getText());
        }

        assertNull(secondBrokerConsumer.receive(SHORT_TIMEOUT));

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testMessageWithJMSXGroupID() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumerA = secondBrokerSession.createConsumer(destination);
        MessageConsumer secondBrokerConsumerB = secondBrokerSession.createConsumer(destination);

        int messagesToSendNum = 20;
        for (int i = 0; i < messagesToSendNum; i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName() + "No." + i);
            if (i % 2 == 0) {
                message.setStringProperty("JMSXGroupID", "Group-A");
            } else {
                message.setStringProperty("JMSXGroupID", "Group-B");
            }
            firstBrokerProducer.send(message);
        }

        Thread.sleep(LONG_TIMEOUT);

        for (int i =0; i < messagesToSendNum/2; i++) {
            Message consumerAReceivedMessage = secondBrokerConsumerA.receive(LONG_TIMEOUT);
            assertNotNull(consumerAReceivedMessage);
            assertTrue(consumerAReceivedMessage instanceof TextMessage);
            assertTrue(((TextMessage) consumerAReceivedMessage).getText().contains(getName()));
            assertEquals(consumerAReceivedMessage.getStringProperty("JMSXGroupID"), "Group-A");

            Message consumerBReceivedMessage = secondBrokerConsumerB.receive(LONG_TIMEOUT);
            assertNotNull(consumerBReceivedMessage);
            assertTrue(consumerBReceivedMessage instanceof TextMessage);
            assertTrue(((TextMessage) consumerBReceivedMessage).getText().contains(getName()));
            assertEquals(consumerBReceivedMessage.getStringProperty("JMSXGroupID"), "Group-B");
        }

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testExpiredAcknowledgeReplication() throws Exception {
        ActiveMQSession firstBrokerSession = (ActiveMQSession) firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        ActiveMQMessageConsumer firstBrokerConsumer =  (ActiveMQMessageConsumer) firstBrokerSession.createConsumer(destination);
        ActiveMQSession secondBrokerSession = (ActiveMQSession) secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);


        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean secondBrokerDestinationQueueView = getQueueView(secondBroker, destination.getPhysicalName());
        assertEquals(secondBrokerDestinationQueueView.browseMessages().size(), 1);

        ActiveMQMessage receivedMessage = (ActiveMQMessage) firstBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);
        MessageAck ack = new MessageAck(receivedMessage, MessageAck.EXPIRED_ACK_TYPE, 1);
        ack.setFirstMessageId(receivedMessage.getMessageId());
        ack.setConsumerId(firstBrokerConsumer.getConsumerId());
        firstBrokerSession.syncSendPacket(ack);

        assertEquals(secondBrokerDestinationQueueView.getDequeueCount(), 0);
        assertEquals(secondBrokerDestinationQueueView.getEnqueueCount(), 1);

        receivedMessage = (ActiveMQMessage) secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);

        firstBrokerSession.close();
    }

    @Test
    public void testPoisonAcknowledgeReplication() throws Exception {
        ActiveMQSession firstBrokerSession = (ActiveMQSession) firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        ActiveMQMessageConsumer firstBrokerConsumer =  (ActiveMQMessageConsumer) firstBrokerSession.createConsumer(destination);
        ActiveMQSession secondBrokerSession = (ActiveMQSession) secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean secondBrokerDestinationQueueView = getQueueView(secondBroker, destination.getPhysicalName());
        assertEquals(secondBrokerDestinationQueueView.browseMessages().size(), 1);

        ActiveMQMessage receivedMessage = (ActiveMQMessage) firstBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);
        MessageAck ack = new MessageAck(receivedMessage, MessageAck.EXPIRED_ACK_TYPE, 1);
        ack.setFirstMessageId(receivedMessage.getMessageId());
        ack.setConsumerId(firstBrokerConsumer.getConsumerId());
        firstBrokerSession.syncSendPacket(ack);

        assertEquals(secondBrokerDestinationQueueView.getDequeueCount(), 0);
        assertEquals(secondBrokerDestinationQueueView.getEnqueueCount(), 1);

        receivedMessage = (ActiveMQMessage) secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);

        firstBrokerSession.close();
    }

    @Test
    public void testReDeliveredAcknowledgeReplication() throws Exception {
        ActiveMQSession firstBrokerSession = (ActiveMQSession) firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        ActiveMQMessageConsumer firstBrokerConsumer =  (ActiveMQMessageConsumer) firstBrokerSession.createConsumer(destination);
        ActiveMQSession secondBrokerSession = (ActiveMQSession) secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean secondBrokerDestinationQueueView = getQueueView(secondBroker, destination.getPhysicalName());
        assertEquals(secondBrokerDestinationQueueView.browseMessages().size(), 1);

        ActiveMQMessage receivedMessage = (ActiveMQMessage) firstBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);
        MessageAck ack = new MessageAck(receivedMessage, MessageAck.REDELIVERED_ACK_TYPE, 1);
        ack.setFirstMessageId(receivedMessage.getMessageId());
        ack.setConsumerId(firstBrokerConsumer.getConsumerId());
        firstBrokerSession.syncSendPacket(ack);

        assertEquals(secondBrokerDestinationQueueView.getDequeueCount(), 0);
        assertEquals(secondBrokerDestinationQueueView.getEnqueueCount(), 1);

        receivedMessage = (ActiveMQMessage) secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);

        firstBrokerSession.close();
    }
}
