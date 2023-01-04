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
package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.MessageReferenceFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.SubscriptionStatistics;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.IOHelper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaBrokerEventListenerTest {

    private final Broker broker = mock(Broker.class);
    private final ActiveMQQueue testQueue = new ActiveMQQueue("TEST.QUEUE");
    private final ActiveMQTopic testTopic = new ActiveMQTopic("TEST.TOPIC");
    private final Destination destinationQueue = mock(Queue.class);
    private final Destination destinationTopic = mock(Topic.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);

    private ReplicaBrokerEventListener listener;
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    @Before
    public void setUp() throws Exception {
        ConnectionContext adminConnectionContext = mock(ConnectionContext.class);
        when(adminConnectionContext.copy()).thenReturn(connectionContext);
        when(broker.getAdminConnectionContext()).thenReturn(adminConnectionContext);
        when(broker.getDestinations(testQueue)).thenReturn(Set.of(destinationQueue));
        when(broker.getDestinations(testTopic)).thenReturn(Set.of(destinationTopic));
        when(connectionContext.isProducerFlowControl()).thenReturn(true);
        when(connectionContext.copy()).thenReturn(new ConnectionContext());
        when(connectionContext.getUserName()).thenReturn(ReplicaSupport.REPLICATION_PLUGIN_USER_NAME);
        BrokerService brokerService = mock(BrokerService.class);
        when(broker.getBrokerService()).thenReturn(brokerService);
        File brokerDataDirectory = new File(IOHelper.getDefaultDataDirectory());
        when(brokerService.getBrokerDataDirectory()).thenReturn(brokerDataDirectory);

        listener = new ReplicaBrokerEventListener(broker);
        listener.initialize();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_UPSERT_whenQueueNotExist() throws Exception {
        listener.sequence = null;
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(replicaEventMessage);

        verify(broker).addDestination(connectionContext, testQueue, true);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_UPSERT_whenQueueExists() throws Exception {
        listener.sequence = null;
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination, testQueue});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(replicaEventMessage);

        verify(broker, never()).addDestination(connectionContext, testQueue, true);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_DELETE_whenDestinationExists() throws Exception {
        listener.sequence = null;
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination, testQueue});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.DESTINATION_DELETE)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(replicaEventMessage);

        verify(broker).removeDestination(connectionContext, testQueue, 1000);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_DELETE_whenDestinationNotExists() throws Exception {
        listener.sequence = null;
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.DESTINATION_DELETE)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(replicaEventMessage);

        verify(broker, never()).removeDestination(connectionContext, testQueue, 1000);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_SEND() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_SEND)
                .setEventData(eventSerializer.serializeMessageData(message));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(replicaEventMessage);

        verify(broker).getAdminConnectionContext();
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());

        ActiveMQMessage value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(message);

        verify(connectionContext).isProducerFlowControl();
        verify(connectionContext).setProducerFlowControl(false);
        verify(connectionContext).setProducerFlowControl(true);

        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_ACK_forQueue() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1:1:1");

        MessageAck ack = new MessageAck();
        ConsumerId consumerId = new ConsumerId("2:2:2:2");
        ack.setConsumerId(consumerId);
        ack.setDestination(testQueue);

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_ACK)
                .setEventData(eventSerializer.serializeReplicationData(ack))
                .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, Collections.singletonList(messageId.toString()));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setProperties(event.getReplicationProperties());

        listener.onMessage(replicaEventMessage);

        ArgumentCaptor<ConsumerInfo> ciArgumentCaptor = ArgumentCaptor.forClass(ConsumerInfo.class);
        verify(broker).addConsumer(any(), ciArgumentCaptor.capture());
        ConsumerInfo consumerInfo = ciArgumentCaptor.getValue();
        assertThat(consumerInfo.getConsumerId()).isEqualTo(consumerId);
        assertThat(consumerInfo.getDestination()).isEqualTo(testQueue);


        ArgumentCaptor<MessageDispatchNotification> mdnArgumentCaptor = ArgumentCaptor.forClass(MessageDispatchNotification.class);
        verify(broker).processDispatchNotification(mdnArgumentCaptor.capture());

        MessageDispatchNotification mdn = mdnArgumentCaptor.getValue();
        assertThat(mdn.getMessageId()).isEqualTo(messageId);
        assertThat(mdn.getDestination()).isEqualTo(testQueue);
        assertThat(mdn.getConsumerId()).isEqualTo(consumerId);

        ArgumentCaptor<MessageAck> ackArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(broker).acknowledge(any(), ackArgumentCaptor.capture());

        MessageAck value = ackArgumentCaptor.getValue();
        assertThat(value.getDestination()).isEqualTo(testQueue);
        assertThat(value.getConsumerId()).isEqualTo(consumerId);

        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_QUEUE_PURGED() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1:1:1");
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.QUEUE_PURGED)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setProperties(event.getReplicationProperties());

        listener.onMessage(replicaEventMessage);

        verify((Queue) destinationQueue).purge(any());

        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_BEGIN() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_BEGIN)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).beginTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_PREPARE() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_PREPARE)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).prepareTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_FORGET() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_FORGET)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).forgetTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_ROLLBACK() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_ROLLBACK)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).rollbackTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_COMMIT() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_COMMIT)
                .setEventData(eventSerializer.serializeReplicationData(transactionId))
                .setReplicationProperty(ReplicaSupport.TRANSACTION_ONE_PHASE_PROPERTY, true);
        message.setContent(event.getEventData());
        message.setProperties(event.getReplicationProperties());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        ArgumentCaptor<Boolean> onePhaseArgumentCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(broker).commitTransaction(any(), messageArgumentCaptor.capture(), onePhaseArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        Boolean onePhase = onePhaseArgumentCaptor.getValue();
        assertThat(onePhase).isTrue();
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_ADD_DURABLE_CONSUMER() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1");
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(testQueue);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        String clientId = "clientId";
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.ADD_DURABLE_CONSUMER)
                .setEventData(eventSerializer.serializeReplicationData(consumerInfo))
                .setReplicationProperty(ReplicaSupport.CLIENT_ID_PROPERTY, clientId);
        message.setContent(event.getEventData());
        message.setProperties(event.getReplicationProperties());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        DurableTopicSubscription subscription = mock(DurableTopicSubscription.class);
        when(broker.addConsumer(any(), any())).thenReturn(subscription);

        listener.onMessage(message);

        ArgumentCaptor<ConsumerInfo> messageArgumentCaptor = ArgumentCaptor.forClass(ConsumerInfo.class);
        ArgumentCaptor<ConnectionContext> connectionContextArgumentCaptor = ArgumentCaptor.forClass(ConnectionContext.class);
        verify(broker).addConsumer(connectionContextArgumentCaptor.capture(), messageArgumentCaptor.capture());
        ConsumerInfo value = messageArgumentCaptor.getValue();
        assertThat(value.getDestination()).isEqualTo(testQueue);
        ConnectionContext connectionContext = connectionContextArgumentCaptor.getValue();
        assertThat(connectionContext.getClientId()).isEqualTo(clientId);
        verify(subscription).deactivate(true, 0);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_REMOVE_DURABLE_CONSUMER() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1");
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(testQueue);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        String clientId = "clientId";
        consumerInfo.setClientId(clientId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.REMOVE_DURABLE_CONSUMER)
                .setEventData(eventSerializer.serializeReplicationData(consumerInfo));
        message.setContent(event.getEventData());
        message.setProperties(event.getReplicationProperties());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        DurableTopicSubscription subscription = mock(DurableTopicSubscription.class);
        when(destinationQueue.getConsumers()).thenReturn(Collections.singletonList(subscription));
        when(subscription.getConsumerInfo()).thenReturn(consumerInfo);
        when(subscription.getContext()).thenReturn(connectionContext);

        listener.onMessage(message);

        ArgumentCaptor<ConsumerInfo> messageArgumentCaptor = ArgumentCaptor.forClass(ConsumerInfo.class);
        verify(broker).removeConsumer(any(), messageArgumentCaptor.capture());
        ConsumerInfo value = messageArgumentCaptor.getValue();
        assertThat(value.getDestination()).isEqualTo(testQueue);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_ACK_forTopic() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1:1:1");

        MessageAck ack = new MessageAck();
        ConsumerId consumerId = new ConsumerId("2:2:2:2");
        ack.setConsumerId(consumerId);
        ack.setDestination(testTopic);

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_ACK)
                .setEventData(eventSerializer.serializeReplicationData(ack))
                .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, Collections.singletonList(messageId.toString()));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setProperties(event.getReplicationProperties());

        listener.onMessage(replicaEventMessage);

        verify(broker, never()).addConsumer(any(), any());

        ArgumentCaptor<MessageDispatchNotification> mdnArgumentCaptor = ArgumentCaptor.forClass(MessageDispatchNotification.class);
        verify(broker).processDispatchNotification(mdnArgumentCaptor.capture());

        MessageDispatchNotification mdn = mdnArgumentCaptor.getValue();
        assertThat(mdn.getMessageId()).isEqualTo(messageId);
        assertThat(mdn.getDestination()).isEqualTo(testTopic);
        assertThat(mdn.getConsumerId()).isEqualTo(consumerId);

        ArgumentCaptor<MessageAck> ackArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(broker).acknowledge(any(), ackArgumentCaptor.capture());

        MessageAck value = ackArgumentCaptor.getValue();
        assertThat(value.getDestination()).isEqualTo(ack.getDestination());
        assertThat(value.getConsumerId()).isEqualTo(ack.getConsumerId());

        verify(replicaEventMessage).acknowledge();
    }
}
