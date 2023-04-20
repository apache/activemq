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
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
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
import org.apache.activemq.replica.storage.ReplicaFailOverStateStorage;
import org.apache.activemq.util.IOHelper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaBrokerEventListenerTest {

    private final MutativeRoleBroker broker = mock(MutativeRoleBroker.class);
    private final ActiveMQQueue sequenceQueue = new ActiveMQQueue(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
    private final ActiveMQQueue testQueue = new ActiveMQQueue("TEST.QUEUE");
    private final ActiveMQTopic testTopic = new ActiveMQTopic("TEST.TOPIC");
    private final Destination sequenceDstinationQueue = mock(Queue.class);
    private final Destination destinationQueue = mock(Queue.class);
    private final Destination destinationTopic = mock(Topic.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final ReplicaReplicationQueueSupplier queueProvider = mock(ReplicaReplicationQueueSupplier.class);
    private final ReplicaFailOverStateStorage replicaFailOverStateStorage = mock(ReplicaFailOverStateStorage.class);
    private final ActionListenerCallback actionListenerCallback = mock(ActionListenerCallback.class);
    private final PrefetchSubscription subscription = mock(PrefetchSubscription.class);

    private ReplicaBrokerEventListener listener;
    private PeriodAcknowledge acknowledgeCallback;
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
        when(queueProvider.getSequenceQueue()).thenReturn(sequenceQueue);
        when(broker.getDestinations(sequenceQueue)).thenReturn(Set.of(sequenceDstinationQueue));
        when(broker.addConsumer(any(), any())).thenReturn(subscription);
        acknowledgeCallback = new PeriodAcknowledge(new ReplicaPolicy());
        listener = new ReplicaBrokerEventListener(broker, queueProvider, acknowledgeCallback, actionListenerCallback, replicaFailOverStateStorage);
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
        verify(broker, times(2)).send(any(), messageArgumentCaptor.capture());

        List<ActiveMQMessage> values = messageArgumentCaptor.getAllValues();
        assertThat(values.get(0).getMessageId()).isEqualTo(message.getMessageId());
        assertThat(values.get(1).getDestination()).isEqualTo(sequenceQueue);

        verify(connectionContext, times(2)).isProducerFlowControl();
        verify(connectionContext, times(2)).setProducerFlowControl(false);
        verify(connectionContext, times(2)).setProducerFlowControl(true);
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
        verify(broker, times(2)).addConsumer(any(), ciArgumentCaptor.capture());
        List<ConsumerInfo> consumerInfos = ciArgumentCaptor.getAllValues();
        assertThat(consumerInfos.get(0).getDestination()).isEqualTo(sequenceQueue);
        assertThat(consumerInfos.get(1).getConsumerId()).isEqualTo(consumerId);
        assertThat(consumerInfos.get(1).getDestination()).isEqualTo(testQueue);


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
        verify(broker, times(2)).beginTransaction(any(), messageArgumentCaptor.capture());
        List<TransactionId> values = messageArgumentCaptor.getAllValues();
        assertThat(values.get(0)).isNotEqualTo(transactionId);
        assertThat(values.get(1)).isEqualTo(transactionId);
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
        verify(broker, times(2)).commitTransaction(any(), messageArgumentCaptor.capture(), onePhaseArgumentCaptor.capture());
        List<TransactionId> values = messageArgumentCaptor.getAllValues();
        assertThat(values.get(0)).isEqualTo(transactionId);
        assertThat(values.get(1)).isNotEqualTo(transactionId);
        List<Boolean> onePhaseValues = onePhaseArgumentCaptor.getAllValues();
        assertThat(onePhaseValues.get(0)).isTrue();
        assertThat(onePhaseValues.get(1)).isTrue();
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
        verify(broker, times(2)).addConsumer(connectionContextArgumentCaptor.capture(), messageArgumentCaptor.capture());
        List<ConsumerInfo> consumerInfos = messageArgumentCaptor.getAllValues();
        assertThat(consumerInfos.get(0).getDestination()).isEqualTo(sequenceQueue);
        assertThat(consumerInfos.get(1).getDestination()).isEqualTo(testQueue);
        ConnectionContext connectionContext = connectionContextArgumentCaptor.getValue();
        assertThat(connectionContext.getClientId()).isEqualTo(clientId);
        verify(subscription).deactivate(true, 0);
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

        ArgumentCaptor<ConsumerInfo> ciArgumentCaptor = ArgumentCaptor.forClass(ConsumerInfo.class);
        verify(broker).addConsumer(any(), ciArgumentCaptor.capture());
        ConsumerInfo consumerInfo = ciArgumentCaptor.getValue();
        assertThat(consumerInfo.getDestination()).isEqualTo(sequenceQueue);

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
    }

    @Test
    public void canHandleEventOfType_BATCH() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);

        ActiveMQMessage sendEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent sendEvent = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_SEND)
                .setEventData(eventSerializer.serializeMessageData(message));
        sendEventMessage.setContent(sendEvent.getEventData());
        sendEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, sendEvent.getEventType().name());
        sendEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        MessageAck ack = new MessageAck();
        ConsumerId consumerId = new ConsumerId("2:2:2:2");
        ack.setConsumerId(consumerId);
        ack.setDestination(testQueue);

        ReplicaEvent ackEvent = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_ACK)
                .setEventData(eventSerializer.serializeReplicationData(ack))
                .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, Collections.singletonList(messageId.toString()));
        ActiveMQMessage ackEventMessage = spy(new ActiveMQMessage());
        ackEventMessage.setType("ReplicaEvent");
        ackEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ackEvent.getEventType().name());
        ackEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "1");
        ackEventMessage.setContent(ackEvent.getEventData());
        ackEventMessage.setProperties(ackEvent.getReplicationProperties());

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.BATCH)
                .setEventData(eventSerializer.serializeListOfObjects(List.of(sendEventMessage, ackEventMessage)));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker).getAdminConnectionContext();
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(2)).send(any(), messageArgumentCaptor.capture());

        List<ActiveMQMessage> values = messageArgumentCaptor.getAllValues();
        assertThat(values.get(0).getMessageId()).isEqualTo(message.getMessageId());
        assertThat(values.get(1).getDestination()).isEqualTo(sequenceQueue);

        verify(connectionContext, times(2)).isProducerFlowControl();
        verify(connectionContext, times(2)).setProducerFlowControl(false);
        verify(connectionContext, times(2)).setProducerFlowControl(true);

        ArgumentCaptor<ConsumerInfo> ciArgumentCaptor = ArgumentCaptor.forClass(ConsumerInfo.class);
        verify(broker, times(2)).addConsumer(any(), ciArgumentCaptor.capture());
        List<ConsumerInfo> consumerInfos = ciArgumentCaptor.getAllValues();
        assertThat(consumerInfos.get(0).getDestination()).isEqualTo(sequenceQueue);
        assertThat(consumerInfos.get(1).getConsumerId()).isEqualTo(consumerId);
        assertThat(consumerInfos.get(1).getDestination()).isEqualTo(testQueue);


        ArgumentCaptor<MessageDispatchNotification> mdnArgumentCaptor = ArgumentCaptor.forClass(MessageDispatchNotification.class);
        verify(broker).processDispatchNotification(mdnArgumentCaptor.capture());

        MessageDispatchNotification mdn = mdnArgumentCaptor.getValue();
        assertThat(mdn.getMessageId()).isEqualTo(messageId);
        assertThat(mdn.getDestination()).isEqualTo(testQueue);
        assertThat(mdn.getConsumerId()).isEqualTo(consumerId);

        ArgumentCaptor<MessageAck> ackArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(broker).acknowledge(any(), ackArgumentCaptor.capture());

        MessageAck ackValue = ackArgumentCaptor.getValue();
        assertThat(ackValue.getDestination()).isEqualTo(testQueue);
        assertThat(ackValue.getConsumerId()).isEqualTo(consumerId);
    }

    @Test
    public void canHandleEventOfType_MESSAGE_SEND_correctSequence() throws Exception {
        listener.sequence = BigInteger.ZERO;
        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_SEND)
                .setEventData(eventSerializer.serializeMessageData(message));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "1");

        listener.onMessage(replicaEventMessage);

        verify(broker).getAdminConnectionContext();
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(2)).send(any(), messageArgumentCaptor.capture());

        List<ActiveMQMessage> values = messageArgumentCaptor.getAllValues();
        assertThat(values.get(0).getMessageId()).isEqualTo(message.getMessageId());
        assertThat(values.get(1).getDestination()).isEqualTo(sequenceQueue);

        verify(connectionContext, times(2)).isProducerFlowControl();
        verify(connectionContext, times(2)).setProducerFlowControl(false);
        verify(connectionContext, times(2)).setProducerFlowControl(true);
    }

    @Test
    public void canHandleEventOfType_MESSAGE_SEND_sequenceIsLowerThanCurrent() throws Exception {
        listener.sequence = BigInteger.ONE;
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

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage value = messageArgumentCaptor.getValue();
        assertThat(value.getDestination()).isEqualTo(sequenceQueue);
    }

    @Test
    public void canHandleEventOfType_MESSAGE_SEND_incorrectSequence() throws Exception {
        listener.sequence = BigInteger.ZERO;
        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_SEND)
                .setEventData(eventSerializer.serializeMessageData(message));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "2");

        CountDownLatch cdl = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            listener.onMessage(replicaEventMessage);
            cdl.countDown();
        });
        thread.start();

        assertThat(cdl.await(2, TimeUnit.SECONDS)).isFalse();

        thread.interrupt();

        verify(broker, never()).send(any(), any());

        verify(replicaEventMessage, never()).acknowledge();
    }


    @Test
    public void canHandleEventOfType_FAIL_OVER() throws Exception {
        listener.sequence = null;
        MessageId messageId = new MessageId("1:1:1:1");
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.FAIL_OVER)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setMessageId(messageId);
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setProperties(event.getReplicationProperties());

        listener.onMessage(replicaEventMessage);

        verify(replicaFailOverStateStorage).updateBrokerState(any(), any(), eq(ReplicaRole.source.name()));
        verify(actionListenerCallback).onFailOverAck();
    }
}
