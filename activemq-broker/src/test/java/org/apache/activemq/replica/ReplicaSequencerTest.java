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
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.IOHelper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaSequencerTest {
    private static final String ACK_SELECTOR = String.format("%s LIKE '%s'", ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK);
    private static final String SEND_SELECTOR = String.format("%s IN ('0:0:0:1','0:0:0:2','0:0:0:3')", ReplicaSupport.MESSAGE_ID_PROPERTY);
    private static final Integer MAXIMUM_MESSAGES = 1000;
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final Broker broker = mock(Broker.class);
    private final ReplicaReplicationQueueSupplier queueProvider = mock(ReplicaReplicationQueueSupplier.class);
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer = mock(ReplicaInternalMessageProducer.class);
    private final ReplicationMessageProducer replicationMessageProducer = mock(ReplicationMessageProducer.class);

    private ReplicaSequencer sequencer;

    private final ActiveMQQueue intermediateQueueDestination = new ActiveMQQueue(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
    private final ActiveMQQueue mainQueueDestination = new ActiveMQQueue(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
    private final ActiveMQQueue sequenceQueueDestination = new ActiveMQQueue(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
    private final Queue intermediateQueue = mock(Queue.class);
    private final Queue mainQueue = mock(Queue.class);
    private final Queue sequenceQueue = mock(Queue.class);

    private final ConsumerId consumerId = new ConsumerId("2:2:2:2");
    private final ConsumerInfo consumerInfo = new ConsumerInfo(consumerId);
    private final PrefetchSubscription mainSubscription = mock(PrefetchSubscription.class);
    private final PrefetchSubscription intermediateSubscription = mock(PrefetchSubscription.class);

    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    @Before
    public void setUp() throws Exception {
        BrokerService brokerService = mock(BrokerService.class);
        when(broker.getBrokerService()).thenReturn(brokerService);

        TaskRunnerFactory taskRunnerFactory = mock(TaskRunnerFactory.class);
        when(brokerService.getTaskRunnerFactory()).thenReturn(taskRunnerFactory);
        TaskRunner taskRunner = mock(TaskRunner.class);
        when(taskRunnerFactory.createTaskRunner(any(), any())).thenReturn(taskRunner);

        when(queueProvider.getIntermediateQueue()).thenReturn(intermediateQueueDestination);
        when(queueProvider.getMainQueue()).thenReturn(mainQueueDestination);
        when(queueProvider.getSequenceQueue()).thenReturn(sequenceQueueDestination);

        when(broker.getDestinations(intermediateQueueDestination)).thenReturn(Set.of(intermediateQueue));
        when(broker.getDestinations(mainQueueDestination)).thenReturn(Set.of(mainQueue));
        when(broker.getDestinations(sequenceQueueDestination)).thenReturn(Set.of(sequenceQueue));

        ConnectionContext adminConnectionContext = mock(ConnectionContext.class);
        when(adminConnectionContext.copy()).thenReturn(connectionContext);
        when(broker.getAdminConnectionContext()).thenReturn(adminConnectionContext);

        when(mainSubscription.getConsumerInfo()).thenReturn(consumerInfo);
        when(mainQueue.getConsumers()).thenReturn(List.of(mainSubscription));

        when(intermediateSubscription.getConsumerInfo()).thenReturn(consumerInfo);
        when(broker.addConsumer(any(), any()))
                .thenAnswer(a -> a.<ConsumerInfo>getArgument(1).getConsumerId().toString().contains("Sequencer")
                        ? intermediateSubscription : mock(PrefetchSubscription.class));

        sequencer = new ReplicaSequencer(broker, queueProvider, replicaInternalMessageProducer, replicationMessageProducer, new ReplicaPolicy());
        sequencer.initialize();

    }

    @Test
    public void restoreSequenceWhenStorageDoesNotExist() throws Exception {
        sequencer.sequence = null;

        sequencer.restoreSequence(null, intermediateQueue);

        assertThat(sequencer.sequence).isNull();
    }

    @Test
    public void restoreSequenceWhenStorageExistAndNoMessagesInQueue() throws Exception {
        sequencer.sequence = null;

        MessageId messageId = new MessageId("1:0:0:1");

        when(intermediateQueue.getAllMessageIds()).thenReturn(List.of());

        sequencer.restoreSequence("1#" + messageId, intermediateQueue);

        assertThat(sequencer.sequence).isEqualTo(1);
    }

    @Test
    public void restoreSequenceWhenStorageExistAndMessageDoesNotExist() throws Exception {
        sequencer.sequence = null;

        MessageId messageId = new MessageId("1:0:0:1");

        when(intermediateQueue.getAllMessageIds()).thenReturn(List.of(new MessageId("1:0:0:2")));

        sequencer.restoreSequence("1#" + messageId, intermediateQueue);

        assertThat(sequencer.sequence).isEqualTo(1);
    }

    @Test
    public void acknowledgeTest() throws Exception {
        MessageId messageId = new MessageId("1:0:0:1");

        MessageAck messageAck = new MessageAck();
        messageAck.setMessageID(messageId);
        messageAck.setConsumerId(consumerId);
        messageAck.setDestination(intermediateQueueDestination);
        messageAck.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of(messageId.toString()));

        when(mainSubscription.getDispatched()).thenReturn(List.of(message));

        ConsumerBrokerExchange cbe = new ConsumerBrokerExchange();
        cbe.setConnectionContext(connectionContext);

        sequencer.acknowledge(cbe, messageAck);

        verify(broker).acknowledge(cbe, messageAck);

        assertThat(sequencer.messageToAck).containsOnly(messageId.toString());
    }

    @Test
    public void iterateAckTest() throws Exception {
        sequencer.messageToAck.clear();

        String firstMessageId = "1:0:0:1";
        sequencer.messageToAck.addLast(firstMessageId);
        sequencer.messageToAck.addLast("1:0:0:2");
        String lastMessageId = "1:0:0:3";
        sequencer.messageToAck.addLast(lastMessageId);

        sequencer.iterateAck();

        ArgumentCaptor<MessageAck> ackArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(broker).acknowledge(any(), ackArgumentCaptor.capture());

        MessageAck value = ackArgumentCaptor.getValue();
        assertThat(value.getAckType()).isEqualTo(MessageAck.STANDARD_ACK_TYPE);
        assertThat(value.getDestination()).isEqualTo(intermediateQueueDestination);
        assertThat(value.getFirstMessageId().toString()).isEqualTo(firstMessageId);
        assertThat(value.getLastMessageId().toString()).isEqualTo(lastMessageId);
        assertThat(value.getMessageCount()).isEqualTo(3);
    }

    @Test
    public void iterateSendTest() throws Exception {
        sequencer.hasConsumer = true;

        MessageId messageId = new MessageId("1:0:0:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());

        when(intermediateSubscription.getDispatched()).thenReturn(List.of(message));

        sequencer.iterateSend();

        ArgumentCaptor<ReplicaEvent> argumentCaptor = ArgumentCaptor.forClass(ReplicaEvent.class);
        verify(replicationMessageProducer).enqueueMainReplicaEvent(any(), argumentCaptor.capture());

        ReplicaEvent value = argumentCaptor.getValue();
        assertThat(value.getEventType()).isEqualTo(ReplicaEventType.BATCH);
        assertThat((List<String>) value.getReplicationProperties().get(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId.toString());
        List<Object> objects = eventSerializer.deserializeListOfObjects(value.getEventData().getData());
        assertThat(objects.size()).isEqualTo(1);
        assertThat(((Message) objects.get(0)).getMessageId()).isEqualTo(messageId);
    }

    @Test
    public void iterateSendTestWhenSomeMessagesAreadyDelivered() throws Exception {
        sequencer.hasConsumer = true;

        MessageId messageId1 = new MessageId("1:0:0:1");
        MessageId messageId2 = new MessageId("1:0:0:2");
        MessageId messageId3 = new MessageId("1:0:0:3");

        ActiveMQMessage message1 = new ActiveMQMessage();
        message1.setMessageId(messageId1);
        message1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message2 = new ActiveMQMessage();
        message2.setMessageId(messageId2);
        message2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message3 = new ActiveMQMessage();
        message3.setMessageId(messageId3);
        message3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());

        when(intermediateSubscription.getDispatched()).thenReturn(new ArrayList<>(List.of(message1, message2, message3)));

        sequencer.deliveredMessages.add(messageId1.toString());
        sequencer.deliveredMessages.add(messageId2.toString());

        sequencer.iterateSend();

        ArgumentCaptor<ReplicaEvent> argumentCaptor = ArgumentCaptor.forClass(ReplicaEvent.class);
        verify(replicationMessageProducer).enqueueMainReplicaEvent(any(), argumentCaptor.capture());

        ReplicaEvent value = argumentCaptor.getValue();
        assertThat(value.getEventType()).isEqualTo(ReplicaEventType.BATCH);
        assertThat((List<String>) value.getReplicationProperties().get(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId3.toString());
        List<Object> objects = eventSerializer.deserializeListOfObjects(value.getEventData().getData());
        assertThat(objects.size()).isEqualTo(1);
        assertThat(((Message) objects.get(0)).getMessageId()).isEqualTo(messageId3);
    }

    @Test
    public void iterateSendTestWhenRecoveryMessageIdIsNotNullAndDispatched() throws Exception {
        sequencer.hasConsumer = true;

        MessageId messageId1 = new MessageId("1:0:0:1");
        MessageId messageId2 = new MessageId("1:0:0:2");
        MessageId messageId3 = new MessageId("1:0:0:3");

        ActiveMQMessage message1 = new ActiveMQMessage();
        message1.setMessageId(messageId1);
        message1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message2 = new ActiveMQMessage();
        message2.setMessageId(messageId2);
        message2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message3 = new ActiveMQMessage();
        message3.setMessageId(messageId3);
        message3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());

        when(intermediateSubscription.getDispatched()).thenReturn(new ArrayList<>(List.of(message1, message2, message3)));

        sequencer.recoveryMessageId = messageId2;

        sequencer.iterateSend();

        ArgumentCaptor<ReplicaEvent> argumentCaptor = ArgumentCaptor.forClass(ReplicaEvent.class);
        verify(replicationMessageProducer).enqueueMainReplicaEvent(any(), argumentCaptor.capture());

        ReplicaEvent value = argumentCaptor.getValue();
        assertThat(value.getEventType()).isEqualTo(ReplicaEventType.BATCH);
        assertThat((List<String>) value.getReplicationProperties().get(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId1.toString(), messageId2.toString());
        List<Object> objects = eventSerializer.deserializeListOfObjects(value.getEventData().getData());
        assertThat(objects.size()).isEqualTo(2);
        assertThat(((Message) objects.get(0)).getMessageId()).isEqualTo(messageId1);
        assertThat(((Message) objects.get(1)).getMessageId()).isEqualTo(messageId2);
    }

    @Test
    public void iterateSendTestWhenRecoveryMessageIdIsNotNullAndNotDispatched() throws Exception {
        sequencer.hasConsumer = true;

        MessageId messageId1 = new MessageId("1:0:0:1");
        MessageId messageId2 = new MessageId("1:0:0:2");
        MessageId messageId3 = new MessageId("1:0:0:3");

        ActiveMQMessage message1 = new ActiveMQMessage();
        message1.setMessageId(messageId1);
        message1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message2 = new ActiveMQMessage();
        message2.setMessageId(messageId2);
        message2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message3 = new ActiveMQMessage();
        message3.setMessageId(messageId3);
        message3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());

        when(intermediateSubscription.getDispatched()).thenReturn(new ArrayList<>(List.of(message1, message2, message3)));

        sequencer.recoveryMessageId = new MessageId("1:0:0:4");

        sequencer.iterateSend();

        ArgumentCaptor<ReplicaEvent> argumentCaptor = ArgumentCaptor.forClass(ReplicaEvent.class);
        verify(replicationMessageProducer).enqueueMainReplicaEvent(any(), argumentCaptor.capture());

        ReplicaEvent value = argumentCaptor.getValue();
        assertThat(value.getEventType()).isEqualTo(ReplicaEventType.BATCH);
        assertThat((List<String>) value.getReplicationProperties().get(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId1.toString(), messageId2.toString(), messageId3.toString());
        List<Object> objects = eventSerializer.deserializeListOfObjects(value.getEventData().getData());
        assertThat(objects.size()).isEqualTo(3);
        assertThat(((Message) objects.get(0)).getMessageId()).isEqualTo(messageId1);
        assertThat(((Message) objects.get(1)).getMessageId()).isEqualTo(messageId2);
        assertThat(((Message) objects.get(2)).getMessageId()).isEqualTo(messageId3);
    }

    @Test
    public void iterateSendTestWhenCompactionPossible() throws Exception {
        sequencer.hasConsumer = true;

        MessageId messageId1 = new MessageId("1:0:0:1");
        MessageId messageId2 = new MessageId("1:0:0:2");
        MessageId messageId3 = new MessageId("1:0:0:3");

        String messageIdToAck = "2:1";

        ActiveMQMessage message1 = new ActiveMQMessage();
        message1.setMessageId(messageId1);
        message1.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        message1.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, messageIdToAck);
        ActiveMQMessage message2 = new ActiveMQMessage();
        message2.setMessageId(messageId2);
        message2.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message3 = new ActiveMQMessage();
        message3.setMessageId(messageId3);
        message3.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        message3.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of(messageIdToAck));

        when(intermediateSubscription.getDispatched()).thenReturn(new ArrayList<>(List.of(message1, message2, message3)));

        sequencer.recoveryMessageId = null;

        sequencer.iterateSend();

        ArgumentCaptor<ReplicaEvent> argumentCaptor = ArgumentCaptor.forClass(ReplicaEvent.class);
        verify(replicationMessageProducer).enqueueMainReplicaEvent(any(), argumentCaptor.capture());

        ReplicaEvent value = argumentCaptor.getValue();
        assertThat(value.getEventType()).isEqualTo(ReplicaEventType.BATCH);
        assertThat((List<String>) value.getReplicationProperties().get(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId2.toString());
        List<Object> objects = eventSerializer.deserializeListOfObjects(value.getEventData().getData());
        assertThat(objects.size()).isEqualTo(1);
        assertThat(((Message) objects.get(0)).getMessageId()).isEqualTo(messageId2);

        ArgumentCaptor<MessageAck> ackCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(broker, times(2)).acknowledge(any(), ackCaptor.capture());

        List<MessageAck> values = ackCaptor.getAllValues();
        MessageAck messageAck = values.get(0);
        assertThat(messageAck.getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);
        assertThat(messageAck.getMessageCount()).isEqualTo(1);
        assertThat(messageAck.getLastMessageId()).isEqualTo(messageId1);
        messageAck = values.get(1);
        assertThat(messageAck.getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);
        assertThat(messageAck.getMessageCount()).isEqualTo(1);
        assertThat(messageAck.getLastMessageId()).isEqualTo(messageId3);
    }

    @Test
    public void iterateSendTestWhenCompactionPossibleAndRecoveryMessageIdIsNotNull() throws Exception {
        sequencer.hasConsumer = true;

        MessageId messageId1 = new MessageId("1:0:0:1");
        MessageId messageId2 = new MessageId("1:0:0:2");
        MessageId messageId3 = new MessageId("1:0:0:3");

        String messageIdToAck = "2:1";

        ActiveMQMessage message1 = new ActiveMQMessage();
        message1.setMessageId(messageId1);
        message1.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        message1.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, messageIdToAck);
        ActiveMQMessage message2 = new ActiveMQMessage();
        message2.setMessageId(messageId2);
        message2.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message3 = new ActiveMQMessage();
        message3.setMessageId(messageId3);
        message3.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        message3.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of(messageIdToAck));

        when(intermediateSubscription.getDispatched()).thenReturn(new ArrayList<>(List.of(message1, message2, message3)));

        sequencer.recoveryMessageId = messageId3;

        sequencer.iterateSend();

        ArgumentCaptor<ReplicaEvent> argumentCaptor = ArgumentCaptor.forClass(ReplicaEvent.class);
        verify(replicationMessageProducer).enqueueMainReplicaEvent(any(), argumentCaptor.capture());

        ReplicaEvent value = argumentCaptor.getValue();
        assertThat(value.getEventType()).isEqualTo(ReplicaEventType.BATCH);
        assertThat((List<String>) value.getReplicationProperties().get(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId1.toString(), messageId2.toString(), messageId3.toString());
        List<Object> objects = eventSerializer.deserializeListOfObjects(value.getEventData().getData());
        assertThat(objects.size()).isEqualTo(3);
        assertThat(((Message) objects.get(0)).getMessageId()).isEqualTo(messageId1);
        assertThat(((Message) objects.get(1)).getMessageId()).isEqualTo(messageId2);
        assertThat(((Message) objects.get(2)).getMessageId()).isEqualTo(messageId3);
    }

    @Test
    public void iterateSendDoNotSendToMainQueueIfNoConsumer() throws Exception {
        sequencer.hasConsumer = false;
        when(intermediateSubscription.isFull()).thenReturn(true);

        ActiveMQMessage activeMQMessage1 = new ActiveMQMessage();
        activeMQMessage1.setMessageId(new MessageId("2:0:0:1"));
        activeMQMessage1.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("0:0:0:1"));
        activeMQMessage1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        ActiveMQMessage activeMQMessage2 = new ActiveMQMessage();
        activeMQMessage2.setMessageId(new MessageId("2:0:0:2"));
        activeMQMessage2.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("0:0:0:2"));
        activeMQMessage2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        ActiveMQMessage activeMQMessage3 = new ActiveMQMessage();
        activeMQMessage3.setMessageId(new MessageId("2:0:0:3"));
        activeMQMessage3.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("0:0:0:3"));
        activeMQMessage3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());

        List<QueueMessageReference> ackMessageReferences = new ArrayList<>();
        ackMessageReferences.add(new IndirectMessageReference(activeMQMessage1));
        ackMessageReferences.add(new IndirectMessageReference(activeMQMessage2));
        ackMessageReferences.add(new IndirectMessageReference(activeMQMessage3));

        when(intermediateQueue.getMatchingMessages(connectionContext, ACK_SELECTOR, MAXIMUM_MESSAGES))
                .thenReturn(ackMessageReferences);

         when(intermediateQueue.getMatchingMessages(connectionContext, SEND_SELECTOR, 1000))
                .thenReturn(new ArrayList<>());

        String messageIdToAck = "2:1";

        MessageId messageId1 = new MessageId("1:0:0:1");
        MessageId messageId2 = new MessageId("1:0:0:2");
        MessageId messageId3 = new MessageId("1:0:0:3");

        ActiveMQMessage message1 = new ActiveMQMessage();
        message1.setMessageId(messageId1);
        message1.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        message1.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, messageIdToAck);
        ActiveMQMessage message2 = new ActiveMQMessage();
        message2.setMessageId(messageId2);
        message2.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message3 = new ActiveMQMessage();
        message3.setMessageId(messageId3);
        message3.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        message3.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of(messageIdToAck));

        when(intermediateSubscription.getDispatched()).thenReturn(new ArrayList<>(List.of(message1, message2, message3)));

        sequencer.recoveryMessageId = null;

        sequencer.iterateSend();

        ArgumentCaptor<MessageAck> ackCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(broker, times(2)).acknowledge(any(), ackCaptor.capture());

        List<MessageAck> values = ackCaptor.getAllValues();
        MessageAck messageAck = values.get(0);
        assertThat(messageAck.getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);
        assertThat(messageAck.getMessageCount()).isEqualTo(1);
        assertThat(messageAck.getLastMessageId()).isEqualTo(messageId1);
        messageAck = values.get(1);
        assertThat(messageAck.getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);
        assertThat(messageAck.getMessageCount()).isEqualTo(1);
        assertThat(messageAck.getLastMessageId()).isEqualTo(messageId3);

        verify(broker, times(2)).addConsumer(any(), any());
        verify(replicationMessageProducer, never()).enqueueMainReplicaEvent(any(), any());

        ArgumentCaptor<String> selectorArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ConnectionContext> contextArgumentCaptor = ArgumentCaptor.forClass(ConnectionContext.class);
        ArgumentCaptor<Integer> maxMessagesArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(intermediateQueue, times(2)).getMatchingMessages(contextArgumentCaptor.capture(), selectorArgumentCaptor.capture(), maxMessagesArgumentCaptor.capture());

        maxMessagesArgumentCaptor.getAllValues().forEach(
                maximumMessages -> assertThat(maximumMessages).isEqualTo(MAXIMUM_MESSAGES)
        );
        assertThat(selectorArgumentCaptor.getAllValues()).containsAll(List.of(ACK_SELECTOR, SEND_SELECTOR));
        contextArgumentCaptor.getAllValues().forEach(
                conContext -> assertThat(conContext).isEqualTo(connectionContext)
        );
    }
}
