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
import org.apache.activemq.broker.region.MessageReferenceFilter;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaSequencerTest {
    private static final Integer MAXIMUM_MESSAGES = new ReplicaPolicy().getCompactorAdditionalMessagesLimit();
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
    public void restoreSequenceWhenNoSequence() throws Exception {
        sequencer.sequence = null;

        sequencer.restoreSequence(null, Collections.emptyList());

        assertThat(sequencer.sequence).isNull();
    }

    @Test
    public void restoreSequenceWhenSequenceExistsButNoRecoverySequences() throws Exception {
        sequencer.sequence = null;

        MessageId messageId = new MessageId("1:0:0:1");
        sequencer.restoreSequence("1#" + messageId, Collections.emptyList());
        verify(replicationMessageProducer, never()).enqueueMainReplicaEvent(any(), any(ReplicaEvent.class));

        assertThat(sequencer.sequence).isEqualTo(1);

        verify(replicationMessageProducer, never()).enqueueMainReplicaEvent(any(), any(ReplicaEvent.class));
    }

    @Test
    public void restoreSequenceWhenStorageExistAndMessageDoesNotExist() throws Exception {
        sequencer.sequence = null;

        MessageId messageId1 = new MessageId("1:0:0:1");
        MessageId messageId2 = new MessageId("1:0:0:2");
        MessageId messageId3 = new MessageId("1:0:0:3");
        MessageId messageId4 = new MessageId("1:0:0:4");

        ActiveMQMessage message1 = new ActiveMQMessage();
        message1.setMessageId(messageId1);
        message1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message2 = new ActiveMQMessage();
        message2.setMessageId(messageId2);
        message2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message3 = new ActiveMQMessage();
        message3.setMessageId(messageId3);
        message3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message4 = new ActiveMQMessage();
        message4.setMessageId(messageId4);
        message4.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());

        when(intermediateSubscription.getDispatched()).thenReturn(new ArrayList<>(List.of(message1, message2, message3, message4)));

        sequencer.restoreSequence("4#" + messageId4, List.of("1#" + messageId1 + "#" + messageId2, "3#" + messageId3 + "#" + messageId4));

        assertThat(sequencer.sequence).isEqualTo(4);

        ArgumentCaptor<ReplicaEvent> argumentCaptor = ArgumentCaptor.forClass(ReplicaEvent.class);
        verify(replicationMessageProducer, times(2)).enqueueMainReplicaEvent(any(), argumentCaptor.capture());

        List<ReplicaEvent> values = argumentCaptor.getAllValues();
        assertThat(values.get(0).getEventType()).isEqualTo(ReplicaEventType.BATCH);
        assertThat((List<String>) values.get(0).getReplicationProperties().get(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId1.toString(), messageId2.toString());
        List<Object> objects = eventSerializer.deserializeListOfObjects(values.get(0).getEventData().getData());
        assertThat(objects.size()).isEqualTo(2);
        assertThat(((Message) objects.get(0)).getMessageId()).isEqualTo(messageId1);
        assertThat(((Message) objects.get(1)).getMessageId()).isEqualTo(messageId2);

        assertThat(values.get(1).getEventType()).isEqualTo(ReplicaEventType.BATCH);
        assertThat((List<String>) values.get(1).getReplicationProperties().get(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId3.toString(), messageId4.toString());
        objects = eventSerializer.deserializeListOfObjects(values.get(1).getEventData().getData());
        assertThat(objects.size()).isEqualTo(2);
        assertThat(((Message) objects.get(0)).getMessageId()).isEqualTo(messageId3);
        assertThat(((Message) objects.get(1)).getMessageId()).isEqualTo(messageId4);
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
        message.setProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.BATCH.name());
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

        String messageId1 = "1:0:0:1";
        sequencer.messageToAck.addLast(messageId1);
        String messageId2 = "1:0:0:2";
        sequencer.messageToAck.addLast(messageId2);
        String messageId3 = "1:0:0:3";
        sequencer.messageToAck.addLast(messageId3);

        sequencer.iterateAck();

        ArgumentCaptor<MessageAck> ackArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(broker, times(3)).acknowledge(any(), ackArgumentCaptor.capture());

        List<MessageAck> values = ackArgumentCaptor.getAllValues();
        assertThat(values.get(0).getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);
        assertThat(values.get(0).getDestination()).isEqualTo(intermediateQueueDestination);
        assertThat(values.get(0).getFirstMessageId().toString()).isEqualTo(messageId1);
        assertThat(values.get(0).getLastMessageId().toString()).isEqualTo(messageId1);
        assertThat(values.get(1).getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);
        assertThat(values.get(1).getDestination()).isEqualTo(intermediateQueueDestination);
        assertThat(values.get(1).getFirstMessageId().toString()).isEqualTo(messageId2);
        assertThat(values.get(1).getLastMessageId().toString()).isEqualTo(messageId2);
        assertThat(values.get(2).getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);
        assertThat(values.get(2).getDestination()).isEqualTo(intermediateQueueDestination);
        assertThat(values.get(2).getFirstMessageId().toString()).isEqualTo(messageId3);
        assertThat(values.get(2).getLastMessageId().toString()).isEqualTo(messageId3);
    }

    @Test
    public void iterateSendMultipleMessagesTest() throws Exception {
        sequencer.hasConsumer = true;
        List messages =  new ArrayList<ActiveMQMessage>();
        MessageId messageId = new MessageId("1:0:0:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        messages.add(message);


        messageId = new MessageId("1:0:0:2");
        message.setMessageId(messageId);
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        messages.add(message);

        when(intermediateSubscription.getDispatched()).thenReturn(messages);

        sequencer.iterateSend();

        ArgumentCaptor<ReplicaEvent> argumentCaptor = ArgumentCaptor.forClass(ReplicaEvent.class);
        verify(replicationMessageProducer).enqueueMainReplicaEvent(any(), argumentCaptor.capture());

        ReplicaEvent value = argumentCaptor.getValue();
        assertThat(value.getEventType()).isEqualTo(ReplicaEventType.BATCH);
        assertThat((List<String>) value.getReplicationProperties().get(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId.toString());
        List<Object> objects = eventSerializer.deserializeListOfObjects(value.getEventData().getData());
        assertThat(objects.size()).isEqualTo(2);
        assertThat(((Message) objects.get(0)).getMessageId()).isEqualTo(messageId);
    }

    @Test
    public void iterateSendSingleMessageTest() throws Exception {
        sequencer.hasConsumer = true;

        MessageId messageId = new MessageId("1:0:0:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());

        when(intermediateSubscription.getDispatched()).thenReturn(List.of(message));

        sequencer.iterateSend();

        ArgumentCaptor<ActiveMQMessage> argumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(replicaInternalMessageProducer, times(3)).sendIgnoringFlowControl(any(), argumentCaptor.capture());

        ActiveMQMessage activeMQMessage = argumentCaptor.getAllValues().get(0);
        assertThat(activeMQMessage.getMessageId()).isEqualTo(messageId);
        assertThat(activeMQMessage.getTransactionId()).isNull();
        assertThat(activeMQMessage.isPersistent()).isFalse();
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

        ArgumentCaptor<ActiveMQMessage> argumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(replicaInternalMessageProducer, times(3)).sendIgnoringFlowControl(any(), argumentCaptor.capture());

        ActiveMQMessage activeMQMessage = argumentCaptor.getAllValues().get(0);
        assertThat(activeMQMessage.getMessageId()).isEqualTo(messageId3);
        assertThat(activeMQMessage.getTransactionId()).isNull();
        assertThat(activeMQMessage.isPersistent()).isFalse();
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

        sequencer.iterateSend();

        ArgumentCaptor<ActiveMQMessage> argumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(replicaInternalMessageProducer, times(3)).sendIgnoringFlowControl(any(), argumentCaptor.capture());

        ActiveMQMessage activeMQMessage = argumentCaptor.getAllValues().get(0);
        assertThat(activeMQMessage.getMessageId()).isEqualTo(messageId2);
        assertThat(activeMQMessage.getTransactionId()).isNull();
        assertThat(activeMQMessage.isPersistent()).isFalse();

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
    public void iterateSendDoNotSendToMainQueueIfNoConsumer() throws Exception {
        sequencer.hasConsumer = false;
        when(intermediateSubscription.isFull()).thenReturn(true);

        ActiveMQMessage ackMessage1 = new ActiveMQMessage();
        ackMessage1.setMessageId(new MessageId("2:0:0:1"));
        ackMessage1.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("0:0:0:1"));
        ackMessage1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        ActiveMQMessage ackMessage2 = new ActiveMQMessage();
        ackMessage2.setMessageId(new MessageId("2:0:0:2"));
        ackMessage2.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("0:0:0:2"));
        ackMessage2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        ActiveMQMessage ackMessage3 = new ActiveMQMessage();
        ackMessage3.setMessageId(new MessageId("2:0:0:3"));
        ackMessage3.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("0:0:0:3"));
        ackMessage3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());

        List<QueueMessageReference> ackMessageReferences = new ArrayList<>();
        ackMessageReferences.add(new IndirectMessageReference(ackMessage1));
        ackMessageReferences.add(new IndirectMessageReference(ackMessage2));
        ackMessageReferences.add(new IndirectMessageReference(ackMessage3));

        when(intermediateQueue.getMatchingMessages(eq(connectionContext), any(ReplicaCompactor.AckMessageReferenceFilter.class), eq(MAXIMUM_MESSAGES)))
                .thenReturn(ackMessageReferences);

        ActiveMQMessage sendMessage1 = new ActiveMQMessage();
        sendMessage1.setMessageId(new MessageId("2:0:0:1"));
        sendMessage1.setProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "0:0:0:1");
        sendMessage1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage sendMessage2 = new ActiveMQMessage();
        sendMessage2.setMessageId(new MessageId("2:0:0:2"));
        sendMessage2.setProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "0:0:0:2");
        sendMessage2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage sendMessage3 = new ActiveMQMessage();
        sendMessage3.setMessageId(new MessageId("2:0:0:3"));
        sendMessage3.setProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "0:0:0:3");
        sendMessage3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());

        List<QueueMessageReference> sendMessageReferences = new ArrayList<>();
        sendMessageReferences.add(new IndirectMessageReference(sendMessage1));
        sendMessageReferences.add(new IndirectMessageReference(sendMessage2));
        sendMessageReferences.add(new IndirectMessageReference(sendMessage3));
         when(intermediateQueue.getMatchingMessages(eq(connectionContext), any(ReplicaCompactor.SendMessageReferenceFilter.class), eq(3)))
                .thenReturn(sendMessageReferences);

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

        verify(broker, times(3)).addConsumer(any(), any());
        verify(replicationMessageProducer, never()).enqueueMainReplicaEvent(any(), any(ReplicaEvent.class));

        ArgumentCaptor<MessageReferenceFilter> filterArgumentCaptor = ArgumentCaptor.forClass(MessageReferenceFilter.class);
        ArgumentCaptor<ConnectionContext> contextArgumentCaptor = ArgumentCaptor.forClass(ConnectionContext.class);
        ArgumentCaptor<Integer> maxMessagesArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(intermediateQueue, times(2)).getMatchingMessages(contextArgumentCaptor.capture(), filterArgumentCaptor.capture(), maxMessagesArgumentCaptor.capture());

        assertThat(maxMessagesArgumentCaptor.getAllValues().get(0)).isEqualTo(MAXIMUM_MESSAGES);
        assertThat(maxMessagesArgumentCaptor.getAllValues().get(1)).isEqualTo(3);
        assertThat(filterArgumentCaptor.getAllValues().get(0)).isInstanceOf(ReplicaCompactor.AckMessageReferenceFilter.class);
        assertThat(filterArgumentCaptor.getAllValues().get(1)).isInstanceOf(ReplicaCompactor.SendMessageReferenceFilter.class);
        contextArgumentCaptor.getAllValues().forEach(
                conContext -> assertThat(conContext).isEqualTo(connectionContext)
        );
    }
}
