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
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaSequencerTest {

    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final Broker broker = mock(Broker.class);
    private final ReplicaReplicationQueueSupplier queueProvider = mock(ReplicaReplicationQueueSupplier.class);
    private final ReplicationMessageProducer replicationMessageProducer = mock(ReplicationMessageProducer.class);

    private final ReplicaSequencer sequencer = new ReplicaSequencer(broker, queueProvider, replicationMessageProducer);

    private final ActiveMQQueue intermediateQueueDestination = new ActiveMQQueue(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
    private final ActiveMQQueue mainQueueDestination = new ActiveMQQueue(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
    private final Queue intermediateQueue = mock(Queue.class);
    private final Queue mainQueue = mock(Queue.class);

    private final File brokerDataDirectory = new File(IOHelper.getDefaultDataDirectory());
    private final File storageDirectory = new File(brokerDataDirectory, ReplicaSupport.REPLICATION_PLUGIN_STORAGE_DIRECTORY);

    private final String storageName = "source_sequence";
    private final ReplicaStorage replicaStorage = new ReplicaStorage(storageName);

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

        when(broker.getDestinations(intermediateQueueDestination)).thenReturn(Set.of(intermediateQueue));
        when(broker.getDestinations(mainQueueDestination)).thenReturn(Set.of(mainQueue));

        ConnectionContext adminConnectionContext = mock(ConnectionContext.class);
        when(adminConnectionContext.copy()).thenReturn(connectionContext);
        when(broker.getAdminConnectionContext()).thenReturn(adminConnectionContext);

        when(brokerService.getBrokerDataDirectory()).thenReturn(brokerDataDirectory);

        when(mainSubscription.getConsumerInfo()).thenReturn(consumerInfo);
        when(mainQueue.getConsumers()).thenReturn(List.of(mainSubscription));

        when(broker.addConsumer(any(), any())).thenReturn(intermediateSubscription);

        sequencer.initialize();

        replicaStorage.initialize(storageDirectory);
    }

    @Test
    public void restoreSequenceWhenStorageDoesNotExist() throws Exception {
        sequencer.sequence = null;

        File storage = new File(storageDirectory, storageName);
        if (storage.exists()) {
            assertThat(storage.delete()).isTrue();
        }

        sequencer.restoreSequence();

        assertThat(sequencer.sequence).isNull();
    }

    @Test
    public void restoreSequenceWhenStorageExistAndNoMessagesInQueue() throws Exception {
        sequencer.sequence = null;

        MessageId messageId = new MessageId("1:1");
        replicaStorage.write("1#" + messageId);

        when(intermediateQueue.getAllMessageIds()).thenReturn(List.of());

        sequencer.restoreSequence();

        assertThat(sequencer.sequence).isEqualTo(1);
    }

    @Test
    public void restoreSequenceWhenStorageExistAndMessageDoesNotExist() throws Exception {
        sequencer.sequence = null;

        MessageId messageId = new MessageId("1:1");
        replicaStorage.write("1#" + messageId);

        when(intermediateQueue.getAllMessageIds()).thenReturn(List.of(new MessageId("1:2")));

        sequencer.restoreSequence();

        assertThat(sequencer.sequence).isEqualTo(1);
    }

    @Test
    public void acknowledgeTest() throws Exception {
        MessageId messageId = new MessageId("1:1");

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

        String firstMessageId = "1:1";
        sequencer.messageToAck.addLast(firstMessageId);
        sequencer.messageToAck.addLast("1:2");
        String lastMessageId = "1:3";
        sequencer.messageToAck.addLast(lastMessageId);

        sequencer.iterateAck();

        ArgumentCaptor<MessageAck> ackArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(intermediateSubscription).acknowledge(any(), ackArgumentCaptor.capture());

        MessageAck value = ackArgumentCaptor.getValue();
        assertThat(value.getAckType()).isEqualTo(MessageAck.STANDARD_ACK_TYPE);
        assertThat(value.getDestination()).isEqualTo(intermediateQueueDestination);
        assertThat(value.getFirstMessageId().toString()).isEqualTo(firstMessageId);
        assertThat(value.getLastMessageId().toString()).isEqualTo(lastMessageId);
        assertThat(value.getMessageCount()).isEqualTo(3);
    }

    @Test
    public void iterateSendTest() throws Exception {
        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);

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
        MessageId messageId1 = new MessageId("1:1");
        MessageId messageId2 = new MessageId("1:2");
        MessageId messageId3 = new MessageId("1:3");

        ActiveMQMessage message1 = new ActiveMQMessage();
        message1.setMessageId(messageId1);
        ActiveMQMessage message2 = new ActiveMQMessage();
        message2.setMessageId(messageId2);
        ActiveMQMessage message3 = new ActiveMQMessage();
        message3.setMessageId(messageId3);

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
    public void batchesSmallMessages() {
        List<MessageReference> list = new ArrayList<>();
        for (int i = 0; i < 1347; i++) {
            list.add(new DummyMessageReference(new MessageId("1:" + i), 1));
        }

        List<List<MessageReference>> batches = sequencer.batches(list);
        assertThat(batches.size()).isEqualTo(3);
        assertThat(batches.get(0).size()).isEqualTo(ReplicaSequencer.MAX_BATCH_LENGTH);
        for (int i = 0; i < ReplicaSequencer.MAX_BATCH_LENGTH; i++) {
            assertThat(batches.get(0).get(i).getMessageId().toString()).isEqualTo("1:" + i);
        }
        assertThat(batches.get(1).size()).isEqualTo(ReplicaSequencer.MAX_BATCH_LENGTH);
        for (int i = 0; i < ReplicaSequencer.MAX_BATCH_LENGTH; i++) {
            assertThat(batches.get(1).get(i).getMessageId().toString()).isEqualTo("1:" + (i + ReplicaSequencer.MAX_BATCH_LENGTH));
        }
        assertThat(batches.get(2).size()).isEqualTo(347);
        for (int i = 0; i < 347; i++) {
            assertThat(batches.get(2).get(i).getMessageId().toString()).isEqualTo("1:" + (i + ReplicaSequencer.MAX_BATCH_LENGTH * 2));
        }
    }

    @Test
    public void batchesBigMessages() {
        List<MessageReference> list = new ArrayList<>();
        list.add(new DummyMessageReference(new MessageId("1:1"), ReplicaSequencer.MAX_BATCH_SIZE + 1));
        list.add(new DummyMessageReference(new MessageId("1:2"), ReplicaSequencer.MAX_BATCH_SIZE / 2 + 1));
        list.add(new DummyMessageReference(new MessageId("1:3"), ReplicaSequencer.MAX_BATCH_SIZE / 2));

        List<List<MessageReference>> batches = sequencer.batches(list);
        System.out.println(batches);
        assertThat(batches.size()).isEqualTo(3);
        assertThat(batches.get(0).size()).isEqualTo(1);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:1");
        assertThat(batches.get(1).size()).isEqualTo(1);
        assertThat(batches.get(1).get(0).getMessageId().toString()).isEqualTo("1:2");
        assertThat(batches.get(2).size()).isEqualTo(1);
        assertThat(batches.get(2).get(0).getMessageId().toString()).isEqualTo("1:3");
    }

    private static class DummyMessageReference implements MessageReference {

        private final MessageId messageId;
        private final int size;

        DummyMessageReference(MessageId messageId, int size) {
            this.messageId = messageId;
            this.size = size;
        }

        @Override
        public MessageId getMessageId() {
            return messageId;
        }

        @Override
        public Message getMessageHardRef() {
            return null;
        }

        @Override
        public Message getMessage() {
            return null;
        }

        @Override
        public boolean isPersistent() {
            return false;
        }

        @Override
        public Message.MessageDestination getRegionDestination() {
            return null;
        }

        @Override
        public int getRedeliveryCounter() {
            return 0;
        }

        @Override
        public void incrementRedeliveryCounter() {

        }

        @Override
        public int getReferenceCount() {
            return 0;
        }

        @Override
        public int incrementReferenceCount() {
            return 0;
        }

        @Override
        public int decrementReferenceCount() {
            return 0;
        }

        @Override
        public ConsumerId getTargetConsumerId() {
            return null;
        }

        @Override
        public int getSize() {
            return size;
        }

        @Override
        public long getExpiration() {
            return 0;
        }

        @Override
        public String getGroupID() {
            return null;
        }

        @Override
        public int getGroupSequence() {
            return 0;
        }

        @Override
        public boolean isExpired() {
            return false;
        }

        @Override
        public boolean isDropped() {
            return false;
        }

        @Override
        public boolean isAdvisory() {
            return false;
        }

        @Override
        public boolean canProcessAsExpired() {
            return false;
        }
    }
}
