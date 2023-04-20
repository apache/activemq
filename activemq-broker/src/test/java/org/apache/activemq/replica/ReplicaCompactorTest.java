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
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaCompactorTest {

    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final Broker broker = mock(Broker.class);
    private final ReplicaReplicationQueueSupplier queueProvider = mock(ReplicaReplicationQueueSupplier.class);
    private final MessageStore messageStore = mock(MessageStore.class);

    private final ActiveMQQueue intermediateQueueDestination = new ActiveMQQueue(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
    private final Queue intermediateQueue = mock(Queue.class);

    private ReplicaCompactor replicaCompactor;

    @Before
    public void setUp() throws Exception {
        ConnectionContext adminConnectionContext = mock(ConnectionContext.class);
        when(adminConnectionContext.copy()).thenReturn(connectionContext);
        when(broker.getAdminConnectionContext()).thenReturn(adminConnectionContext);

        when(queueProvider.getIntermediateQueue()).thenReturn(intermediateQueueDestination);
        when(broker.getDestinations(intermediateQueueDestination)).thenReturn(Set.of(intermediateQueue));
        when(intermediateQueue.getMessageStore()).thenReturn(messageStore);

        ConsumerInfo consumerInfo = new ConsumerInfo();
        PrefetchSubscription originalSubscription = mock(PrefetchSubscription.class);
        when(originalSubscription.getConsumerInfo()).thenReturn(consumerInfo);

        replicaCompactor = new ReplicaCompactor(broker, queueProvider, originalSubscription, 1000);
    }

    @Test
    public void compactWhenSendAndAck() throws Exception {
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

        List<MessageReference> result = replicaCompactor.compactAndFilter(connectionContext, List.of(message1, message2, message3), false);

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getMessageId()).isEqualTo(messageId2);

        verify(broker).beginTransaction(any(), any());

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

        verify(broker).commitTransaction(any(), any(), eq(true));
    }

    @Test
    public void compactWhenMultipleSendsAndAcksWithSameId() throws Exception {
        MessageId messageId1 = new MessageId("1:0:0:1");
        MessageId messageId2 = new MessageId("1:0:0:2");
        MessageId messageId3 = new MessageId("1:0:0:3");
        MessageId messageId4 = new MessageId("1:0:0:4");
        MessageId messageId5 = new MessageId("1:0:0:5");
        MessageId messageId6 = new MessageId("1:0:0:6");

        String messageIdToAck1 = "2:1";

        ActiveMQMessage message1 = new ActiveMQMessage();
        message1.setMessageId(messageId1);
        message1.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message1.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        message1.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, messageIdToAck1);
        ActiveMQMessage message2 = new ActiveMQMessage();
        message2.setMessageId(messageId2);
        message2.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message2.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        ActiveMQMessage message3 = new ActiveMQMessage();
        message3.setMessageId(messageId3);
        message3.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message3.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        message3.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of(messageIdToAck1));
        ActiveMQMessage message4 = new ActiveMQMessage();
        message4.setMessageId(messageId4);
        message4.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message4.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        message4.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, messageIdToAck1);
        ActiveMQMessage message5 = new ActiveMQMessage();
        message5.setMessageId(messageId5);
        message5.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message5.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        message5.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of(messageIdToAck1));
        ActiveMQMessage message6 = new ActiveMQMessage();
        message6.setMessageId(messageId6);
        message6.setBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY, true);
        message6.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        message6.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, messageIdToAck1);

        List<MessageReference> result = replicaCompactor.compactAndFilter(connectionContext,
                List.of(message1, message2, message3, message4, message5, message6), false);

        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).getMessageId()).isEqualTo(messageId2);
        assertThat(result.get(1).getMessageId()).isEqualTo(messageId6);

        verify(broker).beginTransaction(any(), any());

        ArgumentCaptor<MessageAck> ackCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(broker, times(4)).acknowledge(any(), ackCaptor.capture());

        List<MessageAck> values = ackCaptor.getAllValues();
        MessageAck messageAck = values.get(0);
        assertThat(messageAck.getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);
        assertThat(messageAck.getMessageCount()).isEqualTo(1);
        assertThat(messageAck.getLastMessageId()).isEqualTo(messageId1);
        messageAck = values.get(1);
        assertThat(messageAck.getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);
        assertThat(messageAck.getMessageCount()).isEqualTo(1);
        assertThat(messageAck.getLastMessageId()).isEqualTo(messageId3);

        verify(broker).commitTransaction(any(), any(), eq(true));
    }
}
