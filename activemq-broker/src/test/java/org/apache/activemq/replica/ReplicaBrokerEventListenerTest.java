package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.MessageReferenceFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.SubscriptionStatistics;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Set;

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

    private final Broker broker = mock(Broker.class);
    private final ActiveMQQueue testQueue = new ActiveMQQueue("TEST.QUEUE");
    private final ActiveMQQueue testTopic = new ActiveMQQueue("TEST.TOPIC");
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

        listener = new ReplicaBrokerEventListener(broker);
    }

    @Test
    public void canHandleEventOfType_DESTINATION_UPSERT_whenQueueNotExist() throws Exception {
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker).addDestination(connectionContext, testQueue, true);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_UPSERT_whenQueueExists() throws Exception {
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination, testQueue});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker, never()).addDestination(connectionContext, testQueue, true);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_DELETE_whenDestinationExists() throws Exception {
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination, testQueue});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.DESTINATION_DELETE)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker).removeDestination(connectionContext, testQueue, 1000);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_DESTINATION_DELETE_whenDestinationNotExists() throws Exception {
        ActiveMQDestination activeMQDestination = new ActiveMQQueue("NOT.EXIST");
        when(broker.getDestinations()).thenReturn(new ActiveMQDestination[]{activeMQDestination});

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.DESTINATION_DELETE)
                .setEventData(eventSerializer.serializeReplicationData(testQueue));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(replicaEventMessage);

        verify(broker, never()).removeDestination(connectionContext, testQueue, 1000);
        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_MESSAGE_SEND() throws Exception {
        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);

        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGE_SEND)
                .setEventData(eventSerializer.serializeMessageData(message));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

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
    public void canHandleEventOfType_MESSAGE_DROPPED() throws Exception {
        MessageId messageId = new MessageId("1:1:1:1");
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.MESSAGES_DROPPED)
                .setEventData(eventSerializer.serializeReplicationData(testQueue))
                .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, Collections.singletonList(messageId.toString()));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setProperties(event.getReplicationProperties());

        listener.onMessage(replicaEventMessage);

        ArgumentCaptor<MessageReferenceFilter> messageReferenceFilterArgumentCaptor = ArgumentCaptor.forClass(MessageReferenceFilter.class);
        verify((Queue) destinationQueue, times(1)).removeMatchingMessages(any(), messageReferenceFilterArgumentCaptor.capture(), eq(1));

        final MessageReferenceFilter value = messageReferenceFilterArgumentCaptor.getValue();
        assertThat(value).isInstanceOf(ReplicaBrokerEventListener.ListMessageReferenceFilter.class);
        assertThat(((ReplicaBrokerEventListener.ListMessageReferenceFilter) value).messageIds).containsExactly(messageId.toString());

        verify(replicaEventMessage).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_BEGIN() throws Exception {
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_BEGIN)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).beginTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_PREPARE() throws Exception {
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_PREPARE)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).prepareTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_FORGET() throws Exception {
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_FORGET)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).forgetTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_ROLLBACK() throws Exception {
        MessageId messageId = new MessageId("1:1");
        TransactionId transactionId = new LocalTransactionId(new ConnectionId("10101010"), 101010);
        ActiveMQMessage message = spy(new ActiveMQMessage());
        message.setMessageId(messageId);
        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TRANSACTION_ROLLBACK)
                .setEventData(eventSerializer.serializeReplicationData(transactionId));
        message.setContent(event.getEventData());
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());

        listener.onMessage(message);

        ArgumentCaptor<TransactionId> messageArgumentCaptor = ArgumentCaptor.forClass(TransactionId.class);
        verify(broker).rollbackTransaction(any(), messageArgumentCaptor.capture());
        TransactionId value = messageArgumentCaptor.getValue();
        assertThat(value).isEqualTo(transactionId);
        verify(message).acknowledge();
    }

    @Test
    public void canHandleEventOfType_TRANSACTION_COMMIT() throws Exception {
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
    public void canHandleEventOfType_TOPIC_MESSAGE_ACK() throws Exception {
        MessageId messageId = new MessageId("1:1:1:1");
        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testTopic);

        ConsumerInfo consumerInfo = new ConsumerInfo();
        String clientId = "CLIENT_ID";
        consumerInfo.setClientId(clientId);

        SubscriptionStatistics subscriptionStatistics = new SubscriptionStatistics();
        subscriptionStatistics.setEnabled(true);

        DurableTopicSubscription subscription = mock(DurableTopicSubscription.class);
        when(subscription.getConsumerInfo()).thenReturn(consumerInfo);
        when(subscription.getSubscriptionStatistics()).thenReturn(subscriptionStatistics);

        DestinationStatistics destinationStatistics = new DestinationStatistics();
        destinationStatistics.setEnabled(true);

        when(destinationTopic.getConsumers()).thenReturn(Collections.singletonList(subscription));
        when(destinationTopic.getDestinationStatistics()).thenReturn(destinationStatistics);

        ReplicaEvent event = new ReplicaEvent()
                .setEventType(ReplicaEventType.TOPIC_MESSAGE_ACK)
                .setEventData(eventSerializer.serializeReplicationData(message));
        ActiveMQMessage replicaEventMessage = spy(new ActiveMQMessage());
        replicaEventMessage.setType("ReplicaEvent");
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.CLIENT_ID_PROPERTY, clientId);
        replicaEventMessage.setByteProperty(ReplicaSupport.ACK_TYPE_PROPERTY, MessageAck.INDIVIDUAL_ACK_TYPE);
        replicaEventMessage.setContent(event.getEventData());

        listener.onMessage(replicaEventMessage);

        assertThat(destinationStatistics.getDequeues().getCount()).isEqualTo(1);
        assertThat(subscriptionStatistics.getDequeues().getCount()).isEqualTo(1);

        verify(subscription).removePending(eq(message));
        ArgumentCaptor<MessageAck> messageAckArgumentCaptor = ArgumentCaptor.forClass(MessageAck.class);
        verify(destinationTopic).acknowledge(eq(connectionContext), eq(subscription), messageAckArgumentCaptor.capture(), eq(message));
        MessageAck messageAck = messageAckArgumentCaptor.getValue();
        assertThat(messageAck.getAckType()).isEqualTo(MessageAck.INDIVIDUAL_ACK_TYPE);

        verify(replicaEventMessage).acknowledge();
    }
}
