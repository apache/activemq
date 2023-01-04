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

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.filter.DestinationMapEntry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaSourceBrokerTest {

    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<Boolean>() {};
    private final Broker broker = mock(Broker.class);
    private final BrokerService brokerService = mock(BrokerService.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);

    private final URI transportConnectorUri = URI.create("tcp://0.0.0.0:61618?maximumConnections=1&amp;wireFormat.maxFrameSize=104857600");
    private final ReplicaSequencer replicaSequencer = mock(ReplicaSequencer.class);
    private final ReplicaReplicationQueueSupplier queueProvider = new ReplicaReplicationQueueSupplier(broker);
    private ReplicaSourceBroker source;
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final TransportConnector transportConnector = mock(TransportConnector.class);

    private final ActiveMQQueue testDestination = new ActiveMQQueue("TEST.QUEUE");

    @Before
    public void setUp() throws Exception {
        when(broker.getBrokerService()).thenReturn(brokerService);
        when(broker.getAdminConnectionContext()).thenReturn(connectionContext);
        when(brokerService.getBroker()).thenReturn(source);
        when(brokerService.addConnector(transportConnectorUri)).thenReturn(transportConnector);
        when(connectionContext.isProducerFlowControl()).thenReturn(true);
        when(connectionContext.getConnector()).thenReturn(transportConnector);
        when(transportConnector.getName()).thenReturn("test");
        when(connectionContext.getClientId()).thenReturn("clientId");

        ReplicaInternalMessageProducer replicaInternalMessageProducer = new ReplicaInternalMessageProducer(broker);
        ReplicationMessageProducer replicationMessageProducer = new ReplicationMessageProducer(replicaInternalMessageProducer, queueProvider);
        source = new ReplicaSourceBroker(broker, replicationMessageProducer, replicaSequencer, queueProvider, transportConnectorUri);
        when(brokerService.getBroker()).thenReturn(source);

        source.destinationsToReplicate.put(testDestination, IS_REPLICATED);
    }

    @Test
    public void createsQueueOnInitialization() throws Exception {
        source.start();

        ArgumentCaptor<ActiveMQDestination> destinationArgumentCaptor = ArgumentCaptor.forClass(ActiveMQDestination.class);
        verify(broker, times(2)).addDestination(eq(connectionContext), destinationArgumentCaptor.capture(), anyBoolean());

        List<ActiveMQDestination> replicationDestinations = destinationArgumentCaptor.getAllValues();
        assertThat(replicationDestinations.get(0).getPhysicalName()).isEqualTo(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        assertThat(replicationDestinations.get(1).getPhysicalName()).isEqualTo(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
    }

    @Test
    public void createsDestinationEventsOnStartup() throws Exception {
        doAnswer(invocation -> {
            source.addDestination(connectionContext, testDestination, true);
            return null;
        }).when(broker).start();

        Queue queue = mock(Queue.class);
        when(broker.addDestination(connectionContext, testDestination, true)).thenReturn(queue);

        source.start();

        ArgumentCaptor<ActiveMQDestination> destinationArgumentCaptor = ArgumentCaptor.forClass(ActiveMQDestination.class);
        verify(broker, times(3)).addDestination(eq(connectionContext), destinationArgumentCaptor.capture(), anyBoolean());

        List<ActiveMQDestination> destinations = destinationArgumentCaptor.getAllValues();

        ActiveMQDestination mainReplicationDestination = destinations.get(0);
        assertThat(mainReplicationDestination.getPhysicalName()).isEqualTo(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);

        ActiveMQDestination intermediateReplicationDestination = destinations.get(1);
        assertThat(intermediateReplicationDestination.getPhysicalName()).isEqualTo(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);

        ActiveMQDestination precreatedDestination = destinations.get(2);
        assertThat(precreatedDestination).isEqualTo(testDestination);
    }

    @Test
    public void doesNotCreateDestinationEventsForNonReplicableDestinations() throws Exception {
        source.start();

        ActiveMQTopic advisoryTopic = new ActiveMQTopic(AdvisorySupport.ADVISORY_TOPIC_PREFIX + "TEST");
        source.addDestination(connectionContext, advisoryTopic, true);

        ArgumentCaptor<ActiveMQDestination> destinationArgumentCaptor = ArgumentCaptor.forClass(ActiveMQDestination.class);
        verify(broker, times(3)).addDestination(eq(connectionContext), destinationArgumentCaptor.capture(), anyBoolean());

        List<ActiveMQDestination> destinations = destinationArgumentCaptor.getAllValues();

        ActiveMQDestination mainReplicationDestination = destinations.get(0);
        assertThat(mainReplicationDestination.getPhysicalName()).isEqualTo(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);

        ActiveMQDestination intermediateReplicationDestination = destinations.get(1);
        assertThat(intermediateReplicationDestination.getPhysicalName()).isEqualTo(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);

        ActiveMQDestination advisoryTopicDestination = destinations.get(2);
        assertThat(advisoryTopicDestination).isEqualTo(advisoryTopic);

        verify(broker, never()).send(any(), any());
    }

    @Test
    public void replicates_MESSAGE_SEND() throws Exception {
        source.start();

        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(testDestination);

        ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        producerExchange.setConnectionContext(connectionContext);

        source.send(producerExchange, message);

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(2)).send(any(), messageArgumentCaptor.capture());

        final List<ActiveMQMessage> values = messageArgumentCaptor.getAllValues();

        ActiveMQMessage originalMessage = values.get(0);
        assertThat(originalMessage).isEqualTo(message);

        ActiveMQMessage replicaMessage = values.get(1);
        assertThat(replicaMessage.getType()).isEqualTo("ReplicaEvent");
        assertThat(replicaMessage.getDestination().getPhysicalName()).isEqualTo(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        assertThat(replicaMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.MESSAGE_SEND.name());
        assertThat(eventSerializer.deserializeMessageData(replicaMessage.getContent())).isEqualTo(message);

        verifyConnectionContext(connectionContext);
    }

    private void verifyConnectionContext(ConnectionContext context) {
        verify(context).isProducerFlowControl();
        verify(context).setProducerFlowControl(false);
        verify(context).setProducerFlowControl(true);
    }

    @Test
    public void replicates_QUEUE_PURGED() throws Exception {
        source.start();

        source.queuePurged(connectionContext, testDestination);

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage replicaMessage = messageArgumentCaptor.getValue();

        assertThat(replicaMessage.getType()).isEqualTo("ReplicaEvent");
        assertThat(replicaMessage.getDestination().getPhysicalName()).isEqualTo(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        assertThat(replicaMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.QUEUE_PURGED.name());

        ActiveMQDestination sentMessage = (ActiveMQDestination) eventSerializer.deserializeMessageData(replicaMessage.getContent());
        assertThat(sentMessage).isEqualTo(testDestination);
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void replicates_BEGIN_TRANSACTION() throws Exception {
        source.start();

        TransactionId transactionId =  new XATransactionId();

        source.beginTransaction(connectionContext, transactionId);

        verify(broker, times(1)).beginTransaction(any(), eq(transactionId));
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(1)).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage replicationMessage = messageArgumentCaptor.getValue();
        final TransactionId replicatedTransactionId = (TransactionId) eventSerializer.deserializeMessageData(replicationMessage.getContent());
        assertThat(replicationMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.TRANSACTION_BEGIN.name());
        assertThat(replicatedTransactionId).isEqualTo(transactionId);
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void replicates_PREPARE_TRANSACTION() throws Exception {
        source.start();

        TransactionId transactionId = new XATransactionId();

        source.prepareTransaction(connectionContext, transactionId);

        verify(broker, times(1)).prepareTransaction(any(), eq(transactionId));
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(1)).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage replicationMessage = messageArgumentCaptor.getValue();
        final TransactionId replicatedTransactionId = (TransactionId) eventSerializer.deserializeMessageData(replicationMessage.getContent());
        assertThat(replicationMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.TRANSACTION_PREPARE.name());
        assertThat(replicatedTransactionId).isEqualTo(transactionId);
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void replicates_ROLLBACK_TRANSACTION() throws Exception {
        source.start();

        TransactionId transactionId = new XATransactionId();

        source.rollbackTransaction(connectionContext, transactionId);

        verify(broker, times(1)).rollbackTransaction(any(), eq(transactionId));
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(1)).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage replicationMessage = messageArgumentCaptor.getValue();
        final TransactionId replicatedTransactionId = (TransactionId) eventSerializer.deserializeMessageData(replicationMessage.getContent());
        assertThat(replicationMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.TRANSACTION_ROLLBACK.name());
        assertThat(replicatedTransactionId).isEqualTo(transactionId);
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void replicates_FORGET_TRANSACTION() throws Exception {
        source.start();

        TransactionId transactionId =  new XATransactionId();

        source.forgetTransaction(connectionContext, transactionId);

        verify(broker, times(1)).forgetTransaction(any(), eq(transactionId));
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(1)).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage replicationMessage = messageArgumentCaptor.getValue();
        final TransactionId replicatedTransactionId = (TransactionId) eventSerializer.deserializeMessageData(replicationMessage.getContent());
        assertThat(replicationMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.TRANSACTION_FORGET.name());
        assertThat(replicatedTransactionId).isEqualTo(transactionId);
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void replicates_COMMIT_TRANSACTION() throws Exception {
        source.start();

        TransactionId transactionId = new XATransactionId();

        source.commitTransaction(connectionContext, transactionId, true);

        verify(broker, times(1)).commitTransaction(any(), eq(transactionId), eq(true));
        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(1)).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage replicationMessage = messageArgumentCaptor.getValue();
        final TransactionId replicatedTransactionId = (TransactionId) eventSerializer.deserializeMessageData(replicationMessage.getContent());
        assertThat(replicationMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.TRANSACTION_COMMIT.name());
        assertThat(replicatedTransactionId).isEqualTo(transactionId);
        assertThat(replicationMessage.getProperty(ReplicaSupport.TRANSACTION_ONE_PHASE_PROPERTY)).isEqualTo(true);
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void letsCreateConsumerForReplicaQueueFromReplicaConnection() throws Exception {
        source.start();

        when(transportConnector.getName()).thenReturn(ReplicaSourceBroker.REPLICATION_CONNECTOR_NAME);

        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(queueProvider.getMainQueue());
        source.addConsumer(connectionContext, consumerInfo);

        verify(broker).addConsumer(eq(connectionContext), eq(consumerInfo));
    }

    @Test(expected = ActiveMQReplicaException.class)
    public void doesNotLetCreateConsumerForReplicaQueueFromNonReplicaConnection() throws Exception {
        source.start();

        when(transportConnector.getName()).thenReturn("test");

        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(queueProvider.getMainQueue());
        source.addConsumer(connectionContext, consumerInfo);
    }

    @Test
    public void letsCreateConsumerForNonReplicaAdvisoryTopicFromReplicaConnection() throws Exception {
        source.start();

        when(transportConnector.getName()).thenReturn(ReplicaSourceBroker.REPLICATION_CONNECTOR_NAME);

        ActiveMQTopic advisoryTopic = new ActiveMQTopic(AdvisorySupport.ADVISORY_TOPIC_PREFIX + "TEST");
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(advisoryTopic);
        source.addConsumer(connectionContext, consumerInfo);

        verify(broker).addConsumer(eq(connectionContext), eq(consumerInfo));
    }

    @Test
    public void letsCreateConsumerForNonReplicaQueueFromNonReplicaConnection() throws Exception {
        source.start();

        when(transportConnector.getName()).thenReturn("test");

        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(testDestination);
        source.addConsumer(connectionContext, consumerInfo);

        verify(broker).addConsumer(eq(connectionContext), eq(consumerInfo));
    }

    @Test(expected = ActiveMQReplicaException.class)
    public void doesNoLetCreateConsumerForNonReplicaQueueFromReplicaConnection() throws Exception {
        source.start();

        when(transportConnector.getName()).thenReturn(ReplicaSourceBroker.REPLICATION_CONNECTOR_NAME);

        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(testDestination);
        source.addConsumer(connectionContext, consumerInfo);
    }

    @Test(expected = ActiveMQReplicaException.class)
    public void doesNotLetCreateProducerForReplicaQueueFromNonReplicaConnection() throws Exception {
        source.start();

        when(transportConnector.getName()).thenReturn("test");

        ProducerInfo producerInfo = new ProducerInfo();
        producerInfo.setDestination(queueProvider.getMainQueue());
        source.addProducer(connectionContext, producerInfo);
    }

    @Test
    public void letsCreateProducerForReplicaQueueFromReplicaConnection() throws Exception {
        source.start();

        when(transportConnector.getName()).thenReturn(ReplicaSourceBroker.REPLICATION_CONNECTOR_NAME);

        ProducerInfo producerInfo = new ProducerInfo();
        producerInfo.setDestination(queueProvider.getMainQueue());
        source.addProducer(connectionContext, producerInfo);

        verify(broker).addProducer(eq(connectionContext), eq(producerInfo));
    }

    @Test
    public void letsCreateProducerForNonReplicaQueueFromNonReplicaConnection() throws Exception {
        source.start();

        when(transportConnector.getName()).thenReturn("test");

        ProducerInfo producerInfo = new ProducerInfo();
        producerInfo.setDestination(testDestination);
        source.addProducer(connectionContext, producerInfo);

        verify(broker).addProducer(eq(connectionContext), eq(producerInfo));
    }

    @Test(expected = ActiveMQReplicaException.class)
    public void doesNotLetCreateProducerForNonReplicaQueueFromReplicaConnection() throws Exception {
        source.start();

        when(transportConnector.getName()).thenReturn(ReplicaSourceBroker.REPLICATION_CONNECTOR_NAME);

        ProducerInfo producerInfo = new ProducerInfo();
        producerInfo.setDestination(testDestination);
        source.addProducer(connectionContext, producerInfo);
    }

    @Test
    public void replicates_ADD_DURABLE_CONSUMER() throws Exception {
        source.start();

        ActiveMQTopic destination = new ActiveMQTopic("TEST.TOPIC");

        ConsumerInfo message = new ConsumerInfo();
        message.setDestination(destination);
        message.setSubscriptionName("SUBSCRIPTION_NAME");

        source.addConsumer(connectionContext, message);

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage replicaMessage = messageArgumentCaptor.getValue();

        assertThat(replicaMessage.getType()).isEqualTo("ReplicaEvent");
        assertThat(replicaMessage.getDestination().getPhysicalName()).isEqualTo(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        assertThat(replicaMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.ADD_DURABLE_CONSUMER.name());

        final ConsumerInfo ackMessage = (ConsumerInfo) eventSerializer.deserializeMessageData(replicaMessage.getContent());
        assertThat(ackMessage.getDestination()).isEqualTo(destination);
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void replicates_REMOVE_DURABLE_CONSUMER() throws Exception {
        source.start();

        ActiveMQTopic destination = new ActiveMQTopic("TEST.TOPIC");

        ConsumerInfo message = new ConsumerInfo();
        message.setDestination(destination);
        message.setSubscriptionName("SUBSCRIPTION_NAME");

        source.removeConsumer(connectionContext, message);

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());
        ActiveMQMessage replicaMessage = messageArgumentCaptor.getValue();

        assertThat(replicaMessage.getType()).isEqualTo("ReplicaEvent");
        assertThat(replicaMessage.getDestination().getPhysicalName()).isEqualTo(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        assertThat(replicaMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.REMOVE_DURABLE_CONSUMER.name());

        final ConsumerInfo ackMessage = (ConsumerInfo) eventSerializer.deserializeMessageData(replicaMessage.getContent());
        assertThat(ackMessage.getDestination()).isEqualTo(destination);
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void replicates_MESSAGE_ACK_individual() throws Exception {
        source.start();

        MessageId messageId = new MessageId("1:1");

        ConsumerId consumerId = new ConsumerId("2:2:2:2");
        MessageAck messageAck = new MessageAck();
        messageAck.setMessageID(messageId);
        messageAck.setConsumerId(consumerId);
        messageAck.setDestination(testDestination);
        messageAck.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);

        Queue queue = mock(Queue.class);
        when(broker.getDestinations(testDestination)).thenReturn(Set.of(queue));
        PrefetchSubscription subscription = mock(PrefetchSubscription.class);
        when(queue.getConsumers()).thenReturn(List.of(subscription));
        ConsumerInfo consumerInfo = new ConsumerInfo(consumerId);
        when(subscription.getConsumerInfo()).thenReturn(consumerInfo);

        ConsumerBrokerExchange cbe = new ConsumerBrokerExchange();
        cbe.setConnectionContext(connectionContext);
        source.acknowledge(cbe, messageAck);

        ArgumentCaptor<ActiveMQMessage> sendMessageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(1)).send(any(), sendMessageArgumentCaptor.capture());
        ActiveMQMessage replicationMessage = sendMessageArgumentCaptor.getValue();
        final MessageAck originalMessage = (MessageAck) eventSerializer.deserializeMessageData(replicationMessage.getContent());
        assertThat(replicationMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.MESSAGE_ACK.name());
        assertThat(originalMessage.getLastMessageId()).isEqualTo(messageId);
        assertThat(originalMessage.getDestination()).isEqualTo(testDestination);
        assertThat((List<String>) replicationMessage.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY)).containsOnly(messageId.toString());
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void replicates_MESSAGE_ACK_standard() throws Exception {
        source.start();

        MessageId firstMessageId = new MessageId("1:1");
        MessageId secondMessageId = new MessageId("1:2");
        MessageId thirdMessageId = new MessageId("1:3");

        ActiveMQMessage firstMessage = new ActiveMQMessage();
        firstMessage.setMessageId(firstMessageId);
        ActiveMQMessage secondMessage = new ActiveMQMessage();
        secondMessage.setMessageId(secondMessageId);
        ActiveMQMessage thirdMessage = new ActiveMQMessage();
        thirdMessage.setMessageId(thirdMessageId);

        ConsumerId consumerId = new ConsumerId("2:2:2:2");
        MessageAck messageAck = new MessageAck();
        messageAck.setConsumerId(consumerId);
        messageAck.setFirstMessageId(firstMessageId);
        messageAck.setLastMessageId(thirdMessageId);
        messageAck.setDestination(testDestination);
        messageAck.setAckType(MessageAck.STANDARD_ACK_TYPE);

        Queue queue = mock(Queue.class);
        when(broker.getDestinations(testDestination)).thenReturn(Set.of(queue));
        PrefetchSubscription subscription = mock(PrefetchSubscription.class);
        when(queue.getConsumers()).thenReturn(List.of(subscription));
        ConsumerInfo consumerInfo = new ConsumerInfo(consumerId);
        when(subscription.getConsumerInfo()).thenReturn(consumerInfo);
        when(subscription.getDispatched()).thenReturn(List.of(
                new IndirectMessageReference(firstMessage),
                new IndirectMessageReference(secondMessage),
                new IndirectMessageReference(thirdMessage)
        ));

        ConsumerBrokerExchange cbe = new ConsumerBrokerExchange();
        cbe.setConnectionContext(connectionContext);
        source.acknowledge(cbe, messageAck);

        ArgumentCaptor<ActiveMQMessage> sendMessageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker, times(1)).send(any(), sendMessageArgumentCaptor.capture());
        ActiveMQMessage replicationMessage = sendMessageArgumentCaptor.getValue();
        final MessageAck originalMessage = (MessageAck) eventSerializer.deserializeMessageData(replicationMessage.getContent());
        assertThat(replicationMessage.getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)).isEqualTo(ReplicaEventType.MESSAGE_ACK.name());
        assertThat(originalMessage.getFirstMessageId()).isEqualTo(firstMessageId);
        assertThat(originalMessage.getLastMessageId()).isEqualTo(thirdMessageId);
        assertThat(originalMessage.getDestination()).isEqualTo(testDestination);
        assertThat((List<String>) replicationMessage.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY))
                .containsOnly(firstMessageId.toString(), secondMessageId.toString(), thirdMessageId.toString());
        verifyConnectionContext(connectionContext);
    }

    @Test
    public void doesNotReplicateAdvisoryTopics() throws Exception {
        source.start();

        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);
        message.setDestination(testDestination);

        ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        producerExchange.setConnectionContext(connectionContext);

        source.send(producerExchange, message);

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(broker).send(any(), messageArgumentCaptor.capture());

        final List<ActiveMQMessage> values = messageArgumentCaptor.getAllValues();

        ActiveMQMessage originalMessage = values.get(0);
        assertThat(originalMessage).isEqualTo(message);

        verify(connectionContext, never()).isProducerFlowControl();
        verify(connectionContext, never()).setProducerFlowControl(anyBoolean());
    }
}
