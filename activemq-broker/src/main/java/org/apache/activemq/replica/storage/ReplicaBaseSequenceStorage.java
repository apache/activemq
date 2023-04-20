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
package org.apache.activemq.replica.storage;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerStoppedException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.DestinationExtractor;
import org.apache.activemq.replica.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.replica.ReplicaSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public abstract class ReplicaBaseSequenceStorage {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBaseSequenceStorage.class);

    static final String SEQUENCE_NAME_PROPERTY = "SequenceName";
    protected final ProducerId replicationProducerId = new ProducerId();
    private final Broker broker;
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;
    private final String sequenceName;
    protected final ReplicaReplicationQueueSupplier queueProvider;

    protected Queue sequenceQueue;
    protected PrefetchSubscription subscription;

    public ReplicaBaseSequenceStorage(Broker broker, ReplicaReplicationQueueSupplier queueProvider,
            ReplicaInternalMessageProducer replicaInternalMessageProducer, String sequenceName) {
        this.broker = requireNonNull(broker);
        this.replicaInternalMessageProducer = replicaInternalMessageProducer;
        this.sequenceName = requireNonNull(sequenceName);
        this.queueProvider = queueProvider;

        replicationProducerId.setConnectionId(new IdGenerator().generateId());
    }

    protected final List<ActiveMQTextMessage> initializeBase(ConnectionContext connectionContext) throws Exception {
        sequenceQueue = broker.getDestinations(queueProvider.getSequenceQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();

        String selector = String.format("%s LIKE '%s'", SEQUENCE_NAME_PROPERTY, sequenceName);

        ConnectionId connectionId = new ConnectionId(new IdGenerator("ReplicationPlugin.ReplicaSequenceStorage").generateId());
        SessionId sessionId = new SessionId(connectionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerId consumerId = new ConsumerId(sessionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(ReplicaSupport.INTERMEDIATE_QUEUE_PREFETCH_SIZE);
        consumerInfo.setDestination(queueProvider.getSequenceQueue());
        consumerInfo.setSelector(selector);
        subscription = (PrefetchSubscription) broker.addConsumer(connectionContext, consumerInfo);
        sequenceQueue.iterate();

        return subscription.getDispatched().stream().map(MessageReference::getMessage)
                .map(ActiveMQTextMessage.class::cast).collect(Collectors.toList());
    }

    public void deinitialize(ConnectionContext connectionContext) throws Exception {
        sequenceQueue = null;

        if (subscription != null) {
            try {
                broker.removeConsumer(connectionContext, subscription.getConsumerInfo());
            } catch (BrokerStoppedException ignored) {}
            subscription = null;
        }
    }

    public void send(ConnectionContext connectionContext, TransactionId tid, String message, MessageId messageId) throws Exception {
        ActiveMQTextMessage seqMessage = new ActiveMQTextMessage();
        seqMessage.setText(message);
        seqMessage.setTransactionId(tid);
        seqMessage.setDestination(queueProvider.getSequenceQueue());
        seqMessage.setMessageId(messageId);
        seqMessage.setProducerId(replicationProducerId);
        seqMessage.setPersistent(true);
        seqMessage.setResponseRequired(false);
        seqMessage.setStringProperty(SEQUENCE_NAME_PROPERTY, sequenceName);

        replicaInternalMessageProducer.sendIgnoringFlowControl(connectionContext, seqMessage);
    }

    protected void acknowledge(ConnectionContext connectionContext, MessageAck ack) throws Exception {
        ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
        consumerExchange.setConnectionContext(connectionContext);
        consumerExchange.setSubscription(subscription);

        broker.acknowledge(consumerExchange, ack);
    }
}
