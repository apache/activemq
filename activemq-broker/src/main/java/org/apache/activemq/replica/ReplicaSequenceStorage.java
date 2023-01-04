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
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ReplicaSequenceStorage {

    private final Logger logger = LoggerFactory.getLogger(ReplicaSequenceStorage.class);

    static final String SEQUENCE_NAME_PROPERTY = "SequenceName";
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();
    private final ProducerId replicationProducerId = new ProducerId();
    private final Broker broker;
    private final ConnectionContext connectionContext;
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;
    private final String sequenceName;
    private final ReplicaReplicationQueueSupplier queueProvider;

    private Queue sequenceQueue;
    private PrefetchSubscription subscription;

    public ReplicaSequenceStorage(Broker broker, ConnectionContext connectionContext, ReplicaReplicationQueueSupplier queueProvider,
                                  ReplicaInternalMessageProducer replicaInternalMessageProducer, String sequenceName) {
        this.broker = requireNonNull(broker);
        this.connectionContext = connectionContext;
        this.replicaInternalMessageProducer = replicaInternalMessageProducer;
        this.sequenceName = requireNonNull(sequenceName);
        this.queueProvider = queueProvider;

        replicationProducerId.setConnectionId(new IdGenerator().generateId());
    }

    public String initialize() throws Exception {
        sequenceQueue = broker.getDestinations(queueProvider.getSequenceQueue()).stream().findFirst()
            .map(DestinationExtractor::extractQueue).orElseThrow();

        String selector = String.format("%s LIKE '%s'", SEQUENCE_NAME_PROPERTY, sequenceName);

        ConnectionId connectionId = new ConnectionId(new IdGenerator("ReplicationPlugin.ReplicaSequenceStorage").generateId());
        SessionId sessionId = new SessionId(connectionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerId consumerId = new ConsumerId(sessionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(10);
        consumerInfo.setDestination(queueProvider.getSequenceQueue());
        consumerInfo.setSelector(selector);
        subscription = (PrefetchSubscription) broker.addConsumer(connectionContext, consumerInfo);

        List<ActiveMQTextMessage> allMessages = new ArrayList<>();
        for (MessageId messageId : sequenceQueue.getAllMessageIds()) {
            ActiveMQTextMessage message = getMessageByMessageId(messageId);
            if (message.getStringProperty(SEQUENCE_NAME_PROPERTY).equals(sequenceName)) {
                allMessages.add(message);
            }
        }

        if (allMessages.size() == 0) {
            return null;
        }

        if (allMessages.size() > 1) {
            for (int i = 0; i < allMessages.size() - 1; i++) {
                sequenceQueue.removeMessage(allMessages.get(i).getMessageId().toString());
            }
        }

        return allMessages.get(0).getText();
    }

    public void enqueue(TransactionId tid, String message) throws Exception {
        // before enqueue message, we acknowledge all messages currently in queue.
        acknowledgeAll(tid);

        send(tid, message);
    }

    private void acknowledgeAll(TransactionId tid) throws Exception {
        List<MessageReference> dispatched = subscription.getDispatched();
        ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
        consumerExchange.setConnectionContext(connectionContext);
        consumerExchange.setSubscription(subscription);

        for(MessageReference messageReference: dispatched) {
            MessageAck ack = new MessageAck(messageReference.getMessage(), MessageAck.INDIVIDUAL_ACK_TYPE, 1);
            ack.setDestination(queueProvider.getSequenceQueue());
            ack.setTransactionId(tid);
            broker.acknowledge(consumerExchange, ack);
        }
    }

    private void send(TransactionId tid, String message) throws Exception {
        ActiveMQTextMessage seqMessage = new ActiveMQTextMessage();
        seqMessage.setText(message);
        seqMessage.setTransactionId(tid);
        seqMessage.setDestination(queueProvider.getSequenceQueue());
        seqMessage.setMessageId(new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
        seqMessage.setProducerId(replicationProducerId);
        seqMessage.setPersistent(true);
        seqMessage.setResponseRequired(false);
        seqMessage.setStringProperty(SEQUENCE_NAME_PROPERTY, sequenceName);

        replicaInternalMessageProducer.sendIgnoringFlowControl(connectionContext, seqMessage);
    }

    private ActiveMQTextMessage getMessageByMessageId(MessageId messageId) {
        QueueMessageReference messageReference = sequenceQueue.getMessage(messageId.toString());
        return ((ActiveMQTextMessage) messageReference.getMessage());
    }
}
