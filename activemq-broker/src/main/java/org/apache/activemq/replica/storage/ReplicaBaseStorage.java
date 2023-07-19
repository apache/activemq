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
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
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
import org.apache.activemq.replica.ReplicaSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public abstract class ReplicaBaseStorage {

    protected final ProducerId replicationProducerId = new ProducerId();
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();

    protected Broker broker;
    protected ConnectionContext connectionContext;
    protected ReplicaInternalMessageProducer replicaInternalMessageProducer;
    protected ActiveMQQueue destination;
    protected Queue queue;
    private final String idGeneratorPrefix;
    private final String selector;
    protected PrefetchSubscription subscription;

    public ReplicaBaseStorage(Broker broker, ReplicaInternalMessageProducer replicaInternalMessageProducer,
            ActiveMQQueue destination, String idGeneratorPrefix, String selector) {
        this.broker = requireNonNull(broker);
        this.replicaInternalMessageProducer = requireNonNull(replicaInternalMessageProducer);
        this.destination = destination;
        this.idGeneratorPrefix = idGeneratorPrefix;
        this.selector = selector;

        replicationProducerId.setConnectionId(new IdGenerator().generateId());
    }

    protected List<ActiveMQTextMessage> initializeBase(ConnectionContext connectionContext) throws Exception {
        queue = broker.getDestinations(destination).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();
        ConnectionId connectionId = new ConnectionId(new IdGenerator(idGeneratorPrefix).generateId());
        SessionId sessionId = new SessionId(connectionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerId consumerId = new ConsumerId(sessionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(ReplicaSupport.INTERMEDIATE_QUEUE_PREFETCH_SIZE);
        consumerInfo.setDestination(destination);
        if (selector != null) {
            consumerInfo.setSelector(selector);
        }
        subscription = (PrefetchSubscription) broker.addConsumer(connectionContext, consumerInfo);
        queue.iterate();

        return subscription.getDispatched().stream().map(MessageReference::getMessage)
                .map(ActiveMQTextMessage.class::cast).collect(Collectors.toList());
    }

    protected void acknowledgeAll(ConnectionContext connectionContext, TransactionId tid) throws Exception {
        List<MessageReference> dispatched = subscription.getDispatched();

        if (!dispatched.isEmpty()) {
            MessageAck ack = new MessageAck(dispatched.get(dispatched.size() - 1).getMessage(), MessageAck.STANDARD_ACK_TYPE, dispatched.size());
            ack.setFirstMessageId(dispatched.get(0).getMessageId());
            ack.setDestination(destination);
            ack.setTransactionId(tid);
            acknowledge(connectionContext, ack);
        }
    }

    protected void acknowledge(ConnectionContext connectionContext, MessageAck ack) throws Exception {
        ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
        consumerExchange.setConnectionContext(connectionContext);
        consumerExchange.setSubscription(subscription);

        broker.acknowledge(consumerExchange, ack);
    }

    public void enqueue(ConnectionContext connectionContext, TransactionId tid, String message) throws Exception {
        // before enqueue message, we acknowledge all messages currently in queue.
        acknowledgeAll(connectionContext, tid);

        send(connectionContext, tid, message,
                new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
    }

    public void send(ConnectionContext connectionContext, TransactionId tid, String message, MessageId messageId) throws Exception {
        ActiveMQTextMessage seqMessage = new ActiveMQTextMessage();
        seqMessage.setText(message);
        seqMessage.setTransactionId(tid);
        seqMessage.setDestination(destination);
        seqMessage.setMessageId(messageId);
        seqMessage.setProducerId(replicationProducerId);
        seqMessage.setPersistent(true);
        seqMessage.setResponseRequired(false);

        send(connectionContext, seqMessage);
    }

    public void send(ConnectionContext connectionContext, ActiveMQTextMessage seqMessage) throws Exception {
        replicaInternalMessageProducer.sendForcingFlowControl(connectionContext, seqMessage);
    }
}
