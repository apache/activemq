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
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public abstract class ReplicaBaseSequenceStorage extends ReplicaBaseStorage {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBaseSequenceStorage.class);

    static final String SEQUENCE_NAME_PROPERTY = "SequenceName";
    private final String sequenceName;

    public ReplicaBaseSequenceStorage(Broker broker, ReplicaReplicationQueueSupplier queueProvider,
            ReplicaInternalMessageProducer replicaInternalMessageProducer, String sequenceName) {
        super(broker, queueProvider, replicaInternalMessageProducer);
        this.sequenceName = requireNonNull(sequenceName);
    }

    protected final List<ActiveMQTextMessage> initializeBase(ConnectionContext connectionContext) throws Exception {
        String selector = String.format("%s LIKE '%s'", SEQUENCE_NAME_PROPERTY, sequenceName);

        initializeBase(queueProvider.getSequenceQueue(), "ReplicationPlugin.ReplicaSequenceStorage", selector, connectionContext);

        return subscription.getDispatched().stream().map(MessageReference::getMessage)
                .map(ActiveMQTextMessage.class::cast).collect(Collectors.toList());
    }

    public void deinitialize(ConnectionContext connectionContext) throws Exception {
        queue = null;

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
}
