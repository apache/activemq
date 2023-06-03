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
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.MessageReferenceFilter;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReplicaCompactor {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaCompactor.class);

    private final Broker broker;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final PrefetchSubscription subscription;
    private final int additionalMessagesLimit;
    private final ReplicaStatistics replicaStatistics;

    private final Queue intermediateQueue;

    public ReplicaCompactor(Broker broker, ReplicaReplicationQueueSupplier queueProvider, PrefetchSubscription subscription,
            int additionalMessagesLimit, ReplicaStatistics replicaStatistics) {
        this.broker = broker;
        this.queueProvider = queueProvider;
        this.subscription = subscription;
        this.additionalMessagesLimit = additionalMessagesLimit;
        this.replicaStatistics = replicaStatistics;

        intermediateQueue = broker.getDestinations(queueProvider.getIntermediateQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();
    }

    List<MessageReference> compactAndFilter(ConnectionContext connectionContext, List<MessageReference> list,
            boolean withAdditionalMessages) throws Exception {
        List<DeliveredMessageReference> toProcess = list.stream()
                .map(DeliveredMessageReference::new)
                .collect(Collectors.toList());

        int prefetchSize = subscription.getPrefetchSize();
        int maxPageSize = intermediateQueue.getMaxPageSize();
        int maxExpirePageSize = intermediateQueue.getMaxExpirePageSize();
        try {
            if (withAdditionalMessages) {
                subscription.setPrefetchSize(0);
                intermediateQueue.setMaxPageSize(0);
                intermediateQueue.setMaxExpirePageSize(0);
                toProcess.addAll(getAdditionalMessages(connectionContext, list));
            }

            List<DeliveredMessageReference> processed = compactAndFilter0(connectionContext, toProcess);

            Set<MessageId> messageIds = list.stream().map(MessageReference::getMessageId).collect(Collectors.toSet());

            return processed.stream()
                    .map(dmr -> dmr.messageReference)
                    .filter(mr -> messageIds.contains(mr.getMessageId()))
                    .collect(Collectors.toList());
        } finally {
            subscription.setPrefetchSize(prefetchSize);
            intermediateQueue.setMaxPageSize(maxPageSize);
            intermediateQueue.setMaxExpirePageSize(maxExpirePageSize);
        }
    }

    private List<DeliveredMessageReference> getAdditionalMessages(ConnectionContext connectionContext,
            List<MessageReference> toProcess) throws Exception {
        List<MessageReference> dispatched = subscription.getDispatched();
        Set<String> dispatchedMessageIds = dispatched.stream()
                .map(MessageReference::getMessageId)
                .map(MessageId::toString)
                .collect(Collectors.toSet());

        Set<String> toProcessIds = toProcess.stream()
                .map(MessageReference::getMessageId)
                .map(MessageId::toString)
                .collect(Collectors.toSet());

        Set<String> ignore = new HashSet<>();
        for (int i = 0; i < ReplicaSupport.INTERMEDIATE_QUEUE_PREFETCH_SIZE / additionalMessagesLimit + 1; i++) {
            List<QueueMessageReference> acks =
                    intermediateQueue.getMatchingMessages(connectionContext,
                            new AckMessageReferenceFilter(toProcessIds, dispatchedMessageIds, ignore, dispatched),
                            additionalMessagesLimit);
            if (acks.isEmpty()) {
                return new ArrayList<>();
            }

            Set<String> ackedMessageIds = getAckedMessageIds(acks);
            List<QueueMessageReference> sends = intermediateQueue.getMatchingMessages(connectionContext,
                    new SendMessageReferenceFilter(toProcessIds, dispatchedMessageIds, ackedMessageIds), ackedMessageIds.size());
            if (sends.isEmpty()) {
                acks.stream().map(MessageReference::getMessageId).map(MessageId::toString)
                        .forEach(ignore::add);
                continue;
            }

            return Stream.concat(acks.stream(), sends.stream().filter(mr -> !toProcessIds.contains(mr.getMessageId().toString())))
                    .map(mr -> new DeliveredMessageReference(mr, false))
                    .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    private List<DeliveredMessageReference> compactAndFilter0(ConnectionContext connectionContext,
            List<DeliveredMessageReference> list) throws Exception {
        List<DeliveredMessageReference> result = new ArrayList<>(list);

        List<SendsAndAcks> sendsAndAcksList = combineByDestination(list);

        List<DeliveredMessageReference> toDelete = compact(sendsAndAcksList);

        if (toDelete.isEmpty()) {
            return result;
        }

        acknowledge(connectionContext, toDelete);

        Set<MessageId> messageIds = toDelete.stream().map(dmid -> dmid.messageReference.getMessageId()).collect(Collectors.toSet());
        result.removeIf(reference -> messageIds.contains(reference.messageReference.getMessageId()));

        replicaStatistics.increaseTpsCounter(toDelete.size());

        return result;
    }

    private void acknowledge(ConnectionContext connectionContext, List<DeliveredMessageReference> list) throws Exception {
        List<MessageReference> notDelivered = list.stream()
                .filter(dmr -> !dmr.delivered)
                .map(DeliveredMessageReference::getReference)
                .collect(Collectors.toList());
        if (!notDelivered.isEmpty()) {
            intermediateQueue.dispatchNotification(subscription, notDelivered);
        }

        TransactionId transactionId = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
        boolean rollbackOnFail = false;

        try {
            broker.beginTransaction(connectionContext, transactionId);
            rollbackOnFail = true;

            ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
            consumerExchange.setConnectionContext(connectionContext);
            consumerExchange.setSubscription(subscription);

            for (DeliveredMessageReference dmr : list) {
                MessageAck messageAck = new MessageAck();
                messageAck.setMessageID(dmr.messageReference.getMessageId());
                messageAck.setTransactionId(transactionId);
                messageAck.setMessageCount(1);
                messageAck.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);
                messageAck.setDestination(queueProvider.getIntermediateQueue());

                broker.acknowledge(consumerExchange, messageAck);
            }

            broker.commitTransaction(connectionContext, transactionId, true);
        } catch (Exception e) {
            logger.error("Failed to persist messages in the main replication queue", e);
            if (rollbackOnFail) {
                try {
                    broker.rollbackTransaction(connectionContext, transactionId);
                } catch (Exception ex) {
                    logger.error("Could not rollback transaction", ex);
                }
            }
            throw e;
        }
    }

    private static List<SendsAndAcks> combineByDestination(List<DeliveredMessageReference> list) throws Exception {
        Map<String, SendsAndAcks> result = new HashMap<>();
        for (DeliveredMessageReference reference : list) {
            ActiveMQMessage message = (ActiveMQMessage) reference.messageReference.getMessage();

            ReplicaEventType eventType =
                    ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));
            if (eventType != ReplicaEventType.MESSAGE_SEND && eventType != ReplicaEventType.MESSAGE_ACK) {
                continue;
            }

            if (!message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY)
                    || message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY)) {
                continue;
            }

            SendsAndAcks sendsAndAcks =
                    result.computeIfAbsent(message.getStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY),
                            SendsAndAcks::new);

            if (eventType == ReplicaEventType.MESSAGE_SEND) {
                List<DeliveredMessageReference> sends = sendsAndAcks.sendMap
                        .computeIfAbsent(message.getStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY), o -> new ArrayList<>());
                sends.add(new DeliveredMessageReference(message, reference.delivered));
            }
            if (eventType == ReplicaEventType.MESSAGE_ACK) {
                List<String> messageIds = getAckMessageIds(message);
                sendsAndAcks.acks.add(new Ack(messageIds, message, reference.delivered));
            }
        }

        return new ArrayList<>(result.values());
    }

    private List<DeliveredMessageReference> compact(List<SendsAndAcks> sendsAndAcksList) throws IOException {
        List<DeliveredMessageReference> result = new ArrayList<>();
        Set<String> sendMessageIds = new HashSet<>();
        for (SendsAndAcks sendsAndAcks : sendsAndAcksList) {
            for (Ack ack : sendsAndAcks.acks) {
                List<DeliveredMessageReference> sends = new ArrayList<>();
                List<String> sendIds = new ArrayList<>();
                for (String id : ack.messageIdsToAck) {
                    if (!sendsAndAcks.sendMap.containsKey(id)) {
                        continue;
                    }
                    List<DeliveredMessageReference> sendMessages = sendsAndAcks.sendMap.get(id);
                    DeliveredMessageReference message = null;
                    for (DeliveredMessageReference dmr : sendMessages) {
                        if (!sendMessageIds.contains(dmr.messageReference.getMessageId().toString())) {
                            message = dmr;
                            break;
                        }
                    }
                    if (message == null) {
                        continue;
                    }
                    sendIds.add(id);
                    sends.add(message);
                    sendMessageIds.add(message.messageReference.getMessageId().toString());
                }
                if (sendIds.size() == 0) {
                    continue;
                }

                if (ack.messageIdsToAck.size() == sendIds.size() && new HashSet<>(ack.messageIdsToAck).containsAll(sendIds)) {
                    result.addAll(sends);
                    result.add(ack);
                }
            }
        }
        return result;
    }

    private Set<String> getAckedMessageIds(List<QueueMessageReference> ackMessages) throws IOException {
        Set<String> messageIds = new HashSet<>();
        for (QueueMessageReference messageReference : ackMessages) {
            ActiveMQMessage message = (ActiveMQMessage) messageReference.getMessage();

            messageIds.addAll(getAckMessageIds(message));
        }
        return messageIds;
    }

    @SuppressWarnings("unchecked")
    private static List<String> getAckMessageIds(ActiveMQMessage message) throws IOException {
        return (List<String>) message.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY);
    }

    private static class DeliveredMessageReference {
        final MessageReference messageReference;
        final boolean delivered;

        public DeliveredMessageReference(MessageReference messageReference) {
            this(messageReference, true);
        }

        public DeliveredMessageReference(MessageReference messageReference, boolean delivered) {
            this.messageReference = messageReference;
            this.delivered = delivered;
        }

        public QueueMessageReference getReference() {
            if (messageReference instanceof QueueMessageReference) {
                return (QueueMessageReference) messageReference;
            }
            return new IndirectMessageReference(messageReference.getMessage());
        }
    }

    private static class SendsAndAcks {
        final String destination;
        final Map<String, List<DeliveredMessageReference>> sendMap = new LinkedHashMap<>();
        final List<Ack> acks = new ArrayList<>();

        private SendsAndAcks(String destination) {
            this.destination = destination;
        }
    }

    private static class Ack extends DeliveredMessageReference {
        final List<String> messageIdsToAck;
        final ActiveMQMessage message;

        public Ack(List<String> messageIdsToAck, ActiveMQMessage message, boolean needsDelivery) {
            super(message, needsDelivery);
            this.messageIdsToAck = messageIdsToAck;
            this.message = message;
        }
    }

    static class AckMessageReferenceFilter extends InternalMessageReferenceFilter {

        private final Map<String, SendsAndAcks> existingSendsAndAcks;

        public AckMessageReferenceFilter(Set<String> toProcess, Set<String> dispatchedMessageIds,
                Set<String> ignore, List<MessageReference> dispatched) throws Exception {
            super(toProcess, dispatchedMessageIds, ignore, ReplicaEventType.MESSAGE_ACK);
            List<DeliveredMessageReference> list = dispatched.stream()
                    .filter(mr -> !toProcess.contains(mr.getMessageId().toString()))
                    .map(DeliveredMessageReference::new)
                    .collect(Collectors.toList());
            existingSendsAndAcks = combineByDestination(list).stream().collect(Collectors.toMap(o -> o.destination, Function.identity()));
        }

        @Override
        public boolean evaluate(ActiveMQMessage message) throws JMSException {
            if (!message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY)
                    || message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY)) {
                return false;
            }

            String destination = message.getStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY);
            SendsAndAcks sendsAndAcks = existingSendsAndAcks.get(destination);
            if (sendsAndAcks == null) {
                return true;
            }

            List<String> messageIds;
            try {
                messageIds = (List<String>) message.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY);
            } catch (IOException e) {
                throw JMSExceptionSupport.create(e);
            }

            return !sendsAndAcks.sendMap.keySet().containsAll(messageIds);
        }
    }

    static class SendMessageReferenceFilter extends InternalMessageReferenceFilter {

        private final Set<String> ackedMessageIds;

        public SendMessageReferenceFilter(Set<String> toProcess, Set<String> dispatchedMessageIds,
                Set<String> ackedMessageIds) {
            super(toProcess, dispatchedMessageIds, new HashSet<>(), ReplicaEventType.MESSAGE_SEND);
            this.ackedMessageIds = ackedMessageIds;
        }

        @Override
        public boolean evaluate(ActiveMQMessage message) throws JMSException {
            if (!message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY)
                    || message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY)) {
                return false;
            }

            String messageId = message.getStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY);
            return ackedMessageIds.contains(messageId);
        }
    }

    private static abstract class InternalMessageReferenceFilter implements MessageReferenceFilter {

        private final Set<String> toProcess;
        private final Set<String> dispatchedMessageIds;
        private final Set<String> ignore;
        private final ReplicaEventType eventType;

        public InternalMessageReferenceFilter(Set<String> toProcess, Set<String> dispatchedMessageIds,
                Set<String> ignore, ReplicaEventType eventType) {
            this.toProcess = toProcess;
            this.dispatchedMessageIds = dispatchedMessageIds;
            this.ignore = ignore;
            this.eventType = eventType;
        }

        @Override
        public boolean evaluate(ConnectionContext context, MessageReference messageReference) throws JMSException {
            String messageId = messageReference.getMessageId().toString();
            if (ignore.contains(messageId)) {
                return false;
            }

            if (dispatchedMessageIds.contains(messageId) && !toProcess.contains(messageId)) {
                return false;
            }
            ActiveMQMessage message = (ActiveMQMessage) messageReference.getMessage();

            ReplicaEventType eventType =
                    ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));

            if (eventType != this.eventType) {
                return false;
            }
            return evaluate(message);
        }

        public abstract boolean evaluate(ActiveMQMessage message) throws JMSException;
    }
}
