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
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of the STOMP subscription so that acking is correctly done.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class StompSubscription {

    private static final Logger LOG = LoggerFactory.getLogger(StompSubscription.class);

    private static final IdGenerator ACK_ID_GENERATOR = new IdGenerator();

    public static final String AUTO_ACK = Stomp.Headers.Subscribe.AckModeValues.AUTO;
    public static final String CLIENT_ACK = Stomp.Headers.Subscribe.AckModeValues.CLIENT;
    public static final String INDIVIDUAL_ACK = Stomp.Headers.Subscribe.AckModeValues.INDIVIDUAL;

    protected final ProtocolConverter protocolConverter;
    protected final String subscriptionId;
    protected final ConsumerInfo consumerInfo;

    protected final Map<MessageId, StompAckEntry> dispatchedMessage = new LinkedHashMap<>();
    protected final Map<String, StompAckEntry> pendingAcks; // STOMP v1.2 requires ACK ID tracking
    protected final LinkedList<StompAckEntry> transactedMessages = new LinkedList<>();

    protected String ackMode = AUTO_ACK;
    protected ActiveMQDestination destination;
    protected String transformation;

    public StompSubscription(ProtocolConverter stompTransport, String subscriptionId, ConsumerInfo consumerInfo, String transformation, Map<String, StompAckEntry> pendingAcks) {
        this.protocolConverter = stompTransport;
        this.subscriptionId = subscriptionId;
        this.consumerInfo = consumerInfo;
        this.transformation = transformation;
        this.pendingAcks = pendingAcks;
    }

    void onMessageDispatch(MessageDispatch md) throws IOException, JMSException {
        ActiveMQMessage message = (ActiveMQMessage)md.getMessage();

        String ackId = null;
        if (isClientAck() || isIndividualAck()) {
            ackId = ACK_ID_GENERATOR.generateId();
            StompAckEntry pendingAck = new StompAckEntry(md, ackId, this);

            synchronized (this) {
                dispatchedMessage.put(message.getMessageId(), pendingAck);
            }
            if (protocolConverter.isStomp12()) {
                this.pendingAcks.put(ackId, pendingAck);
            }
        } else if (isAutoAck()) {
            MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1);
            protocolConverter.getStompTransport().sendToActiveMQ(ack);
        }

        boolean ignoreTransformation = false;

        if (transformation != null && !( message instanceof ActiveMQBytesMessage ) ) {
            message.setReadOnlyProperties(false);
            message.setStringProperty(Stomp.Headers.TRANSFORMATION, transformation);
        } else {
            if (message.getStringProperty(Stomp.Headers.TRANSFORMATION) != null) {
                ignoreTransformation = true;
            }
        }

        StompFrame command = protocolConverter.convertMessage(message, ignoreTransformation);

        command.setAction(Stomp.Responses.MESSAGE);
        if (subscriptionId != null) {
            command.getHeaders().put(Stomp.Headers.Message.SUBSCRIPTION, subscriptionId);
        }

        if (protocolConverter.isStomp12() && ackId != null) {
            command.getHeaders().put(Stomp.Headers.Message.ACK_ID, ackId);
        }

        try {
            protocolConverter.getStompTransport().sendToStomp(command);
        } catch (IOException ex) {
            if (ackId != null) {
                pendingAcks.remove(ackId);
            }
            throw ex;
        }
    }

    synchronized void onStompAbort(TransactionId transactionId) {
        // Restore the pending ACKs so that their ACK IDs are again valid for a client
        // to operate on.
        LOG.trace("Transaction Abort restoring {} pending ACKs to valid state.", transactedMessages.size());
        for (StompAckEntry ackEntry : transactedMessages) {
            if (protocolConverter.isStomp12()) {
                pendingAcks.put(ackEntry.getAckId(), ackEntry);
            }
        }
        transactedMessages.clear();
    }

    void onStompCommit(TransactionId transactionId) {
        MessageAck ack = null;
        synchronized (this) {
            for (Iterator<StompAckEntry> iterator = dispatchedMessage.values().iterator(); iterator.hasNext();) {
                StompAckEntry ackEntry = iterator.next();
                if (transactedMessages.contains(ackEntry)) {
                    iterator.remove();
                }
            }

            // For individual Ack we already sent an Ack that will be applied on commit
            // we don't send a second standard Ack as that would produce an error.
            if (!transactedMessages.isEmpty() && isClientAck()) {
                ack = new MessageAck(transactedMessages.getLast().getMessageDispatch(), MessageAck.STANDARD_ACK_TYPE, transactedMessages.size());
                ack.setTransactionId(transactionId);
                transactedMessages.clear();
            }
        }
        // avoid contention with onMessageDispatch
        if (ack != null) {
            protocolConverter.getStompTransport().sendToActiveMQ(ack);
        }
    }

    synchronized MessageAck onStompMessageAck(String messageId, TransactionId transactionId) {
        MessageId msgId = new MessageId(messageId);

        final StompAckEntry ackEntry = dispatchedMessage.get(msgId);
        if (ackEntry == null) {
            return null;
        }

        MessageAck ack = new MessageAck();
        ack.setDestination(consumerInfo.getDestination());
        ack.setConsumerId(consumerInfo.getConsumerId());

        if (isClientAck()) {
            if (transactionId == null) {
                ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
            } else {
                ack.setAckType(MessageAck.DELIVERED_ACK_TYPE);
            }
            int count = 0;
            for (Iterator<StompAckEntry> iterator = dispatchedMessage.values().iterator(); iterator.hasNext();) {
                StompAckEntry entry = iterator.next();
                MessageId current = entry.getMessageId();

                if (entry.getAckId() != null) {
                    pendingAcks.remove(entry.getAckId());
                }

                if (transactionId != null) {
                    if (!transactedMessages.contains(entry)) {
                        transactedMessages.add(entry);
                        count++;
                    }
                } else {
                    iterator.remove();
                    count++;
                }

                if (current.equals(msgId)) {
                    ack.setLastMessageId(current);
                    break;
                }
            }
            ack.setMessageCount(count);
            if (transactionId != null) {
                ack.setTransactionId(transactionId);
            }
        } else if (isIndividualAck()) {
            if (ackEntry.getAckId() != null) {
                pendingAcks.remove(ackEntry.getAckId());
            }
            ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);
            ack.setMessageID(msgId);
            ack.setMessageCount(1);
            if (transactionId != null) {
                transactedMessages.add(dispatchedMessage.get(msgId));
                ack.setTransactionId(transactionId);
            } else {
                dispatchedMessage.remove(msgId);
            }
        }

        return ack;
    }

    public MessageAck onStompMessageNack(String messageId, TransactionId transactionId) throws ProtocolException {
        MessageId msgId = new MessageId(messageId);

        if (!dispatchedMessage.containsKey(msgId)) {
            return null;
        }

        final StompAckEntry ackEntry = dispatchedMessage.get(msgId);

        if (ackEntry.getAckId() != null) {
            pendingAcks.remove(ackEntry.getAckId());
        }

        MessageAck ack = new MessageAck();
        ack.setDestination(consumerInfo.getDestination());
        ack.setConsumerId(consumerInfo.getConsumerId());
        ack.setAckType(MessageAck.POISON_ACK_TYPE);
        ack.setMessageID(msgId);
        if (transactionId != null) {
            transactedMessages.add(ackEntry);
            ack.setTransactionId(transactionId);
        } else {
            dispatchedMessage.remove(msgId);
        }

        return ack;
    }

    public String getAckMode() {
        return ackMode;
    }

    public void setAckMode(String ackMode) {
        this.ackMode = ackMode;
    }

    public boolean isAutoAck() {
        return ackMode.equals(AUTO_ACK);
    }

    public boolean isClientAck() {
        return ackMode.equals(CLIENT_ACK);
    }

    public boolean isIndividualAck() {
        return ackMode.equals(INDIVIDUAL_ACK);
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    public ConsumerInfo getConsumerInfo() {
        return consumerInfo;
    }
}
