/*
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
package org.apache.activemq.transport.amqp.protocol;

import static org.apache.activemq.transport.amqp.AmqpSupport.toLong;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.broker.region.AbstractSubscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.transport.amqp.AmqpProtocolConverter;
import org.apache.activemq.transport.amqp.ResponseHandler;
import org.apache.activemq.transport.amqp.message.AutoOutboundTransformer;
import org.apache.activemq.transport.amqp.message.EncodedMessage;
import org.apache.activemq.transport.amqp.message.OutboundTransformer;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AmqpSender wraps the AMQP Sender end of a link from the remote peer
 * which holds the corresponding Receiver which receives messages transfered
 * across the link from the Broker.
 *
 * An AmqpSender is in turn a message consumer subscribed to some destination
 * on the broker.  As messages are dispatched to this sender that are sent on
 * to the remote Receiver end of the lin.
 */
public class AmqpSender extends AmqpAbstractLink<Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSender.class);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    private final OutboundTransformer outboundTransformer = new AutoOutboundTransformer();
    private final AmqpTransferTagGenerator tagCache = new AmqpTransferTagGenerator();
    private final LinkedList<MessageDispatch> outbound = new LinkedList<>();
    private final LinkedList<Delivery> dispatchedInTx = new LinkedList<>();

    private final ConsumerInfo consumerInfo;
    private AbstractSubscription subscription;
    private AtomicInteger prefetchExtension;
    private int currentCreditRequest;
    private int logicalDeliveryCount; // echoes prefetch extension but from protons perspective
    private final boolean presettle;

    private boolean draining;
    private long lastDeliveredSequenceId;

    private Buffer currentBuffer;
    private Delivery currentDelivery;

    /**
     * Creates a new AmqpSender instance that manages the given Sender
     *
     * @param session
     *        the AmqpSession object that is the parent of this instance.
     * @param endpoint
     *        the AMQP Sender instance that this class manages.
     * @param consumerInfo
     *        the ConsumerInfo instance that holds configuration for this sender.
     */
    public AmqpSender(AmqpSession session, Sender endpoint, ConsumerInfo consumerInfo) {
        super(session, endpoint);

        // We don't support second so enforce it as First and let remote decide what to do
        this.endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        // Match what the sender mode is
        this.endpoint.setSenderSettleMode(endpoint.getRemoteSenderSettleMode());

        this.consumerInfo = consumerInfo;
        this.presettle = getEndpoint().getSenderSettleMode() == SenderSettleMode.SETTLED;
    }

    @Override
    public void open() {
        if (!isClosed()) {
            session.registerSender(getConsumerId(), this);
            subscription = (AbstractSubscription)session.getConnection().lookupPrefetchSubscription(consumerInfo);
            prefetchExtension = subscription.getPrefetchExtension();
        }

        super.open();
    }

    @Override
    public void detach() {
        if (!isClosed() && isOpened()) {
            RemoveInfo removeCommand = new RemoveInfo(getConsumerId());
            removeCommand.setLastDeliveredSequenceId(lastDeliveredSequenceId);

            sendToActiveMQ(removeCommand, new ResponseHandler() {

                @Override
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    session.unregisterSender(getConsumerId());
                    AmqpSender.super.detach();
                }
            });
        } else {
            super.detach();
        }
    }

    @Override
    public void close() {
        if (!isClosed() && isOpened()) {
            RemoveInfo removeCommand = new RemoveInfo(getConsumerId());
            removeCommand.setLastDeliveredSequenceId(lastDeliveredSequenceId);

            sendToActiveMQ(removeCommand, new ResponseHandler() {

                @Override
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    if (consumerInfo.isDurable()) {
                        RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
                        rsi.setConnectionId(session.getConnection().getConnectionId());
                        rsi.setSubscriptionName(getEndpoint().getName());
                        rsi.setClientId(session.getConnection().getClientId());

                        sendToActiveMQ(rsi);
                    }

                    session.unregisterSender(getConsumerId());
                    AmqpSender.super.close();
                }
            });
        } else {
            super.close();
        }
    }

    @Override
    public void flow() throws Exception {
        Link endpoint = getEndpoint();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Flow: draining={}, drain={} credit={}, currentCredit={}, senderDeliveryCount={} - Sub={}",
                    draining, endpoint.getDrain(),
                    endpoint.getCredit(), currentCreditRequest, logicalDeliveryCount, subscription);
        }

        final int endpointCredit = endpoint.getCredit();
        if (endpoint.getDrain() && !draining) {

            if (endpointCredit > 0) {
                draining = true;

                // Now request dispatch of the drain amount, we request immediate
                // timeout and an completion message regardless so that we can know
                // when we should marked the link as drained.
                MessagePull pullRequest = new MessagePull();
                pullRequest.setConsumerId(getConsumerId());
                pullRequest.setDestination(getDestination());
                pullRequest.setTimeout(-1);
                pullRequest.setAlwaysSignalDone(true);
                pullRequest.setQuantity(endpointCredit);

                LOG.trace("Pull case -> consumer pull request quantity = {}", endpointCredit);

                sendToActiveMQ(pullRequest);
            } else {
                LOG.trace("Pull case -> sending any Queued messages and marking drained");

                pumpOutbound();
                getEndpoint().drained();
                session.pumpProtonToSocket();
                currentCreditRequest = 0;
                logicalDeliveryCount = 0;
            }
        } else if (endpointCredit >= 0) {

            if (endpointCredit == 0 && currentCreditRequest != 0) {
                prefetchExtension.set(0);
                currentCreditRequest = 0;
                logicalDeliveryCount = 0;
                LOG.trace("Flow: credit 0 for sub:" + subscription);
            } else {
                int deltaToAdd = endpointCredit;
                int logicalCredit = currentCreditRequest - logicalDeliveryCount;
                if (logicalCredit > 0) {
                    deltaToAdd -= logicalCredit;
                } else {
                    // reset delivery counter - dispatch from broker concurrent with credit=0
                    // flow can go negative
                    logicalDeliveryCount = 0;
                }

                if (deltaToAdd > 0) {
                    currentCreditRequest = prefetchExtension.addAndGet(deltaToAdd);
                    subscription.wakeupDestinationsForDispatch();
                    // force dispatch of matched/pending for topics (pending messages accumulate
                    // in the sub and are dispatched on update of prefetch)
                    subscription.setPrefetchSize(0);
                    LOG.trace("Flow: credit addition of {} for sub {}", deltaToAdd, subscription);
                }
            }
        }
    }

    @Override
    public void delivery(Delivery delivery) throws Exception {
        MessageDispatch md = (MessageDispatch) delivery.getContext();
        DeliveryState state = delivery.getRemoteState();

        if (state instanceof TransactionalState) {
            TransactionalState txState = (TransactionalState) state;
            LOG.trace("onDelivery: TX delivery state = {}", state);
            if (txState.getOutcome() != null) {
                Outcome outcome = txState.getOutcome();
                if (outcome instanceof Accepted) {
                    TransactionId txId = new LocalTransactionId(session.getConnection().getConnectionId(), toLong(txState.getTxnId()));

                    // Store the message sent in this TX we might need to re-send on rollback
                    // and we need to ACK it on commit.
                    session.enlist(txId);
                    dispatchedInTx.addFirst(delivery);

                    if (!delivery.remotelySettled()) {
                        TransactionalState txAccepted = new TransactionalState();
                        txAccepted.setOutcome(Accepted.getInstance());
                        txAccepted.setTxnId(txState.getTxnId());

                        delivery.disposition(txAccepted);
                    }
                }
            }
        } else {
            if (state instanceof Accepted) {
                LOG.trace("onDelivery: accepted state = {}", state);
                if (!delivery.remotelySettled()) {
                    delivery.disposition(new Accepted());
                }
                settle(delivery, MessageAck.INDIVIDUAL_ACK_TYPE);
            } else if (state instanceof Rejected) {
                // Rejection is a terminal outcome, we poison the message for dispatch to
                // the DLQ.  If a custom redelivery policy is used on the broker the message
                // can still be redelivered based on the configation of that policy.
                LOG.trace("onDelivery: Rejected state = {}, message poisoned.", state);
                settle(delivery, MessageAck.POISON_ACK_TYPE);
            } else if (state instanceof Released) {
                LOG.trace("onDelivery: Released state = {}", state);
                // re-deliver && don't increment the counter.
                settle(delivery, -1);
            } else if (state instanceof Modified) {
                Modified modified = (Modified) state;
                if (Boolean.TRUE.equals(modified.getDeliveryFailed())) {
                    // increment delivery counter..
                    md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                }
                LOG.trace("onDelivery: Modified state = {}, delivery count now {}", state, md.getRedeliveryCounter());
                byte ackType = -1;
                Boolean undeliverableHere = modified.getUndeliverableHere();
                if (undeliverableHere != null && undeliverableHere) {
                    // receiver does not want the message..
                    // perhaps we should DLQ it?
                    ackType = MessageAck.POISON_ACK_TYPE;
                }
                settle(delivery, ackType);
            }
        }

        pumpOutbound();
    }

    @Override
    public void commit(LocalTransactionId txnId) throws Exception {
        if (!dispatchedInTx.isEmpty()) {
            for (final Delivery delivery : dispatchedInTx) {
                MessageDispatch dispatch = (MessageDispatch) delivery.getContext();

                MessageAck pendingTxAck = new MessageAck(dispatch, MessageAck.INDIVIDUAL_ACK_TYPE, 1);
                pendingTxAck.setFirstMessageId(dispatch.getMessage().getMessageId());
                pendingTxAck.setTransactionId(txnId);

                LOG.trace("Sending commit Ack to ActiveMQ: {}", pendingTxAck);

                sendToActiveMQ(pendingTxAck, new ResponseHandler() {
                    @Override
                    public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            Throwable exception = ((ExceptionResponse) response).getException();
                            exception.printStackTrace();
                            getEndpoint().close();
                        } else {
                            delivery.settle();
                        }
                        session.pumpProtonToSocket();
                    }
                });
            }

            dispatchedInTx.clear();
        }
    }

    @Override
    public void rollback(LocalTransactionId txnId) throws Exception {
        synchronized (outbound) {

            LOG.trace("Rolling back {} messages for redelivery. ", dispatchedInTx.size());

            for (Delivery delivery : dispatchedInTx) {
                // Only settled deliveries should be re-dispatched, unsettled deliveries
                // remain acquired on the remote end and can be accepted again in a new
                // TX or released or rejected etc.
                MessageDispatch dispatch = (MessageDispatch) delivery.getContext();
                dispatch.getMessage().setTransactionId(null);

                if (delivery.remotelySettled()) {
                    dispatch.setRedeliveryCounter(dispatch.getRedeliveryCounter() + 1);
                    outbound.addFirst(dispatch);
                }
            }

            dispatchedInTx.clear();
        }
    }

    /**
     * Event point for incoming message from ActiveMQ on this Sender's
     * corresponding subscription.
     *
     * @param dispatch
     *        the MessageDispatch to process and send across the link.
     *
     * @throws Exception if an error occurs while encoding the message for send.
     */
    public void onMessageDispatch(MessageDispatch dispatch) throws Exception {
        if (!isClosed()) {
            // Lock to prevent stepping on TX redelivery
            synchronized (outbound) {
                outbound.addLast(dispatch);
            }
            pumpOutbound();
            session.pumpProtonToSocket();
        }
    }

    /**
     * Called when the Broker sends a ConsumerControl command to the Consumer that
     * this sender creates to obtain messages to dispatch via the sender for this
     * end of the open link.
     *
     * @param control
     *        The ConsumerControl command to process.
     */
    public void onConsumerControl(ConsumerControl control) {
        if (control.isClose()) {
            close(new ErrorCondition(AmqpError.INTERNAL_ERROR, "Receiver forcably closed"));
            session.pumpProtonToSocket();
        }
    }

    @Override
    public String toString() {
        return "AmqpSender {" + getConsumerId() + "}";
    }

    //----- Property getters and setters -------------------------------------//

    public ConsumerId getConsumerId() {
        return consumerInfo.getConsumerId();
    }

    @Override
    public ActiveMQDestination getDestination() {
        return consumerInfo.getDestination();
    }

    @Override
    public void setDestination(ActiveMQDestination destination) {
        consumerInfo.setDestination(destination);
    }

    //----- Internal Implementation ------------------------------------------//

    public void pumpOutbound() throws Exception {
        while (!isClosed()) {
            while (currentBuffer != null) {
                int sent = getEndpoint().send(currentBuffer.data, currentBuffer.offset, currentBuffer.length);
                if (sent > 0) {
                    currentBuffer.moveHead(sent);
                    if (currentBuffer.length == 0) {
                        if (presettle) {
                            settle(currentDelivery, MessageAck.INDIVIDUAL_ACK_TYPE);
                        } else {
                            getEndpoint().advance();
                        }
                        currentBuffer = null;
                        currentDelivery = null;
                        logicalDeliveryCount++;
                    }
                } else {
                    return;
                }
            }

            if (outbound.isEmpty()) {
                return;
            }

            final MessageDispatch md = outbound.removeFirst();
            try {

                ActiveMQMessage temp = null;
                if (md.getMessage() != null) {
                    temp = (ActiveMQMessage) md.getMessage().copy();
                }

                final ActiveMQMessage jms = temp;
                if (jms == null) {
                    LOG.trace("Sender:[{}] browse done.", getEndpoint().getName());
                    // It's the end of browse signal in response to a MessagePull
                    getEndpoint().drained();
                    draining = false;
                    currentCreditRequest = 0;
                    logicalDeliveryCount = 0;
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Sender:[{}] msgId={} draining={}, drain={}, credit={}, remoteCredit={}, queued={}",
                                  getEndpoint().getName(), jms.getJMSMessageID(), draining, getEndpoint().getDrain(),
                                  getEndpoint().getCredit(), getEndpoint().getRemoteCredit(), getEndpoint().getQueued());
                    }

                    if (draining && getEndpoint().getCredit() == 0) {
                        LOG.trace("Sender:[{}] browse complete.", getEndpoint().getName());
                        getEndpoint().drained();
                        draining = false;
                        currentCreditRequest = 0;
                        logicalDeliveryCount = 0;
                    }

                    jms.setRedeliveryCounter(md.getRedeliveryCounter());
                    jms.setReadOnlyBody(true);
                    final EncodedMessage amqp = outboundTransformer.transform(jms);
                    if (amqp != null && amqp.getLength() > 0) {
                        currentBuffer = new Buffer(amqp.getArray(), amqp.getArrayOffset(), amqp.getLength());
                        if (presettle) {
                            currentDelivery = getEndpoint().delivery(EMPTY_BYTE_ARRAY, 0, 0);
                        } else {
                            final byte[] tag = tagCache.getNextTag();
                            currentDelivery = getEndpoint().delivery(tag, 0, tag.length);
                        }
                        currentDelivery.setContext(md);
                        currentDelivery.setMessageFormat((int) amqp.getMessageFormat());
                    } else {
                        // TODO: message could not be generated what now?
                    }
                }
            } catch (Exception e) {
                LOG.warn("Error detected while flushing outbound messages: {}", e.getMessage());
            }
        }
    }

    private void settle(final Delivery delivery, final int ackType) throws Exception {
        byte[] tag = delivery.getTag();
        if (tag != null && tag.length > 0 && delivery.remotelySettled()) {
            tagCache.returnTag(tag);
        }

        if (ackType == -1) {
            // we are going to settle, but redeliver.. we we won't yet ack to ActiveMQ
            delivery.settle();
            onMessageDispatch((MessageDispatch) delivery.getContext());
        } else {
            MessageDispatch md = (MessageDispatch) delivery.getContext();
            lastDeliveredSequenceId = md.getMessage().getMessageId().getBrokerSequenceId();
            MessageAck ack = new MessageAck();
            ack.setConsumerId(getConsumerId());
            ack.setFirstMessageId(md.getMessage().getMessageId());
            ack.setLastMessageId(md.getMessage().getMessageId());
            ack.setMessageCount(1);
            ack.setAckType((byte) ackType);
            ack.setDestination(md.getDestination());
            LOG.trace("Sending Ack to ActiveMQ: {}", ack);

            sendToActiveMQ(ack, new ResponseHandler() {
                @Override
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    if (response.isException()) {
                        if (response.isException()) {
                            Throwable exception = ((ExceptionResponse) response).getException();
                            exception.printStackTrace();
                            getEndpoint().close();
                        }
                    } else {
                        delivery.settle();
                    }
                    session.pumpProtonToSocket();
                }
            });
        }
    }
}
