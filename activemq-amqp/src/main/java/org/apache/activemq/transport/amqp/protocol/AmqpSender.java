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
package org.apache.activemq.transport.amqp.protocol;

import static org.apache.activemq.transport.amqp.AmqpSupport.toLong;

import java.io.IOException;
import java.util.LinkedList;

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
import org.apache.activemq.transport.amqp.AmqpProtocolConverter;
import org.apache.activemq.transport.amqp.ResponseHandler;
import org.apache.activemq.transport.amqp.message.ActiveMQJMSVendor;
import org.apache.activemq.transport.amqp.message.AutoOutboundTransformer;
import org.apache.activemq.transport.amqp.message.EncodedMessage;
import org.apache.activemq.transport.amqp.message.OutboundTransformer;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
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

    private final OutboundTransformer outboundTransformer = new AutoOutboundTransformer(ActiveMQJMSVendor.INSTANCE);
    private final AmqpTransferTagGenerator tagCache = new AmqpTransferTagGenerator();
    private final LinkedList<MessageDispatch> outbound = new LinkedList<MessageDispatch>();
    private final LinkedList<MessageDispatch> dispatchedInTx = new LinkedList<MessageDispatch>();
    private final String MESSAGE_FORMAT_KEY = outboundTransformer.getPrefixVendor() + "MESSAGE_FORMAT";

    private final ConsumerInfo consumerInfo;
    private final boolean presettle;

    private boolean closed;
    private int currentCredit;
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

        this.currentCredit = endpoint.getRemoteCredit();
        this.consumerInfo = consumerInfo;
        this.presettle = getEndpoint().getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;
    }

    @Override
    public void open() {
        if (!closed) {
            session.regosterSender(getConsumerId(), this);
        }

        super.open();
    }

    @Override
    public void detach() {
        if (!isClosed() && isOpened()) {
            RemoveInfo removeCommand = new RemoveInfo(getConsumerId());
            removeCommand.setLastDeliveredSequenceId(lastDeliveredSequenceId);
            sendToActiveMQ(removeCommand, null);

            session.unregisterSender(getConsumerId());
        }

        super.detach();
    }

    @Override
    public void close() {
        if (!isClosed() && isOpened()) {
            RemoveInfo removeCommand = new RemoveInfo(getConsumerId());
            removeCommand.setLastDeliveredSequenceId(lastDeliveredSequenceId);
            sendToActiveMQ(removeCommand, null);

            if (consumerInfo.isDurable()) {
                RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
                rsi.setConnectionId(session.getConnection().getConnectionId());
                rsi.setSubscriptionName(getEndpoint().getName());
                rsi.setClientId(session.getConnection().getClientId());

                sendToActiveMQ(rsi, null);

                session.unregisterSender(getConsumerId());
            }
        }

        super.close();
    }

    @Override
    public void flow() throws Exception {
        int updatedCredit = getEndpoint().getCredit();

        LOG.trace("Flow: drain={} credit={}, remoteCredit={}",
                  getEndpoint().getDrain(), getEndpoint().getCredit(), getEndpoint().getRemoteCredit());

        if (getEndpoint().getDrain() && (updatedCredit != currentCredit || !draining)) {
            currentCredit = updatedCredit >= 0 ? updatedCredit : 0;
            draining = true;

            // Revert to a pull consumer.
            ConsumerControl control = new ConsumerControl();
            control.setConsumerId(getConsumerId());
            control.setDestination(getDestination());
            control.setPrefetch(0);
            sendToActiveMQ(control, null);

            // Now request dispatch of the drain amount, we request immediate
            // timeout and an completion message regardless so that we can know
            // when we should marked the link as drained.
            MessagePull pullRequest = new MessagePull();
            pullRequest.setConsumerId(getConsumerId());
            pullRequest.setDestination(getDestination());
            pullRequest.setTimeout(-1);
            pullRequest.setAlwaysSignalDone(true);
            pullRequest.setQuantity(currentCredit);
            sendToActiveMQ(pullRequest, null);
        } else if (updatedCredit != currentCredit) {
            currentCredit = updatedCredit >= 0 ? updatedCredit : 0;
            ConsumerControl control = new ConsumerControl();
            control.setConsumerId(getConsumerId());
            control.setDestination(getDestination());
            control.setPrefetch(currentCredit);
            sendToActiveMQ(control, null);
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
                    if (!delivery.remotelySettled()) {
                        TransactionalState txAccepted = new TransactionalState();
                        txAccepted.setOutcome(Accepted.getInstance());
                        txAccepted.setTxnId(((TransactionalState) state).getTxnId());

                        delivery.disposition(txAccepted);
                    }
                    settle(delivery, MessageAck.DELIVERED_ACK_TYPE);
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
                // re-deliver /w incremented delivery counter.
                md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                LOG.trace("onDelivery: Rejected state = {}, delivery count now {}", state, md.getRedeliveryCounter());
                settle(delivery, -1);
            } else if (state instanceof Released) {
                LOG.trace("onDelivery: Released state = {}", state);
                // re-deliver && don't increment the counter.
                settle(delivery, -1);
            } else if (state instanceof Modified) {
                Modified modified = (Modified) state;
                if (modified.getDeliveryFailed()) {
                    // increment delivery counter..
                    md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                }
                LOG.trace("onDelivery: Modified state = {}, delivery count now {}", state, md.getRedeliveryCounter());
                byte ackType = -1;
                Boolean undeliverableHere = modified.getUndeliverableHere();
                if (undeliverableHere != null && undeliverableHere) {
                    // receiver does not want the message..
                    // perhaps we should DLQ it?
                    ackType = MessageAck.POSION_ACK_TYPE;
                }
                settle(delivery, ackType);
            }
        }

        pumpOutbound();
    }

    @Override
    public void commit() throws Exception {
        if (!dispatchedInTx.isEmpty()) {
            for (MessageDispatch md : dispatchedInTx) {
                MessageAck pendingTxAck = new MessageAck(md, MessageAck.INDIVIDUAL_ACK_TYPE, 1);
                pendingTxAck.setFirstMessageId(md.getMessage().getMessageId());
                pendingTxAck.setTransactionId(md.getMessage().getTransactionId());

                LOG.trace("Sending commit Ack to ActiveMQ: {}", pendingTxAck);

                sendToActiveMQ(pendingTxAck, new ResponseHandler() {
                    @Override
                    public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            if (response.isException()) {
                                Throwable exception = ((ExceptionResponse) response).getException();
                                exception.printStackTrace();
                                getEndpoint().close();
                            }
                        }
                        session.pumpProtonToSocket();
                    }
                });
            }

            dispatchedInTx.clear();
        }
    }

    @Override
    public void rollback() throws Exception {
        synchronized (outbound) {

            LOG.trace("Rolling back {} messages for redelivery. ", dispatchedInTx.size());

            for (MessageDispatch dispatch : dispatchedInTx) {
                dispatch.setRedeliveryCounter(dispatch.getRedeliveryCounter() + 1);
                dispatch.getMessage().setTransactionId(null);
                outbound.addFirst(dispatch);
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
        while (!closed) {
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

                    // Topics can dispatch the same Message to more than one consumer
                    // so we must copy to prevent concurrent read / write to the same
                    // message object.
                    if (md.getDestination().isTopic()) {
                        synchronized (md.getMessage()) {
                            temp = (ActiveMQMessage) md.getMessage().copy();
                        }
                    } else {
                        temp = (ActiveMQMessage) md.getMessage();
                    }

                    if (!temp.getProperties().containsKey(MESSAGE_FORMAT_KEY)) {
                        temp.setProperty(MESSAGE_FORMAT_KEY, 0);
                    }
                }

                final ActiveMQMessage jms = temp;
                if (jms == null) {
                    LOG.info("End of browse signals endpoint drained.");
                    // It's the end of browse signal in response to a MessagePull
                    getEndpoint().drained();
                    draining = false;
                } else {
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

            DeliveryState remoteState = delivery.getRemoteState();
            if (remoteState != null && remoteState instanceof TransactionalState) {
                TransactionalState s = (TransactionalState) remoteState;
                long txid = toLong(s.getTxnId());
                LocalTransactionId localTxId = new LocalTransactionId(session.getConnection().getConnectionId(), txid);
                ack.setTransactionId(localTxId);

                // Store the message sent in this TX we might need to
                // re-send on rollback
                md.getMessage().setTransactionId(localTxId);
                dispatchedInTx.addFirst(md);
            }

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
