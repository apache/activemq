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
package org.apache.activemq.transport.amqp.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.InvalidDestinationException;

import org.apache.activemq.transport.amqp.client.util.AsyncResult;
import org.apache.activemq.transport.amqp.client.util.ClientFuture;
import org.apache.activemq.transport.amqp.client.util.UnmodifiableSender;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sender class that manages a Proton sender endpoint.
 */
public class AmqpSender extends AmqpAbstractResource<Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSender.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    public static final long DEFAULT_SEND_TIMEOUT = 15000;

    private final AmqpTransferTagGenerator tagGenerator = new AmqpTransferTagGenerator(true);
    private final AtomicBoolean closed = new AtomicBoolean();

    private final AmqpSession session;
    private final String address;
    private final String senderId;
    private final Target userSpecifiedTarget;

    private boolean presettle;
    private long sendTimeout = DEFAULT_SEND_TIMEOUT;

    private final Set<Delivery> pending = new LinkedHashSet<Delivery>();
    private byte[] encodeBuffer = new byte[1024 * 8];

    /**
     * Create a new sender instance.
     *
     * @param session
     * 		  The parent session that created the session.
     * @param address
     *        The address that this sender produces to.
     * @param senderId
     *        The unique ID assigned to this sender.
     */
    public AmqpSender(AmqpSession session, String address, String senderId) {

        if (address != null && address.isEmpty()) {
            throw new IllegalArgumentException("Address cannot be empty.");
        }

        this.session = session;
        this.address = address;
        this.senderId = senderId;
        this.userSpecifiedTarget = null;
    }

    /**
     * Create a new sender instance using the given Target when creating the link.
     *
     * @param session
     *        The parent session that created the session.
     * @param address
     *        The address that this sender produces to.
     * @param senderId
     *        The unique ID assigned to this sender.
     */
    public AmqpSender(AmqpSession session, Target target, String senderId) {

        if (target == null) {
            throw new IllegalArgumentException("User specified Target cannot be null");
        }

        this.session = session;
        this.userSpecifiedTarget = target;
        this.address = target.getAddress();
        this.senderId = senderId;
    }

    /**
     * Sends the given message to this senders assigned address.
     *
     * @param message
     *        the message to send.
     *
     * @throws IOException if an error occurs during the send.
     */
    public void send(final AmqpMessage message) throws IOException {
        checkClosed();
        final ClientFuture sendRequest = new ClientFuture();

        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                try {
                    doSend(message, sendRequest);
                    session.pumpToProtonTransport();
                } catch (Exception e) {
                    sendRequest.onFailure(e);
                    session.getConnection().fireClientException(e);
                }
            }
        });

        if (sendTimeout <= 0) {
            sendRequest.sync();
        } else {
            sendRequest.sync(sendTimeout, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Close the sender, a closed sender will throw exceptions if any further send
     * calls are made.
     *
     * @throws IOException if an error occurs while closing the sender.
     */
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            final ClientFuture request = new ClientFuture();
            session.getScheduler().execute(new Runnable() {

                @Override
                public void run() {
                    checkClosed();
                    close(request);
                    session.pumpToProtonTransport();
                }
            });

            request.sync();
        }
    }

    /**
     * @return this session's parent AmqpSession.
     */
    public AmqpSession getSession() {
        return session;
    }

    /**
     * @return an unmodifiable view of the underlying Sender instance.
     */
    public Sender getSender() {
        return new UnmodifiableSender(getEndpoint());
    }

    /**
     * @return the assigned address of this sender.
     */
    public String getAddress() {
        return address;
    }

    //----- Sender configuration ---------------------------------------------//

    /**
     * @return will messages be settle on send.
     */
    public boolean isPresettle() {
        return presettle;
    }

    /**
     * Configure is sent messages are marked as settled on send, defaults to false.
     *
     * @param presettle
     * 		  configure if this sender will presettle all sent messages.
     */
    public void setPresettle(boolean presettle) {
        this.presettle = presettle;
    }

    /**
     * @return the currently configured send timeout.
     */
    public long getSendTimeout() {
        return sendTimeout;
    }

    /**
     * Sets the amount of time the sender will block on a send before failing.
     *
     * @param sendTimeout
     *        time in milliseconds to wait.
     */
    public void setSendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    //----- Private Sender implementation ------------------------------------//

    private void checkClosed() {
        if (isClosed()) {
            throw new IllegalStateException("Sender is already closed");
        }
    }

    @Override
    protected void doOpen() {

        Symbol[] outcomes = new Symbol[]{ Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL };
        Source source = new Source();
        source.setAddress(senderId);
        source.setOutcomes(outcomes);

        Target target = userSpecifiedTarget;
        if (target == null) {
            target = new Target();
            target.setAddress(address);
        }

        String senderName = senderId + ":" + address;

        Sender sender = session.getEndpoint().sender(senderName);
        sender.setSource(source);
        sender.setTarget(target);
        if (presettle) {
            sender.setSenderSettleMode(SenderSettleMode.SETTLED);
        } else {
            sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        }
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        setEndpoint(sender);

        super.doOpen();
    }

    @Override
    protected void doOpenCompletion() {
        // Verify the attach response contained a non-null target
        org.apache.qpid.proton.amqp.transport.Target t = getEndpoint().getRemoteTarget();
        if (t != null) {
            super.doOpenCompletion();
        } else {
            // No link terminus was created, the peer will now detach/close us.
        }
    }

    @Override
    protected void doOpenInspection() {
        getStateInspector().inspectOpenedResource(getSender());
    }

    @Override
    protected void doClosedInspection() {
        getStateInspector().inspectClosedResource(getSender());
    }

    @Override
    protected void doDetachedInspection() {
        getStateInspector().inspectDetachedResource(getSender());
    }

    @Override
    protected Exception getOpenAbortException() {
        // Verify the attach response contained a non-null target
        org.apache.qpid.proton.amqp.transport.Target t = getEndpoint().getRemoteTarget();
        if (t != null) {
            return super.getOpenAbortException();
        } else {
            // No link terminus was created, the peer has detach/closed us, create IDE.
            return new InvalidDestinationException("Link creation was refused");
        }
    }

    private void doSend(AmqpMessage message, AsyncResult request) throws Exception {

        LOG.trace("Producer sending message: {}", message);

        byte[] tag = tagGenerator.getNextTag();
        Delivery delivery = null;

        if (presettle) {
            delivery = getEndpoint().delivery(EMPTY_BYTE_ARRAY, 0, 0);
        } else {
            delivery = getEndpoint().delivery(tag, 0, tag.length);
        }

        delivery.setContext(request);

        encodeAndSend(message.getWrappedMessage(), delivery);

        if (presettle) {
            delivery.settle();
            request.onSuccess();
        } else {
            pending.add(delivery);
            getEndpoint().advance();
        }
    }

    private void encodeAndSend(Message message, Delivery delivery) throws IOException {

        int encodedSize;
        while (true) {
            try {
                encodedSize = message.encode(encodeBuffer, 0, encodeBuffer.length);
                break;
            } catch (java.nio.BufferOverflowException e) {
                encodeBuffer = new byte[encodeBuffer.length * 2];
            }
        }

        int sentSoFar = 0;

        while (true) {
            int sent = getEndpoint().send(encodeBuffer, sentSoFar, encodedSize - sentSoFar);
            if (sent > 0) {
                sentSoFar += sent;
                if ((encodedSize - sentSoFar) == 0) {
                    break;
                }
            } else {
                LOG.warn("{} failed to send any data from current Message.", this);
            }
        }
    }

    @Override
    public void processDeliveryUpdates(AmqpConnection connection) throws IOException {
        List<Delivery> toRemove = new ArrayList<Delivery>();

        for (Delivery delivery : pending) {
            DeliveryState state = delivery.getRemoteState();
            if (state == null) {
                continue;
            }

            Outcome outcome = null;
            if (state instanceof TransactionalState) {
                LOG.trace("State of delivery is Transactional, retrieving outcome: {}", state);
                outcome = ((TransactionalState) state).getOutcome();
            } else if (state instanceof Outcome) {
                outcome = (Outcome) state;
            } else {
                LOG.warn("Message send updated with unsupported state: {}", state);
                outcome = null;
            }

            AsyncResult request = (AsyncResult) delivery.getContext();

            if (outcome instanceof Accepted) {
                LOG.trace("Outcome of delivery was accepted: {}", delivery);
                tagGenerator.returnTag(delivery.getTag());
                if (request != null && !request.isComplete()) {
                    request.onSuccess();
                }
            } else if (outcome instanceof Rejected) {
                Exception remoteError = getRemoteError();
                LOG.trace("Outcome of delivery was rejected: {}", delivery);
                tagGenerator.returnTag(delivery.getTag());
                if (request != null && !request.isComplete()) {
                    request.onFailure(remoteError);
                } else {
                    connection.fireClientException(getRemoteError());
                }
            } else if (outcome != null) {
                LOG.warn("Message send updated with unsupported outcome: {}", outcome);
            }

            delivery.settle();
            toRemove.add(delivery);
        }

        pending.removeAll(toRemove);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ address = " + address + "}";
    }
}
