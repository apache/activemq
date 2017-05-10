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
package org.apache.activemq.transport.amqp.client;

import static org.apache.activemq.transport.amqp.AmqpSupport.COPY;
import static org.apache.activemq.transport.amqp.AmqpSupport.JMS_SELECTOR_NAME;
import static org.apache.activemq.transport.amqp.AmqpSupport.NO_LOCAL_NAME;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.InvalidDestinationException;

import org.apache.activemq.transport.amqp.client.util.AsyncResult;
import org.apache.activemq.transport.amqp.client.util.ClientFuture;
import org.apache.activemq.transport.amqp.client.util.IOExceptionSupport;
import org.apache.activemq.transport.amqp.client.util.UnmodifiableProxy;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receiver class that manages a Proton receiver endpoint.
 */
public class AmqpReceiver extends AmqpAbstractResource<Receiver> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpReceiver.class);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final BlockingQueue<AmqpMessage> prefetch = new LinkedBlockingDeque<>();

    private final AmqpSession session;
    private final String address;
    private final String receiverId;

    private final Source userSpecifiedSource;
    private final SenderSettleMode userSpecifiedSenderSettlementMode;
    private final ReceiverSettleMode userSpecifiedReceiverSettlementMode;

    private String subscriptionName;
    private String selector;
    private boolean presettle;
    private boolean noLocal;

    private AsyncResult pullRequest;
    private AsyncResult stopRequest;

    /**
     * Create a new receiver instance.
     *
     * @param session
     *        The parent session that created the receiver.
     * @param address
     *        The address that this receiver should listen on.
     * @param receiverId
     *        The unique ID assigned to this receiver.
     */
    public AmqpReceiver(AmqpSession session, String address, String receiverId) {
        this(session, address, receiverId, null, null);
    }

    /**
     * Create a new receiver instance.
     *
     * @param session
     * 		  The parent session that created the receiver.
     * @param address
     *        The address that this receiver should listen on.
     * @param receiverId
     *        The unique ID assigned to this receiver.
     * @param senderMode
     *        The {@link SenderSettleMode} to use on open.
     * @param receiverMode
     *        The {@link ReceiverSettleMode} to use on open.
     */
    public AmqpReceiver(AmqpSession session, String address, String receiverId, SenderSettleMode senderMode, ReceiverSettleMode receiverMode) {

        if (address != null && address.isEmpty()) {
            throw new IllegalArgumentException("Address cannot be empty.");
        }

        this.userSpecifiedSource = null;
        this.session = session;
        this.address = address;
        this.receiverId = receiverId;
        this.userSpecifiedSenderSettlementMode = senderMode;
        this.userSpecifiedReceiverSettlementMode = receiverMode;
    }

    /**
     * Create a new receiver instance.
     *
     * @param session
     *        The parent session that created the receiver.
     * @param source
     *        The Source instance to use instead of creating and configuring one.
     * @param receiverId
     *        The unique ID assigned to this receiver.
     */
    public AmqpReceiver(AmqpSession session, Source source, String receiverId) {

        if (source == null) {
            throw new IllegalArgumentException("User specified Source cannot be null");
        }

        this.session = session;
        this.userSpecifiedSource = source;
        this.address = source.getAddress();
        this.receiverId = receiverId;
        this.userSpecifiedSenderSettlementMode = null;
        this.userSpecifiedReceiverSettlementMode = null;
    }

    /**
     * Close the receiver, a closed receiver will throw exceptions if any further send
     * calls are made.
     *
     * @throws IOException if an error occurs while closing the receiver.
     */
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            final ClientFuture request = new ClientFuture();
            session.getScheduler().execute(new Runnable() {

                @Override
                public void run() {
                    checkClosed();
                    close(request);
                    session.pumpToProtonTransport(request);
                }
            });

            request.sync();
        }
    }

    /**
     * Detach the receiver, a closed receiver will throw exceptions if any further send
     * calls are made.
     *
     * @throws IOException if an error occurs while closing the receiver.
     */
    public void detach() throws IOException {
        if (closed.compareAndSet(false, true)) {
            final ClientFuture request = new ClientFuture();
            session.getScheduler().execute(new Runnable() {

                @Override
                public void run() {
                    checkClosed();
                    detach(request);
                    session.pumpToProtonTransport(request);
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
     * @return the address that this receiver has been configured to listen on.
     */
    public String getAddress() {
        return address;
    }

    /**
     * Attempts to wait on a message to be delivered to this receiver.  The receive
     * call will wait indefinitely for a message to be delivered.
     *
     * @return a newly received message sent to this receiver.
     *
     * @throws Exception if an error occurs during the receive attempt.
     */
    public AmqpMessage receive() throws Exception {
        checkClosed();
        return prefetch.take();
    }

    /**
     * Attempts to receive a message sent to this receiver, waiting for the given
     * timeout value before giving up and returning null.
     *
     * @param timeout
     * 	      the time to wait for a new message to arrive.
     * @param unit
     * 		  the unit of time that the timeout value represents.
     *
     * @return a newly received message or null if the time to wait period expires.
     *
     * @throws Exception if an error occurs during the receive attempt.
     */
    public AmqpMessage receive(long timeout, TimeUnit unit) throws Exception {
        checkClosed();
        return prefetch.poll(timeout, unit);
    }

    /**
     * If a message is already available in this receiver's prefetch buffer then
     * it is returned immediately otherwise this methods return null without waiting.
     *
     * @return a newly received message or null if there is no currently available message.
     *
     * @throws Exception if an error occurs during the receive attempt.
     */
    public AmqpMessage receiveNoWait() throws Exception {
        checkClosed();
        return prefetch.poll();
    }

    /**
     * Request a remote peer send a Message to this client waiting until one arrives.
     *
     * @return the pulled AmqpMessage or null if none was pulled from the remote.
     *
     * @throws IOException if an error occurs
     */
    public AmqpMessage pull() throws IOException {
        return pull(-1, TimeUnit.MILLISECONDS);
    }

    /**
     * Request a remote peer send a Message to this client using an immediate drain request.
     *
     * @return the pulled AmqpMessage or null if none was pulled from the remote.
     *
     * @throws IOException if an error occurs
     */
    public AmqpMessage pullImmediate() throws IOException {
        return pull(0, TimeUnit.MILLISECONDS);
    }

    /**
     * Request a remote peer send a Message to this client.
     *
     *   {@literal timeout < 0} then it should remain open until a message is received.
     *   {@literal timeout = 0} then it returns a message or null if none available
     *   {@literal timeout > 0} then it should remain open for timeout amount of time.
     *
     * The timeout value when positive is given in milliseconds.
     *
     * @param timeout
     *        the amount of time to tell the remote peer to keep this pull request valid.
     * @param unit
     *        the unit of measure that the timeout represents.
     *
     * @return the pulled AmqpMessage or null if none was pulled from the remote.
     *
     * @throws IOException if an error occurs
     */
    public AmqpMessage pull(final long timeout, final TimeUnit unit) throws IOException {
        checkClosed();
        final ClientFuture request = new ClientFuture();
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();

                long timeoutMills = unit.toMillis(timeout);

                try {
                    LOG.trace("Pull on Receiver {} with timeout = {}", getSubscriptionName(), timeoutMills);
                    if (timeoutMills < 0) {
                        // Wait until message arrives. Just give credit if needed.
                        if (getEndpoint().getCredit() == 0) {
                            LOG.trace("Receiver {} granting 1 additional credit for pull.", getSubscriptionName());
                            getEndpoint().flow(1);
                        }

                        // Await the message arrival
                        pullRequest = request;
                    } else if (timeoutMills == 0) {
                        // If we have no credit then we need to issue some so that we can
                        // try to fulfill the request, then drain down what is there to
                        // ensure we consume what is available and remove all credit.
                        if (getEndpoint().getCredit() == 0){
                            LOG.trace("Receiver {} granting 1 additional credit for pull.", getSubscriptionName());
                            getEndpoint().flow(1);
                        }

                        // Drain immediately and wait for the message(s) to arrive,
                        // or a flow indicating removal of the remaining credit.
                        stop(request);
                    } else if (timeoutMills > 0) {
                        // If we have no credit then we need to issue some so that we can
                        // try to fulfill the request, then drain down what is there to
                        // ensure we consume what is available and remove all credit.
                        if (getEndpoint().getCredit() == 0) {
                            LOG.trace("Receiver {} granting 1 additional credit for pull.", getSubscriptionName());
                            getEndpoint().flow(1);
                        }

                        // Wait for the timeout for the message(s) to arrive, then drain if required
                        // and wait for remaining message(s) to arrive or a flow indicating
                        // removal of the remaining credit.
                        stopOnSchedule(timeoutMills, request);
                    }

                    session.pumpToProtonTransport(request);
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();

        return prefetch.poll();
    }


    /**
     * Controls the amount of credit given to the receiver link.
     *
     * @param credit
     *        the amount of credit to grant.
     *
     * @throws IOException if an error occurs while sending the flow.
     */
    public void flow(final int credit) throws IOException {
        checkClosed();
        final ClientFuture request = new ClientFuture();
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                try {
                    getEndpoint().flow(credit);
                    session.pumpToProtonTransport(request);
                    request.onSuccess();
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    /**
     * Attempts to drain a given amount of credit from the link.
     *
     * @param credit
     *        the amount of credit to drain.
     *
     * @throws IOException if an error occurs while sending the drain.
     */
    public void drain(final int credit) throws IOException {
        checkClosed();
        final ClientFuture request = new ClientFuture();
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                try {
                    getEndpoint().drain(credit);
                    session.pumpToProtonTransport(request);
                    request.onSuccess();
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    /**
     * Stops the receiver, using all link credit and waiting for in-flight messages to arrive.
     *
     * @throws IOException if an error occurs while sending the drain.
     */
    public void stop() throws IOException {
        checkClosed();
        final ClientFuture request = new ClientFuture();
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                try {
                    stop(request);
                    session.pumpToProtonTransport(request);
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    /**
     * Accepts a message that was dispatched under the given Delivery instance and settles the delivery.
     *
     * @param delivery
     *        the Delivery instance to accept.
     *
     * @throws IOException if an error occurs while sending the accept.
     */
    public void accept(Delivery delivery) throws IOException {
        accept(delivery, this.session, true);
    }

    /**
     * Accepts a message that was dispatched under the given Delivery instance.
     *
     * @param delivery
     *        the Delivery instance to accept.
     * @param settle
     *        true if the receiver should settle the delivery or just send the disposition.
     *
     * @throws IOException if an error occurs while sending the accept.
     */
    public void accept(Delivery delivery, boolean settle) throws IOException {
        accept(delivery, this.session, settle);
    }

    /**
     * Accepts a message that was dispatched under the given Delivery instance and settles the delivery.
     *
     * This method allows for the session that is used in the accept to be specified by the
     * caller.  This allows for an accepted message to be involved in a transaction that is
     * being managed by some other session other than the one that created this receiver.
     *
     * @param delivery
     *        the Delivery instance to accept.
     * @param session
     *        the session under which the message is being accepted.
     *
     * @throws IOException if an error occurs while sending the accept.
     */
    public void accept(final Delivery delivery, final AmqpSession session) throws IOException {
        accept(delivery, session, true);
    }

    /**
     * Accepts a message that was dispatched under the given Delivery instance.
     *
     * This method allows for the session that is used in the accept to be specified by the
     * caller.  This allows for an accepted message to be involved in a transaction that is
     * being managed by some other session other than the one that created this receiver.
     *
     * @param delivery
     *        the Delivery instance to accept.
     * @param session
     *        the session under which the message is being accepted.
     * @param settle
     *        true if the receiver should settle the delivery or just send the disposition.
     *
     * @throws IOException if an error occurs while sending the accept.
     */
    public void accept(final Delivery delivery, final AmqpSession session, final boolean settle) throws IOException {
        checkClosed();

        if (delivery == null) {
            throw new IllegalArgumentException("Delivery to accept cannot be null");
        }

        if (session == null) {
            throw new IllegalArgumentException("Session given cannot be null");
        }

        if (session.getConnection() != this.session.getConnection()) {
            throw new IllegalArgumentException("The session used for accept must originate from the connection that created this receiver.");
        }

        final ClientFuture request = new ClientFuture();
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                try {
                    if (!delivery.isSettled()) {
                        if (session.isInTransaction()) {
                            Binary txnId = session.getTransactionId().getRemoteTxId();
                            if (txnId != null) {
                                TransactionalState txState = new TransactionalState();
                                txState.setOutcome(Accepted.getInstance());
                                txState.setTxnId(txnId);
                                delivery.disposition(txState);
                                session.getTransactionContext().registerTxConsumer(AmqpReceiver.this);
                            }
                        } else {
                            delivery.disposition(Accepted.getInstance());
                        }

                        if (settle) {
                            delivery.settle();
                        }
                    }
                    session.pumpToProtonTransport(request);
                    request.onSuccess();
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    /**
     * Mark a message that was dispatched under the given Delivery instance as Modified.
     *
     * @param delivery
     *        the Delivery instance to mark modified.
     * @param deliveryFailed
     *        indicates that the delivery failed for some reason.
     * @param undeliverableHere
     *        marks the delivery as not being able to be process by link it was sent to.
     * @throws IOException if an error occurs while sending the reject.
     */
    public void modified(final Delivery delivery, final Boolean deliveryFailed, final Boolean undeliverableHere) throws IOException {
        checkClosed();

        if (delivery == null) {
            throw new IllegalArgumentException("Delivery to reject cannot be null");
        }

        final ClientFuture request = new ClientFuture();
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                try {
                    if (!delivery.isSettled()) {
                        Modified disposition = new Modified();
                        disposition.setUndeliverableHere(undeliverableHere);
                        disposition.setDeliveryFailed(deliveryFailed);
                        delivery.disposition(disposition);
                        delivery.settle();
                        session.pumpToProtonTransport(request);
                    }
                    request.onSuccess();
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    /**
     * Release a message that was dispatched under the given Delivery instance.
     *
     * @param delivery
     *        the Delivery instance to release.
     *
     * @throws IOException if an error occurs while sending the release.
     */
    public void release(final Delivery delivery) throws IOException {
        checkClosed();

        if (delivery == null) {
            throw new IllegalArgumentException("Delivery to release cannot be null");
        }

        final ClientFuture request = new ClientFuture();
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                try {
                    if (!delivery.isSettled()) {
                        delivery.disposition(Released.getInstance());
                        delivery.settle();
                        session.pumpToProtonTransport(request);
                    }
                    request.onSuccess();
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    /**
     * Reject a message that was dispatched under the given Delivery instance.
     *
     * @param delivery
     *        the Delivery instance to reject.
     *
     * @throws IOException if an error occurs while sending the release.
     */
    public void reject(final Delivery delivery) throws IOException {
        checkClosed();

        if (delivery == null) {
            throw new IllegalArgumentException("Delivery to release cannot be null");
        }

        final ClientFuture request = new ClientFuture();
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                try {
                    if (!delivery.isSettled()) {
                        delivery.disposition(new Rejected());
                        delivery.settle();
                        session.pumpToProtonTransport(request);
                    }
                    request.onSuccess();
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    /**
     * @return an unmodifiable view of the underlying Receiver instance.
     */
    public Receiver getReceiver() {
        return UnmodifiableProxy.receiverProxy(getEndpoint());
    }

    //----- Receiver configuration properties --------------------------------//

    public boolean isPresettle() {
        return presettle;
    }

    public void setPresettle(boolean presettle) {
        this.presettle = presettle;
    }

    public boolean isDurable() {
        return subscriptionName != null;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public void setNoLocal(boolean noLocal) {
        this.noLocal = noLocal;
    }

    public long getDrainTimeout() {
        return session.getConnection().getDrainTimeout();
    }

    //----- Internal implementation ------------------------------------------//

    @Override
    protected void doOpen() {

        Source source = userSpecifiedSource;
        Target target = new Target();

        if (source == null && address != null) {
            source = new Source();
            source.setAddress(address);
            configureSource(source);
        }

        String receiverName = receiverId + ":" + address;

        if (getSubscriptionName() != null && !getSubscriptionName().isEmpty()) {
            // In the case of Durable Topic Subscriptions the client must use the same
            // receiver name which is derived from the subscription name property.
            receiverName = getSubscriptionName();
        }

        Receiver receiver = session.getEndpoint().receiver(receiverName);
        receiver.setSource(source);
        receiver.setTarget(target);

        if (userSpecifiedSenderSettlementMode != null) {
            receiver.setSenderSettleMode(userSpecifiedSenderSettlementMode);
            if (SenderSettleMode.SETTLED.equals(userSpecifiedSenderSettlementMode)) {
                setPresettle(true);
            }
        } else {
            if (isPresettle()) {
                receiver.setSenderSettleMode(SenderSettleMode.SETTLED);
            } else {
                receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
            }
        }

        if (userSpecifiedReceiverSettlementMode != null) {
            receiver.setReceiverSettleMode(userSpecifiedReceiverSettlementMode);
        } else {
            receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        }

        setEndpoint(receiver);

        super.doOpen();
    }

    @Override
    protected void doOpenCompletion() {
        // Verify the attach response contained a non-null Source
        org.apache.qpid.proton.amqp.transport.Source s = getEndpoint().getRemoteSource();
        if (s != null) {
            super.doOpenCompletion();
        } else {
            // No link terminus was created, the peer will now detach/close us.
        }
    }

    @Override
    protected void doClose() {
        getEndpoint().close();
    }

    @Override
    protected void doDetach() {
        getEndpoint().detach();
    }

    @Override
    protected Exception getOpenAbortException() {
        // Verify the attach response contained a non-null Source
        org.apache.qpid.proton.amqp.transport.Source s = getEndpoint().getRemoteSource();
        if (s != null) {
            return super.getOpenAbortException();
        } else {
            // No link terminus was created, the peer has detach/closed us, create IDE.
            return new InvalidDestinationException("Link creation was refused");
        }
    }

    @Override
    protected void doOpenInspection() {
        try {
            getStateInspector().inspectOpenedResource(getReceiver());
        } catch (Throwable error) {
            getStateInspector().markAsInvalid(error.getMessage());
        }
    }

    @Override
    protected void doClosedInspection() {
        try {
            getStateInspector().inspectClosedResource(getReceiver());
        } catch (Throwable error) {
            getStateInspector().markAsInvalid(error.getMessage());
        }
    }

    @Override
    protected void doDetachedInspection() {
        try {
            getStateInspector().inspectDetachedResource(getReceiver());
        } catch (Throwable error) {
            getStateInspector().markAsInvalid(error.getMessage());
        }
    }

    protected void configureSource(Source source) {
        Map<Symbol, DescribedType> filters = new HashMap<>();
        Symbol[] outcomes = new Symbol[]{Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL,
                                         Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL};

        if (getSubscriptionName() != null && !getSubscriptionName().isEmpty()) {
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
            source.setDistributionMode(COPY);
        } else {
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        }

        source.setOutcomes(outcomes);

        Modified modified = new Modified();
        modified.setDeliveryFailed(true);
        modified.setUndeliverableHere(false);

        source.setDefaultOutcome(modified);

        if (isNoLocal()) {
            filters.put(NO_LOCAL_NAME, AmqpNoLocalFilter.NO_LOCAL);
        }

        if (getSelector() != null && !getSelector().trim().equals("")) {
            filters.put(JMS_SELECTOR_NAME, new AmqpJmsSelectorFilter(getSelector()));
        }

        if (!filters.isEmpty()) {
            source.setFilter(filters);
        }
    }

    @Override
    public void processDeliveryUpdates(AmqpConnection connection) throws IOException {
        Delivery incoming = null;
        do {
            incoming = getEndpoint().current();
            if (incoming != null) {
                if(incoming.isReadable() && !incoming.isPartial()) {
                    LOG.trace("{} has incoming Message(s).", this);
                    try {
                        processDelivery(incoming);
                    } catch (Exception e) {
                        throw IOExceptionSupport.create(e);
                    }
                    getEndpoint().advance();
                } else {
                    LOG.trace("{} has a partial incoming Message(s), deferring.", this);
                    incoming = null;
                }
            } else {
                // We have exhausted the locally queued messages on this link.
                // Check if we tried to stop and have now run out of credit.
                if (getEndpoint().getRemoteCredit() <= 0) {
                    if (stopRequest != null) {
                        stopRequest.onSuccess();
                        stopRequest = null;
                    }
                }
            }
        } while (incoming != null);

        super.processDeliveryUpdates(connection);
    }

    private void processDelivery(Delivery incoming) throws Exception {
        doDeliveryInspection(incoming);

        Message message = null;
        try {
            message = decodeIncomingMessage(incoming);
        } catch (Exception e) {
            LOG.warn("Error on transform: {}", e.getMessage());
            deliveryFailed(incoming, true);
            return;
        }

        AmqpMessage amqpMessage = new AmqpMessage(this, message, incoming);
        // Store reference to envelope in delivery context for recovery
        incoming.setContext(amqpMessage);
        prefetch.add(amqpMessage);

        // We processed a message, signal completion
        // of a message pull request if there is one.
        if (pullRequest != null) {
            pullRequest.onSuccess();
            pullRequest = null;
        }
    }

    private void doDeliveryInspection(Delivery delivery) {
        try {
            getStateInspector().inspectDelivery(getReceiver(), delivery);
        } catch (Throwable error) {
            getStateInspector().markAsInvalid(error.getMessage());
        }
    }

    @Override
    public void processFlowUpdates(AmqpConnection connection) throws IOException {
        if (pullRequest != null || stopRequest != null) {
            Receiver receiver = getEndpoint();
            if (receiver.getRemoteCredit() <= 0 && receiver.getQueued() == 0) {
                if (pullRequest != null) {
                    pullRequest.onSuccess();
                    pullRequest = null;
                }

                if (stopRequest != null) {
                    stopRequest.onSuccess();
                    stopRequest = null;
                }
            }
        }

        LOG.trace("Consumer {} flow updated, remote credit = {}", getSubscriptionName(), getEndpoint().getRemoteCredit());

        super.processFlowUpdates(connection);
    }

    protected Message decodeIncomingMessage(Delivery incoming) {
        int count;

        byte[] chunk = new byte[2048];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        while ((count = getEndpoint().recv(chunk, 0, chunk.length)) > 0) {
            stream.write(chunk, 0, count);
        }

        byte[] messageBytes = stream.toByteArray();

        try {
            Message protonMessage = Message.Factory.create();
            protonMessage.decode(messageBytes, 0, messageBytes.length);
            return protonMessage;
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
            }
        }
    }

    protected void deliveryFailed(Delivery incoming, boolean expandCredit) {
        Modified disposition = new Modified();
        disposition.setUndeliverableHere(true);
        disposition.setDeliveryFailed(true);
        incoming.disposition(disposition);
        incoming.settle();
        if (expandCredit) {
            getEndpoint().flow(1);
        }
    }

    private void stop(final AsyncResult request) {
        Receiver receiver = getEndpoint();
        if (receiver.getRemoteCredit() <= 0) {
            if (receiver.getQueued() == 0) {
                // We have no remote credit and all the deliveries have been processed.
                request.onSuccess();
            } else {
                // There are still deliveries to process, wait for them to be.
                stopRequest = request;
            }
        } else {
            // TODO: We don't actually want the additional messages that could be sent while
            // draining. We could explicitly reduce credit first, or possibly use 'echo' instead
            // of drain if it was supported. We would first need to understand what happens
            // if we reduce credit below the number of messages already in-flight before
            // the peer sees the update.
            stopRequest = request;
            receiver.drain(0);

            if (getDrainTimeout() > 0) {
                // If the remote doesn't respond we will close the consumer and break any
                // blocked receive or stop calls that are waiting.
                final ScheduledFuture<?> future = getSession().getScheduler().schedule(new Runnable() {
                    @Override
                    public void run() {
                        LOG.trace("Consumer {} drain request timed out", this);
                        Exception cause = new JmsOperationTimedOutException("Remote did not respond to a drain request in time");
                        locallyClosed(session.getConnection(), cause);
                        stopRequest.onFailure(cause);
                        session.pumpToProtonTransport(stopRequest);
                    }
                }, getDrainTimeout(), TimeUnit.MILLISECONDS);

                stopRequest = new ScheduledRequest(future, stopRequest);
            }
        }
    }

    private void stopOnSchedule(long timeout, final AsyncResult request) {
        LOG.trace("Receiver {} scheduling stop", this);
        // We need to drain the credit if no message(s) arrive to use it.
        final ScheduledFuture<?> future = getSession().getScheduler().schedule(new Runnable() {
            @Override
            public void run() {
                LOG.trace("Receiver {} running scheduled stop", this);
                if (getEndpoint().getRemoteCredit() != 0) {
                    stop(request);
                    session.pumpToProtonTransport(request);
                }
            }
        }, timeout, TimeUnit.MILLISECONDS);

        stopRequest = new ScheduledRequest(future, request);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ address = " + address + "}";
    }

    private void checkClosed() {
        if (isClosed()) {
            throw new IllegalStateException("Receiver is already closed");
        }
    }

    //----- Internal Transaction state callbacks -----------------------------//

    void preCommit() {
    }

    void preRollback() {
    }

    void postCommit() {
    }

    void postRollback() {
    }

    //----- Inner classes used in message pull operations --------------------//

    protected static final class ScheduledRequest implements AsyncResult {

        private final ScheduledFuture<?> sheduledTask;
        private final AsyncResult origRequest;

        public ScheduledRequest(ScheduledFuture<?> completionTask, AsyncResult origRequest) {
            this.sheduledTask = completionTask;
            this.origRequest = origRequest;
        }

        @Override
        public void onFailure(Throwable cause) {
            sheduledTask.cancel(false);
            origRequest.onFailure(cause);
        }

        @Override
        public void onSuccess() {
            boolean cancelled = sheduledTask.cancel(false);
            if (cancelled) {
                // Signal completion. Otherwise wait for the scheduled task to do it.
                origRequest.onSuccess();
            }
        }

        @Override
        public boolean isComplete() {
            return origRequest.isComplete();
        }
    }
}
