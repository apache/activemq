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

import static org.apache.activemq.transport.amqp.AmqpSupport.COPY;
import static org.apache.activemq.transport.amqp.AmqpSupport.JMS_SELECTOR_NAME;
import static org.apache.activemq.transport.amqp.AmqpSupport.NO_LOCAL_NAME;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.InvalidDestinationException;

import org.apache.activemq.transport.amqp.client.util.ClientFuture;
import org.apache.activemq.transport.amqp.client.util.UnmodifiableReceiver;
import org.apache.activemq.util.IOExceptionSupport;
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
    private final BlockingQueue<AmqpMessage> prefetch = new LinkedBlockingDeque<AmqpMessage>();

    private final AmqpSession session;
    private final String address;
    private final String receiverId;
    private final Source userSpecifiedSource;

    private String subscriptionName;
    private String selector;
    private boolean presettle;
    private boolean noLocal;

    /**
     * Create a new receiver instance.
     *
     * @param session
     * 		  The parent session that created the receiver.
     * @param address
     *        The address that this receiver should listen on.
     * @param receiverId
     *        The unique ID assigned to this receiver.
     */
    public AmqpReceiver(AmqpSession session, String address, String receiverId) {

        if (address != null && address.isEmpty()) {
            throw new IllegalArgumentException("Address cannot be empty.");
        }

        this.userSpecifiedSource = null;
        this.session = session;
        this.address = address;
        this.receiverId = receiverId;
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
                    session.pumpToProtonTransport();
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
                    session.pumpToProtonTransport();
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
                    session.pumpToProtonTransport();
                    request.onSuccess();
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    /**
     * Accepts a message that was dispatched under the given Delivery instance.
     *
     * @param delivery
     *        the Delivery instance to accept.
     *
     * @throws IOException if an error occurs while sending the accept.
     */
    public void accept(final Delivery delivery) throws IOException {
        checkClosed();

        if (delivery == null) {
            throw new IllegalArgumentException("Delivery to accept cannot be null");
        }

        final ClientFuture request = new ClientFuture();
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                try {
                    if (!delivery.isSettled()) {
                        delivery.disposition(Accepted.getInstance());
                        delivery.settle();
                        session.pumpToProtonTransport();
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
                        session.pumpToProtonTransport();
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
                        session.pumpToProtonTransport();
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
        return new UnmodifiableReceiver(getEndpoint());
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
        if (isPresettle()) {
            receiver.setSenderSettleMode(SenderSettleMode.SETTLED);
        } else {
            receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        }
        receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

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
        getStateInspector().inspectOpenedResource(getReceiver());
    }

    @Override
    protected void doClosedInspection() {
        getStateInspector().inspectClosedResource(getReceiver());
    }

    @Override
    protected void doDetachedInspection() {
        getStateInspector().inspectDetachedResource(getReceiver());
    }

    protected void configureSource(Source source) {
        Map<Symbol, DescribedType> filters = new HashMap<Symbol, DescribedType>();
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
            }
        } while (incoming != null);

        super.processDeliveryUpdates(connection);
    }

    private void processDelivery(Delivery incoming) throws Exception {
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ address = " + address + "}";
    }

    private void checkClosed() {
        if (isClosed()) {
            throw new IllegalStateException("Receiver is already closed");
        }
    }
}
