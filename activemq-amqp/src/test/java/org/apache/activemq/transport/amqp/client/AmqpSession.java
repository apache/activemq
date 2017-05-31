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
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.transport.amqp.client.util.AsyncResult;
import org.apache.activemq.transport.amqp.client.util.ClientFuture;
import org.apache.activemq.transport.amqp.client.util.UnmodifiableProxy;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Session;

/**
 * Session class that manages a Proton session endpoint.
 */
public class AmqpSession extends AmqpAbstractResource<Session> {

    private final AtomicLong receiverIdGenerator = new AtomicLong();
    private final AtomicLong senderIdGenerator = new AtomicLong();

    private final AmqpConnection connection;
    private final String sessionId;
    private final AmqpTransactionContext txContext;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Create a new session instance.
     *
     * @param connection
     * 		  The parent connection that created the session.
     * @param sessionId
     *        The unique ID value assigned to this session.
     */
    public AmqpSession(AmqpConnection connection, String sessionId) {
        this.connection = connection;
        this.sessionId = sessionId;
        this.txContext = new AmqpTransactionContext(this);
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
            getScheduler().execute(new Runnable() {

                @Override
                public void run() {
                    checkClosed();
                    close(request);
                    pumpToProtonTransport(request);
                }
            });

            request.sync();
        }
    }

    /**
     * Create an anonymous sender.
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the sender.
     */
    public AmqpSender createSender() throws Exception {
        return createSender(null, false, null, null, null);
    }

    /**
     * Create a sender instance using the given address
     *
     * @param address
     *        the address to which the sender will produce its messages.
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the sender.
     */
    public AmqpSender createSender(final String address) throws Exception {
        return createSender(address, false, null, null, null);
    }

    /**
     * Create a sender instance using the given address
     *
     * @param address
     *        the address to which the sender will produce its messages.
     * @param desiredCapabilities
     *        the capabilities that the caller wants the remote to support.
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the sender.
     */
    public AmqpSender createSender(final String address, Symbol[] desiredCapabilities) throws Exception {
        return createSender(address, false, desiredCapabilities, null, null);
    }

    /**
     * Create a sender instance using the given address
     *
     * @param address
     *        the address to which the sender will produce its messages.
     * @param presettle
     *        controls if the created sender produces message that have already been marked settled.
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the sender.
     */
    public AmqpSender createSender(final String address, boolean presettle) throws Exception {
        return createSender(address, presettle, null, null, null);
    }

    /**
     * Create a sender instance using the given address
     *
     * @param address
     *        the address to which the sender will produce its messages.
     * @param senderSettlementMode
     *        controls the settlement mode used by the created Sender
     * @param receiverSettlementMode
     *        controls the desired settlement mode used by the remote Receiver
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the sender.
     */
    public AmqpSender createSender(final String address, final SenderSettleMode senderMode, ReceiverSettleMode receiverMode) throws Exception {
        checkClosed();

        final AmqpSender sender = new AmqpSender(AmqpSession.this, address, getNextSenderId(), senderMode, receiverMode);
        final ClientFuture request = new ClientFuture();

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                sender.setStateInspector(getStateInspector());
                sender.open(request);
                pumpToProtonTransport(request);
            }
        });

        request.sync();

        return sender;
    }

    /**
     * Create a sender instance using the given address
     *
     * @param address
     * 	      the address to which the sender will produce its messages.
     * @param presettle
     *        controls if the created sender produces message that have already been marked settled.
     * @param desiredCapabilities
     *        the capabilities that the caller wants the remote to support.
     * @param offeredCapabilities
     *        the capabilities that the caller wants the advertise support for.
     * @param properties
     *        the properties to send as part of the sender open.
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the sender.
     */
    public AmqpSender createSender(final String address, boolean presettle, Symbol[] desiredCapabilities, Symbol[] offeredCapabilities, Map<Symbol, Object> properties) throws Exception {
        checkClosed();

        final AmqpSender sender = new AmqpSender(AmqpSession.this, address, getNextSenderId());
        sender.setPresettle(presettle);
        sender.setDesiredCapabilities(desiredCapabilities);
        sender.setOfferedCapabilities(offeredCapabilities);
        sender.setProperties(properties);

        final ClientFuture request = new ClientFuture();

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                sender.setStateInspector(getStateInspector());
                sender.open(request);
                pumpToProtonTransport(request);
            }
        });

        request.sync();

        return sender;
    }

    /**
     * Create a sender instance using the given Target
     *
     * @param target
     *        the caller created and configured Target used to create the sender link.
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpSender createSender(Target target) throws Exception {
        return createSender(target, getNextSenderId());
    }

    /**
     * Create a sender instance using the given Target
     *
     * @param target
     *        the caller created and configured Target used to create the sender link.
     * @param sender
     *        the sender ID to assign to the newly created Sender.
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpSender createSender(Target target, String senderId) throws Exception {
        return createSender(target, senderId, null, null, null);
    }

    /**
     * Create a sender instance using the given Target
     *
     * @param target
     *        the caller created and configured Target used to create the sender link.
     * @param sender
     *        the sender ID to assign to the newly created Sender.
     * @param desiredCapabilities
     *        the capabilities that the caller wants the remote to support.
     * @param offeredCapabilities
     *        the capabilities that the caller wants the advertise support for.
     * @param properties
     *        the properties to send as part of the sender open.
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpSender createSender(Target target, String senderId, Symbol[] desiredCapabilities, Symbol[] offeredCapabilities, Map<Symbol, Object> properties) throws Exception {
        checkClosed();

        final AmqpSender sender = new AmqpSender(AmqpSession.this, target, senderId);
        sender.setDesiredCapabilities(desiredCapabilities);
        sender.setOfferedCapabilities(offeredCapabilities);
        sender.setProperties(properties);

        final ClientFuture request = new ClientFuture();

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                sender.setStateInspector(getStateInspector());
                sender.open(request);
                pumpToProtonTransport(request);
            }
        });

        request.sync();

        return sender;
    }

    /**
     * Create a receiver instance using the given address
     *
     * @param address
     *        the address to which the receiver will subscribe for its messages.
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createReceiver(String address) throws Exception {
        return createReceiver(address, null, false);
    }

    /**
     * Create a receiver instance using the given address
     *
     * @param address
     *        the address to which the receiver will subscribe for its messages.
     * @param selector
     *        the JMS selector to use for the subscription
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createReceiver(String address, String selector) throws Exception {
        return createReceiver(address, selector, false);
    }

    /**
     * Create a receiver instance using the given address
     *
     * @param address
     * 	      the address to which the receiver will subscribe for its messages.
     * @param selector
     *        the JMS selector to use for the subscription
     * @param noLocal
     *        should the subscription have messages from its connection filtered.
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createReceiver(String address, String selector, boolean noLocal) throws Exception {
        return createReceiver(address, selector, noLocal, false);
    }

    /**
     * Create a receiver instance using the given address
     *
     * @param address
     *        the address to which the receiver will subscribe for its messages.
     * @param selector
     *        the JMS selector to use for the subscription
     * @param noLocal
     *        should the subscription have messages from its connection filtered.
     * @param presettle
     *        should the receiver be created with a settled sender mode.
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createReceiver(String address, String selector, boolean noLocal, boolean presettle) throws Exception {
        checkClosed();

        final ClientFuture request = new ClientFuture();
        final AmqpReceiver receiver = new AmqpReceiver(AmqpSession.this, address, getNextReceiverId());

        receiver.setNoLocal(noLocal);
        receiver.setPresettle(presettle);
        if (selector != null && !selector.isEmpty()) {
            receiver.setSelector(selector);
        }

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                receiver.setStateInspector(getStateInspector());
                receiver.open(request);
                pumpToProtonTransport(request);
            }
        });

        request.sync();

        return receiver;
    }

    /**
     * Create a receiver instance using the given address
     *
     * @param address
     *        the address to which the receiver will subscribe for its messages.
     * @param senderSettlementMode
     *        controls the desired settlement mode used by the remote Sender
     * @param receiverSettlementMode
     *        controls the settlement mode used by the created Receiver
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createReceiver(String address, SenderSettleMode senderMode, ReceiverSettleMode receiverMode) throws Exception {
        checkClosed();

        final ClientFuture request = new ClientFuture();
        final AmqpReceiver receiver = new AmqpReceiver(AmqpSession.this, address, getNextReceiverId(), senderMode, receiverMode);

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                receiver.setStateInspector(getStateInspector());
                receiver.open(request);
                pumpToProtonTransport(request);
            }
        });

        request.sync();

        return receiver;
    }

    /**
     * Create a receiver instance using the given Source
     *
     * @param source
     *        the caller created and configured Source used to create the receiver link.
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createReceiver(Source source) throws Exception {
        return createReceiver(source, getNextReceiverId());
    }

    /**
     * Create a receiver instance using the given Source
     *
     * @param source
     *        the caller created and configured Source used to create the receiver link.
     * @param receivedId
     *        the ID value to assign to the newly created receiver
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createReceiver(Source source, String receiverId) throws Exception {
        checkClosed();

        final ClientFuture request = new ClientFuture();
        final AmqpReceiver receiver = new AmqpReceiver(AmqpSession.this, source, getNextReceiverId());

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                receiver.setStateInspector(getStateInspector());
                receiver.open(request);
                pumpToProtonTransport(request);
            }
        });

        request.sync();

        return receiver;
    }

    /**
     * Create a receiver instance using the given address that creates a durable subscription.
     *
     * @param address
     *        the address to which the receiver will subscribe for its messages.
     * @param subscriptionName
     *        the name of the subscription that is being created.
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createDurableReceiver(String address, String subscriptionName) throws Exception {
        return createDurableReceiver(address, subscriptionName, null, false);
    }

    /**
     * Create a receiver instance using the given address that creates a durable subscription.
     *
     * @param address
     *        the address to which the receiver will subscribe for its messages.
     * @param subscriptionName
     *        the name of the subscription that is being created.
     * @param selector
     *        the JMS selector to use for the subscription
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createDurableReceiver(String address, String subscriptionName, String selector) throws Exception {
        return createDurableReceiver(address, subscriptionName, selector, false);
    }

    /**
     * Create a receiver instance using the given address that creates a durable subscription.
     *
     * @param address
     *        the address to which the receiver will subscribe for its messages.
     * @param subscriptionName
     *        the name of the subscription that is being created.
     * @param selector
     *        the JMS selector to use for the subscription
     * @param noLocal
     *        should the subscription have messages from its connection filtered.
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createDurableReceiver(String address, String subscriptionName, String selector, boolean noLocal) throws Exception {
        checkClosed();

        if (subscriptionName == null || subscriptionName.isEmpty()) {
            throw new IllegalArgumentException("subscription name must not be null or empty.");
        }

        final ClientFuture request = new ClientFuture();
        final AmqpReceiver receiver = new AmqpReceiver(AmqpSession.this, address, getNextReceiverId());
        receiver.setSubscriptionName(subscriptionName);
        receiver.setNoLocal(noLocal);
        if (selector != null && !selector.isEmpty()) {
            receiver.setSelector(selector);
        }

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                receiver.setStateInspector(getStateInspector());
                receiver.open(request);
                pumpToProtonTransport(request);
            }
        });

        request.sync();

        return receiver;
    }

    /**
     * Create a receiver instance using the given address that creates a durable subscription.
     *
     * @param subscriptionName
     *        the name of the subscription that should be queried for on the remote..
     *
     * @return a newly created receiver that is ready for use if the subscription exists.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver lookupSubscription(String subscriptionName) throws Exception {
        checkClosed();

        if (subscriptionName == null || subscriptionName.isEmpty()) {
            throw new IllegalArgumentException("subscription name must not be null or empty.");
        }

        final ClientFuture request = new ClientFuture();
        final AmqpReceiver receiver = new AmqpReceiver(AmqpSession.this, (String) null, getNextReceiverId());
        receiver.setSubscriptionName(subscriptionName);

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                receiver.setStateInspector(getStateInspector());
                receiver.open(request);
                pumpToProtonTransport(request);
            }
        });

        request.sync();

        return receiver;
    }

    /**
     * @return this session's parent AmqpConnection.
     */
    public AmqpConnection getConnection() {
        return connection;
    }

    public Session getSession() {
        return UnmodifiableProxy.sessionProxy(getEndpoint());
    }

    public boolean isInTransaction() {
        return txContext.isInTransaction();
    }

    @Override
    public String toString() {
        return "AmqpSession { " + sessionId + " }";
    }

    //----- Session Transaction Methods --------------------------------------//

    /**
     * Starts a new transaction associated with this session.
     *
     * @throws Exception if an error occurs starting a new Transaction.
     */
    public void begin() throws Exception {
        if (txContext.isInTransaction()) {
            throw new javax.jms.IllegalStateException("Session already has an active transaction");
        }

        txContext.begin();
    }

    /**
     * Commit the current transaction associated with this session.
     *
     * @throws Exception if an error occurs committing the Transaction.
     */
    public void commit() throws Exception {
        if (!txContext.isInTransaction()) {
            throw new javax.jms.IllegalStateException(
                "Commit called on Session that does not have an active transaction");
        }

        txContext.commit();
    }

    /**
     * Roll back the current transaction associated with this session.
     *
     * @throws Exception if an error occurs rolling back the Transaction.
     */
    public void rollback() throws Exception {
        if (!txContext.isInTransaction()) {
            throw new javax.jms.IllegalStateException(
                "Rollback called on Session that does not have an active transaction");
        }

        txContext.rollback();
    }

    //----- Internal access used to manage resources -------------------------//

    ScheduledExecutorService getScheduler() {
        return connection.getScheduler();
    }

    Connection getProtonConnection() {
        return connection.getProtonConnection();
    }

    void pumpToProtonTransport(AsyncResult request) {
        connection.pumpToProtonTransport(request);
    }

    public AmqpTransactionId getTransactionId() {
        if (txContext != null && txContext.isInTransaction()) {
            return txContext.getTransactionId();
        }

        return null;
    }

    AmqpTransactionContext getTransactionContext() {
        return txContext;
    }

    //----- Private implementation details -----------------------------------//

    @Override
    protected void doOpen() {
        getEndpoint().setIncomingCapacity(Integer.MAX_VALUE);
        super.doOpen();
    }

    @Override
    protected void doOpenInspection() {
        try {
            getStateInspector().inspectOpenedResource(getSession());
        } catch (Throwable error) {
            getStateInspector().markAsInvalid(error.getMessage());
        }
    }

    @Override
    protected void doClosedInspection() {
        try {
            getStateInspector().inspectClosedResource(getSession());
        } catch (Throwable error) {
            getStateInspector().markAsInvalid(error.getMessage());
        }
    }

    private String getNextSenderId() {
        return sessionId + ":" + senderIdGenerator.incrementAndGet();
    }

    private String getNextReceiverId() {
        return sessionId + ":" + receiverIdGenerator.incrementAndGet();
    }

    private void checkClosed() {
        if (isClosed() || connection.isClosed()) {
            throw new IllegalStateException("Session is already closed");
        }
    }
}
