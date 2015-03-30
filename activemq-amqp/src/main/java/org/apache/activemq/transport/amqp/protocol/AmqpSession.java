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

import static org.apache.activemq.transport.amqp.AmqpSupport.COPY;
import static org.apache.activemq.transport.amqp.AmqpSupport.JMS_SELECTOR_FILTER_IDS;
import static org.apache.activemq.transport.amqp.AmqpSupport.NO_LOCAL_FILTER_IDS;
import static org.apache.activemq.transport.amqp.AmqpSupport.createDestination;
import static org.apache.activemq.transport.amqp.AmqpSupport.findFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.InvalidSelectorException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.transport.amqp.AmqpProtocolConverter;
import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.activemq.transport.amqp.ResponseHandler;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps the AMQP Session and provides the services needed to manage the remote
 * peer requests for link establishment.
 */
public class AmqpSession implements AmqpResource {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSession.class);

    private final Map<ConsumerId, AmqpSender> consumers = new HashMap<ConsumerId, AmqpSender>();

    private final AmqpConnection connection;
    private final Session protonSession;
    private final SessionId sessionId;

    private long nextProducerId = 0;
    private long nextConsumerId = 0;

    /**
     * Create new AmqpSession instance whose parent is the given AmqpConnection.
     *
     * @param connection
     *        the parent connection for this session.
     * @param sessionId
     *        the ActiveMQ SessionId that is used to identify this session.
     * @param session
     *        the AMQP Session that this class manages.
     */
    public AmqpSession(AmqpConnection connection, SessionId sessionId, Session session) {
        this.connection = connection;
        this.sessionId = sessionId;
        this.protonSession = session;
    }

    @Override
    public void open() {
        LOG.trace("Session {} opened", getSessionId());

        getEndpoint().setContext(this);
        getEndpoint().setIncomingCapacity(Integer.MAX_VALUE);
        getEndpoint().open();

        connection.sendToActiveMQ(new SessionInfo(getSessionId()));
    }

    @Override
    public void close() {
        LOG.trace("Session {} closed", getSessionId());

        getEndpoint().setContext(null);
        getEndpoint().close();
        getEndpoint().free();

        connection.sendToActiveMQ(new RemoveInfo(getSessionId()));
    }

    /**
     * Commits all pending work for all resources managed under this session.
     *
     * @throws Exception if an error occurs while attempting to commit work.
     */
    public void commit() throws Exception {
        for (AmqpSender consumer : consumers.values()) {
            consumer.commit();
        }
    }

    /**
     * Rolls back any pending work being down under this session.
     *
     * @throws Exception if an error occurs while attempting to roll back work.
     */
    public void rollback() throws Exception {
        for (AmqpSender consumer : consumers.values()) {
            consumer.rollback();
        }
    }

    /**
     * Used to direct all Session managed Senders to push any queued Messages
     * out to the remote peer.
     *
     * @throws Exception if an error occurs while flushing the messages.
     */
    public void flushPendingMessages() throws Exception {
        for (AmqpSender consumer : consumers.values()) {
            consumer.pumpOutbound();
        }
    }

    public void createCoordinator(final Receiver protonReceiver) throws Exception {
        AmqpTransactionCoordinator txCoordinator = new AmqpTransactionCoordinator(this, protonReceiver);
        txCoordinator.flow(connection.getConfiguredReceiverCredit());
        txCoordinator.open();
    }

    public void createReceiver(final Receiver protonReceiver) throws Exception {
        org.apache.qpid.proton.amqp.transport.Target remoteTarget = protonReceiver.getRemoteTarget();

        ProducerInfo producerInfo = new ProducerInfo(getNextProducerId());
        final AmqpReceiver receiver = new AmqpReceiver(this, protonReceiver, producerInfo);

        try {
            Target target = (Target) remoteTarget;
            ActiveMQDestination destination = null;
            String targetNodeName = target.getAddress();

            if (target.getDynamic()) {
                destination = connection.createTemporaryDestination(protonReceiver, target.getCapabilities());
                Target actualTarget = new Target();
                actualTarget.setAddress(destination.getQualifiedName());
                actualTarget.setDynamic(true);
                protonReceiver.setTarget(actualTarget);
                receiver.addCloseAction(new Runnable() {

                    @Override
                    public void run() {
                        connection.deleteTemporaryDestination((ActiveMQTempDestination) receiver.getDestination());
                    }
                });
            } else if (targetNodeName != null && !targetNodeName.isEmpty()) {
                destination = createDestination(remoteTarget);
            }

            receiver.setDestination(destination);
            connection.sendToActiveMQ(producerInfo, new ResponseHandler() {
                @Override
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    if (response.isException()) {
                        ErrorCondition error = null;
                        Throwable exception = ((ExceptionResponse) response).getException();
                        if (exception instanceof SecurityException) {
                            error = new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, exception.getMessage());
                        } else {
                            error = new ErrorCondition(AmqpError.INTERNAL_ERROR, exception.getMessage());
                        }

                        receiver.close(error);
                    } else {
                        receiver.flow(connection.getConfiguredReceiverCredit());
                        receiver.open();
                    }
                    pumpProtonToSocket();
                }
            });

        } catch (AmqpProtocolException exception) {
            receiver.close(new ErrorCondition(Symbol.getSymbol(exception.getSymbolicName()), exception.getMessage()));
        }
    }

    @SuppressWarnings("unchecked")
    public void createSender(final Sender protonSender) throws Exception {
        org.apache.qpid.proton.amqp.messaging.Source source = (org.apache.qpid.proton.amqp.messaging.Source) protonSender.getRemoteSource();

        ConsumerInfo consumerInfo = new ConsumerInfo(getNextConsumerId());
        final AmqpSender sender = new AmqpSender(this, protonSender, consumerInfo);

        try {
            final Map<Symbol, Object> supportedFilters = new HashMap<Symbol, Object>();
            protonSender.setContext(sender);

            boolean noLocal = false;
            String selector = null;

            if (source != null) {
                Map.Entry<Symbol, DescribedType> filter = findFilter(source.getFilter(), JMS_SELECTOR_FILTER_IDS);
                if (filter != null) {
                    selector = filter.getValue().getDescribed().toString();
                    // Validate the Selector.
                    try {
                        SelectorParser.parse(selector);
                    } catch (InvalidSelectorException e) {
                        sender.close(new ErrorCondition(AmqpError.INVALID_FIELD, e.getMessage()));
                        return;
                    }

                    supportedFilters.put(filter.getKey(), filter.getValue());
                }

                filter = findFilter(source.getFilter(), NO_LOCAL_FILTER_IDS);
                if (filter != null) {
                    noLocal = true;
                    supportedFilters.put(filter.getKey(), filter.getValue());
                }
            }

            ActiveMQDestination destination;
            if (source == null) {
                // Attempt to recover previous subscription
                destination = connection.lookupSubscription(protonSender.getName());

                if (destination != null) {
                    source = new org.apache.qpid.proton.amqp.messaging.Source();
                    source.setAddress(destination.getQualifiedName());
                    source.setDurable(TerminusDurability.UNSETTLED_STATE);
                    source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
                    source.setDistributionMode(COPY);
                } else {
                    sender.close(new ErrorCondition(AmqpError.NOT_FOUND, "Unknown subscription link: " + protonSender.getName()));
                    return;
                }
            } else if (source.getDynamic()) {
                // lets create a temp dest.
                destination = connection.createTemporaryDestination(protonSender, source.getCapabilities());
                source = new org.apache.qpid.proton.amqp.messaging.Source();
                source.setAddress(destination.getQualifiedName());
                source.setDynamic(true);
                sender.addCloseAction(new Runnable() {

                    @Override
                    public void run() {
                        connection.deleteTemporaryDestination((ActiveMQTempDestination) sender.getDestination());
                    }
                });
            } else {
                destination = createDestination(source);
            }

            source.setFilter(supportedFilters.isEmpty() ? null : supportedFilters);
            protonSender.setSource(source);

            int senderCredit = protonSender.getRemoteCredit();

            consumerInfo.setSelector(selector);
            consumerInfo.setNoRangeAcks(true);
            consumerInfo.setDestination(destination);
            consumerInfo.setPrefetchSize(senderCredit >= 0 ? senderCredit : 0);
            consumerInfo.setDispatchAsync(true);
            consumerInfo.setNoLocal(noLocal);

            if (source.getDistributionMode() == COPY && destination.isQueue()) {
                consumerInfo.setBrowser(true);
            }

            if ((TerminusDurability.UNSETTLED_STATE.equals(source.getDurable()) ||
                 TerminusDurability.CONFIGURATION.equals(source.getDurable())) && destination.isTopic()) {
                consumerInfo.setSubscriptionName(protonSender.getName());
            }

            connection.sendToActiveMQ(consumerInfo, new ResponseHandler() {
                @Override
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    if (response.isException()) {
                        ErrorCondition error = null;
                        Throwable exception = ((ExceptionResponse) response).getException();
                        if (exception instanceof SecurityException) {
                            error = new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, exception.getMessage());
                        } else if (exception instanceof InvalidSelectorException) {
                            error = new ErrorCondition(AmqpError.INVALID_FIELD, exception.getMessage());
                        } else {
                            error = new ErrorCondition(AmqpError.INTERNAL_ERROR, exception.getMessage());
                        }

                        sender.close(error);
                    } else {
                        sender.open();
                    }
                    pumpProtonToSocket();
                }
            });

        } catch (AmqpProtocolException e) {
            sender.close(new ErrorCondition(Symbol.getSymbol(e.getSymbolicName()), e.getMessage()));
        }
    }

    /**
     * Send all pending work out to the remote peer.
     */
    public void pumpProtonToSocket() {
        connection.pumpProtonToSocket();
    }

    public void regosterSender(ConsumerId consumerId, AmqpSender sender) {
        consumers.put(consumerId, sender);
        connection.regosterSender(consumerId, sender);
    }

    public void unregisterSender(ConsumerId consumerId) {
        consumers.remove(consumerId);
        connection.unregosterSender(consumerId);
    }

    //----- Configuration accessors ------------------------------------------//

    public AmqpConnection getConnection() {
        return connection;
    }

    public SessionId getSessionId() {
        return sessionId;
    }

    public Session getEndpoint() {
        return protonSession;
    }

    //----- Internal Implementation ------------------------------------------//

    protected ConsumerId getNextConsumerId() {
        return new ConsumerId(sessionId, nextConsumerId++);
    }

    protected ProducerId getNextProducerId() {
        return new ProducerId(sessionId, nextProducerId++);
    }
}
