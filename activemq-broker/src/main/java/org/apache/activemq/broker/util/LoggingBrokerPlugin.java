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
package org.apache.activemq.broker.util;

import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.*;
import org.apache.activemq.usage.Usage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple Broker intercepter which allows you to enable/disable logging.
 *
 * @org.apache.xbean.XBean
 */
public class LoggingBrokerPlugin extends BrokerPluginSupport {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingBrokerPlugin.class);

    private boolean logAll = false;
    private boolean logConnectionEvents = true;
    private boolean logSessionEvents = true;
    private boolean logTransactionEvents = false;
    private boolean logConsumerEvents = false;
    private boolean logProducerEvents = false;
    private boolean logInternalEvents = false;
    private boolean perDestinationLogger = false;

    /**
     * JSR-250 callback wrapper; converts checked exceptions to runtime exceptions
     *
     * delegates to afterPropertiesSet, done to prevent backwards incompatible signature change
     */
    @PostConstruct
    private void postConstruct() {
        try {
            afterPropertiesSet();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * @throws Exception
     * @org.apache.xbean.InitMethod
     */
    public void afterPropertiesSet() throws Exception {
        LOG.info("Created LoggingBrokerPlugin: {}", this.toString());
    }

    public boolean isLogAll() {
        return logAll;
    }

    /**
     * Logger all Events that go through the Plugin
     */
    public void setLogAll(boolean logAll) {
        this.logAll = logAll;
    }


    public boolean isLogConnectionEvents() {
        return logConnectionEvents;
    }

    /**
     * Logger Events that are related to connections
     */
    public void setLogConnectionEvents(boolean logConnectionEvents) {
        this.logConnectionEvents = logConnectionEvents;
    }

    public boolean isLogSessionEvents() {
        return logSessionEvents;
    }

    /**
     * Logger Events that are related to sessions
     */
    public void setLogSessionEvents(boolean logSessionEvents) {
        this.logSessionEvents = logSessionEvents;
    }

    public boolean isLogTransactionEvents() {
        return logTransactionEvents;
    }

    /**
     * Logger Events that are related to transaction processing
     */
    public void setLogTransactionEvents(boolean logTransactionEvents) {
        this.logTransactionEvents = logTransactionEvents;
    }

    public boolean isLogConsumerEvents() {
        return logConsumerEvents;
    }

    /**
     * Logger Events that are related to Consumers
     */
    public void setLogConsumerEvents(boolean logConsumerEvents) {
        this.logConsumerEvents = logConsumerEvents;
    }

    public boolean isLogProducerEvents() {
        return logProducerEvents;
    }

    /**
     * Logger Events that are related to Producers
     */
    public void setLogProducerEvents(boolean logProducerEvents) {
        this.logProducerEvents = logProducerEvents;
    }

    public boolean isLogInternalEvents() {
        return logInternalEvents;
    }

    /**
     * Logger Events that are normally internal to the broker
     */
    public void setLogInternalEvents(boolean logInternalEvents) {
        this.logInternalEvents = logInternalEvents;
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Acknowledging message for client ID: {}{}", consumerExchange.getConnectionContext().getClientId(), (ack.getMessageCount() == 1 ? ", " + ack.getLastMessageId() : ""));
            if (ack.getMessageCount() > 1) {
                LOG.trace("Message count: {}, First Message Id: {}, Last Message Id: {}", new Object[]{ ack.getMessageCount(), ack.getFirstMessageId(), ack.getLastMessageId() });
            }
        }
        super.acknowledge(consumerExchange, ack);
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Message Pull from: {} on {}", context.getClientId(), pull.getDestination().getPhysicalName());
        }
        return super.messagePull(context, pull);
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        if (isLogAll() || isLogConnectionEvents()) {
            LOG.info("Adding Connection: {}", info);
        }
        super.addConnection(context, info);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Adding Consumer: {}", info);
        }
        return super.addConsumer(context, info);
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        if (isLogAll() || isLogProducerEvents()) {
            LOG.info("Adding Producer: {}", info);
        }
        super.addProducer(context, info);
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Committing transaction: {}", xid.getTransactionKey());
        }
        super.commitTransaction(context, xid, onePhase);
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Removing subscription: {}", info);
        }
        super.removeSubscription(context, info);
    }

    @Override
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {

        TransactionId[] result = super.getPreparedTransactions(context);
        if ((isLogAll() || isLogTransactionEvents()) && result != null) {
            StringBuffer tids = new StringBuffer();
            for (TransactionId tid : result) {
                if (tids.length() > 0) {
                    tids.append(", ");
                }
                tids.append(tid.getTransactionKey());
            }
            LOG.info("Prepared transactions: {}", tids);
        }
        return result;
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Preparing transaction: {}", xid.getTransactionKey());
        }
        return super.prepareTransaction(context, xid);
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        if (isLogAll() || isLogConnectionEvents()) {
            LOG.info("Removing Connection: {}", info);
        }
        super.removeConnection(context, info, error);
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Removing Consumer: {}", info);
        }
        super.removeConsumer(context, info);
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        if (isLogAll() || isLogProducerEvents()) {
            LOG.info("Removing Producer: {}", info);
        }
        super.removeProducer(context, info);
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Rolling back Transaction: {}", xid.getTransactionKey());
        }
        super.rollbackTransaction(context, xid);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        if (isLogAll() || isLogProducerEvents()) {
            logSend(messageSend.copy());
        }
        super.send(producerExchange, messageSend);
    }

    private void logSend(Message copy) {
        copy.getSize();
        Logger perDestinationsLogger = LOG;
        if (isPerDestinationLogger()) {
            ActiveMQDestination destination = copy.getDestination();
            perDestinationsLogger = LoggerFactory.getLogger(LOG.getName() +
                    "." + destination.getDestinationTypeAsString() + "." + destination.getPhysicalName());
        }
        perDestinationsLogger.info("Sending message: {}", copy);
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Beginning transaction: {}", xid.getTransactionKey());
        }
        super.beginTransaction(context, xid);
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Forgetting transaction: {}", transactionId.getTransactionKey());
        }
        super.forgetTransaction(context, transactionId);
    }

    @Override
    public Connection[] getClients() throws Exception {
        Connection[] result = super.getClients();

        if (isLogAll() || isLogInternalEvents()) {
            if (result == null) {
                LOG.info("Get Clients returned empty list.");
            } else {
                StringBuffer cids = new StringBuffer();
                for (Connection c : result) {
                    cids.append(cids.length() > 0 ? ", " : "");
                    cids.append(c.getConnectionId());
                }
                LOG.info("Connected clients: {}", cids);
            }
        }
        return super.getClients();
    }

    @Override
    public org.apache.activemq.broker.region.Destination addDestination(ConnectionContext context,
            ActiveMQDestination destination, boolean create) throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Adding destination: {}:{}", destination.getDestinationTypeAsString(), destination.getPhysicalName());
        }
        return super.addDestination(context, destination, create);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout)
            throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Removing destination: {}:{}", destination.getDestinationTypeAsString(), destination.getPhysicalName());
        }
        super.removeDestination(context, destination, timeout);
    }

    @Override
    public ActiveMQDestination[] getDestinations() throws Exception {
        ActiveMQDestination[] result = super.getDestinations();
        if (isLogAll() || isLogInternalEvents()) {
            if (result == null) {
                LOG.info("Get Destinations returned empty list.");
            } else {
                StringBuffer destinations = new StringBuffer();
                for (ActiveMQDestination dest : result) {
                    destinations.append(destinations.length() > 0 ? ", " : "");
                    destinations.append(dest.getPhysicalName());
                }
                LOG.info("Get Destinations: {}", destinations);
            }
        }
        return result;
    }

    @Override
    public void start() throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Starting {}", getBrokerName());
        }
        super.start();
    }

    @Override
    public void stop() throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Stopping {}", getBrokerName());
        }
        super.stop();
    }

    @Override
    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
        if (isLogAll() || isLogSessionEvents()) {
            LOG.info("Adding Session: {}", info);
        }
        super.addSession(context, info);
    }

    @Override
    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
        if (isLogAll() || isLogSessionEvents()) {
            LOG.info("Removing Session: {}", info);
        }
        super.removeSession(context, info);
    }

    @Override
    public void addBroker(Connection connection, BrokerInfo info) {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Adding Broker {}", info.getBrokerName());
        }
        super.addBroker(connection, info);
    }

    @Override
    public void removeBroker(Connection connection, BrokerInfo info) {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Removing Broker {}", info.getBrokerName());
        }
        super.removeBroker(connection, info);
    }

    @Override
    public BrokerInfo[] getPeerBrokerInfos() {
        BrokerInfo[] result = super.getPeerBrokerInfos();
        if (isLogAll() || isLogInternalEvents()) {
            if (result == null) {
                LOG.info("Get Peer Broker Infos returned empty list.");
            } else {
                StringBuffer peers = new StringBuffer();
                for (BrokerInfo bi : result) {
                    peers.append(peers.length() > 0 ? ", " : "");
                    peers.append(bi.getBrokerName());
                }
                LOG.info("Get Peer Broker Infos: {}", peers);
            }
        }
        return result;
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        if (isLogAll() || isLogInternalEvents() || isLogConsumerEvents()) {
            LOG.info("preProcessDispatch: {}", messageDispatch);
        }
        super.preProcessDispatch(messageDispatch);
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
        if (isLogAll() || isLogInternalEvents() || isLogConsumerEvents()) {
            LOG.info("postProcessDispatch: {}", messageDispatch);
        }
        super.postProcessDispatch(messageDispatch);
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        if (isLogAll() || isLogInternalEvents() || isLogConsumerEvents()) {
            LOG.info("ProcessDispatchNotification: {}", messageDispatchNotification);
        }
        super.processDispatchNotification(messageDispatchNotification);
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        Set<ActiveMQDestination> result = super.getDurableDestinations();
        if (isLogAll() || isLogInternalEvents()) {
            if (result == null) {
                LOG.info("Get Durable Destinations returned empty list.");
            } else {
                StringBuffer destinations = new StringBuffer();
                for (ActiveMQDestination dest : result) {
                    destinations.append(destinations.length() > 0 ? ", " : "");
                    destinations.append(dest.getPhysicalName());
                }
                LOG.info("Get Durable Destinations: {}", destinations);
            }
        }
        return result;
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Adding destination info: {}", info);
        }
        super.addDestinationInfo(context, info);
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Removing destination info: {}", info);
        }
        super.removeDestinationInfo(context, info);
    }

    @Override
    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
        if (isLogAll() || isLogInternalEvents()) {
            String msg = "Unable to display message.";

            msg = message.getMessage().toString();

            LOG.info("Message has expired: {}", msg);
        }
        super.messageExpired(context, message, subscription);
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference,
                                         Subscription subscription, Throwable poisonCause) {
        if (isLogAll() || isLogInternalEvents()) {
            String msg = "Unable to display message.";

            msg = messageReference.getMessage().toString();

            LOG.info("Sending to DLQ: {}", msg);
        }
        return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
    }

    @Override
    public void fastProducer(ConnectionContext context, ProducerInfo producerInfo,ActiveMQDestination destination) {
        if (isLogAll() || isLogProducerEvents() || isLogInternalEvents()) {
            LOG.info("Fast Producer: {}", producerInfo);
        }
        super.fastProducer(context, producerInfo, destination);
    }

    @Override
    public void isFull(ConnectionContext context, Destination destination, Usage<?> usage) {
        if (isLogAll() || isLogProducerEvents() || isLogInternalEvents()) {
            LOG.info("Destination is full: {}", destination.getName());
        }
        super.isFull(context, destination, usage);
    }

    @Override
    public void messageConsumed(ConnectionContext context, MessageReference messageReference) {
        if (isLogAll() || isLogConsumerEvents() || isLogInternalEvents()) {
            String msg = "Unable to display message.";

            msg = messageReference.getMessage().toString();

            LOG.info("Message consumed: {}", msg);
        }
        super.messageConsumed(context, messageReference);
    }

    @Override
    public void messageDelivered(ConnectionContext context, MessageReference messageReference) {
        if (isLogAll() || isLogConsumerEvents() || isLogInternalEvents()) {
            String msg = "Unable to display message.";

            msg = messageReference.getMessage().toString();

            LOG.info("Message delivered: {}", msg);
        }
        super.messageDelivered(context, messageReference);
    }

    @Override
    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
        if (isLogAll() || isLogInternalEvents()) {
            String msg = "Unable to display message.";

            msg = messageReference.getMessage().toString();

            LOG.info("Message discarded: {}", msg);
        }
        super.messageDiscarded(context, sub, messageReference);
    }

    @Override
    public void slowConsumer(ConnectionContext context, Destination destination, Subscription subs) {
        if (isLogAll() || isLogConsumerEvents() || isLogInternalEvents()) {
            LOG.info("Detected slow consumer on {}", destination.getName());
            StringBuffer buf = new StringBuffer("Connection(");
            buf.append(subs.getConsumerInfo().getConsumerId().getConnectionId());
            buf.append(") Session(");
            buf.append(subs.getConsumerInfo().getConsumerId().getSessionId());
            buf.append(")");
            LOG.info(buf.toString());
        }
        super.slowConsumer(context, destination, subs);
    }

    @Override
    public void nowMasterBroker() {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Is now the master broker: {}", getBrokerName());
        }
        super.nowMasterBroker();
    }

    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("LoggingBrokerPlugin(");
        buf.append("logAll=");
        buf.append(isLogAll());
        buf.append(", logConnectionEvents=");
        buf.append(isLogConnectionEvents());
        buf.append(", logSessionEvents=");
        buf.append(isLogSessionEvents());
        buf.append(", logConsumerEvents=");
        buf.append(isLogConsumerEvents());
        buf.append(", logProducerEvents=");
        buf.append(isLogProducerEvents());
        buf.append(", logTransactionEvents=");
        buf.append(isLogTransactionEvents());
        buf.append(", logInternalEvents=");
        buf.append(isLogInternalEvents());
        buf.append(")");
        return buf.toString();
    }

    public void setPerDestinationLogger(boolean perDestinationLogger) {
        this.perDestinationLogger = perDestinationLogger;
    }

    public boolean isPerDestinationLogger() {
        return perDestinationLogger;
    }
}
