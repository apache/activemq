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

import java.io.IOException;
import java.util.Set;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.usage.Usage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

/**
 * A simple Broker intercepter which allows you to enable/disable logging.
 * 
 * @org.apache.xbean.XBean
 */

public class LoggingBrokerPlugin extends BrokerPluginSupport implements
        InitializingBean {

    private static final Log LOG = LogFactory.getLog(LoggingBrokerPlugin.class);

    private boolean logAll = false;
    private boolean logMessageEvents = false;
    private boolean logConnectionEvents = true;
    private boolean logTransactionEvents = false;
    private boolean logConsumerEvents = false;
    private boolean logProducerEvents = false;
    private boolean logInternalEvents = false;

    public void afterPropertiesSet() throws Exception {
        LOG.info("Created LoggingBrokerPlugin: " + this.toString());
    }

    public boolean isLogAll() {
        return logAll;
    }
    
    /**
     * Log all Events that go through the Plugin
     */
    public void setLogAll(boolean logAll) {
        this.logAll = logAll;
    }

    public boolean isLogMessageEvents() {
        return logMessageEvents;
    }

    /**
     * Log Events that are related to message processing
     */
    public void setLogMessageEvents(boolean logMessageEvents) {
        this.logMessageEvents = logMessageEvents;
    }

    public boolean isLogConnectionEvents() {
        return logConnectionEvents;
    }

    /**
     * Log Events that are related to connections and sessions
     */
    public void setLogConnectionEvents(boolean logConnectionEvents) {
        this.logConnectionEvents = logConnectionEvents;
    }

    public boolean isLogTransactionEvents() {
        return logTransactionEvents;
    }

    /**
     * Log Events that are related to transaction processing
     */
    public void setLogTransactionEvents(boolean logTransactionEvents) {
        this.logTransactionEvents = logTransactionEvents;
    }

    public boolean isLogConsumerEvents() {
        return logConsumerEvents;
    }

    /**
     * Log Events that are related to Consumers
     */
    public void setLogConsumerEvents(boolean logConsumerEvents) {
        this.logConsumerEvents = logConsumerEvents;
    }

    public boolean isLogProducerEvents() {
        return logProducerEvents;
    }

    /**
     * Log Events that are related to Producers
     */
    public void setLogProducerEvents(boolean logProducerEvents) {
        this.logProducerEvents = logProducerEvents;
    }

    public boolean isLogInternalEvents() {
        return logInternalEvents;
    }

    /**
     * Log Events that are normally internal to the broker
     */
    public void setLogInternalEvents(boolean logInternalEvents) {
        this.logInternalEvents = logInternalEvents;
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange,
            MessageAck ack) throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Achknowledging message for client ID : "
                    + consumerExchange.getConnectionContext().getClientId());
            if (LOG.isTraceEnabled()) {
                LOG.trace("First Message Id: " + ack.getFirstMessageId()
                        + ", Last Message Id: " + ack.getLastMessageId());
            }
        }
        super.acknowledge(consumerExchange, ack);
    }

    public Response messagePull(ConnectionContext context, MessagePull pull)
            throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Message Pull from : " + context.getClientId() + " on "
                    + pull.getDestination().getPhysicalName());
        }
        return super.messagePull(context, pull);
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info)
            throws Exception {
        if (isLogAll() || isLogConnectionEvents()) {
            LOG.info("Adding Connection : " + context);
        }
        super.addConnection(context, info);
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info)
            throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Adding Consumer : " + info);
        }
        return super.addConsumer(context, info);
    }

    public void addProducer(ConnectionContext context, ProducerInfo info)
            throws Exception {
        if (isLogAll() || isLogProducerEvents()) {
            LOG.info("Adding Producer :" + info);
        }
        super.addProducer(context, info);
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid,
            boolean onePhase) throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Commiting transaction : " + xid.getTransactionKey());
        }
        super.commitTransaction(context, xid, onePhase);
    }

    public void removeSubscription(ConnectionContext context,
            RemoveSubscriptionInfo info) throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Removing subscription : " + info);
        }
        super.removeSubscription(context, info);
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context)
            throws Exception {

        TransactionId[] result = super.getPreparedTransactions(context);
        if ((isLogAll() || isLogTransactionEvents()) && result != null) {
            StringBuffer tids = new StringBuffer();
            for (TransactionId tid : result) {
                if (tids.length() > 0) {
                    tids.append(", ");
                }
                tids.append(tid.getTransactionKey());
            }
            LOG.info("Prepared transactions : " + tids);
        }
        return result;
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid)
            throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Preparing transaction : " + xid.getTransactionKey());
        }
        return super.prepareTransaction(context, xid);
    }

    public void removeConnection(ConnectionContext context,
            ConnectionInfo info, Throwable error) throws Exception {
        if (isLogAll() || isLogConnectionEvents()) {
            LOG.info("Removing Connection : " + info);
        }
        super.removeConnection(context, info, error);
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info)
            throws Exception {
        if (isLogAll() || isLogConsumerEvents()) {
            LOG.info("Removing Consumer : " + info);
        }
        super.removeConsumer(context, info);
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info)
            throws Exception {
        if (isLogAll() || isLogProducerEvents()) {
            LOG.info("Removing Producer : " + info);
        }
        super.removeProducer(context, info);
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid)
            throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Rolling back Transaction : " + xid.getTransactionKey());
        }
        super.rollbackTransaction(context, xid);
    }

    public void send(ProducerBrokerExchange producerExchange,
            Message messageSend) throws Exception {
        if (isLogAll() || isLogProducerEvents()) {
            LOG.info("Sending message : " + messageSend);
        }
        super.send(producerExchange, messageSend);
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid)
            throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Beginning transaction : " + xid.getTransactionKey());
        }
        super.beginTransaction(context, xid);
    }

    public void forgetTransaction(ConnectionContext context,
            TransactionId transactionId) throws Exception {
        if (isLogAll() || isLogTransactionEvents()) {
            LOG.info("Forgetting transaction : "
                    + transactionId.getTransactionKey());
        }
        super.forgetTransaction(context, transactionId);
    }

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
                LOG.info("Connected clients : " + cids);
            }
        }
        return super.getClients();
    }

    public org.apache.activemq.broker.region.Destination addDestination(
            ConnectionContext context, ActiveMQDestination destination)
            throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Adding destination : "
                    + destination.getDestinationTypeAsString() + ":"
                    + destination.getPhysicalName());
        }
        return super.addDestination(context, destination);
    }

    public void removeDestination(ConnectionContext context,
            ActiveMQDestination destination, long timeout) throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Removing destination : "
                    + destination.getDestinationTypeAsString() + ":"
                    + destination.getPhysicalName());
        }
        super.removeDestination(context, destination, timeout);
    }

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
                LOG.info("Get Destinations : " + destinations);
            }
        }
        return result;
    }

    public void start() throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Starting " + getBrokerName());
        }
        super.start();
    }

    public void stop() throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Stopping " + getBrokerName());
        }
        super.stop();
    }

    public void addSession(ConnectionContext context, SessionInfo info)
            throws Exception {
        if (isLogAll() || isLogConnectionEvents()) {
            LOG.info("Adding Session : " + info);
        }
        super.addSession(context, info);
    }

    public void removeSession(ConnectionContext context, SessionInfo info)
            throws Exception {
        if (isLogAll() || isLogConnectionEvents()) {
            LOG.info("Removing Session : " + info);
        }
        super.removeSession(context, info);
    }

    public void addBroker(Connection connection, BrokerInfo info) {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Adding Broker " + info.getBrokerName());
        }
        super.addBroker(connection, info);
    }

    public void removeBroker(Connection connection, BrokerInfo info) {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Removing Broker " + info.getBrokerName());
        }
        super.removeBroker(connection, info);
    }

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
                LOG.info("Get Peer Broker Infos : " + peers);
            }
        }
        return result;
    }

    public void preProcessDispatch(MessageDispatch messageDispatch) {
        if (isLogAll() || isLogInternalEvents() || isLogConsumerEvents()) {
            LOG.info("preProcessDispatch :" + messageDispatch);
        }
        super.preProcessDispatch(messageDispatch);
    }

    public void postProcessDispatch(MessageDispatch messageDispatch) {
        if (isLogAll() || isLogInternalEvents() || isLogConsumerEvents()) {
            LOG.info("postProcessDispatch :" + messageDispatch);
        }
        super.postProcessDispatch(messageDispatch);
    }

    public void processDispatchNotification(
            MessageDispatchNotification messageDispatchNotification)
            throws Exception {
        if (isLogAll() || isLogInternalEvents() || isLogConsumerEvents()) {
            LOG.info("ProcessDispatchNotification :"
                    + messageDispatchNotification);
        }
        super.processDispatchNotification(messageDispatchNotification);
    }

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
                LOG.info("Get Durable Destinations : " + destinations);
            }
        }
        return result;
    }

    public void addDestinationInfo(ConnectionContext context,
            DestinationInfo info) throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Adding destination info : " + info);
        }
        super.addDestinationInfo(context, info);
    }

    public void removeDestinationInfo(ConnectionContext context,
            DestinationInfo info) throws Exception {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Removing destination info : " + info);
        }
        super.removeDestinationInfo(context, info);
    }

    public void messageExpired(ConnectionContext context,
            MessageReference message) {
        if (isLogAll() || isLogInternalEvents()) {
            String msg = "Unable to display message.";
            try {
                msg = message.getMessage().toString();
            } catch (IOException ioe) {
            }
            LOG.info("Message has expired : " + msg);
        }
        super.messageExpired(context, message);
    }

    public void sendToDeadLetterQueue(ConnectionContext context,
            MessageReference messageReference) {
        if (isLogAll() || isLogInternalEvents()) {
            String msg = "Unable to display message.";
            try {
                msg = messageReference.getMessage().toString();
            } catch (IOException ioe) {
            }
            LOG.info("Sending to DLQ : " + msg);
        }
    }

    public void fastProducer(ConnectionContext context,
            ProducerInfo producerInfo) {
        if (isLogAll() || isLogProducerEvents() || isLogInternalEvents()) {
            LOG.info("Fast Producer : " + producerInfo);
        }
        super.fastProducer(context, producerInfo);
    }

    public void isFull(ConnectionContext context, Destination destination,
            Usage usage) {
        if (isLogAll() || isLogProducerEvents() || isLogInternalEvents()) {
            LOG.info("Destination is full : " + destination.getName());
        }
        super.isFull(context, destination, usage);
    }

    public void messageConsumed(ConnectionContext context,
            MessageReference messageReference) {
        if (isLogAll() || isLogConsumerEvents() || isLogInternalEvents()) {
            String msg = "Unable to display message.";
            try {
                msg = messageReference.getMessage().toString();
            } catch (IOException ioe) {
            }
            LOG.info("Message consumed : " + msg);
        }
        super.messageConsumed(context, messageReference);
    }

    public void messageDelivered(ConnectionContext context,
            MessageReference messageReference) {
        if (isLogAll() || isLogConsumerEvents() || isLogInternalEvents()) {
            String msg = "Unable to display message.";
            try {
                msg = messageReference.getMessage().toString();
            } catch (IOException ioe) {
            }
            LOG.info("Message delivered : " + msg);
        }
        super.messageDelivered(context, messageReference);
    }

    public void messageDiscarded(ConnectionContext context,
            MessageReference messageReference) {
        if (isLogAll() || isLogInternalEvents()) {
            String msg = "Unable to display message.";
            try {
                msg = messageReference.getMessage().toString();
            } catch (IOException ioe) {
            }
            LOG.info("Message discarded : " + msg);
        }
        super.messageDiscarded(context, messageReference);
    }

    public void slowConsumer(ConnectionContext context,
            Destination destination, Subscription subs) {
        if (isLogAll() || isLogConsumerEvents() || isLogInternalEvents()) {
            LOG.info("Detected slow consumer on " + destination.getName());
            StringBuffer buf = new StringBuffer("Connection(");
            buf.append(subs.getConsumerInfo().getConsumerId().getConnectionId());
            buf.append(") Session(");
            buf.append(subs.getConsumerInfo().getConsumerId().getSessionId());
            buf.append(")");
            LOG.info(buf);
        }
        super.slowConsumer(context, destination, subs);
    }

    public void nowMasterBroker() {
        if (isLogAll() || isLogInternalEvents()) {
            LOG.info("Is now the master broker : " + getBrokerName());
        }
        super.nowMasterBroker();
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("LoggingBrokerPlugin(");
        buf.append("logAll=");
        buf.append(isLogAll());
        buf.append(", logConnectionEvents=");
        buf.append(isLogConnectionEvents());
        buf.append(", logConsumerEvents=");
        buf.append(isLogConsumerEvents());
        buf.append(", logProducerEvents=");
        buf.append(isLogProducerEvents());
        buf.append(", logMessageEvents=");
        buf.append(isLogMessageEvents());
        buf.append(", logTransactionEvents=");
        buf.append(isLogTransactionEvents());
        buf.append(", logInternalEvents=");
        buf.append(isLogInternalEvents());
        buf.append(")");
        return buf.toString();
    }
}
