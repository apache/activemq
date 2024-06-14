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
package org.apache.activemq.broker;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.activemq.Service;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.PListStore;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.usage.Usage;

/**
 * The Message Broker which routes messages, maintains subscriptions and
 * connections, acknowledges messages and handles transactions.
 */
public interface Broker extends Region, Service {

    /**
     * Get a Broker from the Broker Stack that is a particular class
     *
     * @param type a Broker type.
     * @return a Broker instance.
     */
    Broker getAdaptor(Class<?> type);

    /**
     * Get the id of the broker
     */
    BrokerId getBrokerId();

    /**
     * Get the name of the broker
     */
    String getBrokerName();

    /**
     * A remote Broker connects
     */
    void addBroker(Connection connection, BrokerInfo info);

    /**
     * Remove a BrokerInfo
     *
     * @param connection Broker connection
     * @param info metadata about the Broker
     */
    void removeBroker(Connection connection, BrokerInfo info);

    /**
     * A client is establishing a connection with the broker.
     *
     * @throws Exception TODO
     */
    void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception;

    /**
     * A client is disconnecting from the broker.
     *
     * @param context the environment the operation is being executed under.
     * @param info metadata about the Broker
     * @param error null if the client requested the disconnect or the error
     *                that caused the client to disconnect.
     * @throws Exception TODO
     */
    void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception;

    /**
     * Adds a session.
     *
     * @param context connection context
     * @param info metadata about the Broker
     * @throws Exception TODO
     */
    void addSession(ConnectionContext context, SessionInfo info) throws Exception;

    /**
     * Removes a session.
     *
     * @param context connection context
     * @param info metadata about the Broker
     * @throws Exception TODO
     */
    void removeSession(ConnectionContext context, SessionInfo info) throws Exception;

    /**
     * Adds a producer.
     *
     * @param context the environment the operation is being executed under.
     * @throws Exception TODO
     */
    @Override
    void addProducer(ConnectionContext context, ProducerInfo info) throws Exception;

    /**
     * Removes a producer.
     *
     * @param context the environment the operation is being executed under.
     * @throws Exception TODO
     */
    @Override
    void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception;

    /**
     * @return all clients added to the Broker.
     * @throws Exception TODO
     */
    Connection[] getClients() throws Exception;

    /**
     * @return all destinations added to the Broker.
     * @throws Exception TODO
     */
    ActiveMQDestination[] getDestinations() throws Exception;

    /**
     * return a reference destination map of a region based on the destination type
     *
     * @param destination ActiveMQ Destination
     *
     * @return destination Map
     */
    Map<ActiveMQDestination, Destination> getDestinationMap(ActiveMQDestination destination);

    /**
     * Gets a list of all the prepared xa transactions.
     *
     * @param context transaction ids
     * @return array of TransactionId values
     * @throws Exception TODO
     */
    TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception;

    /**
     * Starts a transaction.
     *
     * @param context connection context
     * @param xid transaction id
     * @throws Exception TODO
     */
    void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception;

    /**
     * Prepares a transaction. Only valid for xa transactions.
     *
     * @param context connection context
     * @param xid transaction id
     * @return id
     * @throws Exception TODO
     */
    int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception;

    /**
     * Rollback a transaction.
     *
     * @param context connection context
     * @param xid transaction id
     * @throws Exception TODO
     */
    void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception;

    /**
     * Commits a transaction.
     *
     * @param context connection context
     * @param xid transaction id
     * @param onePhase is COMMIT_ONE_PHASE
     * @throws Exception TODO
     */
    void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception;

    /**
     * Forgets a transaction.
     *
     * @param context connection context
     * @param transactionId transaction id
     * @throws Exception TODO
     */
    void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception;

    /**
     * Get the BrokerInfo's of any connected Brokers
     *
     * @return array of peer BrokerInfos
     */
    BrokerInfo[] getPeerBrokerInfos();

    /**
     * Notify the Broker that a dispatch is going to happen
     *
     * @param messageDispatch MessageDispatch object being dispatched
     */
    void preProcessDispatch(MessageDispatch messageDispatch);

    /**
     * Notify the Broker that a dispatch has happened
     *
     * @param messageDispatch MessageDispatch that has dispatched
     */
    void postProcessDispatch(MessageDispatch messageDispatch);

    /**
     * @return true if the broker has stopped
     */
    boolean isStopped();

    /**
     * @return a Set of all durable destinations
     */
    Set<ActiveMQDestination> getDurableDestinations();

    /**
     * Add and process a DestinationInfo object
     *
     * @param context connection context
     * @param info destination info
     * @throws Exception TODO
     */
    void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception;

    /**
     * Remove and process a DestinationInfo object
     *
     * @param context connection context
     * @param info destination info
     *
     * @throws Exception TODO
     */
    void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception;

    /**
     * @return true if fault-tolerant
     */
    boolean isFaultTolerantConfiguration();

    /**
     * @return the connection context used to make administration operations on
     *         startup or via JMX MBeans
     */
    ConnectionContext getAdminConnectionContext();

    /**
     * Sets the default administration connection context used when configuring
     * the broker on startup or via JMX
     *
     * @param adminConnectionContext default administration connection context
     */
    void setAdminConnectionContext(ConnectionContext adminConnectionContext);

    /**
     * @return the temp data store
     */
    PListStore getTempDataStore();

    /**
     * @return the URI that can be used to connect to the local Broker
     */
    URI getVmConnectorURI();

    /**
     * called when the brokerService starts
     */
    void brokerServiceStarted();

    /**
     * @return the BrokerService
     */
    BrokerService getBrokerService();

    /**
     * Ensure we get the Broker at the top of the Stack
     *
     * @return the broker at the top of the Stack
     */
    Broker getRoot();

    /**
     * Determine if a message has expired -allows default behaviour to be
     * overridden - as the timestamp set by the producer can be out of sync with
     * the broker
     *
     * @param messageReference message reference
     * @return true if the message is expired
     */
    boolean isExpired(MessageReference messageReference);

    /**
     * A Message has Expired
     *
     * @param context connection context
     * @param messageReference message reference
     * @param subscription (maybe null)
     */
    void messageExpired(ConnectionContext context, MessageReference messageReference, Subscription subscription);

    /**
     * A message needs to go to the DLQ
     *
     *
     * @param context connection context
     * @param messageReference message reference
     * @param poisonCause reason for dlq submission, may be null
     * @return true if Message was placed in a DLQ false if discarded.
     */
    boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference, Subscription subscription, Throwable poisonCause);

    /**
     * @return the broker sequence id
     */
    long getBrokerSequenceId();

    /**
     * called when message is consumed
     * @param context connection context
     * @param messageReference message reference
     */
    void messageConsumed(ConnectionContext context, MessageReference messageReference);

    /**
     * Called when message is delivered to the broker
     * @param context connection context
     * @param messageReference message reference
     */
    void messageDelivered(ConnectionContext context, MessageReference messageReference);

    /**
     * Called when message is dispatched to a consumer
     * @param context connection context
     * @param sub subscription
     * @param messageReference message reference
     */
    void messageDispatched(ConnectionContext context, Subscription sub, MessageReference messageReference);

    /**
     * Called when a message is discarded - e.g. running low on memory
     * This will happen only if the policy is enabled - e.g. non-durable topics
     * @param context connection context
     * @param sub subscription
     * @param messageReference message reference
     */
    void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference);

    /**
     * Called when there is a slow consumer
     * @param context connection context
     * @param destination destination
     * @param subs subscription
     */
    void slowConsumer(ConnectionContext context, Destination destination, Subscription subs);

    /**
     * Called to notify a producer is too fast
     * @param context connection context
     * @param producerInfo producer info
     * @param destination destination
     */
    void fastProducer(ConnectionContext context,ProducerInfo producerInfo,ActiveMQDestination destination);

    /**
     * Called when a Usage reaches a limit
     * @param context create new scratch file from selection
     * @param destination destination
     * @param usage usage
     */
    void isFull(ConnectionContext context,Destination destination,Usage<?> usage);

    void virtualDestinationAdded(ConnectionContext context, VirtualDestination virtualDestination);

    void virtualDestinationRemoved(ConnectionContext context, VirtualDestination virtualDestination);

    /**
     * called when the broker becomes the master in a master/slave
     * configuration
     */
    void nowMasterBroker();

    /**
     * called to get scheduler for executing TimerTask
     */
    Scheduler getScheduler();

    /**
     * called to get Java thread pool executor
     */
    ThreadPoolExecutor getExecutor();

    /**
     * called to when a network bridge is started
     * @param brokerInfo metadata about the broker
     * @param createdByDuplex is created by duplex
     * @param remoteIp ip address of the broker
     */
    void networkBridgeStarted(BrokerInfo brokerInfo, boolean createdByDuplex, String remoteIp);

    /**
     * called to when a network bridge is stopped
     * @param brokerInfo metadata about the broker
     */
    void networkBridgeStopped(BrokerInfo brokerInfo);

    void queuePurged(ConnectionContext context, ActiveMQDestination destination);
}
