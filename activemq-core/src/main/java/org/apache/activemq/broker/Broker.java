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
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.activemq.Service;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.store.kahadb.plist.PListStore;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.usage.Usage;

/**
 * The Message Broker which routes messages, maintains subscriptions and
 * connections, acknowledges messages and handles transactions.
 * 
 * @version $Revision: 1.8 $
 */
public interface Broker extends Region, Service {

    /**
     * Get a Broker from the Broker Stack that is a particular class
     * 
     * @param type
     * @return
     */
    Broker getAdaptor(Class type);

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
     * @param connection
     * @param info
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
     * @param info
     * @param error null if the client requested the disconnect or the error
     *                that caused the client to disconnect.
     * @throws Exception TODO
     */
    void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception;

    /**
     * Adds a session.
     * 
     * @param context
     * @param info
     * @throws Exception TODO
     */
    void addSession(ConnectionContext context, SessionInfo info) throws Exception;

    /**
     * Removes a session.
     * 
     * @param context
     * @param info
     * @throws Exception TODO
     */
    void removeSession(ConnectionContext context, SessionInfo info) throws Exception;

    /**
     * Adds a producer.
     * 
     * @param context the enviorment the operation is being executed under.
     * @throws Exception TODO
     */
    void addProducer(ConnectionContext context, ProducerInfo info) throws Exception;

    /**
     * Removes a producer.
     * 
     * @param context the enviorment the operation is being executed under.
     * @throws Exception TODO
     */
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
     * Gets a list of all the prepared xa transactions.
     * 
     * @param context transaction ids
     * @return
     * @throws Exception TODO
     */
    TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception;

    /**
     * Starts a transaction.
     * 
     * @param context
     * @param xid
     * @throws Exception TODO
     */
    void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception;

    /**
     * Prepares a transaction. Only valid for xa transactions.
     * 
     * @param context
     * @param xid
     * @return id
     * @throws Exception TODO
     */
    int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception;

    /**
     * Rollsback a transaction.
     * 
     * @param context
     * @param xid
     * @throws Exception TODO
     */

    void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception;

    /**
     * Commits a transaction.
     * 
     * @param context
     * @param xid
     * @param onePhase
     * @throws Exception TODO
     */
    void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception;

    /**
     * Forgets a transaction.
     * 
     * @param context
     * @param transactionId
     * @throws Exception
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
     * @param messageDispatch
     */
    void preProcessDispatch(MessageDispatch messageDispatch);

    /**
     * Notify the Broker that a dispatch has happened
     * 
     * @param messageDispatch
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
     * @param context
     * @param info
     * @throws Exception
     */
    void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception;

    /**
     * Remove and process a DestinationInfo object
     * 
     * @param context
     * @param info
     * @throws Exception
     */
    void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception;

    /**
     * @return true if fault tolerant
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
     * @param adminConnectionContext
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
     * overriden - as the timestamp set by the producer can be out of sync with
     * the broker
     * 
     * @param messageReference
     * @return true if the message is expired
     */
    boolean isExpired(MessageReference messageReference);

    /**
     * A Message has Expired
     * 
     * @param context
     * @param messageReference
     */
    void messageExpired(ConnectionContext context, MessageReference messageReference);

    /**
     * A message needs to go the a DLQ
     * 
     * @param context
     * @param messageReference
     */
    void sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference);
    
    /**
     * @return the broker sequence id
     */
    long getBrokerSequenceId();
    
    /**
     * called when message is consumed
     * @param context
     * @param messageReference
     */
    void messageConsumed(ConnectionContext context, MessageReference messageReference);
    
    /**
     * Called when message is delivered to the broker
     * @param context
     * @param messageReference
     */
    void messageDelivered(ConnectionContext context, MessageReference messageReference);
    
    /**
     * Called when a message is discarded - e.g. running low on memory
     * This will happen only if the policy is enabled - e.g. non durable topics
     * @param context
     * @param sub 
     * @param messageReference
     */
    void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference);
    
    /**
     * Called when there is a slow consumer
     * @param context
     * @param destination 
     * @param subs
     */
    void slowConsumer(ConnectionContext context,Destination destination, Subscription subs);
    
    /**
     * Called to notify a producer is too fast
     * @param context
     * @param producerInfo
     */
    void fastProducer(ConnectionContext context,ProducerInfo producerInfo);
    
    /**
     * Called when a Usage reaches a limit
     * @param context
     * @param destination 
     * @param usage
     */
    void isFull(ConnectionContext context,Destination destination,Usage usage);
    
    /**
     *  called when the broker becomes the master in a master/slave
     *  configuration
     */
    void nowMasterBroker();
    
    Scheduler getScheduler();
    
    ThreadPoolExecutor getExecutor();

    void networkBridgeStarted(BrokerInfo brokerInfo);

    void networkBridgeStopped(BrokerInfo brokerInfo);


}
