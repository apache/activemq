/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.activemq.Service;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.kaha.Store;

/**
 * The Message Broker which routes messages,
 * maintains subscriptions and connections, acknowledges messages and handles
 * transactions.
 *
 * @version $Revision: 1.8 $
 */
public interface Broker extends Region, Service {
    
    /**
     * Get a Broker from the Broker Stack that is a particular class
     * @param type
     * @return
     */
    public Broker getAdaptor(Class type);

    /**
     * Get the id of the broker
     */
    public BrokerId getBrokerId();

    /**
     * Get the name of the broker
     */
    public String getBrokerName();
    
    /**
     * A remote Broker connects
     */
    public void addBroker(Connection connection, BrokerInfo info);
    
    /**
     * Remove a BrokerInfo
     * @param connection
     * @param info
     */
    public void removeBroker(Connection connection,BrokerInfo info);
    

    /**
     * A client is establishing a connection with the broker.
     * @throws Exception TODO
     */
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception;
    
    /**
     * A client is disconnecting from the broker.
     * @param context the environment the operation is being executed under.
     * @param info 
     * @param error null if the client requested the disconnect or the error that caused the client to disconnect.
     * @throws Exception TODO
     */
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception;

    /**
     * Adds a session.
     * @param context
     * @param info
     * @throws Exception TODO
     */
    public void addSession(ConnectionContext context, SessionInfo info) throws Exception;

    /**
     * Removes a session.
     * @param context
     * @param info
     * @throws Exception TODO
     */
    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception;

    /**
     * Adds a producer.
     * @param context the enviorment the operation is being executed under.
     * @throws Exception TODO
     */
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception;

    /**
     * Removes a producer.
     * @param context the enviorment the operation is being executed under.
     * @throws Exception TODO
     */
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception;
      
    /**
     * @return all clients added to the Broker.
     * @throws Exception TODO
     */
    public Connection[] getClients() throws Exception;

    /**
     * @return all destinations added to the Broker.
     * @throws Exception TODO
     */
    public ActiveMQDestination[] getDestinations() throws Exception;
    
    /**
     * Gets a list of all the prepared xa transactions.
     * @param context transaction ids
     * @return 
     * @throws Exception TODO
     */
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception;

    /**
     * Starts a transaction.
     * @param context
     * @param xid
     * @throws Exception TODO
     */
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception;

    /**
     * Prepares a transaction. Only valid for xa transactions.
     * @param context
     * @param xid
     * @return id
     * @throws Exception TODO
     */
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception;

    /**
     * Rollsback a transaction.
     * @param context
     * @param xid
     * @throws Exception TODO
     */

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception;

    /**
     * Commits a transaction.
     * @param context
     * @param xid
     * @param onePhase
     * @throws Exception TODO
     */
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception;

    /**
     * Forgets a transaction.
     * @param context 
     * @param transactionId 
     * @throws Exception 
     */
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception;
    
    /**
     * Get the BrokerInfo's of any connected Brokers
     * @return array of peer BrokerInfos
     */
    BrokerInfo[] getPeerBrokerInfos();
    
    
    /**
     * Notify the Broker that a dispatch has happened
     * @param messageDispatch
     */
    public void processDispatch(MessageDispatch messageDispatch);
  
    /**
     * @return true if the broker has stopped
     */
    public boolean isStopped();
    
    /**
     * @return a Set of all durable destinations
     */
    public Set getDurableDestinations();
    
    /**
     * Add and process a DestinationInfo object
     * @param context
     * @param info
     * @throws Exception
     */
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception;
    
    
    /**
     * Remove and process a DestinationInfo object
     * @param context
     * @param info
     * @throws Exception
     */
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception;

    
    /**
     * @return true if fault tolerant
     */
    public boolean isFaultTolerantConfiguration();
    
    /**
     * @return the connection context used to make administration operations on startup or via JMX MBeans
     */
    public abstract ConnectionContext getAdminConnectionContext();

    /**
     * Sets the default administration connection context used when configuring the broker on startup or via JMX
     * @param adminConnectionContext 
     */
    public abstract void setAdminConnectionContext(ConnectionContext adminConnectionContext);
      
    /**
     * @return the temp data store
     */
    public Store getTempDataStore();
    
    /**
     * @return the URI that can be used to connect to the local Broker
     */
    public URI getVmConnectorURI();
    
    /**
     * called when the brokerService starts
     */
    public void brokerServiceStarted();
    
    /**
     * @return the BrokerService
     */
    BrokerService getBrokerService();
    
    /**
     * Ensure we get the Broker at the top of the Stack
     * @return the broker at the top of the Stack
     */
    Broker getRoot();
    
    /**
     * A Message has Expired
     * @param context
     * @param messageReference
     * @throws Exception 
     */
    public void messageExpired(ConnectionContext context, MessageReference messageReference);
    
    /**
     * A message needs to go the a DLQ
     * @param context
     * @param messageReference
     * @throws Exception
     */
    public void sendToDeadLetterQueue(ConnectionContext context,MessageReference messageReference);
}
