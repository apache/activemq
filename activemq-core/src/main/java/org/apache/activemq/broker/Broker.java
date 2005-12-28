/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.activemq.Service;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;

/**
 * The Message Broker which routes messages,
 * maintains subscriptions and connections, acknowledges messages and handles
 * transactions.
 *
 * @version $Revision: 1.8 $
 */
public interface Broker extends Region, Service {

    /**
     * Get the id of the broker
     * @param context
     * @param info 
     * @param client
     */
    public BrokerId getBrokerId();

    /**
     * Get the name of the broker
     */
    public String getBrokerName();

    /**
     * A client is establishing a connection with the broker.
     * @param context
     * @param info 
     * @param client
     */
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Throwable;
    
    /**
     * A client is disconnecting from the broker.
     * @param context the environment the operation is being executed under.
     * @param info 
     * @param client
     * @param error null if the client requested the disconnect or the error that caused the client to disconnect.
     */
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Throwable;

    /**
     * Adds a session.
     * @param context
     * @param info
     * @throws Throwable
     */
    public void addSession(ConnectionContext context, SessionInfo info) throws Throwable;

    /**
     * Removes a session.
     * @param context
     * @param info
     * @throws Throwable
     */
    public void removeSession(ConnectionContext context, SessionInfo info) throws Throwable;

    /**
     * Adds a producer.
     * @param context the enviorment the operation is being executed under.
     */
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Throwable;

    /**
     * Removes a producer.
     * @param context the enviorment the operation is being executed under.
     */
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Throwable;
      
    /**
     * @return all clients added to the Broker.
     * @throws Throwable
     */
    public Connection[] getClients() throws Throwable;

    /**
     * @return all destinations added to the Broker.
     * @throws Throwable
     */
    public ActiveMQDestination[] getDestinations() throws Throwable;
    
    /**
     * Gets a list of all the prepared xa transactions.
     * @param client
     */
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Throwable;

    /**
     * Starts a transaction.
     * @param client
     * @param xid
     */
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Throwable;

    /**
     * Prepares a transaction. Only valid for xa transactions.
     * @param client
     * @param xid
     * @return
     */
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Throwable;

    /**
     * Rollsback a transaction.
     * @param client
     * @param xid
     */

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Throwable;

    /**
     * Commits a transaction.
     * @param client
     * @param xid
     * @param onePhase
     */
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Throwable;

    /**
     * Forgets a transaction.
     * @param client
     * @param xid
     * @param onePhase
     * @throws Throwable 
     */
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Throwable;
    
}
