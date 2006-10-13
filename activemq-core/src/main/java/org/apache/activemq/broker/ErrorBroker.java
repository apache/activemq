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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
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
import org.apache.activemq.kaha.Store;

/**
 * Implementation of the broker where all it's methods throw an
 * BrokerStoppedException.
 * 
 * @version $Revision$
 */
public class ErrorBroker implements Broker {

    private final String message;

    public ErrorBroker(String message) {
        this.message = message;
    }

    public Map getDestinationMap() {
        return Collections.EMPTY_MAP;
    }

    public Set getDestinations(ActiveMQDestination destination) {
        return Collections.EMPTY_SET;
    }

    public Broker getAdaptor(Class type) {
        if (type.isInstance(this)) {
            return this;
        }
        return null;
    }

    public BrokerId getBrokerId() {
        throw new BrokerStoppedException(this.message);
    }

    public String getBrokerName() {
        throw new BrokerStoppedException(this.message);
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public Connection[] getClients() throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public ActiveMQDestination[] getDestinations() throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void send(ConnectionContext context, Message message) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void acknowledge(ConnectionContext context, MessageAck ack) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void gc() {
        throw new BrokerStoppedException(this.message);
    }

    public void start() throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void stop() throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void addBroker(Connection connection, BrokerInfo info) {
        throw new BrokerStoppedException(this.message);

    }

    public void removeBroker(Connection connection, BrokerInfo info) {
        throw new BrokerStoppedException(this.message);
    }

    public BrokerInfo[] getPeerBrokerInfos() {
        throw new BrokerStoppedException(this.message);
    }

    public void processDispatch(MessageDispatch messageDispatch) {
        throw new BrokerStoppedException(this.message);
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public boolean isSlaveBroker() {
        throw new BrokerStoppedException(this.message);
    }

    public boolean isStopped() {
        return true;
    }

    public Set getDurableDestinations() {
        throw new BrokerStoppedException(this.message);
    }

    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public boolean isFaultTolerantConfiguration() {
        throw new BrokerStoppedException(this.message);
    }

    public ConnectionContext getAdminConnectionContext() {
        throw new BrokerStoppedException(this.message);
    }

    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
        throw new BrokerStoppedException(this.message);
    }

    public Response messagePull(ConnectionContext context, MessagePull pull) {
        throw new BrokerStoppedException(this.message);
    }
    
    public PendingDurableSubscriberMessageStoragePolicy getPendingDurableSubscriberPolicy() {
        throw new BrokerStoppedException(this.message);
    }
  
    public void setPendingDurableSubscriberPolicy(PendingDurableSubscriberMessageStoragePolicy pendingDurableSubscriberPolicy) {
        throw new BrokerStoppedException(this.message);
    }
    
    public Store getTempDataStore() {
        throw new BrokerStoppedException(this.message);
    }

}
