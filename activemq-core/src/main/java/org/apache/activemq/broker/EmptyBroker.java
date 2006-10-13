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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Dumb implementation - used to be overriden by listeners
 * 
 * @version $Revision$
 */
public class EmptyBroker implements Broker {

    public BrokerId getBrokerId() {
        return null;
    }

    public String getBrokerName() {
        return null;
    }

    public Broker getAdaptor(Class type) {
        if (type.isInstance(this)) {
            return this;
        }
        return null;
    }

    public Map getDestinationMap() {
        return Collections.EMPTY_MAP;
    }

    public Set getDestinations(ActiveMQDestination destination) {
        return Collections.EMPTY_SET;
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {

    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {

    }

    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {

    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {

    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {

    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {

    }

    public Connection[] getClients() throws Exception {

        return null;
    }

    public ActiveMQDestination[] getDestinations() throws Exception {

        return null;
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {

        return null;
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {

    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {

        return 0;
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {

    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {

    }

    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {

    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {

        return null;
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {

    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        return null;
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {

    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {

    }

    public void send(ConnectionContext context, Message message) throws Exception {

    }

    public void acknowledge(ConnectionContext context, MessageAck ack) throws Exception {

    }

    public void gc() {

    }

    public void start() throws Exception {

    }

    public void stop() throws Exception {

    }

    public void addBroker(Connection connection, BrokerInfo info) {

    }

    public void removeBroker(Connection connection, BrokerInfo info) {

    }

    public BrokerInfo[] getPeerBrokerInfos() {
        return null;
    }

    /**
     * Notifiy the Broker that a dispatch has happened
     * 
     * @param messageDispatch
     */
    public void processDispatch(MessageDispatch messageDispatch) {

    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {

    }

    public boolean isSlaveBroker() {
        return false;
    }

    public boolean isStopped() {
        return false;
    }

    public Set getDurableDestinations() {
        return null;
    }

    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
    }

    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
    }

    public boolean isFaultTolerantConfiguration() {
        return false;
    }

    public ConnectionContext getAdminConnectionContext() {
        return null;
    }

    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
    }

    
    public Response messagePull(ConnectionContext context, MessagePull pull) {
        return null;
    }
    
    public PendingDurableSubscriberMessageStoragePolicy getPendingDurableSubscriberPolicy() {
        return null;
    }
  
    public void setPendingDurableSubscriberPolicy(PendingDurableSubscriberMessageStoragePolicy pendingDurableSubscriberPolicy) {
    }
    
    public Store getTempDataStore() {
        return null;
    }

}
