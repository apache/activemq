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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
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
import org.apache.activemq.store.kahadb.plist.PListStore;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.usage.Usage;

/**
 * Implementation of the broker where all it's methods throw an
 * BrokerStoppedException.
 * 
 * 
 */
public class ErrorBroker implements Broker {

    private final String message;

    public ErrorBroker(String message) {
        this.message = message;
    }

    @SuppressWarnings("unchecked")
    public Map<ActiveMQDestination, Destination> getDestinationMap() {
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

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination,boolean flag) throws Exception {
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

    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
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

    public void preProcessDispatch(MessageDispatch messageDispatch) {
        throw new BrokerStoppedException(this.message);
    }

    public void postProcessDispatch(MessageDispatch messageDispatch) {
        throw new BrokerStoppedException(this.message);
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    public boolean isStopped() {
        return true;
    }

    public Set<ActiveMQDestination> getDurableDestinations() {
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

    public PListStore getTempDataStore() {
        throw new BrokerStoppedException(this.message);
    }

    public URI getVmConnectorURI() {
        throw new BrokerStoppedException(this.message);
    }

    public void brokerServiceStarted() {
        throw new BrokerStoppedException(this.message);
    }

    public BrokerService getBrokerService() {
        throw new BrokerStoppedException(this.message);
    }

    public boolean isExpired(MessageReference messageReference) {
        throw new BrokerStoppedException(this.message);
    }

    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
        throw new BrokerStoppedException(this.message);
    }

    public void sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference,
                                      Subscription subscription) {
        throw new BrokerStoppedException(this.message);
    }

    public Broker getRoot() {
        throw new BrokerStoppedException(this.message);
    }
    
    public long getBrokerSequenceId() {
        throw new BrokerStoppedException(this.message);
    }
    
    public void fastProducer(ConnectionContext context,ProducerInfo producerInfo) {
        throw new BrokerStoppedException(this.message);
    }

    public void isFull(ConnectionContext context,Destination destination, Usage usage) {
        throw new BrokerStoppedException(this.message);
    }

    public void messageConsumed(ConnectionContext context,MessageReference messageReference) {
        throw new BrokerStoppedException(this.message);
    }

    public void messageDelivered(ConnectionContext context,MessageReference messageReference) {
        throw new BrokerStoppedException(this.message);
    }

    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
        throw new BrokerStoppedException(this.message);
    }

    public void slowConsumer(ConnectionContext context, Destination destination,Subscription subs) {
        throw new BrokerStoppedException(this.message);
    }
    
    public void nowMasterBroker() {   
        throw new BrokerStoppedException(this.message);
    }

    public void processConsumerControl(ConsumerBrokerExchange consumerExchange,
            ConsumerControl control) {
        throw new BrokerStoppedException(this.message);
    }

    public Scheduler getScheduler() {
        throw new BrokerStoppedException(this.message);
    }

    public ThreadPoolExecutor getExecutor() {
        throw new BrokerStoppedException(this.message);
    }

    public void networkBridgeStarted(BrokerInfo brokerInfo, boolean createdByDuplex, String remoteIp) {
        throw new BrokerStoppedException(this.message);
    }

    public void networkBridgeStopped(BrokerInfo brokerInfo) {
        throw new BrokerStoppedException(this.message);
    }
}
