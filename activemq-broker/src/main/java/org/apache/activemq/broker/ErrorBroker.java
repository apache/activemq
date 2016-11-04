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
import org.apache.activemq.broker.region.virtual.VirtualDestination;
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
import org.apache.activemq.store.PListStore;
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

    @Override
    public Map<ActiveMQDestination, Destination> getDestinationMap() {
        return Collections.emptyMap();
    }

    @Override
    public Map<ActiveMQDestination, Destination> getDestinationMap(ActiveMQDestination destination) {
        return Collections.emptyMap();
    }

    @Override
    public Set<Destination> getDestinations(ActiveMQDestination destination) {
        return Collections.emptySet();
    }

    @Override
    public Broker getAdaptor(Class<?> type) {
        return type.isInstance(this) ? this : null;
    }

    @Override
    public BrokerId getBrokerId() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public String getBrokerName() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public Connection[] getClients() throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public ActiveMQDestination[] getDestinations() throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination,boolean flag) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void gc() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void start() throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void stop() throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void addBroker(Connection connection, BrokerInfo info) {
        throw new BrokerStoppedException(this.message);

    }

    @Override
    public void removeBroker(Connection connection, BrokerInfo info) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public BrokerInfo[] getPeerBrokerInfos() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public boolean isStopped() {
        return true;
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public boolean isFaultTolerantConfiguration() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public ConnectionContext getAdminConnectionContext() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public PListStore getTempDataStore() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public URI getVmConnectorURI() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void brokerServiceStarted() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public BrokerService getBrokerService() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public boolean isExpired(MessageReference messageReference) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference,
                                         Subscription subscription, Throwable poisonCause) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public Broker getRoot() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public long getBrokerSequenceId() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void fastProducer(ConnectionContext context,ProducerInfo producerInfo,ActiveMQDestination destination) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void isFull(ConnectionContext context,Destination destination, Usage<?> usage) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void messageConsumed(ConnectionContext context,MessageReference messageReference) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void messageDelivered(ConnectionContext context,MessageReference messageReference) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void slowConsumer(ConnectionContext context, Destination destination,Subscription subs) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void virtualDestinationAdded(ConnectionContext context,
            VirtualDestination virtualDestination) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void virtualDestinationRemoved(ConnectionContext context,
            VirtualDestination virtualDestination) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void nowMasterBroker() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void processConsumerControl(ConsumerBrokerExchange consumerExchange,
            ConsumerControl control) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void reapplyInterceptor() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public Scheduler getScheduler() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public ThreadPoolExecutor getExecutor() {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void networkBridgeStarted(BrokerInfo brokerInfo, boolean createdByDuplex, String remoteIp) {
        throw new BrokerStoppedException(this.message);
    }

    @Override
    public void networkBridgeStopped(BrokerInfo brokerInfo) {
        throw new BrokerStoppedException(this.message);
    }
}
