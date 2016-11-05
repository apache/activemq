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
 * Allows you to intercept broker operation so that features such as security
 * can be implemented as a pluggable filter.
 *
 *
 */
public class BrokerFilter implements Broker {

    protected final Broker next;

    public BrokerFilter(Broker next) {
        this.next = next;
    }

    public Broker getNext() {
        return next;
    }

    @Override
    public Broker getAdaptor(Class<?> type) {
        return type.isInstance(this) ? this : getNext().getAdaptor(type);
    }

    @Override
    public Map<ActiveMQDestination, Destination> getDestinationMap() {
        return getNext().getDestinationMap();
    }

    @Override
    public Map<ActiveMQDestination, Destination> getDestinationMap(ActiveMQDestination destination) {
        return getNext().getDestinationMap(destination);
    }

    @Override
    public Set<Destination> getDestinations(ActiveMQDestination destination) {
        return getNext().getDestinations(destination);
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        getNext().acknowledge(consumerExchange, ack);
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        return getNext().messagePull(context, pull);
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        getNext().addConnection(context, info);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        return getNext().addConsumer(context, info);
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        getNext().addProducer(context, info);
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        getNext().commitTransaction(context, xid, onePhase);
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        getNext().removeSubscription(context, info);
    }

    @Override
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        return getNext().getPreparedTransactions(context);
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        return getNext().prepareTransaction(context, xid);
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        getNext().removeConnection(context, info, error);
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        getNext().removeConsumer(context, info);
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        getNext().removeProducer(context, info);
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        getNext().rollbackTransaction(context, xid);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        getNext().send(producerExchange, messageSend);
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        getNext().beginTransaction(context, xid);
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        getNext().forgetTransaction(context, transactionId);
    }

    @Override
    public Connection[] getClients() throws Exception {
        return getNext().getClients();
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination,boolean createIfTemporary) throws Exception {
        return getNext().addDestination(context, destination,createIfTemporary);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        getNext().removeDestination(context, destination, timeout);
    }

    @Override
    public ActiveMQDestination[] getDestinations() throws Exception {
        return getNext().getDestinations();
    }

    @Override
    public void start() throws Exception {
        getNext().start();
    }

    @Override
    public void stop() throws Exception {
        getNext().stop();
    }

    @Override
    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
        getNext().addSession(context, info);
    }

    @Override
    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
        getNext().removeSession(context, info);
    }

    @Override
    public BrokerId getBrokerId() {
        return getNext().getBrokerId();
    }

    @Override
    public String getBrokerName() {
        return getNext().getBrokerName();
    }

    @Override
    public void gc() {
        getNext().gc();
    }

    @Override
    public void addBroker(Connection connection, BrokerInfo info) {
        getNext().addBroker(connection, info);
    }

    @Override
    public void removeBroker(Connection connection, BrokerInfo info) {
        getNext().removeBroker(connection, info);
    }

    @Override
    public BrokerInfo[] getPeerBrokerInfos() {
        return getNext().getPeerBrokerInfos();
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        getNext().preProcessDispatch(messageDispatch);
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
        getNext().postProcessDispatch(messageDispatch);
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        getNext().processDispatchNotification(messageDispatchNotification);
    }

    @Override
    public boolean isStopped() {
        return getNext().isStopped();
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        return getNext().getDurableDestinations();
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        getNext().addDestinationInfo(context, info);
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        getNext().removeDestinationInfo(context, info);
    }

    @Override
    public boolean isFaultTolerantConfiguration() {
        return getNext().isFaultTolerantConfiguration();
    }

    @Override
    public ConnectionContext getAdminConnectionContext() {
        return getNext().getAdminConnectionContext();
    }

    @Override
    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
        getNext().setAdminConnectionContext(adminConnectionContext);
    }

    @Override
    public PListStore getTempDataStore() {
        return getNext().getTempDataStore();
    }

    @Override
    public URI getVmConnectorURI() {
        return getNext().getVmConnectorURI();
    }

    @Override
    public void brokerServiceStarted() {
        getNext().brokerServiceStarted();
    }

    @Override
    public BrokerService getBrokerService() {
        return getNext().getBrokerService();
    }

    @Override
    public boolean isExpired(MessageReference messageReference) {
        return getNext().isExpired(messageReference);
    }

    @Override
    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
        getNext().messageExpired(context, message, subscription);
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference,
                                         Subscription subscription, Throwable poisonCause) {
        return getNext().sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
    }

    @Override
    public Broker getRoot() {
        return getNext().getRoot();
    }

    @Override
    public long getBrokerSequenceId() {
        return getNext().getBrokerSequenceId();
    }


    @Override
    public void fastProducer(ConnectionContext context,ProducerInfo producerInfo,ActiveMQDestination destination) {
        getNext().fastProducer(context, producerInfo, destination);
    }

    @Override
    public void isFull(ConnectionContext context,Destination destination, Usage<?> usage) {
        getNext().isFull(context,destination, usage);
    }

    @Override
    public void messageConsumed(ConnectionContext context,MessageReference messageReference) {
        getNext().messageConsumed(context, messageReference);
    }

    @Override
    public void messageDelivered(ConnectionContext context,MessageReference messageReference) {
        getNext().messageDelivered(context, messageReference);
    }

    @Override
    public void messageDiscarded(ConnectionContext context,Subscription sub, MessageReference messageReference) {
        getNext().messageDiscarded(context, sub, messageReference);
    }

    @Override
    public void slowConsumer(ConnectionContext context, Destination destination,Subscription subs) {
        getNext().slowConsumer(context, destination,subs);
    }

    @Override
    public void virtualDestinationAdded(ConnectionContext context,
            VirtualDestination virtualDestination) {
        getNext().virtualDestinationAdded(context, virtualDestination);
    }

    @Override
    public void virtualDestinationRemoved(ConnectionContext context,
            VirtualDestination virtualDestination) {
        getNext().virtualDestinationRemoved(context, virtualDestination);
    }

    @Override
    public void nowMasterBroker() {
        getNext().nowMasterBroker();
    }

    @Override
    public void processConsumerControl(ConsumerBrokerExchange consumerExchange,
            ConsumerControl control) {
        getNext().processConsumerControl(consumerExchange, control);
    }

    @Override
    public void reapplyInterceptor() {
        getNext().reapplyInterceptor();
    }

    @Override
    public Scheduler getScheduler() {
       return getNext().getScheduler();
    }

    @Override
    public ThreadPoolExecutor getExecutor() {
       return getNext().getExecutor();
    }

    @Override
    public void networkBridgeStarted(BrokerInfo brokerInfo, boolean createdByDuplex, String remoteIp) {
        getNext().networkBridgeStarted(brokerInfo, createdByDuplex, remoteIp);
    }

    @Override
    public void networkBridgeStopped(BrokerInfo brokerInfo) {
        getNext().networkBridgeStopped(brokerInfo);
    }
}
