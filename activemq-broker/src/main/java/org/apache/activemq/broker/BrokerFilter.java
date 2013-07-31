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

    @Override
    public Broker getAdaptor(Class type) {
        if (type.isInstance(this)) {
            return this;
        }
        return next.getAdaptor(type);
    }

    @Override
    public Map<ActiveMQDestination, Destination> getDestinationMap() {
        return next.getDestinationMap();
    }

    @Override
    public Set <Destination>getDestinations(ActiveMQDestination destination) {
        return next.getDestinations(destination);
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        next.acknowledge(consumerExchange, ack);
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        return next.messagePull(context, pull);
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        next.addConnection(context, info);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        return next.addConsumer(context, info);
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.addProducer(context, info);
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        next.commitTransaction(context, xid, onePhase);
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        next.removeSubscription(context, info);
    }

    @Override
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        return next.getPreparedTransactions(context);
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        return next.prepareTransaction(context, xid);
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        next.removeConnection(context, info, error);
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        next.removeConsumer(context, info);
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.removeProducer(context, info);
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        next.rollbackTransaction(context, xid);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        next.send(producerExchange, messageSend);
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        next.beginTransaction(context, xid);
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        next.forgetTransaction(context, transactionId);
    }

    @Override
    public Connection[] getClients() throws Exception {
        return next.getClients();
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination,boolean createIfTemporary) throws Exception {
        return next.addDestination(context, destination,createIfTemporary);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        next.removeDestination(context, destination, timeout);
    }

    @Override
    public ActiveMQDestination[] getDestinations() throws Exception {
        return next.getDestinations();
    }

    @Override
    public void start() throws Exception {
        next.start();
    }

    @Override
    public void stop() throws Exception {
        next.stop();
    }

    @Override
    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
        next.addSession(context, info);
    }

    @Override
    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
        next.removeSession(context, info);
    }

    @Override
    public BrokerId getBrokerId() {
        return next.getBrokerId();
    }

    @Override
    public String getBrokerName() {
        return next.getBrokerName();
    }

    @Override
    public void gc() {
        next.gc();
    }

    @Override
    public void addBroker(Connection connection, BrokerInfo info) {
        next.addBroker(connection, info);
    }

    @Override
    public void removeBroker(Connection connection, BrokerInfo info) {
        next.removeBroker(connection, info);
    }

    @Override
    public BrokerInfo[] getPeerBrokerInfos() {
        return next.getPeerBrokerInfos();
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        next.preProcessDispatch(messageDispatch);
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
        next.postProcessDispatch(messageDispatch);
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        next.processDispatchNotification(messageDispatchNotification);
    }

    @Override
    public boolean isStopped() {
        return next.isStopped();
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        return next.getDurableDestinations();
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        next.addDestinationInfo(context, info);
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        next.removeDestinationInfo(context, info);
    }

    @Override
    public boolean isFaultTolerantConfiguration() {
        return next.isFaultTolerantConfiguration();
    }

    @Override
    public ConnectionContext getAdminConnectionContext() {
        return next.getAdminConnectionContext();
    }

    @Override
    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
        next.setAdminConnectionContext(adminConnectionContext);
    }

    @Override
    public PListStore getTempDataStore() {
        return next.getTempDataStore();
    }

    @Override
    public URI getVmConnectorURI() {
        return next.getVmConnectorURI();
    }

    @Override
    public void brokerServiceStarted() {
        next.brokerServiceStarted();
    }

    @Override
    public BrokerService getBrokerService() {
        return next.getBrokerService();
    }

    @Override
    public boolean isExpired(MessageReference messageReference) {
        return next.isExpired(messageReference);
    }

    @Override
    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
        next.messageExpired(context, message, subscription);
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference,
                                         Subscription subscription, Throwable poisonCause) {
        return next.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
    }

    @Override
    public Broker getRoot() {
        return next.getRoot();
    }

    @Override
    public long getBrokerSequenceId() {
        return next.getBrokerSequenceId();
    }


    @Override
    public void fastProducer(ConnectionContext context,ProducerInfo producerInfo,ActiveMQDestination destination) {
        next.fastProducer(context, producerInfo, destination);
    }

    @Override
    public void isFull(ConnectionContext context,Destination destination, Usage usage) {
        next.isFull(context,destination, usage);
    }

    @Override
    public void messageConsumed(ConnectionContext context,MessageReference messageReference) {
        next.messageConsumed(context, messageReference);
    }

    @Override
    public void messageDelivered(ConnectionContext context,MessageReference messageReference) {
        next.messageDelivered(context, messageReference);
    }

    @Override
    public void messageDiscarded(ConnectionContext context,Subscription sub, MessageReference messageReference) {
        next.messageDiscarded(context, sub, messageReference);
    }

    @Override
    public void slowConsumer(ConnectionContext context, Destination destination,Subscription subs) {
        next.slowConsumer(context, destination,subs);
    }

    @Override
    public void nowMasterBroker() {
        next.nowMasterBroker();
    }

    @Override
    public void processConsumerControl(ConsumerBrokerExchange consumerExchange,
            ConsumerControl control) {
        next.processConsumerControl(consumerExchange, control);
    }

    @Override
    public Scheduler getScheduler() {
       return next.getScheduler();
    }

    @Override
    public ThreadPoolExecutor getExecutor() {
       return next.getExecutor();
    }

    @Override
    public void networkBridgeStarted(BrokerInfo brokerInfo, boolean createdByDuplex, String remoteIp) {
        next.networkBridgeStarted(brokerInfo, createdByDuplex, remoteIp);
    }

    @Override
    public void networkBridgeStopped(BrokerInfo brokerInfo) {
        next.networkBridgeStopped(brokerInfo);
    }
}
