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
import java.util.concurrent.atomic.AtomicReference;
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
 * Like a BrokerFilter but it allows you to switch the getNext().broker. This
 * has more overhead than a BrokerFilter since access to the getNext().broker
 * has to synchronized since it is mutable
 * 
 * 
 */
public class MutableBrokerFilter implements Broker {

    protected AtomicReference<Broker> next = new AtomicReference<Broker>();

    public MutableBrokerFilter(Broker next) {
        this.next.set(next);
    }

    public Broker getAdaptor(Class type) {
        if (type.isInstance(this)) {
            return this;
        }
        return next.get().getAdaptor(type);
    }

    public Broker getNext() {
        return next.get();
    }

    public void setNext(Broker next) {
    	this.next.set(next);
    }

    public Map<ActiveMQDestination, Destination> getDestinationMap() {
        return getNext().getDestinationMap();
    }

    public Set getDestinations(ActiveMQDestination destination) {
        return getNext().getDestinations(destination);
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        getNext().acknowledge(consumerExchange, ack);
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        getNext().addConnection(context, info);
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        return getNext().addConsumer(context, info);
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        getNext().addProducer(context, info);
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        getNext().commitTransaction(context, xid, onePhase);
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        getNext().removeSubscription(context, info);
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        return getNext().getPreparedTransactions(context);
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        return getNext().prepareTransaction(context, xid);
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        getNext().removeConnection(context, info, error);
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        getNext().removeConsumer(context, info);
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        getNext().removeProducer(context, info);
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        getNext().rollbackTransaction(context, xid);
    }

    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        getNext().send(producerExchange, messageSend);
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        getNext().beginTransaction(context, xid);
    }

    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        getNext().forgetTransaction(context, transactionId);
    }

    public Connection[] getClients() throws Exception {
        return getNext().getClients();
    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination,boolean createIfTemporary) throws Exception {
        return getNext().addDestination(context, destination,createIfTemporary);
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        getNext().removeDestination(context, destination, timeout);
    }

    public ActiveMQDestination[] getDestinations() throws Exception {
        return getNext().getDestinations();
    }

    public void start() throws Exception {
        getNext().start();
    }

    public void stop() throws Exception {
        getNext().stop();
    }

    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
        getNext().addSession(context, info);
    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
        getNext().removeSession(context, info);
    }

    public BrokerId getBrokerId() {
        return getNext().getBrokerId();
    }

    public String getBrokerName() {
        return getNext().getBrokerName();
    }

    public void gc() {
        getNext().gc();
    }

    public void addBroker(Connection connection, BrokerInfo info) {
        getNext().addBroker(connection, info);
    }

    public void removeBroker(Connection connection, BrokerInfo info) {
        getNext().removeBroker(connection, info);
    }

    public BrokerInfo[] getPeerBrokerInfos() {
        return getNext().getPeerBrokerInfos();
    }

    public void preProcessDispatch(MessageDispatch messageDispatch) {
        getNext().preProcessDispatch(messageDispatch);
    }

    public void postProcessDispatch(MessageDispatch messageDispatch) {
        getNext().postProcessDispatch(messageDispatch);
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        getNext().processDispatchNotification(messageDispatchNotification);
    }

    public boolean isStopped() {
        return getNext().isStopped();
    }

    public Set<ActiveMQDestination> getDurableDestinations() {
        return getNext().getDurableDestinations();
    }

    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        getNext().addDestinationInfo(context, info);

    }

    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        getNext().removeDestinationInfo(context, info);

    }

    public boolean isFaultTolerantConfiguration() {
        return getNext().isFaultTolerantConfiguration();
    }

    public ConnectionContext getAdminConnectionContext() {
        return getNext().getAdminConnectionContext();
    }

    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
        getNext().setAdminConnectionContext(adminConnectionContext);
    }

    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        return getNext().messagePull(context, pull);
    }

    public PListStore getTempDataStore() {
        return getNext().getTempDataStore();
    }

    public URI getVmConnectorURI() {
        return getNext().getVmConnectorURI();
    }

    public void brokerServiceStarted() {
        getNext().brokerServiceStarted();
    }

    public BrokerService getBrokerService() {
        return getNext().getBrokerService();
    }

    public boolean isExpired(MessageReference messageReference) {
        return getNext().isExpired(messageReference);
    }

    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
        getNext().messageExpired(context, message, subscription);
    }

    public void sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference,
                                      Subscription subscription) {
        getNext().sendToDeadLetterQueue(context, messageReference, subscription);
    }

    public Broker getRoot() {
        return getNext().getRoot();
    }
    
    public long getBrokerSequenceId() {
        return getNext().getBrokerSequenceId();
    }
    
    public void fastProducer(ConnectionContext context,ProducerInfo producerInfo) {
        getNext().fastProducer(context, producerInfo);
    }

    public void isFull(ConnectionContext context,Destination destination, Usage usage) {
        getNext().isFull(context,destination, usage);
    }

    public void messageConsumed(ConnectionContext context,MessageReference messageReference) {
        getNext().messageConsumed(context, messageReference);
    }

    public void messageDelivered(ConnectionContext context,MessageReference messageReference) {
        getNext().messageDelivered(context, messageReference);
    }

    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
        getNext().messageDiscarded(context, sub, messageReference);
    }

    public void slowConsumer(ConnectionContext context, Destination dest, Subscription subs) {
        getNext().slowConsumer(context, dest,subs);
    }
    
    public void nowMasterBroker() {   
       getNext().nowMasterBroker();
    }

    public void processConsumerControl(ConsumerBrokerExchange consumerExchange,
            ConsumerControl control) {
        getNext().processConsumerControl(consumerExchange, control);
    }

    public Scheduler getScheduler() {
       return getNext().getScheduler();
    }

    public ThreadPoolExecutor getExecutor() {
       return getNext().getExecutor();
    }

    public void networkBridgeStarted(BrokerInfo brokerInfo, boolean createdByDuplex) {
        getNext().networkBridgeStarted(brokerInfo, createdByDuplex);
    }

    public void networkBridgeStopped(BrokerInfo brokerInfo) {
        getNext().networkBridgeStopped(brokerInfo);
    }
}
