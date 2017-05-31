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
 * Dumb implementation - used to be overriden by listeners
 *
 *
 */
public class EmptyBroker implements Broker {

    @Override
    public BrokerId getBrokerId() {
        return null;
    }

    @Override
    public String getBrokerName() {
        return null;
    }

    @Override
    public Broker getAdaptor(Class<?> type) {
        return type.isInstance(this) ? this : null;
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
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
    }

    @Override
    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
    }

    @Override
    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
    }

    @Override
    public Connection[] getClients() throws Exception {
        return null;
    }

    @Override
    public ActiveMQDestination[] getDestinations() throws Exception {
        return null;
    }

    @Override
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        return null;
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        return 0;
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination,boolean flag) throws Exception {
        return null;
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        return null;
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
    }

    @Override
    public void gc() {
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public void addBroker(Connection connection, BrokerInfo info) {
    }

    @Override
    public void removeBroker(Connection connection, BrokerInfo info) {
    }

    @Override
    public BrokerInfo[] getPeerBrokerInfos() {
        return null;
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
    }

    @Override
    public boolean isStopped() {
        return false;
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        return null;
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
    }

    @Override
    public boolean isFaultTolerantConfiguration() {
        return false;
    }

    @Override
    public ConnectionContext getAdminConnectionContext() {
        return null;
    }

    @Override
    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        return null;
    }

    @Override
    public PListStore getTempDataStore() {
        return null;
    }

    @Override
    public URI getVmConnectorURI() {
        return null;
    }

    @Override
    public void brokerServiceStarted() {
    }

    @Override
    public BrokerService getBrokerService() {
        return null;
    }

    @Override
    public boolean isExpired(MessageReference messageReference) {
        return false;
    }

    @Override
    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference,
                                         Subscription subscription, Throwable poisonCause) {
        return false;
    }

    @Override
    public Broker getRoot() {
        return null;
    }

    @Override
    public long getBrokerSequenceId() {
        return -1l;
    }

    @Override
    public void fastProducer(ConnectionContext context,ProducerInfo producerInfo,ActiveMQDestination destination) {
    }

    @Override
    public void isFull(ConnectionContext context, Destination destination,Usage<?> usage) {
    }

    @Override
    public void messageConsumed(ConnectionContext context,MessageReference messageReference) {
    }

    @Override
    public void messageDelivered(ConnectionContext context,MessageReference messageReference) {
    }

    @Override
    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
    }

    @Override
    public void slowConsumer(ConnectionContext context,Destination destination, Subscription subs) {
    }

    @Override
    public void virtualDestinationAdded(ConnectionContext context, VirtualDestination virtualDestination) {
    }

    @Override
    public void virtualDestinationRemoved(ConnectionContext context, VirtualDestination virtualDestination) {
    }

    @Override
    public void nowMasterBroker() {
    }

    @Override
    public void networkBridgeStarted(BrokerInfo brokerInfo, boolean createdByDuplex, String remoteIp) {
    }

    @Override
    public void networkBridgeStopped(BrokerInfo brokerInfo) {
    }

    @Override
    public void processConsumerControl(ConsumerBrokerExchange consumerExchange, ConsumerControl control) {
    }

    @Override
    public void reapplyInterceptor() {
    }

    @Override
    public Scheduler getScheduler() {
        return null;
    }

    @Override
    public ThreadPoolExecutor getExecutor() {
        return null;
    }

}
