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
import org.apache.activemq.usage.Usage;

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

    @SuppressWarnings("unchecked")
    public Map<ActiveMQDestination, Destination> getDestinationMap() {
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

    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {

    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {

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

    public void preProcessDispatch(MessageDispatch messageDispatch) {
    }

    public void postProcessDispatch(MessageDispatch messageDispatch) {
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {

    }

    public boolean isStopped() {
        return false;
    }

    public Set<ActiveMQDestination> getDurableDestinations() {
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

    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        return null;
    }

    public PListStore getTempDataStore() {
        return null;
    }

    public URI getVmConnectorURI() {
        return null;
    }

    public void brokerServiceStarted() {
    }

    public BrokerService getBrokerService() {
        return null;
    }

    public boolean isExpired(MessageReference messageReference) {
        return false;
    }

    public void messageExpired(ConnectionContext context, MessageReference message) {
    }

    public void sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference) {
    }

    public Broker getRoot() {
        return null;
    }
    
    public long getBrokerSequenceId() {
        return -1l;
    }
    
    public void fastProducer(ConnectionContext context,ProducerInfo producerInfo) {
    }

    public void isFull(ConnectionContext context, Destination destination,Usage usage) {
    }

    public void messageConsumed(ConnectionContext context,MessageReference messageReference) {
    }

    public void messageDelivered(ConnectionContext context,MessageReference messageReference) {
    }

    public void messageDiscarded(ConnectionContext context,MessageReference messageReference) {
    }

    public void slowConsumer(ConnectionContext context,Destination destination, Subscription subs) {
    }

    public void nowMasterBroker() {        
    }

    public void processConsumerControl(ConsumerBrokerExchange consumerExchange,
            ConsumerControl control) {     
    }
}
