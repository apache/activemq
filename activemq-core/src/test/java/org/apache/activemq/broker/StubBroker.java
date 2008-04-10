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
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
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

public class StubBroker implements Broker {
    public LinkedList<AddConnectionData> addConnectionData = new LinkedList<AddConnectionData>();
    public LinkedList<RemoveConnectionData> removeConnectionData = new LinkedList<RemoveConnectionData>();

    public class AddConnectionData {
        public final ConnectionContext connectionContext;
        public final ConnectionInfo connectionInfo;

        public AddConnectionData(ConnectionContext context, ConnectionInfo info) {
            connectionContext = context;
            connectionInfo = info;
        }
    }

    public static class RemoveConnectionData {
        public final ConnectionContext connectionContext;
        public final ConnectionInfo connectionInfo;
        public final Throwable error;

        public RemoveConnectionData(ConnectionContext context, ConnectionInfo info, Throwable error) {
            connectionContext = context;
            connectionInfo = info;
            this.error = error;
        }
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        addConnectionData.add(new AddConnectionData(context, info));
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        removeConnectionData.add(new RemoveConnectionData(context, info, error));
    }

    // --- Blank Methods, fill in as needed ---
    public void addBroker(Connection connection, BrokerInfo info) {
    }

    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
    }

    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
    }

    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
    }

    public Broker getAdaptor(Class type) {
        return null;
    }

    public ConnectionContext getAdminConnectionContext() {
        return null;
    }

    public BrokerId getBrokerId() {
        return null;
    }

    public String getBrokerName() {
        return null;
    }

    public Connection[] getClients() throws Exception {
        return null;
    }

    public ActiveMQDestination[] getDestinations() throws Exception {
        return null;
    }

    public Set<ActiveMQDestination> getDurableDestinations() {
        return null;
    }

    public BrokerInfo[] getPeerBrokerInfos() {
        return null;
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        return null;
    }

    public boolean isFaultTolerantConfiguration() {
        return false;
    }

    public boolean isSlaveBroker() {
        return false;
    }

    public boolean isStopped() {
        return false;
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        return 0;
    }

    public void preProcessDispatch(MessageDispatch messageDispatch) {
    }

    public void postProcessDispatch(MessageDispatch messageDispatch) {
    }

    public void removeBroker(Connection connection, BrokerInfo info) {
    }

    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
    }

    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        return null;
    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        return null;
    }

    public void gc() {
    }

    public Map<ActiveMQDestination, Destination> getDestinationMap() {
        return null;
    }

    public Set getDestinations(ActiveMQDestination destination) {
        return null;
    }

    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        return null;
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
    }

    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public PendingDurableSubscriberMessageStoragePolicy getPendingDurableSubscriberPolicy() {
        return null;
    }

    public void setPendingDurableSubscriberPolicy(PendingDurableSubscriberMessageStoragePolicy pendingDurableSubscriberPolicy) {
    }

    public Store getTempDataStore() {
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

    public void messageExpired(ConnectionContext context, MessageReference messageReference) {
    }

    public void sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference) {
    }

    public Broker getRoot() {
        return this;
    }
    
    public long getBrokerSequenceId() {
        return -1l;
    }
}
