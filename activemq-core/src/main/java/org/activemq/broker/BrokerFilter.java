/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.broker;

import org.activemq.broker.region.Destination;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.BrokerId;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.ProducerInfo;
import org.activemq.command.RemoveSubscriptionInfo;
import org.activemq.command.SessionInfo;
import org.activemq.command.TransactionId;

/**
 * Allows you to intercept broker operation so that features such as security can be 
 * implemented as a pluggable filter.
 * 
 * @version $Revision: 1.10 $
 */
public class BrokerFilter implements Broker {
    
    final protected Broker next;

    public BrokerFilter(Broker next) {
        this.next=next;
    }
    
    public void acknowledge(ConnectionContext context, MessageAck ack) throws Throwable {
        next.acknowledge(context, ack);
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Throwable {
        next.addConnection(context, info);
    }

    public void addConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        next.addConsumer(context, info);
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
        next.addProducer(context, info);
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Throwable {
        next.commitTransaction(context, xid, onePhase);
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Throwable {
        next.removeSubscription(context, info);
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Throwable {
        return next.getPreparedTransactions(context);
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        return next.prepareTransaction(context, xid);
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Throwable {
        next.removeConnection(context, info, error);
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        next.removeConsumer(context, info);
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
        next.removeProducer(context, info);
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        next.rollbackTransaction(context, xid);
    }

    public void send(ConnectionContext context, Message messageSend) throws Throwable {
        next.send(context, messageSend);
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        next.beginTransaction(context, xid);
    }

    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Throwable {
        next.forgetTransaction(context, transactionId);
    }

    public Connection[] getClients() throws Throwable {
        return next.getClients();
    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Throwable {
        return next.addDestination(context, destination);
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Throwable {
        next.removeDestination(context, destination, timeout);
    }

    public ActiveMQDestination[] getDestinations() throws Throwable {
        return next.getDestinations();
    }

    public void start() throws Exception {
        next.start();
    }

    public void stop() throws Exception {
        next.stop();
    }

    public void addSession(ConnectionContext context, SessionInfo info) throws Throwable {
        next.addSession(context, info);
    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Throwable {
        next.removeSession(context, info);
    }

    public BrokerId getBrokerId() {
        return next.getBrokerId();
    }

    public String getBrokerName() {
        return next.getBrokerName();
    }
	
    public void gc() {
        next.gc();
    }

}
