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
 * Implementation of the broker where all it's methods throw an IllegalStateException.
 * 
 * @version $Revision$
 */
public class ErrorBroker implements Broker {

    private final String message;

    public ErrorBroker(String message) {
        this.message=message;
    }
    
    public BrokerId getBrokerId() {
        throw new IllegalStateException(this.message);
    }

    public String getBrokerName() {
        throw new IllegalStateException(this.message);
    }
	
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void addSession(ConnectionContext context, SessionInfo info) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public Connection[] getClients() throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public ActiveMQDestination[] getDestinations() throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void addConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void send(ConnectionContext context, Message message) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void acknowledge(ConnectionContext context, MessageAck ack) throws Throwable {
        throw new IllegalStateException(this.message);
    }

    public void gc() {
        throw new IllegalStateException(this.message);
    }

    public void start() throws Exception {
        throw new IllegalStateException(this.message);
    }

    public void stop() throws Exception {
        throw new IllegalStateException(this.message);
    }
}
