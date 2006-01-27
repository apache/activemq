/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.broker;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;

/**
 * Like a BrokerFilter but it allows you to switch the getNext().broker.  This has more 
 * overhead than a BrokerFilter since access to the getNext().broker has to synchronized
 * since it is mutable
 * 
 * @version $Revision: 1.10 $
 */
public class MutableBrokerFilter implements Broker {
    
    private Broker next;
    private final Object mutext = new Object();

    public MutableBrokerFilter(Broker next) {
        this.next = next;
    }
    
    public Broker getAdaptor(Class type){
        if (type.isInstance(this)){
            return this;
        }
        return next.getAdaptor(type);
    }
    
    public Broker getNext() {
        synchronized(mutext) {
            return next;
        }
    }
    
    public void setNext(Broker next) {
        synchronized(mutext) {
            this.next=next;
        }
    }
    
    public void acknowledge(ConnectionContext context, MessageAck ack) throws Throwable {
        getNext().acknowledge(context, ack);
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Throwable {
        getNext().addConnection(context, info);
    }

    public void addConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        getNext().addConsumer(context, info);
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
        getNext().addProducer(context, info);
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Throwable {
        getNext().commitTransaction(context, xid, onePhase);
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Throwable {
        getNext().removeSubscription(context, info);
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Throwable {
        return getNext().getPreparedTransactions(context);
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        return getNext().prepareTransaction(context, xid);
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Throwable {
        getNext().removeConnection(context, info, error);
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        getNext().removeConsumer(context, info);
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
        getNext().removeProducer(context, info);
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        getNext().rollbackTransaction(context, xid);
    }

    public void send(ConnectionContext context, Message messageSend) throws Throwable {
        getNext().send(context, messageSend);
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        getNext().beginTransaction(context, xid);
    }

    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Throwable {
        getNext().forgetTransaction(context, transactionId);
    }

    public Connection[] getClients() throws Throwable {
        return getNext().getClients();
    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Throwable {
        return getNext().addDestination(context, destination);
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Throwable {
        getNext().removeDestination(context, destination, timeout);
    }

    public ActiveMQDestination[] getDestinations() throws Throwable {
        return getNext().getDestinations();
    }

    public void start() throws Exception {
        getNext().start();
    }

    public void stop() throws Exception {
        getNext().stop();
    }

    public void addSession(ConnectionContext context, SessionInfo info) throws Throwable {
        getNext().addSession(context, info);
    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Throwable {
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

    public void addBroker(Connection connection,BrokerInfo info){
        getNext().addBroker(connection, info);      
    }
    
    public void removeBroker(Connection connection,BrokerInfo info){
        getNext().removeBroker(connection, info);
    }

    public BrokerInfo[] getPeerBrokerInfos(){
       return getNext().getPeerBrokerInfos();
    }
    
    public void processDispatch(MessageDispatch messageDispatch){
        getNext().processDispatch(messageDispatch);
    }
    
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Throwable{
        getNext().processDispatchNotification(messageDispatchNotification);
    }
    
    public boolean isSlaveBroker(){
        return getNext().isSlaveBroker();
    }
    
    public boolean isStopped(){
        return getNext().isStopped();
    }

}
