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

import java.util.Set;
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
 * Dumb implementation - used to be overriden by listeners
 * 
 * @version $Revision$
 */
public class EmptyBroker implements Broker{

    public BrokerId getBrokerId(){
        return null;
    }

    public String getBrokerName(){
        return null;
    }
    
    public Broker getAdaptor(Class type){
        if (type.isInstance(this)){
            return this;
        }
        return null;
    }

    public void addConnection(ConnectionContext context,ConnectionInfo info) throws Throwable{

    }

    public void removeConnection(ConnectionContext context,ConnectionInfo info,Throwable error) throws Throwable{

    }

    public void addSession(ConnectionContext context,SessionInfo info) throws Throwable{

    }

    public void removeSession(ConnectionContext context,SessionInfo info) throws Throwable{

    }

    public void addProducer(ConnectionContext context,ProducerInfo info) throws Throwable{

    }

    public void removeProducer(ConnectionContext context,ProducerInfo info) throws Throwable{

    }

    public Connection[] getClients() throws Throwable{

        return null;
    }

    public ActiveMQDestination[] getDestinations() throws Throwable{

        return null;
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Throwable{

        return null;
    }

    public void beginTransaction(ConnectionContext context,TransactionId xid) throws Throwable{

    }

    public int prepareTransaction(ConnectionContext context,TransactionId xid) throws Throwable{

        return 0;
    }

    public void rollbackTransaction(ConnectionContext context,TransactionId xid) throws Throwable{

    }

    public void commitTransaction(ConnectionContext context,TransactionId xid,boolean onePhase) throws Throwable{

    }

    public void forgetTransaction(ConnectionContext context,TransactionId transactionId) throws Throwable{

    }

    public Destination addDestination(ConnectionContext context,ActiveMQDestination destination) throws Throwable{

        return null;
    }

    public void removeDestination(ConnectionContext context,ActiveMQDestination destination,long timeout) throws Throwable{

    }

    public void addConsumer(ConnectionContext context,ConsumerInfo info) throws Throwable{

    }

    public void removeConsumer(ConnectionContext context,ConsumerInfo info) throws Throwable{

    }

    public void removeSubscription(ConnectionContext context,RemoveSubscriptionInfo info) throws Throwable{

    }

    public void send(ConnectionContext context,Message message) throws Throwable{

    }

    public void acknowledge(ConnectionContext context,MessageAck ack) throws Throwable{

    }

    public void gc(){

    }

    public void start() throws Exception{

    }

    public void stop() throws Exception{

    }

    public void addBroker(Connection connection,BrokerInfo info){
        
    }
    
    public void removeBroker(Connection connection,BrokerInfo info){
       
    }

    public BrokerInfo[] getPeerBrokerInfos(){
        return null;
    }
    
    /**
     * Notifiy the Broker that a dispatch has happened
     * @param messageDispatch
     */
    public void processDispatch(MessageDispatch messageDispatch){
        
    }
    
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification){
        
    }
    
    public boolean isSlaveBroker(){
        return false;
    }
    
    public boolean isStopped(){
        return false;
    }
    
    public Set getDurableDestinations(){
        return null;
    }

}
