/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.net.URI;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
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

/**
 * Allows you to intercept broker operation so that features such as security can be 
 * implemented as a pluggable filter.
 * 
 * @version $Revision: 1.10 $
 */
public class BrokerFilter implements Broker {
    
    final protected Broker next;

    public BrokerFilter(Broker next){
        this.next=next;
    }

    public Broker getAdaptor(Class type){
        if(type.isInstance(this)){
            return this;
        }
        return next.getAdaptor(type);
    }

    public Map getDestinationMap(){
        return next.getDestinationMap();
    }

    public Set getDestinations(ActiveMQDestination destination){
        return next.getDestinations(destination);
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange,MessageAck ack) throws Exception{
        next.acknowledge(consumerExchange,ack);
    }

    public Response messagePull(ConnectionContext context,MessagePull pull) throws Exception{
        return next.messagePull(context,pull);
    }

    public void addConnection(ConnectionContext context,ConnectionInfo info) throws Exception{
        next.addConnection(context,info);
    }

    public Subscription addConsumer(ConnectionContext context,ConsumerInfo info) throws Exception{
        return next.addConsumer(context,info);
    }

    public void addProducer(ConnectionContext context,ProducerInfo info) throws Exception{
        next.addProducer(context,info);
    }

    public void commitTransaction(ConnectionContext context,TransactionId xid,boolean onePhase) throws Exception{
        next.commitTransaction(context,xid,onePhase);
    }

    public void removeSubscription(ConnectionContext context,RemoveSubscriptionInfo info) throws Exception{
        next.removeSubscription(context,info);
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception{
        return next.getPreparedTransactions(context);
    }

    public int prepareTransaction(ConnectionContext context,TransactionId xid) throws Exception{
        return next.prepareTransaction(context,xid);
    }

    public void removeConnection(ConnectionContext context,ConnectionInfo info,Throwable error) throws Exception{
        next.removeConnection(context,info,error);
    }

    public void removeConsumer(ConnectionContext context,ConsumerInfo info) throws Exception{
        next.removeConsumer(context,info);
    }

    public void removeProducer(ConnectionContext context,ProducerInfo info) throws Exception{
        next.removeProducer(context,info);
    }

    public void rollbackTransaction(ConnectionContext context,TransactionId xid) throws Exception{
        next.rollbackTransaction(context,xid);
    }

    public void send(ProducerBrokerExchange producerExchange,Message messageSend) throws Exception{
        next.send(producerExchange,messageSend);
    }

    public void beginTransaction(ConnectionContext context,TransactionId xid) throws Exception{
        next.beginTransaction(context,xid);
    }

    public void forgetTransaction(ConnectionContext context,TransactionId transactionId) throws Exception{
        next.forgetTransaction(context,transactionId);
    }

    public Connection[] getClients() throws Exception{
        return next.getClients();
    }

    public Destination addDestination(ConnectionContext context,ActiveMQDestination destination) throws Exception{
        return next.addDestination(context,destination);
    }

    public void removeDestination(ConnectionContext context,ActiveMQDestination destination,long timeout)
            throws Exception{
        next.removeDestination(context,destination,timeout);
    }

    public ActiveMQDestination[] getDestinations() throws Exception{
        return next.getDestinations();
    }

    public void start() throws Exception{
        next.start();
    }

    public void stop() throws Exception{
        next.stop();
    }

    public void addSession(ConnectionContext context,SessionInfo info) throws Exception{
        next.addSession(context,info);
    }

    public void removeSession(ConnectionContext context,SessionInfo info) throws Exception{
        next.removeSession(context,info);
    }

    public BrokerId getBrokerId(){
        return next.getBrokerId();
    }

    public String getBrokerName(){
        return next.getBrokerName();
    }

    public void gc(){
        next.gc();
    }

    public void addBroker(Connection connection,BrokerInfo info){
        next.addBroker(connection,info);
    }

    public void removeBroker(Connection connection,BrokerInfo info){
        next.removeBroker(connection,info);
    }

    public BrokerInfo[] getPeerBrokerInfos(){
        return next.getPeerBrokerInfos();
    }

    public void preProcessDispatch(MessageDispatch messageDispatch){
        next.preProcessDispatch(messageDispatch);
    }

    public void postProcessDispatch(MessageDispatch messageDispatch){
        next.postProcessDispatch(messageDispatch);
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception{
        next.processDispatchNotification(messageDispatchNotification);
    }

    public boolean isStopped(){
        return next.isStopped();
    }

    public Set getDurableDestinations(){
        return next.getDurableDestinations();
    }

    public void addDestinationInfo(ConnectionContext context,DestinationInfo info) throws Exception{
        next.addDestinationInfo(context,info);
    }

    public void removeDestinationInfo(ConnectionContext context,DestinationInfo info) throws Exception{
        next.removeDestinationInfo(context,info);
    }

    public boolean isFaultTolerantConfiguration(){
        return next.isFaultTolerantConfiguration();
    }

    public ConnectionContext getAdminConnectionContext(){
        return next.getAdminConnectionContext();
    }

    public void setAdminConnectionContext(ConnectionContext adminConnectionContext){
        next.setAdminConnectionContext(adminConnectionContext);
    }

    public Store getTempDataStore(){
        return next.getTempDataStore();
    }

    public URI getVmConnectorURI(){
        return next.getVmConnectorURI();
    }

    public void brokerServiceStarted(){
        next.brokerServiceStarted();
    }

    public BrokerService getBrokerService(){
        return next.getBrokerService();
    }

    public boolean isExpired(MessageReference messageReference){
        return next.isExpired(messageReference);
    }

    public void messageExpired(ConnectionContext context,MessageReference message){
        next.messageExpired(context,message);
    }

    public void sendToDeadLetterQueue(ConnectionContext context,MessageReference messageReference){
        next.sendToDeadLetterQueue(context,messageReference);
    }

    public Broker getRoot(){
        return next.getRoot();
    }
}
