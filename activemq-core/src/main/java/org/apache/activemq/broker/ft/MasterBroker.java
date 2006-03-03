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
package org.apache.activemq.broker.ft;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.InsertableMutableBrokerFilter;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.transport.Transport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Message Broker which passes messages to a slave
 * 
 * @version $Revision: 1.8 $
 */
public class MasterBroker extends InsertableMutableBrokerFilter{
    private static final Log log=LogFactory.getLog(MasterBroker.class);
    private Transport slave;
    private AtomicBoolean started=new AtomicBoolean(false);

    public MasterBroker(MutableBrokerFilter parent,Transport slave){
        super(parent);
        this.slave=slave;
    }

    public void startProcessing(){
        started.set(true);
    }

    public void stop() throws Exception{
        super.stop();
        stopProcessing();
    }
    public void stopProcessing(){
        if (started.compareAndSet(true,false)){
            remove();
        }
    }

    
    
    /**
     * A client is establishing a connection with the broker.
     * @param context
     * @param info 
     * @param client
     */
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception{
        super.addConnection(context,info);
        sendAsyncToSlave(info);
    }
    
    /**
     * A client is disconnecting from the broker.
     * @param context the environment the operation is being executed under.
     * @param info 
     * @param client
     * @param error null if the client requested the disconnect or the error that caused the client to disconnect.
     */
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception{
        super.removeConnection(context,info,error);
        sendAsyncToSlave(new RemoveInfo(info.getConnectionId()));
    }

    /**
     * Adds a session.
     * @param context
     * @param info
     */
    public void addSession(ConnectionContext context, SessionInfo info) throws Exception{
        super.addSession(context, info);
        sendAsyncToSlave(info);
    }

    /**
     * Removes a session.
     * @param context
     * @param info
     */
    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception{
        super.removeSession(context, info);
        sendAsyncToSlave(new RemoveInfo(info.getSessionId()));
    }

    /**
     * Adds a producer.
     * @param context the enviorment the operation is being executed under.
     */
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception{
        super.addProducer(context,info);
        sendAsyncToSlave(info);
    }

    /**
     * Removes a producer.
     * @param context the enviorment the operation is being executed under.
     */
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception{
        super.removeProducer(context, info);
        sendAsyncToSlave(new RemoveInfo(info.getProducerId()));
    }
    
    public void addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        super.addConsumer(context, info);
        sendAsyncToSlave(info);
    }

    
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        super.removeSubscription(context, info);
        sendAsyncToSlave(info);
    }
      
    

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception{
        super.beginTransaction(context, xid);
        TransactionInfo info = new TransactionInfo(context.getConnectionId(),xid,TransactionInfo.BEGIN);
        sendAsyncToSlave(info);
    }

    /**
     * Prepares a transaction. Only valid for xa transactions.
     * @param client
     * @param xid
     * @return
     */
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception{
        int result = super.prepareTransaction(context, xid);
        TransactionInfo info = new TransactionInfo(context.getConnectionId(),xid,TransactionInfo.PREPARE);
        sendAsyncToSlave(info);
        return result;
    }

    /**
     * Rollsback a transaction.
     * @param client
     * @param xid
     */

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception{
        super.rollbackTransaction(context, xid);
        TransactionInfo info = new TransactionInfo(context.getConnectionId(),xid,TransactionInfo.ROLLBACK);
        sendAsyncToSlave(info);
        
    }

    /**
     * Commits a transaction.
     * @param client
     * @param xid
     * @param onePhase
     */
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception{
        super.commitTransaction(context, xid,onePhase);
        TransactionInfo info = new TransactionInfo(context.getConnectionId(),xid,TransactionInfo.COMMIT_ONE_PHASE);
        sendSyncToSlave(info);
    }

    /**
     * Forgets a transaction.
     * @param client
     * @param xid
     * @param onePhase
     */
    public void forgetTransaction(ConnectionContext context, TransactionId xid) throws Exception{
        super.forgetTransaction(context, xid);
        TransactionInfo info = new TransactionInfo(context.getConnectionId(),xid,TransactionInfo.FORGET);
        sendAsyncToSlave(info);
    }
    
    /**
     * Notifiy the Broker that a dispatch has happened
     * @param messageDispatch
     */
    public void processDispatch(MessageDispatch messageDispatch){
        super.processDispatch(messageDispatch);
        MessageDispatchNotification mdn = new MessageDispatchNotification();
        mdn.setConsumerId(messageDispatch.getConsumerId());
        mdn.setDeliverySequenceId(messageDispatch.getDeliverySequenceId());
        mdn.setDestination(messageDispatch.getDestination());
        mdn.setMessageId(messageDispatch.getMessage().getMessageId());
        sendAsyncToSlave(mdn);
    }
    
    public void send(ConnectionContext context, Message message) throws Exception{
        /**
         * A message can be dispatched before the super.send() method returns
         * so - here the order is switched to avoid problems on the slave
         * with receiving acks for messages not received yey
         */
        sendToSlave(message);
        super.send(context,message);
    }
    
   
    public void acknowledge(ConnectionContext context, MessageAck ack) throws Exception{
        super.acknowledge(context, ack);
        sendToSlave(ack);
    }
    

    protected void sendToSlave(Message message){
        
        if (message.isPersistent() && !message.isInTransaction()){
            sendSyncToSlave(message);
        }else{
            sendAsyncToSlave(message);
        }
        
        
    }
    
    protected void sendToSlave(MessageAck ack){
       
        if (ack.isInTransaction()){
            sendAsyncToSlave(ack);
        }else{
            sendSyncToSlave(ack);
        }
    }

    protected void sendAsyncToSlave(Command command){
        try{

            slave.oneway(command);

        }catch(Throwable e){
            log.error("Slave Failed",e);
            stopProcessing();
        }
    }

    protected void sendSyncToSlave(Command command){
        try{

            Response response=slave.request(command);
            if (response.isException()){
                ExceptionResponse er=(ExceptionResponse)response;
                log.error("Slave Failed",er.getException());
            }

        }catch(Throwable e){
            log.error("Slave Failed",e);
           
        }
    }

}
