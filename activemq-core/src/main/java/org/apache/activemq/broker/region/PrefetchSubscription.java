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
package org.apache.activemq.broker.region;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.BrokerSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * A subscription that honors the pre-fetch option of the ConsumerInfo.
 * 
 * @version $Revision: 1.15 $
 */
abstract public class PrefetchSubscription extends AbstractSubscription{
    
    static private final Log log=LogFactory.getLog(PrefetchSubscription.class);
    final protected PendingMessageCursor pending;
    final protected LinkedList dispatched=new LinkedList();
    
    protected int prefetchExtension=0;
    boolean dispatching=false;
    
    long enqueueCounter;
    long dispatchCounter;
    long dequeueCounter;
    
    public PrefetchSubscription(Broker broker,ConnectionContext context,ConsumerInfo info, PendingMessageCursor cursor)
                    throws  InvalidSelectorException{
        super(broker,context,info);
        pending = cursor;
    }
    
    public PrefetchSubscription(Broker broker,ConnectionContext context,ConsumerInfo info)
    throws  InvalidSelectorException{
       this(broker,context,info,new VMPendingMessageCursor()); 
    }

    
    /**
     * Allows a message to be pulled on demand by a client
     */
    synchronized public Response pullMessage(ConnectionContext context, MessagePull pull) throws Exception {
    	// The slave should not deliver pull messages.  TODO: when the slave becomes a master,
    	// He should send a NULL message to all the consumers to 'wake them up' in case 
    	// they were waiting for a message.
        if (getPrefetchSize() == 0 && !isSlaveBroker()) {
            prefetchExtension++;
            
            final long dispatchCounterBeforePull = dispatchCounter;
            dispatchMatched();
            
        	// If there was nothing dispatched.. we may need to setup a timeout.
        	if( dispatchCounterBeforePull == dispatchCounter ) {
        		// imediate timeout used by receiveNoWait()
        		if( pull.getTimeout() == -1 ) {
        			// Send a NULL message.
	            	add(QueueMessageReference.NULL_MESSAGE);
	            	dispatchMatched();
        }
        		if( pull.getTimeout() > 0 ) {
	            	Scheduler.executeAfterDelay(new Runnable(){
							public void run() {
								pullTimeout(dispatchCounterBeforePull);
							}
						}, pull.getTimeout());
        		}
        	}
        }
        return null;
    }
    
    /**
     * Occurs when a pull times out.  If nothing has been dispatched
     * since the timeout was setup, then send the NULL message.
     */
    synchronized private void pullTimeout(long dispatchCounterBeforePull) {    	
    	if( dispatchCounterBeforePull == dispatchCounter ) {
        	try {
				add(QueueMessageReference.NULL_MESSAGE);
				dispatchMatched();
			} catch (Exception e) {
				context.getConnection().serviceException(e);
			}
    	}
	}
        
    synchronized public void add(MessageReference node) throws Exception{
        enqueueCounter++;
      
        if(!isFull() && pending.isEmpty() ){
            dispatch(node);
        }else{
            optimizePrefetch();
            synchronized(pending){
                if( pending.isEmpty() ) {
                    log.debug("Prefetch limit.");
                }
                pending.addMessageLast(node);
            }
        }
    }

    synchronized public void processMessageDispatchNotification(MessageDispatchNotification mdn) throws Exception {
        synchronized(pending){
            pending.reset();
            while(pending.hasNext()){
                MessageReference node=pending.next();
                if(node.getMessageId().equals(mdn.getMessageId())){
                    pending.remove();
                    createMessageDispatch(node,node.getMessage());
                    dispatched.addLast(node);
                    return;
                }
            }
            throw new JMSException("Slave broker out of sync with master: Dispatched message ("+mdn.getMessageId()+") was not in the pending list: "+pending);
        }
    }

    synchronized public void acknowledge(final ConnectionContext context,final MessageAck ack) throws Exception{
        // Handle the standard acknowledgment case.
        if(ack.isStandardAck()){
            // Acknowledge all dispatched messages up till the message id of the acknowledgment.
            int index=0;
            boolean inAckRange=false;
            for(Iterator iter=dispatched.iterator();iter.hasNext();){
                final MessageReference node=(MessageReference) iter.next();
                MessageId messageId=node.getMessageId();
                if(ack.getFirstMessageId()==null||ack.getFirstMessageId().equals(messageId)){
                    inAckRange=true;
                }
                if(inAckRange){
                    // Don't remove the nodes until we are committed.
                    if(!context.isInTransaction()){
                    	dequeueCounter++;
                    	node.getRegionDestination().getDestinationStatistics().getDequeues().increment();
                        iter.remove();
                    }else{
                        // setup a Synchronization to remove nodes from the dispatched list.
                        context.getTransaction().addSynchronization(new Synchronization(){
                            public void afterCommit() throws Exception{
                                synchronized(PrefetchSubscription.this){
                                    dequeueCounter++;
                                    dispatched.remove(node);
                                    node.getRegionDestination().getDestinationStatistics().getDequeues().increment();
                                    prefetchExtension=Math.max(0,prefetchExtension-1);
                                }
                            }

                            public void afterRollback() throws Exception {
                            	super.afterRollback();
                            }
                        });
                    }
                    index++;
                    acknowledge(context,ack,node);
                    if(ack.getLastMessageId().equals(messageId)){
                        if(context.isInTransaction()) {
                            // extend prefetch window only if not a pulling consumer
                            if (getPrefetchSize() != 0) {
                            prefetchExtension=Math.max(prefetchExtension,index+1);
                            }
                        }
                        else {
                            prefetchExtension=Math.max(0,prefetchExtension-(index+1));
                        }
                        dispatchMatched();
                        return;
                    }
                }
            }
            log.info("Could not correlate acknowledgment with dispatched message: "+ack);
        }else if(ack.isDeliveredAck()){
            // Message was delivered but not acknowledged: update pre-fetch counters.
            // Acknowledge all dispatched messages up till the message id of the acknowledgment.
            int index=0;
            for(Iterator iter=dispatched.iterator();iter.hasNext();index++){
                final MessageReference node=(MessageReference) iter.next();
                if(ack.getLastMessageId().equals(node.getMessageId())){
                    prefetchExtension=Math.max(prefetchExtension,index+1);
                    dispatchMatched();
                    return;
                }
            }
            throw new JMSException("Could not correlate acknowledgment with dispatched message: "+ack);
        }else if(ack.isPoisonAck()){
            // TODO: what if the message is already in a DLQ???
            // Handle the poison ACK case: we need to send the message to a DLQ
            if(ack.isInTransaction())
                throw new JMSException("Poison ack cannot be transacted: "+ack);
            // Acknowledge all dispatched messages up till the message id of the acknowledgment.
            int index=0;
            boolean inAckRange=false;
            for(Iterator iter=dispatched.iterator();iter.hasNext();){
                final MessageReference node=(MessageReference) iter.next();
                MessageId messageId=node.getMessageId();
                if(ack.getFirstMessageId()==null||ack.getFirstMessageId().equals(messageId)){
                    inAckRange=true;
                }
                if(inAckRange){
                    sendToDLQ(context, node);
                    node.getRegionDestination().getDestinationStatistics().getDequeues().increment();
                    iter.remove();
                    dequeueCounter++;
                    index++;
                    acknowledge(context,ack,node);
                    if(ack.getLastMessageId().equals(messageId)){
                        prefetchExtension=Math.max(0,prefetchExtension-(index+1));
                        dispatchMatched();
                        return;
                    }
                }
            }
            throw new JMSException("Could not correlate acknowledgment with dispatched message: "+ack);
        }
        
        if( isSlaveBroker() ) {
        	throw new JMSException("Slave broker out of sync with master: Acknowledgment ("+ack+") was not in the dispatch list: "+dispatched);
        } else {
        	log.debug("Acknowledgment out of sync (Normally occurs when failover connection reconnects): "+ack);
        }
    }

    /**
     * @param context
     * @param node
     * @throws IOException
     * @throws Exception
     */
    protected void sendToDLQ(final ConnectionContext context, final MessageReference node) throws IOException, Exception {
        // Send the message to the DLQ
        Message message=node.getMessage();
        if(message!=null){
            // The original destination and transaction id do not get filled when the message is first
            // sent,
            // it is only populated if the message is routed to another destination like the DLQ
            DeadLetterStrategy deadLetterStrategy=node.getRegionDestination().getDeadLetterStrategy();
            ActiveMQDestination deadLetterDestination=deadLetterStrategy.getDeadLetterQueueFor(message.getDestination());
            BrokerSupport.resend(context, message, deadLetterDestination);

        }
    }

    /**
     * Used to determine if the broker can dispatch to the consumer.
     * @return
     */
    protected boolean isFull(){
        return isSlaveBroker() || dispatched.size()-prefetchExtension>=info.getPrefetchSize();
    }
    
    /**
     * @return true when 60% or more room is left for dispatching messages
     */
    public boolean isLowWaterMark(){
        return (dispatched.size()-prefetchExtension) <= (info.getPrefetchSize() *.4);
    }
    
    /**
     * @return true when 10% or less room is left for dispatching messages
     */
    public boolean isHighWaterMark(){
        return (dispatched.size()-prefetchExtension) >= (info.getPrefetchSize() *.9);
    }
    
    public int getPendingQueueSize(){
    	synchronized(pending) {
    		return pending.size();
    	}
    }
    
    synchronized public int getDispatchedQueueSize(){
        return dispatched.size();
    }
    
    synchronized public long getDequeueCounter(){
        return dequeueCounter;
    }
    
    synchronized public long getDispatchedCounter() {
        return dispatchCounter;
    }
    
    synchronized public long getEnqueueCounter() {
        return enqueueCounter;
    }
    
    public boolean isRecoveryRequired(){
        return pending.isRecoveryRequired();
    }
    
    /**
     * optimize message consumer prefetch if the consumer supports it
     *
     */
    public void optimizePrefetch(){
    	/*
        if(info!=null&&info.isOptimizedAcknowledge()&&context!=null&&context.getConnection()!=null
                        &&context.getConnection().isManageable()){
            if(info.getCurrentPrefetchSize()!=info.getPrefetchSize() && isLowWaterMark()){
                info.setCurrentPrefetchSize(info.getPrefetchSize());
                updateConsumerPrefetch(info.getPrefetchSize());
            }else if(info.getCurrentPrefetchSize()==info.getPrefetchSize() && isHighWaterMark()){
                // want to purge any outstanding acks held by the consumer
                info.setCurrentPrefetchSize(1);
                updateConsumerPrefetch(1);
            }
        }
        */
    }
    
    public void add(ConnectionContext context, Destination destination) throws Exception {
        super.add(context,destination);
        pending.add(context,destination);
    }

    public void remove(ConnectionContext context, Destination destination) throws Exception {
        super.remove(context,destination);
        pending.remove(context,destination);
       
    }


    protected void dispatchMatched() throws IOException{
        if(!dispatching){
            dispatching=true;
            try{
                pending.reset();
                while(pending.hasNext()&&!isFull()){
                    MessageReference node=pending.next();
                    pending.remove();
                    dispatch(node);
                }
            }finally{
                dispatching=false;
            }
        }
    }

    protected boolean dispatch(final MessageReference node) throws IOException{
        final Message message=node.getMessage();
        if(message==null){
            return false;
        }
        // Make sure we can dispatch a message.
        if(canDispatch(node)&&!isSlaveBroker()){
        	
            MessageDispatch md=createMessageDispatch(node,message);
            // NULL messages don't count... they don't get Acked.
            if( node != QueueMessageReference.NULL_MESSAGE ) {
            dispatchCounter++;
            dispatched.addLast(node);            
            } else {
            	prefetchExtension=Math.max(0,prefetchExtension-1);
            }
            
            if(info.isDispatchAsync()){
                md.setTransmitCallback(new Runnable(){
                    public void run(){
                        // Since the message gets queued up in async dispatch, we don't want to
                        // decrease the reference count until it gets put on the wire.
                        onDispatch(node,message);
                    }
                });
                context.getConnection().dispatchAsync(md);
            }else{
                context.getConnection().dispatchSync(md);
                onDispatch(node,message);
            }
            return true;
        } else {
            return false;
        }
    }

    synchronized protected void onDispatch(final MessageReference node,final Message message){
        if(node.getRegionDestination()!=null){
        	if( node != QueueMessageReference.NULL_MESSAGE ) {
            node.getRegionDestination().getDestinationStatistics().getDispatched().increment();
        	}
            try{
                dispatchMatched();
            }catch(IOException e){
                context.getConnection().serviceExceptionAsync(e);
            }
        }
    }
    
    /**
     * inform the MessageConsumer on the client to change it's prefetch
     * @param newPrefetch
     */
    public void updateConsumerPrefetch(int newPrefetch){
        if (context != null && context.getConnection() != null && context.getConnection().isManageable()){
            ConsumerControl cc = new ConsumerControl();
            cc.setConsumerId(info.getConsumerId());
            cc.setPrefetch(newPrefetch);
            context.getConnection().dispatchAsync(cc);
        }
    }

    /**
     * @param node
     * @param message
     * @return MessageDispatch
     */
    protected MessageDispatch createMessageDispatch(MessageReference node,Message message){
        if( node == QueueMessageReference.NULL_MESSAGE ) {
        MessageDispatch md=new MessageDispatch();
            md.setMessage(null);
        md.setConsumerId(info.getConsumerId());
            md.setDestination( null );
            return md;
        } else {
            MessageDispatch md=new MessageDispatch();
            md.setConsumerId(info.getConsumerId());
        md.setDestination(node.getRegionDestination().getActiveMQDestination());
        md.setMessage(message);
        md.setRedeliveryCounter(node.getRedeliveryCounter());
        return md;
    }
    }

    /**
     * Use when a matched message is about to be dispatched to the client.
     * 
     * @param node
     * @return false if the message should not be dispatched to the client (another sub may have already dispatched it
     *         for example).
     * @throws IOException 
     */
    abstract protected boolean canDispatch(MessageReference node) throws IOException;

    /**
     * Used during acknowledgment to remove the message.
     * 
     * @throws IOException
     */
    protected void acknowledge(ConnectionContext context,final MessageAck ack,final MessageReference node)
                    throws IOException{}

}
