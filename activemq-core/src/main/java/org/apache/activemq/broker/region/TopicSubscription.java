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
import org.apache.activemq.broker.region.policy.MessageEvictionStrategy;
import org.apache.activemq.broker.region.policy.OldestMessageEvictionStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.transaction.Synchronization;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;


public class TopicSubscription extends AbstractSubscription{
	
    private static final Log log=LogFactory.getLog(TopicSubscription.class);
    
    final protected LinkedList matched=new LinkedList();
    final protected ActiveMQDestination dlqDestination=new ActiveMQQueue("ActiveMQ.DLQ");
    final protected UsageManager usageManager;
    protected AtomicLong dispatchedCounter=new AtomicLong();
    protected AtomicLong prefetchExtension=new AtomicLong();
    private int maximumPendingMessages=-1;
    private MessageEvictionStrategy messageEvictionStrategy = new OldestMessageEvictionStrategy();
    private int discarded = 0;
    private final Object matchedListMutex=new Object();
    private final AtomicLong enqueueCounter = new AtomicLong(0);
    private final AtomicLong dequeueCounter = new AtomicLong(0);
    
    boolean singleDestination=true;
    Destination destination;    
    
    public TopicSubscription(Broker broker,ConnectionContext context,ConsumerInfo info,UsageManager usageManager)
                    throws InvalidSelectorException{
        super(broker,context,info);
        this.usageManager=usageManager;
    }

    public void add(MessageReference node) throws InterruptedException,IOException{
        
        enqueueCounter.incrementAndGet();        
        node.incrementReferenceCount();
        
        if(!isFull()&&!isSlaveBroker()){
            optimizePrefetch();
            // if maximumPendingMessages is set we will only discard messages which
            // have not been dispatched (i.e. we allow the prefetch buffer to be filled)
            dispatch(node);
        }else{
            if(maximumPendingMessages!=0){
                synchronized(matchedListMutex){
                    matched.addLast(node);
                    // NOTE - be careful about the slaveBroker!
                    if (maximumPendingMessages > 0) {
                        
                        // calculate the high water mark from which point we will eagerly evict expired messages
                        int max = messageEvictionStrategy.getEvictExpiredMessagesHighWatermark();
                        if (maximumPendingMessages > 0 && maximumPendingMessages < max) {
                            max = maximumPendingMessages;
                        }
                        if (!matched.isEmpty() && matched.size() > max) {
                            removeExpiredMessages(matched);
                        }

                        // lets discard old messages as we are a slow consumer
                        while (!matched.isEmpty() && matched.size() > maximumPendingMessages) {
                            MessageReference[] oldMessages = messageEvictionStrategy.evictMessages(matched);
                            int messagesToEvict = oldMessages.length;
                            for(int i = 0; i < messagesToEvict; i++) {
                            	oldMessages[i].decrementReferenceCount();
                                discarded++;
                                if (log.isDebugEnabled()) {
                                    log.debug("Discarding message " + oldMessages[i]);
                                }
							}
                            
                            // lets avoid an infinite loop if we are given a bad eviction strategy
                            // for a bad strategy lets just not evict
                            if (messagesToEvict == 0) {
                                log.warn("No messages to evict returned from eviction strategy: " + messageEvictionStrategy);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Discard any expired messages from the matched list. Called from a synchronized block.
     * @throws IOException 
     */
    protected void removeExpiredMessages(LinkedList messages) throws IOException {
        for(Iterator i=matched.iterator();i.hasNext();){
            MessageReference node=(MessageReference) i.next();
            if (node.isExpired()) {
                i.remove();
                dispatchedCounter.incrementAndGet();
                node.decrementReferenceCount();
                break;
            }
        }
    }

    public void processMessageDispatchNotification(MessageDispatchNotification mdn){
        synchronized(matchedListMutex){
            for(Iterator i=matched.iterator();i.hasNext();){
                MessageReference node=(MessageReference) i.next();
                if(node.getMessageId().equals(mdn.getMessageId())){
                    i.remove();
                    dispatchedCounter.incrementAndGet();
                    node.decrementReferenceCount();
                    break;
                }
            }
        }
    }

    synchronized public void acknowledge(final ConnectionContext context,final MessageAck ack) throws Exception{
        
        // Handle the standard acknowledgment case.
        boolean wasFull=isFull();
        if(ack.isStandardAck()||ack.isPoisonAck()){
            if(context.isInTransaction()){
                prefetchExtension.addAndGet(ack.getMessageCount());
                context.getTransaction().addSynchronization(new Synchronization(){
                    public void afterCommit() throws Exception{
                    	synchronized( TopicSubscription.this ) {
	                    	if( singleDestination && destination!=null) {
	                    		destination.getDestinationStatistics().getDequeues().add(ack.getMessageCount());
	                    	}
                    	}                    
                        dequeueCounter.addAndGet(ack.getMessageCount());
                        prefetchExtension.set(Math.max(0,prefetchExtension.get()-ack.getMessageCount()));
                    }
                });
            }else{
            	
            	if( singleDestination && destination!=null ) {
            		destination.getDestinationStatistics().getDequeues().add(ack.getMessageCount());
            	}
            	            
                dequeueCounter.addAndGet(ack.getMessageCount());
                prefetchExtension.set(Math.max(0,prefetchExtension.get()-ack.getMessageCount()));
            }
            if(wasFull&&!isFull()){
                dispatchMatched();
            }
            return;
        }else if(ack.isDeliveredAck()){
            // Message was delivered but not acknowledged: update pre-fetch counters.
            prefetchExtension.addAndGet(ack.getMessageCount());
            if(wasFull&&!isFull()){
                dispatchMatched();
            }
            return;
        }
        throw new JMSException("Invalid acknowledgment: "+ack);
    }

    public Response pullMessage(ConnectionContext context, MessagePull pull) throws Exception {
        // not supported for topics
        return null;
    }

    public int getPendingQueueSize(){
        return matched();
    }

    public int getDispatchedQueueSize(){
        return (int)(dispatchedCounter.get()-dequeueCounter.get());
    }

    public int getMaximumPendingMessages(){
        return maximumPendingMessages;
    }
    
	public long getDispatchedCounter() {
		return dispatchedCounter.get();
	}

	public long getEnqueueCounter() {
		return enqueueCounter.get();
	}
    
    public long getDequeueCounter(){
        return dequeueCounter.get();
    }

    /**
     * @return the number of messages discarded due to being a slow consumer
     */
    public int discarded() {
        synchronized(matchedListMutex) {
            return discarded;
        }
    }

    /**
     * @return the number of matched messages (messages targeted for the subscription but not
     * yet able to be dispatched due to the prefetch buffer being full).
     */
    public int matched() {
        synchronized(matchedListMutex) {
            return matched.size();
        }
    }


    /**
     * Sets the maximum number of pending messages that can be matched against this consumer before old messages are
     * discarded.
     */
    public void setMaximumPendingMessages(int maximumPendingMessages){
        this.maximumPendingMessages=maximumPendingMessages;
    }

    public MessageEvictionStrategy getMessageEvictionStrategy() {
        return messageEvictionStrategy;
    }

    /**
     * Sets the eviction strategy used to decide which message to evict when the slow consumer
     * needs to discard messages
     */
    public void setMessageEvictionStrategy(MessageEvictionStrategy messageEvictionStrategy) {
        this.messageEvictionStrategy = messageEvictionStrategy;
    }

    
    // Implementation methods
    // -------------------------------------------------------------------------

    private boolean isFull(){
        return getDispatchedQueueSize()-prefetchExtension.get()>=info.getPrefetchSize();
    }
    
    /**
     * @return true when 60% or more room is left for dispatching messages
     */
    public boolean isLowWaterMark(){
        return (getDispatchedQueueSize()-prefetchExtension.get()) <= (info.getPrefetchSize() *.4);
    }
    
    /**
     * @return true when 10% or less room is left for dispatching messages
     */
    public boolean isHighWaterMark(){
        return (getDispatchedQueueSize()-prefetchExtension.get()) >= (info.getPrefetchSize() *.9);
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

    private void dispatchMatched() throws IOException{
        synchronized(matchedListMutex){
            for(Iterator iter=matched.iterator();iter.hasNext()&&!isFull();){
                MessageReference message=(MessageReference) iter.next();
                iter.remove();
                dispatch(message);
            }
        }
    }

    private void dispatch(final MessageReference node) throws IOException{
        Message message=(Message) node;
        // Make sure we can dispatch a message.
        MessageDispatch md=new MessageDispatch();
        md.setMessage(message);
        md.setConsumerId(info.getConsumerId());
        md.setDestination(node.getRegionDestination().getActiveMQDestination());
        dispatchedCounter.incrementAndGet();
       
        // Keep track if this subscription is receiving messages from a single destination.
        if( singleDestination ) {
        	if( destination == null ) {
        		destination = node.getRegionDestination();
        	} else {
        		if( destination != node.getRegionDestination() ) {
        			singleDestination = false;
        		}
        	}
        }
                
        if(info.isDispatchAsync()){
            md.setConsumer(new Runnable(){
                public void run(){
                    node.getRegionDestination().getDestinationStatistics().getDispatched().increment();	
                    node.decrementReferenceCount();
                }
            });
            context.getConnection().dispatchAsync(md);
        }else{
            context.getConnection().dispatchSync(md);
            node.getRegionDestination().getDestinationStatistics().getDispatched().increment();
            node.decrementReferenceCount();
        }
    }

    public String toString(){
        return "TopicSubscription:"+" consumer="+info.getConsumerId()+", destinations="+destinations.size()
                        +", dispatched="+getDispatchedQueueSize()+", delivered="+getDequeueCounter()+", matched="+matched()+", discarded="+discarded();
    }

    public void destroy() {
        synchronized(matchedListMutex){
            for (Iterator iter = matched.iterator(); iter.hasNext();) {
                MessageReference node = (MessageReference) iter.next();
                node.decrementReferenceCount();
            }
            matched.clear();
        }
    }

    public int getPrefetchSize() {
        return (int) (info.getPrefetchSize() + prefetchExtension.get());
    }

}
