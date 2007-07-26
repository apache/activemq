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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.StoreQueueCursor;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.broker.region.group.MessageGroupHashBucketFactory;
import org.apache.activemq.broker.region.group.MessageGroupMap;
import org.apache.activemq.broker.region.group.MessageGroupMapFactory;
import org.apache.activemq.broker.region.group.MessageGroupSet;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.Response;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.thread.Valve;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.BrokerSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Queue is a List of MessageEntry objects that are dispatched to matching
 * subscriptions.
 * 
 * @version $Revision: 1.28 $
 */
public class Queue implements Destination, Task {

    private final Log log;
    private final ActiveMQDestination destination;
    private final List consumers = new CopyOnWriteArrayList();
    private final Valve dispatchValve = new Valve(true);
    private final UsageManager usageManager;
    private final DestinationStatistics destinationStatistics = new DestinationStatistics();
    private  PendingMessageCursor messages;
    private final LinkedList pagedInMessages = new LinkedList();
    private LockOwner exclusiveOwner;
    private MessageGroupMap messageGroupOwners;

    private int garbageSize = 0;
    private int garbageSizeBeforeCollection = 1000;
    private DispatchPolicy dispatchPolicy = new RoundRobinDispatchPolicy();
    private final MessageStore store;
    private DeadLetterStrategy deadLetterStrategy = new SharedDeadLetterStrategy();
    private MessageGroupMapFactory messageGroupMapFactory = new MessageGroupHashBucketFactory();
    private int maximumPagedInMessages = garbageSizeBeforeCollection * 2;
    private final MessageEvaluationContext queueMsgConext = new MessageEvaluationContext();
    private final Object exclusiveLockMutex = new Object();
    private final Object doDispatchMutex = new Object();
    private TaskRunner taskRunner;
    private boolean started = false;
    final Broker broker;
    
    public Queue(Broker broker,ActiveMQDestination destination, final UsageManager memoryManager, MessageStore store, DestinationStatistics parentStats,
            TaskRunnerFactory taskFactory, Store tmpStore) throws Exception {
        this.broker=broker;
        this.destination = destination;
        this.usageManager = new UsageManager(memoryManager,destination.toString());
        this.usageManager.setUsagePortion(1.0f);
        this.store = store;
        if(destination.isTemporary()){
            this.messages=new VMPendingMessageCursor();
        }else{
            this.messages=new StoreQueueCursor(this,tmpStore);
        }
        
        this.taskRunner = taskFactory.createTaskRunner(this, "Queue  "+destination.getPhysicalName());

        // Let the store know what usage manager we are using so that he can
        // flush messages to disk
        // when usage gets high.
        if (store != null) {
            store.setUsageManager(usageManager);
        }

        //let's copy the enabled property from the parent DestinationStatistics
        this.destinationStatistics.setEnabled(parentStats.isEnabled());
        destinationStatistics.setParent(parentStats);
        this.log = LogFactory.getLog(getClass().getName() + "." + destination.getPhysicalName());

        
    }
    
    public void initialize() throws Exception{
        if(store!=null){
            // Restore the persistent messages.
            messages.setUsageManager(getUsageManager());
            if(messages.isRecoveryRequired()){
                store.recover(new MessageRecoveryListener(){

                    public void recoverMessage(Message message){
                        // Message could have expired while it was being loaded..
                        if(message.isExpired()){
                            broker.messageExpired(createConnectionContext(),message);
                            destinationStatistics.getMessages().decrement();
                            return;
                        }
                        message.setRegionDestination(Queue.this);
                        synchronized(messages){
                            try{
                                messages.addMessageLast(message);
                            }catch(Exception e){
                                log.fatal("Failed to add message to cursor",e);
                            }
                        }
                        destinationStatistics.getMessages().increment();
                    }

                    public void recoverMessageReference(MessageId messageReference) throws Exception{
                        throw new RuntimeException("Should not be called.");
                    }

                    public void finished(){
                    }

                    public boolean hasSpace(){
                        return true;
                    }
                });
            }
        }
    }

    /**
     * Lock a node
     * 
     * @param node
     * @param lockOwner
     * @return true if can be locked
     * @see org.apache.activemq.broker.region.Destination#lock(org.apache.activemq.broker.region.MessageReference,
     *      org.apache.activemq.broker.region.LockOwner)
     */
    public boolean lock(MessageReference node,LockOwner lockOwner){
        synchronized(exclusiveLockMutex){
        	if (exclusiveOwner == lockOwner)
				return true;
			if (exclusiveOwner != null)
				return false;
        }
     	return true;
    }

    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
        sub.add(context, this);
        destinationStatistics.getConsumers().increment();
        maximumPagedInMessages += sub.getConsumerInfo().getPrefetchSize();

        
        
        MessageEvaluationContext msgContext=context.getMessageEvaluationContext();
        try{
            synchronized (consumers) {
				consumers.add(sub);
				if (sub.getConsumerInfo().isExclusive()) {
					LockOwner owner = (LockOwner) sub;
					if (exclusiveOwner == null) {
						exclusiveOwner = owner;
					} else {
						// switch the owner if the priority is higher.
						if (owner.getLockPriority() > exclusiveOwner.getLockPriority()) {
							exclusiveOwner = owner;
						}
					}
				}
			}
            // page in messages
            doPageIn();
            // synchronize with dispatch method so that no new messages are sent
            // while
            // setting up a subscription. avoid out of order messages,
			// duplicates
            // etc.
            dispatchValve.turnOff();
            try { 
	            msgContext.setDestination(destination);
	            synchronized(pagedInMessages){
	                // Add all the matching messages in the queue to the
	                // subscription.
	                for(Iterator i=pagedInMessages.iterator();i.hasNext();){
	                    QueueMessageReference node=(QueueMessageReference)i.next();
	                    if(node.isDropped()){
	                        continue;
	                    }
	                    try{
	                        msgContext.setMessageReference(node);
	                        if(sub.matches(node,msgContext)){
	                            sub.add(node);
	                        }
	                    }catch(IOException e){
	                        log.warn("Could not load message: "+e,e);
	                    }
	                }
	            }
            } finally {
                dispatchValve.turnOn();
            }
        }finally{
            msgContext.clear();
        }
    }

    public void removeSubscription(ConnectionContext context, Subscription sub) throws Exception {

        destinationStatistics.getConsumers().decrement();
        maximumPagedInMessages -= sub.getConsumerInfo().getPrefetchSize();

        // synchronize with dispatch method so that no new messages are sent
        // while
        // removing up a subscription.
        dispatchValve.turnOff();
        try {

            synchronized (consumers) {
				consumers.remove(sub);
				if (sub.getConsumerInfo().isExclusive()) {
					LockOwner owner = (LockOwner) sub;
					// Did we loose the exclusive owner??
					if (exclusiveOwner == owner) {

						// Find the exclusive consumer with the higest Lock
						// Priority.
						exclusiveOwner = null;
						for (Iterator iter = consumers.iterator(); iter.hasNext();) {
							Subscription s = (Subscription) iter.next();
							LockOwner so = (LockOwner) s;
							if (s.getConsumerInfo().isExclusive() && (exclusiveOwner == null || so.getLockPriority() > exclusiveOwner.getLockPriority()))
								exclusiveOwner = so;
						}
					}
				}
				if (consumers.isEmpty()) {
					messages.gc();
				}

			}
            sub.remove(context, this);

            boolean wasExclusiveOwner = false;
            if (exclusiveOwner == sub) {
                exclusiveOwner = null;
                wasExclusiveOwner = true;
            }

            ConsumerId consumerId = sub.getConsumerInfo().getConsumerId();
            MessageGroupSet ownedGroups = getMessageGroupOwners().removeConsumer(consumerId);

            if (!sub.getConsumerInfo().isBrowser()) {
                MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
                try {
                    msgContext.setDestination(destination);

                    // lets copy the messages to dispatch to avoid deadlock
                    List messagesToDispatch = new ArrayList();
                    synchronized (pagedInMessages) {
                        for(Iterator i =  pagedInMessages.iterator();i.hasNext();) {
                            QueueMessageReference node = (QueueMessageReference) i.next();
                            if (node.isDropped()) {
                                continue;
                            }

                            String groupID = node.getGroupID();

                            // Re-deliver all messages that the sub locked
                            if (node.getLockOwner() == sub || wasExclusiveOwner || (groupID != null && ownedGroups.contains(groupID))) {
                                messagesToDispatch.add(node);
                            }
                        }
                    }

                    // now lets dispatch from the copy of the collection to
                    // avoid deadlocks
                    for (Iterator iter = messagesToDispatch.iterator(); iter.hasNext();) {
                        QueueMessageReference node = (QueueMessageReference) iter.next();
                        node.incrementRedeliveryCounter();
                        node.unlock();
                        msgContext.setMessageReference(node);
                        dispatchPolicy.dispatch(node, msgContext, consumers);
                    }
                }
                finally {
                    msgContext.clear();
                }
            }
        }
        finally {
            dispatchValve.turnOn();
        }

    }
    
    private final LinkedList<Runnable> messagesWaitingForSpace = new LinkedList<Runnable>();
    private final Runnable sendMessagesWaitingForSpaceTask = new Runnable() {
    	public void run() {
    		
    		// We may need to do this in async thread since this is run for within a synchronization
    		// that the UsageManager is holding.
    		
    		synchronized( messagesWaitingForSpace ) {
	    		while( !usageManager.isFull() && !messagesWaitingForSpace.isEmpty()) {
	    			Runnable op = messagesWaitingForSpace.removeFirst();
	    			op.run();
	    		}
    		}
    		
    	};
    };

    public void send(final ProducerBrokerExchange producerExchange,final Message message) throws Exception {
    	final ConnectionContext context = producerExchange.getConnectionContext(); 
        // There is delay between the client sending it and it arriving at the
        // destination.. it may have expired.
    	
    	final boolean sendProducerAck = ( !message.isResponseRequired() || producerExchange.getProducerState().getInfo().getWindowSize() > 0 ) && !context.isInRecoveryMode();
        if(message.isExpired()){
            broker.messageExpired(context,message);
            destinationStatistics.getMessages().decrement();
            if( sendProducerAck ) {
        		ProducerAck ack = new ProducerAck(producerExchange.getProducerState().getInfo().getProducerId(), message.getSize());
				context.getConnection().dispatchAsync(ack);	    	            	        		
            }
            return;
        }
        if ( context.isProducerFlowControl() && usageManager.isFull() ) {
            if(usageManager.isSendFailIfNoSpace()){
                throw new javax.jms.ResourceAllocationException("Usage Manager memory limit reached");
            } 
            	
        	// We can avoid blocking due to low usage if the producer is sending a sync message or
        	// if it is using a producer window
        	if( producerExchange.getProducerState().getInfo().getWindowSize() > 0 || message.isResponseRequired() ) {
        		synchronized( messagesWaitingForSpace ) {
            		messagesWaitingForSpace.add(new Runnable() {
        				public void run() {
        					        			                    					
	            	        try {							
	        			        
	        					// While waiting for space to free up... the message may have expired.
	            	        	if(message.isExpired()) {
	        			            broker.messageExpired(context,message);
	                                destinationStatistics.getMessages().decrement();
	        			        } else {
	        			        	doMessageSend(producerExchange, message);
	        			        }
	        			        
	            	            if( sendProducerAck ) {
	            	        		ProducerAck ack = new ProducerAck(producerExchange.getProducerState().getInfo().getProducerId(), message.getSize());
	            					context.getConnection().dispatchAsync(ack);	    	            	        		
	            	            } else {
	            	            	Response response = new Response();
    				                response.setCorrelationId(message.getCommandId());
    								context.getConnection().dispatchAsync(response);	    								
	            	            }
	            	            
							} catch (Exception e) {
	            	        	if( !sendProducerAck && !context.isInRecoveryMode() ) {
    				                ExceptionResponse response = new ExceptionResponse(e);
    				                response.setCorrelationId(message.getCommandId());
    								context.getConnection().dispatchAsync(response);	    								
	            	        	}
							}
        				}
        			});
            		
            		// If the user manager is not full, then the task will not get called..
	            	if( !usageManager.notifyCallbackWhenNotFull(sendMessagesWaitingForSpaceTask) ) {
	            		// so call it directly here.
	            		sendMessagesWaitingForSpaceTask.run();
	            	}			            	
            		context.setDontSendReponse(true);
            		return;
        		}
        		
        	} else {
        		
        		// Producer flow control cannot be used, so we have do the flow control at the broker 
        		// by blocking this thread until there is space available.	            		
                while( !usageManager.waitForSpace(1000) ) {
                    if( context.getStopping().get() )
                        throw new IOException("Connection closed, send aborted.");
                }
                
                // The usage manager could have delayed us by the time
                // we unblock the message could have expired..
                if(message.isExpired()){
                    if (log.isDebugEnabled()) {
                        log.debug("Expired message: " + message);
                    }
                    return;
                }
        	}
        }
        doMessageSend(producerExchange, message);
        if( sendProducerAck ) {
    		ProducerAck ack = new ProducerAck(producerExchange.getProducerState().getInfo().getProducerId(), message.getSize());
			context.getConnection().dispatchAsync(ack);	    	            	        		
        }
    }

	void doMessageSend(final ProducerBrokerExchange producerExchange, final Message message) throws IOException, Exception {
		final ConnectionContext context = producerExchange.getConnectionContext();
		message.setRegionDestination(this);
        if(store!=null&&message.isPersistent()){
            store.addMessage(context,message);
        }
        if(context.isInTransaction()){
        	// If this is a transacted message.. increase the usage now so that a big TX does not blow up
        	// our memory.  This increment is decremented once the tx finishes..
            message.incrementReferenceCount();
            context.getTransaction().addSynchronization(new Synchronization(){
                public void afterCommit() throws Exception{
                	try { 
                        // It could take while before we receive the commit
                        // op, by that time the message could have expired..
	                    if(message.isExpired()){
	                        broker.messageExpired(context,message);
                            destinationStatistics.getMessages().decrement();
	                        return;
	                    }
	                    sendMessage(context,message);
                	} finally {
                        message.decrementReferenceCount();
                	}
                }
                
                @Override
                public void afterRollback() throws Exception {
                    message.decrementReferenceCount();
                }
            });
        }else{
        	// Add to the pending list, this takes care of incrementing the usage manager.
            sendMessage(context,message);            
        }
	}    

    public void dispose(ConnectionContext context) throws IOException {
        if (store != null) {
            store.removeAllMessages(context);
        }
        destinationStatistics.setParent(null);
    }

    public void dropEvent() {
        dropEvent(false);
    }

    public void dropEvent(boolean skipGc){
        // TODO: need to also decrement when messages expire.
        destinationStatistics.getMessages().decrement();
        synchronized(pagedInMessages){
            garbageSize++;
        }
        if(!skipGc&&garbageSize>garbageSizeBeforeCollection){
            gc();
        }
        try{
            taskRunner.wakeup();
        }catch(InterruptedException e){
            log.warn("Task Runner failed to wakeup ",e);
        }
    }

    public void gc() {
        synchronized (pagedInMessages) {
            for(Iterator i = pagedInMessages.iterator(); i.hasNext();) {
                // Remove dropped messages from the queue.
                QueueMessageReference node = (QueueMessageReference) i.next();
                if (node.isDropped()) {
                    garbageSize--;
                    i.remove();
                    continue;
                }
            }
        }
    }

    public void acknowledge(ConnectionContext context, Subscription sub, MessageAck ack, MessageReference node) throws IOException {
        if (store != null && node.isPersistent()) {
            // the original ack may be a ranged ack, but we are trying to delete
            // a specific
            // message store here so we need to convert to a non ranged ack.
            if (ack.getMessageCount() > 0) {
                // Dup the ack
                MessageAck a = new MessageAck();
                ack.copy(a);
                ack = a;
                // Convert to non-ranged.
                ack.setFirstMessageId(node.getMessageId());
                ack.setLastMessageId(node.getMessageId());
                ack.setMessageCount(1);
            }
            store.removeMessage(context, ack);
        }
    }

    Message loadMessage(MessageId messageId) throws IOException {
        Message msg = store.getMessage(messageId);
        if (msg != null) {
            msg.setRegionDestination(this);
        }
        return msg;
    }

    public String toString() {
        int size = 0;
        synchronized (messages) {
            size = messages.size();
        }
        return "Queue: destination=" + destination.getPhysicalName() + ", subscriptions=" + consumers.size() + ", memory=" + usageManager.getPercentUsage()
                + "%, size=" + size + ", in flight groups=" + messageGroupOwners;
    }

    public void start() throws Exception {
        started = true;
        if (usageManager != null) {
            usageManager.start();
        }
        messages.start();
        doPageIn(false);
    }

    public void stop() throws Exception {
        started = false;
        if( taskRunner!=null ) {
            taskRunner.shutdown();
        }
        if(messages!=null){
            messages.stop();
        }
        if (usageManager != null) {
            usageManager.stop();
        }
    }

    // Properties
    // -------------------------------------------------------------------------
    public ActiveMQDestination getActiveMQDestination() {
        return destination;
    }

    public String getDestination() {
        return destination.getPhysicalName();
    }

    public UsageManager getUsageManager() {
        return usageManager;
    }

    public DestinationStatistics getDestinationStatistics() {
        return destinationStatistics;
    }

    public MessageGroupMap getMessageGroupOwners() {
        if (messageGroupOwners == null) {
            messageGroupOwners = getMessageGroupMapFactory().createMessageGroupMap();
        }
        return messageGroupOwners;
    }

    public DispatchPolicy getDispatchPolicy() {
        return dispatchPolicy;
    }

    public void setDispatchPolicy(DispatchPolicy dispatchPolicy) {
        this.dispatchPolicy = dispatchPolicy;
    }

    public DeadLetterStrategy getDeadLetterStrategy() {
        return deadLetterStrategy;
    }

    public void setDeadLetterStrategy(DeadLetterStrategy deadLetterStrategy) {
        this.deadLetterStrategy = deadLetterStrategy;
    }

    public MessageGroupMapFactory getMessageGroupMapFactory() {
        return messageGroupMapFactory;
    }

    public void setMessageGroupMapFactory(MessageGroupMapFactory messageGroupMapFactory) {
        this.messageGroupMapFactory = messageGroupMapFactory;
    }

    public String getName() {
        return getActiveMQDestination().getPhysicalName();
    }

    public PendingMessageCursor getMessages(){
        return this.messages;
    }
    public void setMessages(PendingMessageCursor messages){
        this.messages=messages;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    private MessageReference createMessageReference(Message message) {
        MessageReference result =  new IndirectMessageReference(this, store, message);
        result.decrementReferenceCount();
        return result;
    }

    public MessageStore getMessageStore() {
        return store;
    }

    public Message[] browse() {
        ArrayList l = new ArrayList();
        try{
            doPageIn(true);
        }catch(Exception e){
            log.error("caught an exception browsing " + this,e);
        }
        synchronized(pagedInMessages) {
            for (Iterator i = pagedInMessages.iterator();i.hasNext();) {
                MessageReference r = (MessageReference)i.next();
                r.incrementReferenceCount();
                try {
                    Message m = r.getMessage();
                    if (m != null) {
                        l.add(m);
                    }
                }catch(IOException e){
                    log.error("caught an exception browsing " + this,e);
                }
                finally {
                    r.decrementReferenceCount();
                }
            }
        }
        synchronized(messages){
            try{
                messages.reset();
                while(messages.hasNext()){
                    try{
                        MessageReference r=messages.next();
                        r.incrementReferenceCount();
                        try{
                            Message m=r.getMessage();
                            if(m!=null){
                                l.add(m);
                            }
                        }finally{
                            r.decrementReferenceCount();
                        }
                    }catch(IOException e){
                        log.error("caught an exception brwsing "+this,e);
                    }
                }
            }finally{
                messages.release();
            }
        }

        return (Message[]) l.toArray(new Message[l.size()]);
    }

    public Message getMessage(String messageId){
        synchronized(messages){
            try{
                messages.reset();
                while(messages.hasNext()){
                    try{
                        MessageReference r=messages.next();
                        if(messageId.equals(r.getMessageId().toString())){
                            r.incrementReferenceCount();
                            try{
                                Message m=r.getMessage();
                                if(m!=null){
                                    return m;
                                }
                            }finally{
                                r.decrementReferenceCount();
                            }
                            break;
                        }
                    }catch(IOException e){
                        log.error("got an exception retrieving message "+messageId);
                    }
                }
            }finally{
                messages.release();
            }
        }
        return null;
    }

    public void purge() throws Exception {
        
        pageInMessages();
        
        synchronized (pagedInMessages) {
            ConnectionContext c = createConnectionContext();
            for(Iterator i = pagedInMessages.iterator(); i.hasNext();){
                try {
                    QueueMessageReference r = (QueueMessageReference) i.next();

                    // We should only delete messages that can be locked.
                    if (r.lock(LockOwner.HIGH_PRIORITY_LOCK_OWNER)) {
                        MessageAck ack = new MessageAck();
                        ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
                        ack.setDestination(destination);
                        ack.setMessageID(r.getMessageId());
                        acknowledge(c, null, ack, r);
                        r.drop();
                        dropEvent(true);
                    }
                }
                catch (IOException e) {
                }
            }

            // Run gc() by hand. Had we run it in the loop it could be
            // quite expensive.
            gc();
        }
    }
    

    /**
     * Removes the message matching the given messageId
     */
    public boolean removeMessage(String messageId) throws Exception {
        return removeMatchingMessages(createMessageIdFilter(messageId), 1) > 0;
    }

    /**
     * Removes the messages matching the given selector
     * 
     * @return the number of messages removed
     */
    public int removeMatchingMessages(String selector) throws Exception {
        return removeMatchingMessages(selector, -1);
    }
    
    /**
     * Removes the messages matching the given selector up to the maximum number of matched messages
     * 
     * @return the number of messages removed
     */
    public int removeMatchingMessages(String selector, int maximumMessages) throws Exception {
        return removeMatchingMessages(createSelectorFilter(selector), maximumMessages);
    }

    /**
     * Removes the messages matching the given filter up to the maximum number of matched messages
     * 
     * @return the number of messages removed
     */
    public int removeMatchingMessages(MessageReferenceFilter filter, int maximumMessages) throws Exception {
        pageInMessages();
        int counter = 0;
        synchronized (pagedInMessages) {
            ConnectionContext c = createConnectionContext();
           for(Iterator i = pagedInMessages.iterator(); i.hasNext();) {
               IndirectMessageReference r = (IndirectMessageReference) i.next();
                if (filter.evaluate(c, r)) {
                    removeMessage(c, r);
                    if (++counter >= maximumMessages && maximumMessages > 0) {
                        break;
                    }
                    
                }
            }
        }
        return counter;
    }

    /**
     * Copies the message matching the given messageId
     */
    public boolean copyMessageTo(ConnectionContext context, String messageId, ActiveMQDestination dest) throws Exception {
        return copyMatchingMessages(context, createMessageIdFilter(messageId), dest, 1) > 0;
    }
    
    /**
     * Copies the messages matching the given selector
     * 
     * @return the number of messages copied
     */
    public int copyMatchingMessagesTo(ConnectionContext context, String selector, ActiveMQDestination dest) throws Exception {
        return copyMatchingMessagesTo(context, selector, dest, -1);
    }
    
    /**
     * Copies the messages matching the given selector up to the maximum number of matched messages
     * 
     * @return the number of messages copied
     */
    public int copyMatchingMessagesTo(ConnectionContext context, String selector, ActiveMQDestination dest, int maximumMessages) throws Exception {
        return copyMatchingMessages(context, createSelectorFilter(selector), dest, maximumMessages);
    }

    /**
     * Copies the messages matching the given filter up to the maximum number of matched messages
     * 
     * @return the number of messages copied
     */
    public int copyMatchingMessages(ConnectionContext context, MessageReferenceFilter filter, ActiveMQDestination dest, int maximumMessages) throws Exception {
        pageInMessages();
        int counter = 0;
        synchronized (pagedInMessages) {
            for(Iterator i = pagedInMessages.iterator(); i.hasNext();) {
                MessageReference r = (MessageReference) i.next();
                if (filter.evaluate(context, r)) {
                    r.incrementReferenceCount();
                    try {
                        Message m = r.getMessage();
                        BrokerSupport.resend(context, m, dest);
                        if (++counter >= maximumMessages && maximumMessages > 0) {
                            break;
                        }
                    }
                    finally {
                        r.decrementReferenceCount();
                    }
                }
            }
        }
        return counter;
    }

    /**
     * Moves the message matching the given messageId
     */
    public boolean moveMessageTo(ConnectionContext context, String messageId, ActiveMQDestination dest) throws Exception {
        return moveMatchingMessagesTo(context, createMessageIdFilter(messageId), dest, 1) > 0;
    }
    
    /**
     * Moves the messages matching the given selector
     * 
     * @return the number of messages removed
     */
    public int moveMatchingMessagesTo(ConnectionContext context, String selector, ActiveMQDestination dest) throws Exception {
        return moveMatchingMessagesTo(context, selector, dest, -1);
    }
    
    /**
     * Moves the messages matching the given selector up to the maximum number of matched messages
     */
    public int moveMatchingMessagesTo(ConnectionContext context, String selector, ActiveMQDestination dest, int maximumMessages) throws Exception {
        return moveMatchingMessagesTo(context, createSelectorFilter(selector), dest, maximumMessages);
    }

    /**
     * Moves the messages matching the given filter up to the maximum number of matched messages
     */
    public int moveMatchingMessagesTo(ConnectionContext context, MessageReferenceFilter filter, ActiveMQDestination dest, int maximumMessages) throws Exception {
        pageInMessages();
        int counter = 0;
        synchronized (pagedInMessages) {
            for(Iterator i = pagedInMessages.iterator(); i.hasNext();) {
                IndirectMessageReference r = (IndirectMessageReference) i.next();
                if (filter.evaluate(context, r)) {
                    // We should only move messages that can be locked.
                    if (lockMessage(r)) {
                        r.incrementReferenceCount();
                        try {
                            Message m = r.getMessage();
                            BrokerSupport.resend(context, m, dest);
                            removeMessage(context, r);
                            if (++counter >= maximumMessages && maximumMessages > 0) {
                                break;
                            }
                        }
                        finally {
                            r.decrementReferenceCount();
                        }
                    }
                }
            }
        }
        return counter;
    }
    
    /**
     * @return
     * @see org.apache.activemq.thread.Task#iterate()
     */
    public boolean iterate(){
        try{
            pageInMessages(false);
         }catch(Exception e){
             log.error("Failed to page in more queue messages ",e);
         }
        return false;
    }

    protected MessageReferenceFilter createMessageIdFilter(final String messageId) {
        return new MessageReferenceFilter() {
            public boolean evaluate(ConnectionContext context, MessageReference r) {
                return messageId.equals(r.getMessageId().toString());
            }
        };
    }
    
    protected MessageReferenceFilter createSelectorFilter(String selector) throws InvalidSelectorException {
        final BooleanExpression selectorExpression = new SelectorParser().parse(selector);

        return new MessageReferenceFilter() {
            public boolean evaluate(ConnectionContext context, MessageReference r) throws JMSException {
                MessageEvaluationContext messageEvaluationContext = context.getMessageEvaluationContext();
                
                messageEvaluationContext.setMessageReference(r);
                if (messageEvaluationContext.getDestination() == null) {
                    messageEvaluationContext.setDestination(getActiveMQDestination());
                }
                
                return selectorExpression.matches(messageEvaluationContext);
            }
        };
    }

        
    protected void removeMessage(ConnectionContext c, IndirectMessageReference r) throws IOException {
        MessageAck ack = new MessageAck();
        ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
        ack.setDestination(destination);
        ack.setMessageID(r.getMessageId());
        acknowledge(c, null, ack, r);
        r.drop();
        dropEvent();
    }

    protected boolean lockMessage(IndirectMessageReference r) {
        return r.lock(LockOwner.HIGH_PRIORITY_LOCK_OWNER);
    }

    protected ConnectionContext createConnectionContext() {
        ConnectionContext answer = new ConnectionContext();
        answer.getMessageEvaluationContext().setDestination(getActiveMQDestination());
        return answer;
    }
    
      
    final void sendMessage(final ConnectionContext context,Message msg) throws Exception{
        synchronized(messages){
            messages.addMessageLast(msg);
        }
        destinationStatistics.getEnqueues().increment();
        destinationStatistics.getMessages().increment();
        pageInMessages(false);
    }
    
    private List doPageIn() throws Exception{
        return doPageIn(true);
    }
    
    private List doPageIn(boolean force) throws Exception{
    	        
        final int toPageIn=maximumPagedInMessages-pagedInMessages.size();
        List result=null;
        if((force||!consumers.isEmpty())&&toPageIn>0){
            messages.setMaxBatchSize(toPageIn);
            try{
                dispatchValve.increment();
                int count=0;
                result=new ArrayList(toPageIn);
                synchronized(messages){
                	
                    try{
                        messages.reset();
                        while(messages.hasNext()&&count<toPageIn){
                            MessageReference node=messages.next();
                            messages.remove();
                            if(!node.isExpired()){
                                node=createMessageReference(node.getMessage());
                                result.add(node);
                                count++;
                            }else{
                                broker.messageExpired(createConnectionContext(),node);
                                destinationStatistics.getMessages().decrement();
                            }
                        }
                    }finally{
                        messages.release();
                    }
                }
                synchronized(pagedInMessages){
                    pagedInMessages.addAll(result);
                }
            }finally{
                queueMsgConext.clear();
                dispatchValve.decrement();
            }
        }
        return result;
    }

    private void doDispatch(List list) throws Exception{
        if(list!=null&&!list.isEmpty()){
            try{
                dispatchValve.increment();
                for(int i=0;i<list.size();i++){
                    MessageReference node=(MessageReference)list.get(i);
                    queueMsgConext.setDestination(destination);
                    queueMsgConext.setMessageReference(node);
                    dispatchPolicy.dispatch(node,queueMsgConext,consumers);
                }
            }finally{
                queueMsgConext.clear();
                dispatchValve.decrement();
            }
        }
    }
    
    private void pageInMessages() throws Exception{
        pageInMessages(true);
    }
    private void pageInMessages(boolean force) throws Exception{
        synchronized(doDispatchMutex) {
            doDispatch(doPageIn(force));
        }
    }

    
}
