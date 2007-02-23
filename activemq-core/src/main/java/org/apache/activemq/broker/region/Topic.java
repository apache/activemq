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
import java.util.Set;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.FixedCountSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.SimpleDispatchPolicy;
import org.apache.activemq.broker.region.policy.SubscriptionRecoveryPolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.thread.Valve;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArraySet;

/**
 * The Topic is a destination that sends a copy of a message to every active
 * Subscription registered.
 * 
 * @version $Revision: 1.21 $
 */
public class Topic implements Destination {
    private static final Log log = LogFactory.getLog(Topic.class);
    protected final ActiveMQDestination destination;
    protected final CopyOnWriteArrayList consumers = new CopyOnWriteArrayList();
    protected final Valve dispatchValve = new Valve(true);
    protected final TopicMessageStore store;//this could be NULL! (If an advsiory)
    protected final UsageManager usageManager;
    protected final DestinationStatistics destinationStatistics = new DestinationStatistics();

    private DispatchPolicy dispatchPolicy = new SimpleDispatchPolicy();
    private SubscriptionRecoveryPolicy subscriptionRecoveryPolicy = new FixedCountSubscriptionRecoveryPolicy();
    private boolean sendAdvisoryIfNoConsumers;
    private DeadLetterStrategy deadLetterStrategy = new SharedDeadLetterStrategy();
    private final ConcurrentHashMap durableSubcribers = new ConcurrentHashMap();
    
    public Topic(ActiveMQDestination destination, TopicMessageStore store, UsageManager memoryManager, DestinationStatistics parentStats,
            TaskRunnerFactory taskFactory) {

        this.destination = destination;
        this.store = store; //this could be NULL! (If an advsiory)
        this.usageManager = new UsageManager(memoryManager);
        this.usageManager.setLimit(Long.MAX_VALUE);
        
        // Let the store know what usage manager we are using so that he can flush messages to disk
        // when usage gets high.
        if( store!=null ) {
            store.setUsageManager(usageManager);
        }

        this.destinationStatistics.setParent(parentStats);
    }

    public boolean lock(MessageReference node, LockOwner sub) {
        return true;
    }

    public void addSubscription(ConnectionContext context, final Subscription sub) throws Exception {
        
        sub.add(context, this);
        destinationStatistics.getConsumers().increment();

        if ( !sub.getConsumerInfo().isDurable() ) {

            // Do a retroactive recovery if needed.
            if (sub.getConsumerInfo().isRetroactive()) {
                
                // synchronize with dispatch method so that no new messages are sent
                // while we are recovering a subscription to avoid out of order messages.
                dispatchValve.turnOff();
                try {
                    
                    synchronized(consumers) {
                        consumers.add(sub);
                    }
                    subscriptionRecoveryPolicy.recover(context, this, sub);
                    
                } finally {
                    dispatchValve.turnOn();
                }
                
            } else {
                synchronized(consumers) {
                    consumers.add(sub);
                }
            }            
        } else {
            DurableTopicSubscription dsub = (DurableTopicSubscription) sub;
            durableSubcribers.put(dsub.getSubscriptionKey(), dsub);
        }
    }
    
    public void removeSubscription(ConnectionContext context, Subscription sub) throws Exception {
        if ( !sub.getConsumerInfo().isDurable() ) {
            destinationStatistics.getConsumers().decrement();
            synchronized(consumers) {
                consumers.remove(sub);
            }
        }
        sub.remove(context, this);
    }
       
    public void deleteSubscription(ConnectionContext context, SubscriptionKey key) throws IOException {
        if (store != null) {
            store.deleteSubscription(key.clientId, key.subscriptionName);
            Object removed = durableSubcribers.remove(key);
            if(removed != null) {
                destinationStatistics.getConsumers().decrement();
            }
        }
    }
    
    public void activate(ConnectionContext context, final DurableTopicSubscription subscription) throws Exception {
        
        // synchronize with dispatch method so that no new messages are sent
        // while
        // we are recovering a subscription to avoid out of order messages.
        dispatchValve.turnOff();
        try {
        
            synchronized(consumers) {           
                consumers.add(subscription);
            }
            
            if (store == null )
                return;
            
            // Recover the durable subscription.
            String clientId = subscription.getClientId();
            String subscriptionName = subscription.getSubscriptionName();
            String selector = subscription.getConsumerInfo().getSelector();
            SubscriptionInfo info = store.lookupSubscription(clientId, subscriptionName);
            if (info != null) {
                // Check to see if selector changed.
                String s1 = info.getSelector();
                if (s1 == null ^ selector == null || (s1 != null && !s1.equals(selector))) {
                    // Need to delete the subscription
                    store.deleteSubscription(clientId, subscriptionName);
                    info = null;
                }
            }
            // Do we need to create the subscription?
            if (info == null) {
                store.addSubsciption(clientId, subscriptionName, selector, subscription.getConsumerInfo().isRetroactive());
            }
    
            final MessageEvaluationContext msgContext = new MessageEvaluationContext();
            msgContext.setDestination(destination);
            if(subscription.isRecoveryRequired()){
                store.recoverSubscription(clientId,subscriptionName,new MessageRecoveryListener(){
                    public void recoverMessage(Message message) throws Exception{
                        message.setRegionDestination(Topic.this);
                        try{
                            msgContext.setMessageReference(message);
                            if(subscription.matches(message,msgContext)){
                                subscription.add(message);
                            }
                        }catch(InterruptedException e){
                            Thread.currentThread().interrupt();
                        }catch(IOException e){
                            // TODO: Need to handle this better.
                            e.printStackTrace();
                        }
                    }

                    public void recoverMessageReference(String messageReference) throws Exception{
                        throw new RuntimeException("Should not be called.");
                    }

                    public void finished(){}
                });
            }
            
            if( true && subscription.getConsumerInfo().isRetroactive() ) {
                // If nothing was in the persistent store, then try to use the recovery policy.
                if( subscription.getEnqueueCounter() == 0 ) {
                    subscriptionRecoveryPolicy.recover(context, this, subscription);
                } else {
                    // TODO: implement something like
                    // subscriptionRecoveryPolicy.recoverNonPersistent(context, this, sub);
                }
            }
        
        }
        finally {
            dispatchValve.turnOn();
        }
    }

    public void deactivate(ConnectionContext context, DurableTopicSubscription sub) throws Exception {        
        synchronized(consumers) {           
            consumers.remove(sub);
        }
        sub.remove(context, this);
    }    


    public void send(final ConnectionContext context, final Message message) throws Exception {

        if (context.isProducerFlowControl() ) {
            if (usageManager.isSendFailIfNoSpace() && usageManager.isFull()) {
                throw new javax.jms.ResourceAllocationException("Usage Manager memory limit reached");
            } else {
                while( !usageManager.waitForSpace(1000) ) {
                    if( context.getStopping().get() )
                        throw new IOException("Connection closed, send aborted.");
                }
                usageManager.waitForSpace();
            }    
        }

        message.setRegionDestination(this);

        if (store != null && message.isPersistent() && !canOptimizeOutPersistence() )
            store.addMessage(context, message);

        message.incrementReferenceCount();
        try {

            if (context.isInTransaction()) {
                context.getTransaction().addSynchronization(new Synchronization() {
                    public void afterCommit() throws Exception {
                        dispatch(context, message);
                    }
                });

            }
            else {
                dispatch(context, message);
            }

        }
        finally {
            message.decrementReferenceCount();
        }

    }

    private boolean canOptimizeOutPersistence() {
        return durableSubcribers.size()==0;
    }

    public String toString() {
        return "Topic: destination=" + destination.getPhysicalName() + ", subscriptions=" + consumers.size();
    }

    public void acknowledge(ConnectionContext context, Subscription sub, final MessageAck ack, final MessageReference node) throws IOException {
        if (store != null && node.isPersistent()) {
            DurableTopicSubscription dsub = (DurableTopicSubscription) sub;
            store.acknowledge(context, dsub.getClientId(), dsub.getSubscriptionName(), node.getMessageId());
        }
    }

    public void dispose(ConnectionContext context) throws IOException {
        if (store != null) {
            store.removeAllMessages(context);
        }
        destinationStatistics.setParent(null);
    }

    public void gc() {
    }

    public Message loadMessage(MessageId messageId) throws IOException {
        return store != null ? store.getMessage(messageId) : null;
    }

    public void start() throws Exception {
        this.subscriptionRecoveryPolicy.start();
    }

    public void stop() throws Exception {
        this.subscriptionRecoveryPolicy.stop();
    }
    
    public Message[] browse(){
        final Set result=new CopyOnWriteArraySet();
        try{
            if(store!=null){
                store.recover(new MessageRecoveryListener(){
                    public void recoverMessage(Message message) throws Exception{
                        result.add(message);
                    }

                    public void recoverMessageReference(String messageReference) throws Exception{}

                    public void finished(){}
                });
                Message[] msgs=subscriptionRecoveryPolicy.browse(getActiveMQDestination());
                if(msgs!=null){
                    for(int i=0;i<msgs.length;i++){
                        result.add(msgs[i]);
                    }
                }
            }
        }catch(Throwable e){
            log.warn("Failed to browse Topic: "+getActiveMQDestination().getPhysicalName(),e);
        }
        return (Message[]) result.toArray(new Message[result.size()]);
    }

    // Properties
    // -------------------------------------------------------------------------

    public UsageManager getUsageManager() {
        return usageManager;
    }

    public DestinationStatistics getDestinationStatistics() {
        return destinationStatistics;
    }

    public ActiveMQDestination getActiveMQDestination() {
        return destination;
    }

    public String getDestination() {
        return destination.getPhysicalName();
    }
    
    public DispatchPolicy getDispatchPolicy() {
        return dispatchPolicy;
    }

    public void setDispatchPolicy(DispatchPolicy dispatchPolicy) {
        this.dispatchPolicy = dispatchPolicy;
    }

    public SubscriptionRecoveryPolicy getSubscriptionRecoveryPolicy() {
        return subscriptionRecoveryPolicy;
    }

    public void setSubscriptionRecoveryPolicy(SubscriptionRecoveryPolicy subscriptionRecoveryPolicy) {
        this.subscriptionRecoveryPolicy = subscriptionRecoveryPolicy;
    }

    public boolean isSendAdvisoryIfNoConsumers() {
        return sendAdvisoryIfNoConsumers;
    }

    public void setSendAdvisoryIfNoConsumers(boolean sendAdvisoryIfNoConsumers) {
        this.sendAdvisoryIfNoConsumers = sendAdvisoryIfNoConsumers;
    }

    public MessageStore getMessageStore() {
        return store;
    }

    public DeadLetterStrategy getDeadLetterStrategy() {
        return deadLetterStrategy;
    }

    public void setDeadLetterStrategy(DeadLetterStrategy deadLetterStrategy) {
        this.deadLetterStrategy = deadLetterStrategy;
    }

    public String getName() {
        return getActiveMQDestination().getPhysicalName();
    }


    // Implementation methods
    // -------------------------------------------------------------------------
    protected void dispatch(ConnectionContext context, Message message) throws Exception {
        destinationStatistics.getEnqueues().increment();
        dispatchValve.increment();
        MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
        try {
            if (!subscriptionRecoveryPolicy.add(context, message)) {
                return;
            }
            synchronized(consumers) {
                if (consumers.isEmpty()) {
                    onMessageWithNoConsumers(context, message);
                    return;
                }
            }

            msgContext.setDestination(destination);
            msgContext.setMessageReference(message);

            if (!dispatchPolicy.dispatch(context, message, msgContext, consumers)) {
                onMessageWithNoConsumers(context, message);
            }
        }
        finally {
            msgContext.clear();
            dispatchValve.decrement();
        }
    }

    /**
     * Provides a hook to allow messages with no consumer to be processed in
     * some way - such as to send to a dead letter queue or something..
     */
    protected void onMessageWithNoConsumers(ConnectionContext context, Message message) throws Exception {
        if (!message.isPersistent()) {
            if (sendAdvisoryIfNoConsumers) {
                // allow messages with no consumers to be dispatched to a dead
                // letter queue
                if (!AdvisorySupport.isAdvisoryTopic(destination)) {
                    
                    // The original destination and transaction id do not get filled when the message is first sent,
                    // it is only populated if the message is routed to another destination like the DLQ
                    if( message.getOriginalDestination()!=null )
                        message.setOriginalDestination(message.getDestination());
                    if( message.getOriginalTransactionId()!=null )
                        message.setOriginalTransactionId(message.getTransactionId());

                    ActiveMQTopic advisoryTopic = AdvisorySupport.getNoTopicConsumersAdvisoryTopic(destination);
                    message.setDestination(advisoryTopic);
                    message.setTransactionId(null);
                    message.evictMarshlledForm();

                    // Disable flow control for this since since we don't want to block.
                    boolean originalFlowControl = context.isProducerFlowControl();
                    try {
                        context.setProducerFlowControl(false);
                        context.getBroker().send(context, message);
                    } finally {
                        context.setProducerFlowControl(originalFlowControl);
                    }
                    
                }
            }
        }
    }


}
