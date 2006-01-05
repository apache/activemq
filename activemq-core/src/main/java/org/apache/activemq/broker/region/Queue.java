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
package org.apache.activemq.broker.region;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.group.MessageGroupHashBucket;
import org.apache.activemq.broker.region.group.MessageGroupMap;
import org.apache.activemq.broker.region.group.MessageGroupSet;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.thread.Valve;
import org.apache.activemq.transaction.Synchronization;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * The Queue is a List of MessageEntry objects that are dispatched to matching
 * subscriptions.
 * 
 * @version $Revision: 1.28 $
 */
public class Queue implements Destination {

    private final Log log;

    protected final ActiveMQDestination destination;
    protected final List consumers = new CopyOnWriteArrayList();
    protected final LinkedList messages = new LinkedList();
    protected final Valve dispatchValve = new Valve(true);
    protected final UsageManager usageManager;
    protected final DestinationStatistics destinationStatistics = new DestinationStatistics();

    private Subscription exclusiveOwner;
    private MessageGroupMap messageGroupOwners;
    private int messageGroupHashBucketCount = 1024;

    protected long garbageSize = 0;
    protected long garbageSizeBeforeCollection = 1000;
    private DispatchPolicy dispatchPolicy = new RoundRobinDispatchPolicy();
    protected final MessageStore store;
    protected int highestSubscriptionPriority;
    private DeadLetterStrategy deadLetterStrategy = new SharedDeadLetterStrategy();

    public Queue(ActiveMQDestination destination, final UsageManager memoryManager, MessageStore store,
            DestinationStatistics parentStats, TaskRunnerFactory taskFactory) throws Throwable {
        this.destination = destination;
        this.usageManager = memoryManager;
        this.store = store;

        destinationStatistics.setParent(parentStats);
        this.log = LogFactory.getLog(getClass().getName() + "." + destination.getPhysicalName());

        if (store != null) {
            // Restore the persistent messages.
            store.recover(new MessageRecoveryListener() {
                public void recoverMessage(Message message) {
                    message.setRegionDestination(Queue.this);
                    MessageReference reference = createMessageReference(message);
                    messages.add(reference);
                    reference.decrementReferenceCount();
                    destinationStatistics.getMessages().increment();
                }

                public void recoverMessageReference(String messageReference) throws Throwable {
                    throw new RuntimeException("Should not be called.");
                }
            });
        }
    }

    public synchronized boolean lock(MessageReference node, Subscription sub) {
        if (exclusiveOwner == sub)
            return true;
        if (exclusiveOwner != null)
            return false;
        if (sub.getConsumerInfo().getPriority() != highestSubscriptionPriority)
            return false;
        if (sub.getConsumerInfo().isExclusive()) {
            exclusiveOwner = sub;
        }
        return true;
    }

    public void addSubscription(ConnectionContext context, Subscription sub) throws Throwable {
        sub.add(context, this);
        destinationStatistics.getConsumers().increment();

        // synchronize with dispatch method so that no new messages are sent
        // while
        // setting up a subscription. avoid out of order messages, duplicates
        // etc.
        dispatchValve.turnOff();

        MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
        try {
            synchronized (consumers) {
                consumers.add(sub);
            }

            highestSubscriptionPriority = calcHighestSubscriptionPriority();
            msgContext.setDestination(destination);

            synchronized (messages) {
                // Add all the matching messages in the queue to the
                // subscription.
                for (Iterator iter = messages.iterator(); iter.hasNext();) {

                    IndirectMessageReference node = (IndirectMessageReference) iter.next();
                    if (node.isDropped() ) {
                        continue;
                    }

                    try {
                        msgContext.setMessageReference(node);
                        if (sub.matches(node, msgContext)) {
                            sub.add(node);
                        }
                    }
                    catch (IOException e) {
                        log.warn("Could not load message: " + e, e);
                    }
                }
            }

        }
        finally {
            msgContext.clear();
            dispatchValve.turnOn();
        }
    }

    public void removeSubscription(ConnectionContext context, Subscription sub) throws Throwable {

        destinationStatistics.getConsumers().decrement();

        // synchronize with dispatch method so that no new messages are sent
        // while
        // removing up a subscription.
        dispatchValve.turnOff();
        try {

            synchronized (consumers) {
                consumers.remove(sub);
            }
            sub.remove(context, this);

            highestSubscriptionPriority = calcHighestSubscriptionPriority();

            boolean wasExclusiveOwner = false;
            if (exclusiveOwner == sub) {
                exclusiveOwner = null;
                wasExclusiveOwner = true;
            }

            ConsumerId consumerId = sub.getConsumerInfo().getConsumerId();
            MessageGroupSet ownedGroups = getMessageGroupOwners().removeConsumer(consumerId);

            synchronized (messages) {
                if (!sub.getConsumerInfo().isBrowser()) {
                    MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
                    try {
                        msgContext.setDestination(destination);
                        
                        for (Iterator iter = messages.iterator(); iter.hasNext();) {
                            IndirectMessageReference node = (IndirectMessageReference) iter.next();                            
                            if (node.isDropped() ) {
                                continue;
                            }
    
                            String groupID = node.getGroupID();
    
                            // Re-deliver all messages that the sub locked
                            if (node.getLockOwner() == sub || wasExclusiveOwner || (groupID != null && ownedGroups.contains(groupID))) {
                                node.incrementRedeliveryCounter();
                                node.unlock();
                                msgContext.setMessageReference(node);
                                dispatchPolicy.dispatch(context, node, msgContext, consumers);
                            }
                        }
                    } finally {
                        msgContext.clear();
                    }
                }
            }

        }
        finally {
            dispatchValve.turnOn();
        }

    }

    public void send(final ConnectionContext context, final Message message) throws Throwable {

        if( context.isProducerFlowControl() )
            usageManager.waitForSpace();
        
        message.setRegionDestination(this);

        if (store != null && message.isPersistent())
            store.addMessage(context, message);

        final MessageReference node = createMessageReference(message);
        try {

            if (context.isInTransaction()) {
                context.getTransaction().addSynchronization(new Synchronization() {
                    public void afterCommit() throws Throwable {
                        dispatch(context, node, message);
                    }
                });
            }
            else {
                dispatch(context, node, message);
            }
        } finally {
            node.decrementReferenceCount();
        }
    }

    public void dispose(ConnectionContext context) throws IOException {
        if (store != null) {
            store.removeAllMessages(context);
        }
        destinationStatistics.setParent(null);
    }

    public void dropEvent() {
        // TODO: need to also decrement when messages expire.
        destinationStatistics.getMessages().decrement();
        synchronized (messages) {
            garbageSize++;
            if (garbageSize > garbageSizeBeforeCollection) {
                gc();
            }
        }
    }

    public void gc() {
        synchronized (messages) {
            for (Iterator iter = messages.iterator(); iter.hasNext();) {
                // Remove dropped messages from the queue.
                IndirectMessageReference node = (IndirectMessageReference) iter.next();
                if (node.isDropped()) {                    
                    garbageSize--;
                    iter.remove();
                    continue;
                }
            }
        }
    }

    public void acknowledge(ConnectionContext context, Subscription sub, final MessageAck ack, final MessageReference node) throws IOException {
        if (store != null && node.isPersistent()) {
            store.removeMessage(context, ack);
        }
    }

    public Message loadMessage(MessageId messageId) throws IOException {
        Message msg = store.getMessage(messageId);
        if( msg!=null ) {
            msg.setRegionDestination(this);
        }
        return msg;
    }

    public String toString() {
        return "Queue: destination=" + destination.getPhysicalName() + ", subscriptions=" + consumers.size() + ", memory=" + usageManager.getPercentUsage()
                + "%, size=" + messages.size() + ", in flight groups=" + messageGroupOwners;
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    // Properties
    // -------------------------------------------------------------------------
    public ActiveMQDestination getActiveMQDestination() {
        return destination;
    }

    public UsageManager getUsageManager() {
        return usageManager;
    }

    public DestinationStatistics getDestinationStatistics() {
        return destinationStatistics;
    }

    public MessageGroupMap getMessageGroupOwners() {
        if (messageGroupOwners == null) {
            messageGroupOwners = new MessageGroupHashBucket(messageGroupHashBucketCount );
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

    public int getMessageGroupHashBucketCount() {
        return messageGroupHashBucketCount;
    }

    public void setMessageGroupHashBucketCount(int messageGroupHashBucketCount) {
        this.messageGroupHashBucketCount = messageGroupHashBucketCount;
    }
    

    // Implementation methods
    // -------------------------------------------------------------------------
    private MessageReference createMessageReference(Message message) {
        return new IndirectMessageReference(this, message);
    }

    private void dispatch(ConnectionContext context, MessageReference node, Message message) throws Throwable {

        dispatchValve.increment();
        MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
        try {
            destinationStatistics.onMessageEnqueue(message);
            synchronized (messages) {
                messages.add(node);
            }

            synchronized(consumers) {
                if (consumers.isEmpty()) {
                    log.debug("No subscriptions registered, will not dispatch message at this time.");
                    return;
                }
            }

            msgContext.setDestination(destination);
            msgContext.setMessageReference(node);

            dispatchPolicy.dispatch(context, node, msgContext, consumers);
        }
        finally {
            msgContext.clear();
            dispatchValve.decrement();
        }
    }

    private int calcHighestSubscriptionPriority() {
        int rc = Integer.MIN_VALUE;
        synchronized (consumers) {
            for (Iterator iter = consumers.iterator(); iter.hasNext();) {
                Subscription sub = (Subscription) iter.next();
                if (sub.getConsumerInfo().getPriority() > rc) {
                    rc = sub.getConsumerInfo().getPriority();
                }
            }
        }
        return rc;
    }

    public MessageStore getMessageStore() {
        return store;
    }

}
