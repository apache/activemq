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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
import org.apache.activemq.util.BrokerSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

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
    private final LinkedList messages = new LinkedList();
    protected final Valve dispatchValve = new Valve(true);
    protected final UsageManager usageManager;
    protected final DestinationStatistics destinationStatistics = new DestinationStatistics();

    private LockOwner exclusiveOwner;
    private MessageGroupMap messageGroupOwners;
    private int messageGroupHashBucketCount = 1024;

    protected long garbageSize = 0;
    protected long garbageSizeBeforeCollection = 1000;
    private DispatchPolicy dispatchPolicy = new RoundRobinDispatchPolicy();
    protected final MessageStore store;
    protected int highestSubscriptionPriority;
    private DeadLetterStrategy deadLetterStrategy = new SharedDeadLetterStrategy();

    public Queue(ActiveMQDestination destination, final UsageManager memoryManager, MessageStore store,
            DestinationStatistics parentStats, TaskRunnerFactory taskFactory) throws Exception {
        this.destination = destination;
        this.usageManager = new UsageManager(memoryManager);
        this.usageManager.setLimit(Long.MAX_VALUE);
        this.store = store;

        // Let the store know what usage manager we are using so that he can flush messages to disk
        // when usage gets high.
        if( store!=null ) {
            store.setUsageManager(usageManager);
        }
        
        destinationStatistics.setParent(parentStats);
        this.log = LogFactory.getLog(getClass().getName() + "." + destination.getPhysicalName());

        if (store != null) {
            // Restore the persistent messages.
            store.recover(new MessageRecoveryListener() {
                public void recoverMessage(Message message) {
                    message.setRegionDestination(Queue.this);
                    MessageReference reference = createMessageReference(message);
                    synchronized (messages) {
                        messages.add(reference);
                    }
                    reference.decrementReferenceCount();
                    destinationStatistics.getMessages().increment();
                }

                public void recoverMessageReference(String messageReference) throws Exception {
                    throw new RuntimeException("Should not be called.");
                }
                
                public void finished(){
                }
            });
        }
    }

    public synchronized boolean lock(MessageReference node, LockOwner lockOwner) {
        if (exclusiveOwner == lockOwner)
            return true;
        if (exclusiveOwner != null)
            return false;
        if (lockOwner.getLockPriority() < highestSubscriptionPriority)
            return false;
        if (lockOwner.isLockExclusive()) {
            exclusiveOwner = lockOwner;
        }
        return true;
    }

    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
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
                    if (node.isDropped()) {
                        continue;
                    }

                    try {
                        msgContext.setMessageReference(node);
                        if (sub.matches(node, msgContext)) {
                            sub.add(node);
                        }
                    } catch (IOException e) {
                        log.warn("Could not load message: " + e, e);
                    }
                }
            }

        } finally {
            msgContext.clear();
            dispatchValve.turnOn();
        }
    }

    public void removeSubscription(ConnectionContext context, Subscription sub) throws Exception {

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

            if (!sub.getConsumerInfo().isBrowser()) {
                MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
                try {
                    msgContext.setDestination(destination);

                    // lets copy the messages to dispatch to avoid deadlock
                    List messagesToDispatch = new ArrayList();
                    synchronized (messages) {
                        for (Iterator iter = messages.iterator(); iter.hasNext();) {
                            IndirectMessageReference node = (IndirectMessageReference) iter.next();
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
                    
                    // now lets dispatch from the copy of the collection to avoid deadlocks
                    for (Iterator iter = messagesToDispatch.iterator(); iter.hasNext();) {
                        IndirectMessageReference node = (IndirectMessageReference) iter.next();
                        node.incrementRedeliveryCounter();
                        node.unlock();
                        msgContext.setMessageReference(node);
                        dispatchPolicy.dispatch(context, node, msgContext, consumers);
                    }
                }
                finally {
                    msgContext.clear();
                }
            }
        } finally {
            dispatchValve.turnOn();
        }

    }

    public void send(final ConnectionContext context, final Message message) throws Exception {

        if (context.isProducerFlowControl())
            usageManager.waitForSpace();

        message.setRegionDestination(this);

        if (store != null && message.isPersistent())
            store.addMessage(context, message);

        final MessageReference node = createMessageReference(message);
        try {

            if (context.isInTransaction()) {
                context.getTransaction().addSynchronization(new Synchronization() {
                    public void afterCommit() throws Exception {
                        dispatch(context, node, message);
                    }
                });
            } else {
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

    public void acknowledge(ConnectionContext context, Subscription sub, MessageAck ack, MessageReference node) throws IOException {
        if (store != null && node.isPersistent()) {
            // the original ack may be a ranged ack, but we are trying to delete a specific 
            // message store here so we need to convert to a non ranged ack.
            if( ack.getMessageCount() > 0 ) {
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

    public Message loadMessage(MessageId messageId) throws IOException {
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
        return "Queue: destination=" + destination.getPhysicalName() + ", subscriptions=" + consumers.size()
                + ", memory=" + usageManager.getPercentUsage() + "%, size=" + size + ", in flight groups="
                + messageGroupOwners;
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
            messageGroupOwners = new MessageGroupHashBucket(messageGroupHashBucketCount);
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

    private void dispatch(ConnectionContext context, MessageReference node, Message message) throws Exception {

        dispatchValve.increment();
        MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
        try {
            destinationStatistics.onMessageEnqueue(message);
            synchronized (messages) {
                messages.add(node);
            }

            synchronized (consumers) {
                if (consumers.isEmpty()) {
                    log.debug("No subscriptions registered, will not dispatch message at this time.");
                    return;
                }
            }

            msgContext.setDestination(destination);
            msgContext.setMessageReference(node);

            dispatchPolicy.dispatch(context, node, msgContext, consumers);
        } finally {
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

    public Message[] browse() {

        ArrayList l = new ArrayList();
        synchronized (messages) {
            for (Iterator iter = messages.iterator(); iter.hasNext();) {
                try {
                    MessageReference r = (MessageReference) iter.next();
                    r.incrementReferenceCount();
                    try {
                        Message m = r.getMessage();
                        if (m != null) {
                            l.add(m);
                        }
                    } finally {
                        r.decrementReferenceCount();
                    }
                } catch (IOException e) {
                }
            }
        }

        return (Message[]) l.toArray(new Message[l.size()]);
    }

    public boolean removeMessage(String messageId) {
        synchronized (messages) {
            ConnectionContext c = new ConnectionContext();
            for (Iterator iter = messages.iterator(); iter.hasNext();) {
                try {
                    IndirectMessageReference r = (IndirectMessageReference) iter.next();
                    if (messageId.equals(r.getMessageId().toString())) {
                        
                        // We should only delete messages that can be locked.
                        if( r.lock(LockOwner.HIGH_PRIORITY_LOCK_OWNER) )  {
                            MessageAck ack = new MessageAck();
                            ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
                            ack.setDestination(destination);
                            ack.setMessageID(r.getMessageId());
                            acknowledge(c, null, ack, r);
                            r.drop();
                            dropEvent();
                            iter.remove();
                            return true;
                        }
                    }
                } catch (IOException e) {
                }
            }
        }
        return false;
    }

    public Message getMessage(String messageId) {
        synchronized (messages) {
            for (Iterator iter = messages.iterator(); iter.hasNext();) {
                try {
                    MessageReference r = (MessageReference) iter.next();
                    if (messageId.equals(r.getMessageId().toString())) {
                        r.incrementReferenceCount();
                        try {
                            Message m = r.getMessage();
                            if (m != null) {
                                return m;
                            }
                        } finally {
                            r.decrementReferenceCount();
                        }
                        break;
                    }
                } catch (IOException e) {
                }
            }
        }
        return null;
    }

    public void purge() {
        synchronized (messages) {
            ConnectionContext c = new ConnectionContext();
            for (Iterator iter = messages.iterator(); iter.hasNext();) {
                try {
                    IndirectMessageReference r = (IndirectMessageReference) iter.next();
                    
                    // We should only delete messages that can be locked.
                    if( r.lock(LockOwner.HIGH_PRIORITY_LOCK_OWNER) )  {
                        MessageAck ack = new MessageAck();
                        ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
                        ack.setDestination(destination);
                        ack.setMessageID(r.getMessageId());
                        acknowledge(c, null, ack, r);
                        r.drop();
                        dropEvent();
                        iter.remove();
                    }
                } catch (IOException e) {
                }
            }
        }
    }

    public boolean copyMessageTo(ConnectionContext context, String messageId, ActiveMQDestination dest) throws Exception {
        synchronized (messages) {
            for (Iterator iter = messages.iterator(); iter.hasNext();) {
                try {
                    MessageReference r = (MessageReference) iter.next();
                    if (messageId.equals(r.getMessageId().toString())) {
                        r.incrementReferenceCount();
                        try {
                            Message m = r.getMessage();
                            BrokerSupport.resend(context, m, dest);                            
                        } finally {
                            r.decrementReferenceCount();
                        }                        
                        return true;
                    }
                } catch (IOException e) {
                }
            }
        }
        return false;
    }



}
