/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.JMSException;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.cursors.FilePendingMessageCursor;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.broker.region.policy.MessageEvictionStrategy;
import org.apache.activemq.broker.region.policy.OldestMessageEvictionStrategy;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TopicSubscription extends AbstractSubscription {

    private static final Log LOG = LogFactory.getLog(TopicSubscription.class);
    private static final AtomicLong CURSOR_NAME_COUNTER = new AtomicLong(0);
    
    protected PendingMessageCursor matched;
    protected final SystemUsage usageManager;
    protected AtomicLong dispatchedCounter = new AtomicLong();
       
    boolean singleDestination = true;
    Destination destination;

    private int maximumPendingMessages = -1;
    private MessageEvictionStrategy messageEvictionStrategy = new OldestMessageEvictionStrategy();
    private int discarded;
    private final Object matchedListMutex = new Object();
    private final AtomicLong enqueueCounter = new AtomicLong(0);
    private final AtomicLong dequeueCounter = new AtomicLong(0);
    private int memoryUsageHighWaterMark = 95;

    public TopicSubscription(Broker broker,ConnectionContext context, ConsumerInfo info, SystemUsage usageManager) throws Exception {
        super(broker, context, info);
        this.usageManager = usageManager;
        String matchedName = "TopicSubscription:" + CURSOR_NAME_COUNTER.getAndIncrement() + "[" + info.getConsumerId().toString() + "]";
        if (info.getDestination().isTemporary() || broker == null || broker.getTempDataStore()==null ) {
            this.matched = new VMPendingMessageCursor();
        } else {
            this.matched = new FilePendingMessageCursor(broker,matchedName);
        }
    }

    public void init() throws Exception {
        this.matched.setSystemUsage(usageManager);
        this.matched.start();
    }

    public void add(MessageReference node) throws Exception {
        enqueueCounter.incrementAndGet();
        node.incrementReferenceCount();
        if (!isFull() && matched.isEmpty()  && !isSlave()) {
            // if maximumPendingMessages is set we will only discard messages
            // which
            // have not been dispatched (i.e. we allow the prefetch buffer to be
            // filled)
            dispatch(node);
        } else {
            if (maximumPendingMessages != 0) {
                synchronized (matchedListMutex) {
                    matched.addMessageLast(node);
                    // NOTE - be careful about the slaveBroker!
                    if (maximumPendingMessages > 0) {
                        // calculate the high water mark from which point we
                        // will eagerly evict expired messages
                        int max = messageEvictionStrategy.getEvictExpiredMessagesHighWatermark();
                        if (maximumPendingMessages > 0 && maximumPendingMessages < max) {
                            max = maximumPendingMessages;
                        }
                        if (!matched.isEmpty() && matched.size() > max) {
                            removeExpiredMessages();
                        }
                        // lets discard old messages as we are a slow consumer
                        while (!matched.isEmpty() && matched.size() > maximumPendingMessages) {
                            int pageInSize = matched.size() - maximumPendingMessages;
                            // only page in a 1000 at a time - else we could
                            // blow da memory
                            pageInSize = Math.max(1000, pageInSize);
                            LinkedList list = null;
                            MessageReference[] oldMessages=null;
                            synchronized(matched){
                            list = matched.pageInList(pageInSize);
                            	oldMessages = messageEvictionStrategy.evictMessages(list);
                            }
                            int messagesToEvict = 0;
                            if (oldMessages != null){
	                            messagesToEvict = oldMessages.length;
	                            for (int i = 0; i < messagesToEvict; i++) {
	                                MessageReference oldMessage = oldMessages[i];
	                                discard(oldMessage);
	                            }
                            }
                            // lets avoid an infinite loop if we are given a bad
                            // eviction strategy
                            // for a bad strategy lets just not evict
                            if (messagesToEvict == 0) {
                                LOG.warn("No messages to evict returned from eviction strategy: " + messageEvictionStrategy);
                                break;
                            }
                        }
                    }
                }
                dispatchMatched();
            }
        }
    }

    /**
     * Discard any expired messages from the matched list. Called from a
     * synchronized block.
     * 
     * @throws IOException
     */
    protected void removeExpiredMessages() throws IOException {
        try {
            matched.reset();
            while (matched.hasNext()) {
                MessageReference node = matched.next();
                if (broker.isExpired(node)) {
                    matched.remove();
                    dispatchedCounter.incrementAndGet();
                    node.decrementReferenceCount();
                    broker.messageExpired(getContext(), node);
                    break;
                }
            }
        } finally {
            matched.release();
        }
    }

    public void processMessageDispatchNotification(MessageDispatchNotification mdn) {
        synchronized (matchedListMutex) {
            try {
                matched.reset();
                while (matched.hasNext()) {
                    MessageReference node = matched.next();
                    if (node.getMessageId().equals(mdn.getMessageId())) {
                        matched.remove();
                        dispatchedCounter.incrementAndGet();
                        node.decrementReferenceCount();
                        break;
                    }
                }
            } finally {
                matched.release();
            }
        }
    }

    public synchronized void acknowledge(final ConnectionContext context, final MessageAck ack) throws Exception {
        // Handle the standard acknowledgment case.
        if (ack.isStandardAck() || ack.isPoisonAck()) {
            if (context.isInTransaction()) {
                context.getTransaction().addSynchronization(new Synchronization() {

                    public void afterCommit() throws Exception {
                       synchronized (TopicSubscription.this) {
                            if (singleDestination && destination != null) {
                                destination.getDestinationStatistics().getDequeues().add(ack.getMessageCount());
                            }
                        }
                        dequeueCounter.addAndGet(ack.getMessageCount());
                        dispatchMatched();
                    }
                });
            } else {
                if (singleDestination && destination != null) {
                    destination.getDestinationStatistics().getDequeues().add(ack.getMessageCount());
                    destination.getDestinationStatistics().getInflight().subtract(ack.getMessageCount());
                }
                dequeueCounter.addAndGet(ack.getMessageCount());
            }
            dispatchMatched();
            return;
        } else if (ack.isDeliveredAck()) {
            // Message was delivered but not acknowledged: update pre-fetch
            // counters.
            dequeueCounter.addAndGet(ack.getMessageCount());
            destination.getDestinationStatistics().getInflight().subtract(ack.getMessageCount());
            dispatchMatched();
            return;
        }
        throw new JMSException("Invalid acknowledgment: " + ack);
    }

    public Response pullMessage(ConnectionContext context, MessagePull pull) throws Exception {
        // not supported for topics
        return null;
    }

    public int getPendingQueueSize() {
        return matched();
    }

    public int getDispatchedQueueSize() {
        return (int)(dispatchedCounter.get() - dequeueCounter.get());
    }

    public int getMaximumPendingMessages() {
        return maximumPendingMessages;
    }

    public long getDispatchedCounter() {
        return dispatchedCounter.get();
    }

    public long getEnqueueCounter() {
        return enqueueCounter.get();
    }

    public long getDequeueCounter() {
        return dequeueCounter.get();
    }

    /**
     * @return the number of messages discarded due to being a slow consumer
     */
    public int discarded() {
        synchronized (matchedListMutex) {
            return discarded;
        }
    }

    /**
     * @return the number of matched messages (messages targeted for the
     *         subscription but not yet able to be dispatched due to the
     *         prefetch buffer being full).
     */
    public int matched() {
        synchronized (matchedListMutex) {
            return matched.size();
        }
    }

    /**
     * Sets the maximum number of pending messages that can be matched against
     * this consumer before old messages are discarded.
     */
    public void setMaximumPendingMessages(int maximumPendingMessages) {
        this.maximumPendingMessages = maximumPendingMessages;
    }

    public MessageEvictionStrategy getMessageEvictionStrategy() {
        return messageEvictionStrategy;
    }

    /**
     * Sets the eviction strategy used to decide which message to evict when the
     * slow consumer needs to discard messages
     */
    public void setMessageEvictionStrategy(MessageEvictionStrategy messageEvictionStrategy) {
        this.messageEvictionStrategy = messageEvictionStrategy;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    public boolean isFull() {
        return getDispatchedQueueSize()  >= info.getPrefetchSize();
    }
    
    public int getInFlightSize() {
        return getDispatchedQueueSize();
    }
    
    
    /**
     * @return true when 60% or more room is left for dispatching messages
     */
    public boolean isLowWaterMark() {
        return getDispatchedQueueSize() <= (info.getPrefetchSize() * .4);
    }

    /**
     * @return true when 10% or less room is left for dispatching messages
     */
    public boolean isHighWaterMark() {
        return getDispatchedQueueSize() >= (info.getPrefetchSize() * .9);
    }

    /**
     * @param memoryUsageHighWaterMark the memoryUsageHighWaterMark to set
     */
    public void setMemoryUsageHighWaterMark(int memoryUsageHighWaterMark) {
        this.memoryUsageHighWaterMark = memoryUsageHighWaterMark;
    }

    /**
     * @return the memoryUsageHighWaterMark
     */
    public int getMemoryUsageHighWaterMark() {
        return this.memoryUsageHighWaterMark;
    }

    /**
     * @return the usageManager
     */
    public SystemUsage getUsageManager() {
        return this.usageManager;
    }

    /**
     * @return the matched
     */
    public PendingMessageCursor getMatched() {
        return this.matched;
    }

    /**
     * @param matched the matched to set
     */
    public void setMatched(PendingMessageCursor matched) {
        this.matched = matched;
    }

    /**
     * inform the MessageConsumer on the client to change it's prefetch
     * 
     * @param newPrefetch
     */
    public void updateConsumerPrefetch(int newPrefetch) {
        if (context != null && context.getConnection() != null && context.getConnection().isManageable()) {
            ConsumerControl cc = new ConsumerControl();
            cc.setConsumerId(info.getConsumerId());
            cc.setPrefetch(newPrefetch);
            context.getConnection().dispatchAsync(cc);
        }
    }

    private void dispatchMatched() throws IOException {       
        synchronized (matchedListMutex) {
            if (!matched.isEmpty() && !isFull()) {
                try {
                    matched.reset();
                   
                    while (matched.hasNext() && !isFull()) {
                        MessageReference message = (MessageReference) matched
                                .next();
                        matched.remove();
                        // Message may have been sitting in the matched list a
                        // while
                        // waiting for the consumer to ak the message.
                        if (message.isExpired()) {
                            discard(message);
                            continue; // just drop it.
                        }
                        dispatch(message);
                    }
                } finally {
                    matched.release();
                }
            }
        }
    }

    private void dispatch(final MessageReference node) throws IOException {
        Message message = (Message)node;
        // Make sure we can dispatch a message.
        MessageDispatch md = new MessageDispatch();
        md.setMessage(message);
        md.setConsumerId(info.getConsumerId());
        md.setDestination(node.getRegionDestination().getActiveMQDestination());
        dispatchedCounter.incrementAndGet();
        // Keep track if this subscription is receiving messages from a single
        // destination.
        if (singleDestination) {
            if (destination == null) {
                destination = node.getRegionDestination();
            } else {
                if (destination != node.getRegionDestination()) {
                    singleDestination = false;
                }
            }
        }
        if (info.isDispatchAsync()) {
            md.setTransmitCallback(new Runnable() {

                public void run() {
                    node.getRegionDestination().getDestinationStatistics().getDispatched().increment();
                    node.getRegionDestination().getDestinationStatistics().getInflight().increment();
                    node.decrementReferenceCount();
                }
            });
            context.getConnection().dispatchAsync(md);
        } else {
            context.getConnection().dispatchSync(md);
            node.getRegionDestination().getDestinationStatistics().getDispatched().increment();
            node.getRegionDestination().getDestinationStatistics().getInflight().increment();
            node.decrementReferenceCount();
        }
    }

    private void discard(MessageReference message) {
        message.decrementReferenceCount();
        matched.remove(message);
        discarded++;
        dequeueCounter.incrementAndGet();
        destination.getDestinationStatistics().getDequeues().increment();
        destination.getDestinationStatistics().getInflight().decrement();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Discarding message " + message);
        }
        broker.getRoot().sendToDeadLetterQueue(getContext(), message);
    }

    public String toString() {
        return "TopicSubscription:" + " consumer=" + info.getConsumerId() + ", destinations=" + destinations.size() + ", dispatched=" + getDispatchedQueueSize() + ", delivered="
               + getDequeueCounter() + ", matched=" + matched() + ", discarded=" + discarded();
    }

    public void destroy() {
        synchronized (matchedListMutex) {
            try {
                matched.destroy();
            } catch (Exception e) {
                LOG.warn("Failed to destroy cursor", e);
            }
        }
    }

    public int getPrefetchSize() {
        return (int)info.getPrefetchSize();
    }
    
    /**
     * Get the list of inflight messages
     * @return the list
     */
    public synchronized List<MessageReference> getInFlightMessages(){
    	List<MessageReference> result = new ArrayList<MessageReference>();
        synchronized(matched) {
            result.addAll(matched.pageInList(1000));
        }
        return result;
    }

}
