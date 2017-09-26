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

import org.apache.activemq.ActiveMQMessageAudit;
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
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.transport.TransmitCallback;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicSubscription extends AbstractSubscription {

    private static final Logger LOG = LoggerFactory.getLogger(TopicSubscription.class);
    private static final AtomicLong CURSOR_NAME_COUNTER = new AtomicLong(0);

    protected PendingMessageCursor matched;
    protected final SystemUsage usageManager;
    boolean singleDestination = true;
    Destination destination;
    private final Scheduler scheduler;

    private int maximumPendingMessages = -1;
    private MessageEvictionStrategy messageEvictionStrategy = new OldestMessageEvictionStrategy();
    private int discarded;
    private final Object matchedListMutex = new Object();
    private int memoryUsageHighWaterMark = 95;
    // allow duplicate suppression in a ring network of brokers
    protected int maxProducersToAudit = 1024;
    protected int maxAuditDepth = 1000;
    protected boolean enableAudit = false;
    protected ActiveMQMessageAudit audit;
    protected boolean active = false;
    protected boolean discarding = false;

    //Used for inflight message size calculations
    protected final Object dispatchLock = new Object();
    protected final List<MessageReference> dispatched = new ArrayList<MessageReference>();

    public TopicSubscription(Broker broker,ConnectionContext context, ConsumerInfo info, SystemUsage usageManager) throws Exception {
        super(broker, context, info);
        this.usageManager = usageManager;
        String matchedName = "TopicSubscription:" + CURSOR_NAME_COUNTER.getAndIncrement() + "[" + info.getConsumerId().toString() + "]";
        if (info.getDestination().isTemporary() || broker.getTempDataStore()==null ) {
            this.matched = new VMPendingMessageCursor(false);
        } else {
            this.matched = new FilePendingMessageCursor(broker,matchedName,false);
        }

        this.scheduler = broker.getScheduler();
    }

    public void init() throws Exception {
        this.matched.setSystemUsage(usageManager);
        this.matched.setMemoryUsageHighWaterMark(getCursorMemoryHighWaterMark());
        this.matched.start();
        if (enableAudit) {
            audit= new ActiveMQMessageAudit(maxAuditDepth, maxProducersToAudit);
        }
        this.active=true;
    }

    @Override
    public void add(MessageReference node) throws Exception {
        if (isDuplicate(node)) {
            return;
        }
        // Lets use an indirect reference so that we can associate a unique
        // locator /w the message.
        node = new IndirectMessageReference(node.getMessage());
        getSubscriptionStatistics().getEnqueues().increment();
        synchronized (matchedListMutex) {
            // if this subscriber is already discarding a message, we don't want to add
            // any more messages to it as those messages can only be advisories generated in the process,
            // which can trigger the recursive call loop
            if (discarding) return;

            if (!isFull() && matched.isEmpty()) {
                // if maximumPendingMessages is set we will only discard messages which
                // have not been dispatched (i.e. we allow the prefetch buffer to be filled)
                dispatch(node);
                setSlowConsumer(false);
            } else {
                if (info.getPrefetchSize() > 1 && matched.size() > info.getPrefetchSize()) {
                    // Slow consumers should log and set their state as such.
                    if (!isSlowConsumer()) {
                        LOG.warn("{}: has twice its prefetch limit pending, without an ack; it appears to be slow", toString());
                        setSlowConsumer(true);
                        for (Destination dest: destinations) {
                            dest.slowConsumer(getContext(), this);
                        }
                    }
                }
                if (maximumPendingMessages != 0) {
                    boolean warnedAboutWait = false;
                    while (active) {
                        while (matched.isFull()) {
                            if (getContext().getStopping().get()) {
                                LOG.warn("{}: stopped waiting for space in pendingMessage cursor for: {}", toString(), node.getMessageId());
                                getSubscriptionStatistics().getEnqueues().decrement();
                                return;
                            }
                            if (!warnedAboutWait) {
                                LOG.info("{}: Pending message cursor [{}] is full, temp usag ({}%) or memory usage ({}%) limit reached, blocking message add() pending the release of resources.",
                                        new Object[]{
                                                toString(),
                                                matched,
                                                matched.getSystemUsage().getTempUsage().getPercentUsage(),
                                                matched.getSystemUsage().getMemoryUsage().getPercentUsage()
                                        });
                                warnedAboutWait = true;
                            }
                            matchedListMutex.wait(20);
                        }
                        // Temporary storage could be full - so just try to add the message
                        // see https://issues.apache.org/activemq/browse/AMQ-2475
                        if (matched.tryAddMessageLast(node, 10)) {
                            break;
                        }
                    }
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
                            // only page in a 1000 at a time - else we could blow the memory
                            pageInSize = Math.max(1000, pageInSize);
                            LinkedList<MessageReference> list = null;
                            MessageReference[] oldMessages=null;
                            synchronized(matched){
                                list = matched.pageInList(pageInSize);
                                oldMessages = messageEvictionStrategy.evictMessages(list);
                                for (MessageReference ref : list) {
                                    ref.decrementReferenceCount();
                                }
                            }
                            int messagesToEvict = 0;
                            if (oldMessages != null){
                                messagesToEvict = oldMessages.length;
                                for (int i = 0; i < messagesToEvict; i++) {
                                    MessageReference oldMessage = oldMessages[i];
                                    discard(oldMessage);
                                }
                            }
                            // lets avoid an infinite loop if we are given a bad eviction strategy
                            // for a bad strategy lets just not evict
                            if (messagesToEvict == 0) {
                                LOG.warn("No messages to evict returned for {} from eviction strategy: {} out of {} candidates", new Object[]{
                                        destination, messageEvictionStrategy, list.size()
                                });
                                break;
                            }
                        }
                    }
                    dispatchMatched();
                }
            }
        }
    }

    private boolean isDuplicate(MessageReference node) {
        boolean duplicate = false;
        if (enableAudit && audit != null) {
            duplicate = audit.isDuplicate(node);
            if (LOG.isDebugEnabled()) {
                if (duplicate) {
                    LOG.debug("{}, ignoring duplicate add: {}", this, node.getMessageId());
                }
            }
        }
        return duplicate;
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
                node.decrementReferenceCount();
                if (node.isExpired()) {
                    matched.remove();
                    node.decrementReferenceCount();
                    if (broker.isExpired(node)) {
                        ((Destination) node.getRegionDestination()).getDestinationStatistics().getExpired().increment();
                        broker.messageExpired(getContext(), node, this);
                    }
                    break;
                }
            }
        } finally {
            matched.release();
        }
    }

    @Override
    public void processMessageDispatchNotification(MessageDispatchNotification mdn) {
        synchronized (matchedListMutex) {
            try {
                matched.reset();
                while (matched.hasNext()) {
                    MessageReference node = matched.next();
                    node.decrementReferenceCount();
                    if (node.getMessageId().equals(mdn.getMessageId())) {
                        synchronized(dispatchLock) {
                            matched.remove();
                            getSubscriptionStatistics().getDispatched().increment();
                            dispatched.add(node);
                            getSubscriptionStatistics().getInflightMessageSize().addSize(node.getSize());
                            node.decrementReferenceCount();
                        }
                        break;
                    }
                }
            } finally {
                matched.release();
            }
        }
    }

    @Override
    public synchronized void acknowledge(final ConnectionContext context, final MessageAck ack) throws Exception {
        super.acknowledge(context, ack);

        if (ack.isStandardAck()) {
            updateStatsOnAck(context, ack);
        } else if (ack.isPoisonAck()) {
            if (ack.isInTransaction()) {
                throw new JMSException("Poison ack cannot be transacted: " + ack);
            }
            updateStatsOnAck(context, ack);
            contractPrefetchExtension(ack.getMessageCount());
        } else if (ack.isIndividualAck()) {
            updateStatsOnAck(context, ack);
            if (ack.isInTransaction()) {
                expandPrefetchExtension(1);
            }
        } else if (ack.isExpiredAck()) {
            updateStatsOnAck(ack);
            contractPrefetchExtension(ack.getMessageCount());
        } else if (ack.isDeliveredAck()) {
            // Message was delivered but not acknowledged: update pre-fetch counters.
           expandPrefetchExtension(ack.getMessageCount());
        } else if (ack.isRedeliveredAck()) {
            // No processing for redelivered needed
            return;
        } else {
            throw new JMSException("Invalid acknowledgment: " + ack);
        }

        dispatchMatched();
    }

    private void updateStatsOnAck(final ConnectionContext context, final MessageAck ack) {
        if (context.isInTransaction()) {
            context.getTransaction().addSynchronization(new Synchronization() {

                @Override
                public void afterRollback() {
                    contractPrefetchExtension(ack.getMessageCount());
                }

                @Override
                public void afterCommit() throws Exception {
                    contractPrefetchExtension(ack.getMessageCount());
                    updateStatsOnAck(ack);
                    dispatchMatched();
                }
            });
        } else {
            updateStatsOnAck(ack);
        }
    }

    @Override
    public Response pullMessage(ConnectionContext context, final MessagePull pull) throws Exception {

        // The slave should not deliver pull messages.
        if (getPrefetchSize() == 0) {

            final long currentDispatchedCount = getSubscriptionStatistics().getDispatched().getCount();
            prefetchExtension.set(pull.getQuantity());
            dispatchMatched();

            // If there was nothing dispatched.. we may need to setup a timeout.
            if (currentDispatchedCount == getSubscriptionStatistics().getDispatched().getCount() || pull.isAlwaysSignalDone()) {

                // immediate timeout used by receiveNoWait()
                if (pull.getTimeout() == -1) {
                    // Send a NULL message to signal nothing pending.
                    dispatch(null);
                    prefetchExtension.set(0);
                }

                if (pull.getTimeout() > 0) {
                    scheduler.executeAfterDelay(new Runnable() {

                        @Override
                        public void run() {
                            pullTimeout(currentDispatchedCount, pull.isAlwaysSignalDone());
                        }
                    }, pull.getTimeout());
                }
            }
        }
        return null;
    }

    /**
     * Occurs when a pull times out. If nothing has been dispatched since the
     * timeout was setup, then send the NULL message.
     */
    private final void pullTimeout(long currentDispatchedCount, boolean alwaysSendDone) {
        synchronized (matchedListMutex) {
            if (currentDispatchedCount == getSubscriptionStatistics().getDispatched().getCount() || alwaysSendDone) {
                try {
                    dispatch(null);
                } catch (Exception e) {
                    context.getConnection().serviceException(e);
                } finally {
                    prefetchExtension.set(0);
                }
            }
        }
    }

    /**
     * Update the statistics on message ack.
     * @param ack
     */
    private void updateStatsOnAck(final MessageAck ack) {
        synchronized(dispatchLock) {
            boolean inAckRange = false;
            List<MessageReference> removeList = new ArrayList<MessageReference>();
            for (final MessageReference node : dispatched) {
                MessageId messageId = node.getMessageId();
                if (ack.getFirstMessageId() == null
                        || ack.getFirstMessageId().equals(messageId)) {
                    inAckRange = true;
                }
                if (inAckRange) {
                    removeList.add(node);
                    if (ack.getLastMessageId().equals(messageId)) {
                        break;
                    }
                }
            }

            for (final MessageReference node : removeList) {
                dispatched.remove(node);
                getSubscriptionStatistics().getInflightMessageSize().addSize(-node.getSize());
                getSubscriptionStatistics().getDequeues().increment();
                ((Destination)node.getRegionDestination()).getDestinationStatistics().getDequeues().increment();
                ((Destination)node.getRegionDestination()).getDestinationStatistics().getInflight().decrement();
                if (info.isNetworkSubscription()) {
                    ((Destination)node.getRegionDestination()).getDestinationStatistics().getForwards().add(ack.getMessageCount());
                }
                if (ack.isExpiredAck()) {
                    destination.getDestinationStatistics().getExpired().add(ack.getMessageCount());
                }
                if (!ack.isInTransaction()) {
                    contractPrefetchExtension(1);
                }
            }
        }
    }

    @Override
    public int countBeforeFull() {
        return getPrefetchSize() == 0 ? prefetchExtension.get() : info.getPrefetchSize() + prefetchExtension.get() - getDispatchedQueueSize();
    }

    @Override
    public int getPendingQueueSize() {
        return matched();
    }

    @Override
    public long getPendingMessageSize() {
        synchronized (matchedListMutex) {
            return matched.messageSize();
        }
    }

    @Override
    public int getDispatchedQueueSize() {
        return (int)(getSubscriptionStatistics().getDispatched().getCount() -
                     getSubscriptionStatistics().getDequeues().getCount());
    }

    public int getMaximumPendingMessages() {
        return maximumPendingMessages;
    }

    @Override
    public long getDispatchedCounter() {
        return getSubscriptionStatistics().getDispatched().getCount();
    }

    @Override
    public long getEnqueueCounter() {
        return getSubscriptionStatistics().getEnqueues().getCount();
    }

    @Override
    public long getDequeueCounter() {
        return getSubscriptionStatistics().getDequeues().getCount();
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

    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    public synchronized void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
        if (audit != null) {
            audit.setMaximumNumberOfProducersToTrack(maxProducersToAudit);
        }
    }

    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }

    public synchronized void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
        if (audit != null) {
            audit.setAuditDepth(maxAuditDepth);
        }
    }

    public boolean isEnableAudit() {
        return enableAudit;
    }

    public synchronized void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
        if (enableAudit && audit == null) {
            audit = new ActiveMQMessageAudit(maxAuditDepth,maxProducersToAudit);
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    @Override
    public boolean isFull() {
        return getPrefetchSize() == 0 ? prefetchExtension.get() == 0 : getDispatchedQueueSize() - prefetchExtension.get() >= info.getPrefetchSize();
    }

    @Override
    public int getInFlightSize() {
        return getDispatchedQueueSize();
    }

    /**
     * @return true when 60% or more room is left for dispatching messages
     */
    @Override
    public boolean isLowWaterMark() {
        return (getDispatchedQueueSize() - prefetchExtension.get()) <= (info.getPrefetchSize() * .4);
    }

    /**
     * @return true when 10% or less room is left for dispatching messages
     */
    @Override
    public boolean isHighWaterMark() {
        return (getDispatchedQueueSize() - prefetchExtension.get()) >= (info.getPrefetchSize() * .9);
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
    @Override
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
                        MessageReference message = matched.next();
                        message.decrementReferenceCount();
                        matched.remove();
                        // Message may have been sitting in the matched list a while
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
        Message message = node != null ? node.getMessage() : null;
        if (node != null) {
            node.incrementReferenceCount();
        }
        // Make sure we can dispatch a message.
        MessageDispatch md = new MessageDispatch();
        md.setMessage(message);
        md.setConsumerId(info.getConsumerId());
        if (node != null) {
            md.setDestination(((Destination)node.getRegionDestination()).getActiveMQDestination());
            synchronized(dispatchLock) {
                getSubscriptionStatistics().getDispatched().increment();
                dispatched.add(node);
                getSubscriptionStatistics().getInflightMessageSize().addSize(node.getSize());
            }

            // Keep track if this subscription is receiving messages from a single destination.
            if (singleDestination) {
                if (destination == null) {
                    destination = (Destination)node.getRegionDestination();
                } else {
                    if (destination != node.getRegionDestination()) {
                        singleDestination = false;
                    }
                }
            }

            if (getPrefetchSize() == 0) {
                decrementPrefetchExtension(1);
            }
        }

        if (info.isDispatchAsync()) {
            if (node != null) {
                md.setTransmitCallback(new TransmitCallback() {

                    @Override
                    public void onSuccess() {
                        Destination regionDestination = (Destination) node.getRegionDestination();
                        regionDestination.getDestinationStatistics().getDispatched().increment();
                        regionDestination.getDestinationStatistics().getInflight().increment();
                        node.decrementReferenceCount();
                    }

                    @Override
                    public void onFailure() {
                        Destination regionDestination = (Destination) node.getRegionDestination();
                        regionDestination.getDestinationStatistics().getDispatched().increment();
                        regionDestination.getDestinationStatistics().getInflight().increment();
                        node.decrementReferenceCount();
                    }
                });
            }
            context.getConnection().dispatchAsync(md);
        } else {
            context.getConnection().dispatchSync(md);
            if (node != null) {
                Destination regionDestination = (Destination) node.getRegionDestination();
                regionDestination.getDestinationStatistics().getDispatched().increment();
                regionDestination.getDestinationStatistics().getInflight().increment();
                node.decrementReferenceCount();
            }
        }
    }

    private void discard(MessageReference message) {
        discarding = true;
        try {
            message.decrementReferenceCount();
            matched.remove(message);
            discarded++;
            if (destination != null) {
                destination.getDestinationStatistics().getDequeues().increment();
            }
            LOG.debug("{}, discarding message {}", this, message);
            Destination dest = (Destination) message.getRegionDestination();
            if (dest != null) {
                dest.messageDiscarded(getContext(), this, message);
            }
            broker.getRoot().sendToDeadLetterQueue(getContext(), message, this, new Throwable("TopicSubDiscard. ID:" + info.getConsumerId()));
        } finally {
            discarding = false;
        }
    }

    @Override
    public String toString() {
        return "TopicSubscription:" + " consumer=" + info.getConsumerId() + ", destinations=" + destinations.size() + ", dispatched=" + getDispatchedQueueSize() + ", delivered="
                + getDequeueCounter() + ", matched=" + matched() + ", discarded=" + discarded() + ", prefetchExtension=" + prefetchExtension.get()
                + ", usePrefetchExtension=" + isUsePrefetchExtension();
    }

    @Override
    public void destroy() {
        this.active=false;
        synchronized (matchedListMutex) {
            try {
                matched.destroy();
            } catch (Exception e) {
                LOG.warn("Failed to destroy cursor", e);
            }
        }
        setSlowConsumer(false);
        synchronized(dispatchLock) {
            dispatched.clear();
        }
    }

    @Override
    public int getPrefetchSize() {
        return info.getPrefetchSize();
    }

    @Override
    public void setPrefetchSize(int newSize) {
        info.setPrefetchSize(newSize);
        try {
            dispatchMatched();
        } catch(Exception e) {
            LOG.trace("Caught exception on dispatch after prefetch size change.");
        }
    }

}
