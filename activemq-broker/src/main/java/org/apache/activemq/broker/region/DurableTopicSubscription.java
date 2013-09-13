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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.StoreDurableSubscriberCursor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.apache.activemq.util.SubscriptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurableTopicSubscription extends PrefetchSubscription implements UsageListener {

    private static final Logger LOG = LoggerFactory.getLogger(DurableTopicSubscription.class);
    private final ConcurrentHashMap<MessageId, Integer> redeliveredMessages = new ConcurrentHashMap<MessageId, Integer>();
    private final ConcurrentHashMap<ActiveMQDestination, Destination> durableDestinations = new ConcurrentHashMap<ActiveMQDestination, Destination>();
    private final SubscriptionKey subscriptionKey;
    private final boolean keepDurableSubsActive;
    private final AtomicBoolean active = new AtomicBoolean();
    private final AtomicLong offlineTimestamp = new AtomicLong(-1);

    public DurableTopicSubscription(Broker broker, SystemUsage usageManager, ConnectionContext context, ConsumerInfo info, boolean keepDurableSubsActive)
            throws JMSException {
        super(broker, usageManager, context, info);
        this.pending = new StoreDurableSubscriberCursor(broker, context.getClientId(), info.getSubscriptionName(), info.getPrefetchSize(), this);
        this.pending.setSystemUsage(usageManager);
        this.pending.setMemoryUsageHighWaterMark(getCursorMemoryHighWaterMark());
        this.keepDurableSubsActive = keepDurableSubsActive;
        subscriptionKey = new SubscriptionKey(context.getClientId(), info.getSubscriptionName());
    }

    public final boolean isActive() {
        return active.get();
    }

    public final long getOfflineTimestamp() {
        return offlineTimestamp.get();
    }

    public void setOfflineTimestamp(long timestamp) {
        offlineTimestamp.set(timestamp);
    }

    @Override
    public boolean isFull() {
        return !active.get() || super.isFull();
    }

    @Override
    public void gc() {
    }

    /**
     * store will have a pending ack for all durables, irrespective of the
     * selector so we need to ack if node is un-matched
     */
    @Override
    public void unmatched(MessageReference node) throws IOException {
        MessageAck ack = new MessageAck();
        ack.setAckType(MessageAck.UNMATCHED_ACK_TYPE);
        ack.setMessageID(node.getMessageId());
        Destination regionDestination = (Destination) node.getRegionDestination();
        regionDestination.acknowledge(this.getContext(), this, ack, node);
    }

    @Override
    protected void setPendingBatchSize(PendingMessageCursor pending, int numberToDispatch) {
        // statically configured via maxPageSize
    }

    @Override
    public void add(ConnectionContext context, Destination destination) throws Exception {
        if (!destinations.contains(destination)) {
            super.add(context, destination);
        }
        // do it just once per destination
        if (durableDestinations.containsKey(destination.getActiveMQDestination())) {
            return;
        }
        durableDestinations.put(destination.getActiveMQDestination(), destination);

        if (active.get() || keepDurableSubsActive) {
            Topic topic = (Topic) destination;
            topic.activate(context, this);
            if (pending.isEmpty(topic)) {
                topic.recoverRetroactiveMessages(context, this);
            }
            this.enqueueCounter += pending.size();
        } else if (destination.getMessageStore() != null) {
            TopicMessageStore store = (TopicMessageStore) destination.getMessageStore();
            try {
                this.enqueueCounter += store.getMessageCount(subscriptionKey.getClientId(), subscriptionKey.getSubscriptionName());
            } catch (IOException e) {
                JMSException jmsEx = new JMSException("Failed to retrieve enqueueCount from store " + e);
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
        }
        dispatchPending();
    }

    public void activate(SystemUsage memoryManager, ConnectionContext context, ConsumerInfo info, RegionBroker regionBroker) throws Exception {
        if (!active.get()) {
            this.context = context;
            this.info = info;

            LOG.debug("Activating {}", this);
            if (!keepDurableSubsActive) {
                for (Destination destination : durableDestinations.values()) {
                    Topic topic = (Topic) destination;
                    add(context, topic);
                    topic.activate(context, this);
                }

                // On Activation we should update the configuration based on our new consumer info.
                ActiveMQDestination dest = this.info.getDestination();
                if (dest != null && regionBroker.getDestinationPolicy() != null) {
                    PolicyEntry entry = regionBroker.getDestinationPolicy().getEntryFor(dest);
                    if (entry != null) {
                        entry.configure(broker, usageManager, this);
                    }
                }
            }

            synchronized (pendingLock) {
                if (!((StoreDurableSubscriberCursor) pending).isStarted() || !keepDurableSubsActive) {
                    pending.setSystemUsage(memoryManager);
                    pending.setMemoryUsageHighWaterMark(getCursorMemoryHighWaterMark());
                    pending.setMaxAuditDepth(getMaxAuditDepth());
                    pending.setMaxProducersToAudit(getMaxProducersToAudit());
                    pending.start();
                    // If nothing was in the persistent store, then try to use the
                    // recovery policy.
                    if (pending.isEmpty()) {
                        for (Destination destination : durableDestinations.values()) {
                            Topic topic = (Topic) destination;
                            topic.recoverRetroactiveMessages(context, this);
                        }
                    }
                }
            }
            this.active.set(true);
            this.offlineTimestamp.set(-1);
            dispatchPending();
            this.usageManager.getMemoryUsage().addUsageListener(this);
        }
    }

    public void deactivate(boolean keepDurableSubsActive) throws Exception {
        LOG.debug("Deactivating keepActive={}, {}", keepDurableSubsActive, this);
        active.set(false);
        offlineTimestamp.set(System.currentTimeMillis());
        this.usageManager.getMemoryUsage().removeUsageListener(this);

        ArrayList<Topic> topicsToDeactivate = new ArrayList<Topic>();
        List<MessageReference> savedDispateched = null;

        synchronized (pendingLock) {
            if (!keepDurableSubsActive) {
                pending.stop();
            }

            synchronized (dispatchLock) {
                for (Destination destination : durableDestinations.values()) {
                    Topic topic = (Topic) destination;
                    if (!keepDurableSubsActive) {
                        topicsToDeactivate.add(topic);
                    } else {
                        topic.getDestinationStatistics().getInflight().subtract(dispatched.size());
                    }
                }

                // Before we add these back to pending they need to be in producer order not
                // dispatch order so we can add them to the front of the pending list.
                Collections.reverse(dispatched);

                for (final MessageReference node : dispatched) {
                    // Mark the dispatched messages as redelivered for next time.
                    Integer count = redeliveredMessages.get(node.getMessageId());
                    if (count != null) {
                        redeliveredMessages.put(node.getMessageId(), Integer.valueOf(count.intValue() + 1));
                    } else {
                        redeliveredMessages.put(node.getMessageId(), Integer.valueOf(1));
                    }
                    if (keepDurableSubsActive && pending.isTransient()) {
                        pending.addMessageFirst(node);
                        pending.rollback(node.getMessageId());
                    } else {
                        node.decrementReferenceCount();
                    }
                }

                if (!topicsToDeactivate.isEmpty()) {
                    savedDispateched = new ArrayList<MessageReference>(dispatched);
                }
                dispatched.clear();
            }
            if (!keepDurableSubsActive && pending.isTransient()) {
                try {
                    pending.reset();
                    while (pending.hasNext()) {
                        MessageReference node = pending.next();
                        node.decrementReferenceCount();
                        pending.remove();
                    }
                } finally {
                    pending.release();
                }
            }
        }
        for(Topic topic: topicsToDeactivate) {
            topic.deactivate(context, this, savedDispateched);
        }
        prefetchExtension.set(0);
    }

    @Override
    protected MessageDispatch createMessageDispatch(MessageReference node, Message message) {
        MessageDispatch md = super.createMessageDispatch(node, message);
        if (node != QueueMessageReference.NULL_MESSAGE) {
            Integer count = redeliveredMessages.get(node.getMessageId());
            if (count != null) {
                md.setRedeliveryCounter(count.intValue());
            }
        }
        return md;
    }

    @Override
    public void add(MessageReference node) throws Exception {
        if (!active.get() && !keepDurableSubsActive) {
            return;
        }
        super.add(node);
    }

    @Override
    protected void dispatchPending() throws IOException {
        if (isActive()) {
            super.dispatchPending();
        }
    }

    public void removePending(MessageReference node) throws IOException {
        pending.remove(node);
    }

    @Override
    protected void doAddRecoveredMessage(MessageReference message) throws Exception {
        synchronized (pending) {
            pending.addRecoveredMessage(message);
        }
    }

    @Override
    public int getPendingQueueSize() {
        if (active.get() || keepDurableSubsActive) {
            return super.getPendingQueueSize();
        }
        // TODO: need to get from store
        return 0;
    }

    @Override
    public void setSelector(String selector) throws InvalidSelectorException {
        throw new UnsupportedOperationException("You cannot dynamically change the selector for durable topic subscriptions");
    }

    @Override
    protected boolean canDispatch(MessageReference node) {
        return true;  // let them go, our dispatchPending gates the active / inactive state.
    }

    @Override
    protected void acknowledge(ConnectionContext context, MessageAck ack, MessageReference node) throws IOException {
        this.setTimeOfLastMessageAck(System.currentTimeMillis());
        Destination regionDestination = (Destination) node.getRegionDestination();
        regionDestination.acknowledge(context, this, ack, node);
        redeliveredMessages.remove(node.getMessageId());
        node.decrementReferenceCount();
    }

    @Override
    public synchronized String toString() {
        return "DurableTopicSubscription-" + getSubscriptionKey() + ", id=" + info.getConsumerId() + ", active=" + isActive() + ", destinations="
                + durableDestinations.size() + ", total=" + enqueueCounter + ", pending=" + getPendingQueueSize() + ", dispatched=" + dispatchCounter
                + ", inflight=" + dispatched.size() + ", prefetchExtension=" + getPrefetchExtension();
    }

    public SubscriptionKey getSubscriptionKey() {
        return subscriptionKey;
    }

    /**
     * Release any references that we are holding.
     */
    @Override
    public void destroy() {
        synchronized (pendingLock) {
            try {
                pending.reset();
                while (pending.hasNext()) {
                    MessageReference node = pending.next();
                    node.decrementReferenceCount();
                }

            } finally {
                pending.release();
                pending.clear();
            }
        }
        synchronized (dispatchLock) {
            for (MessageReference node : dispatched) {
                node.decrementReferenceCount();
            }
            dispatched.clear();
        }
        setSlowConsumer(false);
    }

    @Override
    public void onUsageChanged(Usage usage, int oldPercentUsage, int newPercentUsage) {
        if (oldPercentUsage > newPercentUsage && oldPercentUsage >= 90) {
            try {
                dispatchPending();
            } catch (IOException e) {
                LOG.warn("problem calling dispatchMatched", e);
            }
        }
    }

    @Override
    protected boolean isDropped(MessageReference node) {
        return false;
    }

    public boolean isKeepDurableSubsActive() {
        return keepDurableSubsActive;
    }
}
