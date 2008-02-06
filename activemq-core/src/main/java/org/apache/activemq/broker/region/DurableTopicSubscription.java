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
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.cursors.StoreDurableSubscriberCursor;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DurableTopicSubscription extends PrefetchSubscription implements UsageListener {

    private static final Log LOG = LogFactory.getLog(DurableTopicSubscription.class);
    private final ConcurrentHashMap<MessageId, Integer> redeliveredMessages = new ConcurrentHashMap<MessageId, Integer>();
    private final ConcurrentHashMap<ActiveMQDestination, Destination> destinations = new ConcurrentHashMap<ActiveMQDestination, Destination>();
    private final SubscriptionKey subscriptionKey;
    private final boolean keepDurableSubsActive;
    private boolean active;

    public DurableTopicSubscription(Broker broker, Destination dest,SystemUsage usageManager, ConnectionContext context, ConsumerInfo info, boolean keepDurableSubsActive)
        throws JMSException {
        super(broker,usageManager, context, info);
        this.pending = new StoreDurableSubscriberCursor(broker,context.getClientId(), info.getSubscriptionName(), info.getPrefetchSize(), this);
        this.pending.setSystemUsage(usageManager);
        this.keepDurableSubsActive = keepDurableSubsActive;
        subscriptionKey = new SubscriptionKey(context.getClientId(), info.getSubscriptionName());
        if (dest != null && dest.getMessageStore() != null) {
            TopicMessageStore store = (TopicMessageStore)dest.getMessageStore();
            try {
                this.enqueueCounter=store.getMessageCount(subscriptionKey.getClientId(),subscriptionKey.getSubscriptionName());
            } catch (IOException e) {
                JMSException jmsEx = new JMSException("Failed to retrieve eunqueueCount from store "+ e);
                jmsEx.setLinkedException(e);
                throw jmsEx;
            }
        }
    }

    public boolean isActive() {
        return active;
    }

    protected boolean isFull() {
        return !active || super.isFull();
    }

    public void gc() {
    }

    public void add(ConnectionContext context, Destination destination) throws Exception {
        super.add(context, destination);
        destinations.put(destination.getActiveMQDestination(), destination);
        if (active || keepDurableSubsActive) {
            Topic topic = (Topic)destination;
            topic.activate(context, this);
            if (pending.isEmpty(topic)) {
                topic.recoverRetroactiveMessages(context, this);
            }
        }
        dispatchPending();
    }

    public void activate(SystemUsage memoryManager, ConnectionContext context,
            ConsumerInfo info) throws Exception {
        LOG.debug("Activating " + this);
        if (!active) {
            this.active = true;
            this.context = context;
            this.info = info;
            int prefetch = info.getPrefetchSize();
            if (prefetch>0) {
            prefetch += prefetch/2;
            }
            int depth = Math.max(prefetch, this.pending.getMaxAuditDepth());
            this.pending.setMaxAuditDepth(depth);
            if (!keepDurableSubsActive) {
                for (Iterator<Destination> iter = destinations.values()
                        .iterator(); iter.hasNext();) {
                    Topic topic = (Topic) iter.next();
                    topic.activate(context, this);
                }
            }
            synchronized (pending) {
                pending.setSystemUsage(memoryManager);
                pending.start();

                // If nothing was in the persistent store, then try to use the
                // recovery policy.
                if (pending.isEmpty()) {
                    for (Iterator<Destination> iter = destinations.values()
                            .iterator(); iter.hasNext();) {
                        Topic topic = (Topic) iter.next();
                        topic.recoverRetroactiveMessages(context, this);
                    }
                }
            }
            dispatchPending();
            this.usageManager.getMemoryUsage().addUsageListener(this);
        }
    }

    public void deactivate(boolean keepDurableSubsActive) throws Exception {
        active = false;
        this.usageManager.getMemoryUsage().removeUsageListener(this);
        synchronized (pending) {
            pending.stop();
        }
        if (!keepDurableSubsActive) {
            for (Iterator<Destination> iter = destinations.values().iterator(); iter.hasNext();) {
                Topic topic = (Topic)iter.next();
                topic.deactivate(context, this);
            }
        }
        for (final MessageReference node : dispatched) {
            // Mark the dispatched messages as redelivered for next time.
            Integer count = redeliveredMessages.get(node.getMessageId());
            if (count != null) {
                redeliveredMessages.put(node.getMessageId(), Integer.valueOf(count.intValue() + 1));
            } else {
                redeliveredMessages.put(node.getMessageId(), Integer.valueOf(1));
            }
            if (keepDurableSubsActive&& pending.isTransient()) {
                synchronized (pending) {
                    pending.addMessageFirst(node);
                }
            } else {
                node.decrementReferenceCount();
            }
        }
        synchronized(dispatched) {
            dispatched.clear();
        }
        if (!keepDurableSubsActive && pending.isTransient()) {
            synchronized (pending) {
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
        prefetchExtension = 0;
    }
    
    
    protected MessageDispatch createMessageDispatch(MessageReference node, Message message) {
        MessageDispatch md = super.createMessageDispatch(node, message);
        Integer count = redeliveredMessages.get(node.getMessageId());
        if (count != null) {
            md.setRedeliveryCounter(count.intValue());
        }
        return md;
    }

    public void add(MessageReference node) throws Exception {
        if (!active && !keepDurableSubsActive) {
            return;
        }
        super.add(node);
    }

    protected void doAddRecoveredMessage(MessageReference message) throws Exception {
        synchronized(pending) {
        pending.addRecoveredMessage(message);
        }
    }

    public int getPendingQueueSize() {
        if (active || keepDurableSubsActive) {
            return super.getPendingQueueSize();
        }
        // TODO: need to get from store
        return 0;
    }

    public void setSelector(String selector) throws InvalidSelectorException {
        throw new UnsupportedOperationException("You cannot dynamically change the selector for durable topic subscriptions");
    }

    protected boolean canDispatch(MessageReference node) {
        return active;
    }

    protected void acknowledge(ConnectionContext context, MessageAck ack, MessageReference node) throws IOException {
        node.getRegionDestination().acknowledge(context, this, ack, node);
        redeliveredMessages.remove(node.getMessageId());
        node.decrementReferenceCount();
    }

    
    public synchronized String toString() {
        return "DurableTopicSubscription:" + " consumer=" + info.getConsumerId() + ", destinations=" + destinations.size() + ", total=" + enqueueCounter + ", pending="
               + getPendingQueueSize() + ", dispatched=" + dispatchCounter + ", inflight=" + dispatched.size() + ", prefetchExtension=" + this.prefetchExtension;
    }

    public SubscriptionKey getSubscriptionKey() {
        return subscriptionKey;
    }

    /**
     * Release any references that we are holding.
     */
    public void destroy() {
        synchronized (pending) {
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
        synchronized(dispatched) {
            for (Iterator iter = dispatched.iterator(); iter.hasNext();) {
                MessageReference node = (MessageReference) iter.next();
                node.decrementReferenceCount();
            }
            dispatched.clear();
        }
    }

    /**
     * @param usageManager
     * @param oldPercentUsage
     * @param newPercentUsage
     * @see org.apache.activemq.usage.UsageListener#onMemoryUseChanged(org.apache.activemq.usage.SystemUsage,
     *      int, int)
     */
    public void onUsageChanged(Usage usage, int oldPercentUsage, int newPercentUsage) {
        if (oldPercentUsage > newPercentUsage && oldPercentUsage >= 90) {
            try {
                dispatchPending();
            } catch (IOException e) {
                LOG.warn("problem calling dispatchMatched", e);
            }
        }
    }
    
    protected boolean isDropped(MessageReference node) {
       return false;
     }
}
