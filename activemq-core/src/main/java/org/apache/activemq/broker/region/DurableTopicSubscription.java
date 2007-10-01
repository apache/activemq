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
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.cursors.StoreDurableSubscriberCursor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DurableTopicSubscription extends PrefetchSubscription implements UsageListener {

    private static final Log LOG = LogFactory.getLog(PrefetchSubscription.class);
    private final ConcurrentHashMap<MessageId, Integer> redeliveredMessages = new ConcurrentHashMap<MessageId, Integer>();
    private final ConcurrentHashMap<ActiveMQDestination, Destination> destinations = new ConcurrentHashMap<ActiveMQDestination, Destination>();
    private final SubscriptionKey subscriptionKey;
    private final boolean keepDurableSubsActive;
    private final SystemUsage usageManager;
    private boolean active;

    public DurableTopicSubscription(Broker broker, SystemUsage usageManager, ConnectionContext context, ConsumerInfo info, boolean keepDurableSubsActive)
        throws InvalidSelectorException {
        super(broker, context, info);
        this.pending = new StoreDurableSubscriberCursor(context.getClientId(), info.getSubscriptionName(), broker.getTempDataStore(), info.getPrefetchSize(), this);
        this.usageManager = usageManager;
        this.keepDurableSubsActive = keepDurableSubsActive;
        subscriptionKey = new SubscriptionKey(context.getClientId(), info.getSubscriptionName());
    }

    public synchronized boolean isActive() {
        return active;
    }

    protected synchronized boolean isFull() {
        return !active || super.isFull();
    }

    public synchronized void gc() {
    }

    public synchronized void add(ConnectionContext context, Destination destination) throws Exception {
        super.add(context, destination);
        destinations.put(destination.getActiveMQDestination(), destination);
        if (active || keepDurableSubsActive) {
            Topic topic = (Topic)destination;
            topic.activate(context, this);
            if (pending.isEmpty(topic)) {
                topic.recoverRetroactiveMessages(context, this);
            }
        }
        dispatchMatched();
    }

    public synchronized void activate(SystemUsage memoryManager, ConnectionContext context, ConsumerInfo info) throws Exception {
        LOG.debug("Activating " + this);
        if (!active) {
            this.active = true;
            this.context = context;
            this.info = info;
            if (!keepDurableSubsActive) {
                for (Iterator<Destination> iter = destinations.values().iterator(); iter.hasNext();) {
                    Topic topic = (Topic)iter.next();
                    topic.activate(context, this);
                }
            }
            pending.setSystemUsage(memoryManager);
            pending.start();

            // If nothing was in the persistent store, then try to use the
            // recovery policy.
            if (pending.isEmpty()) {
                for (Iterator<Destination> iter = destinations.values().iterator(); iter.hasNext();) {
                    Topic topic = (Topic)iter.next();
                    topic.recoverRetroactiveMessages(context, this);
                }
            }
            dispatchMatched();
            this.usageManager.getMemoryUsage().addUsageListener(this);
        }
    }

    public synchronized void deactivate(boolean keepDurableSubsActive) throws Exception {
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
        for (Iterator iter = dispatched.iterator(); iter.hasNext();) {
            // Mark the dispatched messages as redelivered for next time.
            MessageReference node = (MessageReference)iter.next();
            Integer count = redeliveredMessages.get(node.getMessageId());
            if (count != null) {
                redeliveredMessages.put(node.getMessageId(), Integer.valueOf(count.intValue() + 1));
            } else {
                redeliveredMessages.put(node.getMessageId(), Integer.valueOf(1));
            }
            if (keepDurableSubsActive) {
                synchronized (pending) {
                    pending.addMessageFirst(node);
                }
            } else {
                node.decrementReferenceCount();
            }
            iter.remove();
        }
        if (!keepDurableSubsActive) {
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

    public synchronized void add(MessageReference node) throws Exception {
        if (!active && !keepDurableSubsActive) {
            return;
        }
        node.incrementReferenceCount();
        super.add(node);
    }

    protected synchronized void doAddRecoveredMessage(MessageReference message) throws Exception {
        pending.addRecoveredMessage(message);
    }

    public synchronized int getPendingQueueSize() {
        if (active || keepDurableSubsActive) {
            return super.getPendingQueueSize();
        }
        // TODO: need to get from store
        return 0;
    }

    public void setSelector(String selector) throws InvalidSelectorException {
        throw new UnsupportedOperationException("You cannot dynamically change the selector for durable topic subscriptions");
    }

    protected synchronized boolean canDispatch(MessageReference node) {
        return active;
    }

    protected synchronized void acknowledge(ConnectionContext context, MessageAck ack, MessageReference node) throws IOException {
        node.getRegionDestination().acknowledge(context, this, ack, node);
        redeliveredMessages.remove(node.getMessageId());
        node.decrementReferenceCount();
    }

    public String getSubscriptionName() {
        return subscriptionKey.getSubscriptionName();
    }

    public synchronized String toString() {
        return "DurableTopicSubscription:" + " consumer=" + info.getConsumerId() + ", destinations=" + destinations.size() + ", total=" + enqueueCounter + ", pending="
               + getPendingQueueSize() + ", dispatched=" + dispatchCounter + ", inflight=" + dispatched.size() + ", prefetchExtension=" + this.prefetchExtension;
    }

    public String getClientId() {
        return subscriptionKey.getClientId();
    }

    public SubscriptionKey getSubscriptionKey() {
        return subscriptionKey;
    }

    /**
     * Release any references that we are holding.
     */
    public synchronized void destroy() {
        try {
            synchronized (pending) {
                pending.reset();
                while (pending.hasNext()) {
                    MessageReference node = pending.next();
                    node.decrementReferenceCount();
                }
            }
        } finally {
            pending.release();
            pending.clear();
        }
        for (Iterator iter = dispatched.iterator(); iter.hasNext();) {
            MessageReference node = (MessageReference)iter.next();
            node.decrementReferenceCount();
        }
        dispatched.clear();
    }

    /**
     * @param memoryManager
     * @param oldPercentUsage
     * @param newPercentUsage
     * @see org.apache.activemq.usage.UsageListener#onMemoryUseChanged(org.apache.activemq.usage.SystemUsage,
     *      int, int)
     */
    public void onUsageChanged(Usage usage, int oldPercentUsage, int newPercentUsage) {
        if (oldPercentUsage > newPercentUsage && oldPercentUsage >= 90) {
            try {
                dispatchMatched();
            } catch (IOException e) {
                LOG.warn("problem calling dispatchMatched", e);
            }
        }
    }
}
