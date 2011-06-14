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
package org.apache.activemq.broker.region.cursors;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * persist pending messages pending message (messages awaiting dispatch to a
 * consumer) cursor
 * 
 * 
 */
public class StoreDurableSubscriberCursor extends AbstractPendingMessageCursor {

    private static final Logger LOG = LoggerFactory.getLogger(StoreDurableSubscriberCursor.class);
    private final String clientId;
    private final String subscriberName;
    private final Map<Destination, TopicStorePrefetch> topics = new HashMap<Destination, TopicStorePrefetch>();
    private final List<PendingMessageCursor> storePrefetches = new CopyOnWriteArrayList<PendingMessageCursor>();
    private final PendingMessageCursor nonPersistent;
    private PendingMessageCursor currentCursor;
    private final DurableTopicSubscription subscription;
    private boolean immediatePriorityDispatch = true;
    /**
     * @param broker Broker for this cursor
     * @param clientId clientId for this cursor
     * @param subscriberName subscriber name for this cursor
     * @param maxBatchSize currently ignored
     * @param subscription  subscription for this cursor
     */
    public StoreDurableSubscriberCursor(Broker broker,String clientId, String subscriberName,int maxBatchSize, DurableTopicSubscription subscription) {
        super(AbstractPendingMessageCursor.isPrioritizedMessageSubscriber(broker,subscription));
        this.subscription=subscription;
        this.clientId = clientId;
        this.subscriberName = subscriberName;
        if (broker.getBrokerService().isPersistent()) {
            this.nonPersistent = new FilePendingMessageCursor(broker,clientId + subscriberName,this.prioritizedMessages);
        } else {
            this.nonPersistent = new VMPendingMessageCursor(this.prioritizedMessages);
        }
        
        this.nonPersistent.setMaxBatchSize(maxBatchSize);
        this.nonPersistent.setSystemUsage(systemUsage);
        this.storePrefetches.add(this.nonPersistent);

        if (prioritizedMessages) {
            setMaxAuditDepth(10*getMaxAuditDepth());
        }
    }

    @Override
    public synchronized void start() throws Exception {
        if (!isStarted()) {
            super.start();
            for (PendingMessageCursor tsp : storePrefetches) {
            	tsp.setMessageAudit(getMessageAudit());
                tsp.start();
            }
        }
    }

    @Override
    public synchronized void stop() throws Exception {
        if (isStarted()) {
            if (subscription.isKeepDurableSubsActive()) {
                super.gc();
                super.getMessageAudit().clear();
                for (PendingMessageCursor tsp : storePrefetches) {
                    tsp.gc();
                    tsp.getMessageAudit().clear();
                }
            } else {
                super.stop();
                for (PendingMessageCursor tsp : storePrefetches) {
                    tsp.stop();
                }
            }
        }
    }

    /**
     * Add a destination
     * 
     * @param context
     * @param destination
     * @throws Exception
     */
    @Override
    public synchronized void add(ConnectionContext context, Destination destination) throws Exception {
        if (destination != null && !AdvisorySupport.isAdvisoryTopic(destination.getActiveMQDestination())) {
            TopicStorePrefetch tsp = new TopicStorePrefetch(this.subscription,(Topic)destination, clientId, subscriberName);
            tsp.setMaxBatchSize(destination.getMaxPageSize());
            tsp.setSystemUsage(systemUsage);
            tsp.setMessageAudit(getMessageAudit());
            tsp.setEnableAudit(isEnableAudit());
            tsp.setMemoryUsageHighWaterMark(getMemoryUsageHighWaterMark());
            tsp.setUseCache(isUseCache());
            topics.put(destination, tsp);
            storePrefetches.add(tsp);
            if (isStarted()) {
                tsp.start();
            }
        }
    }

    /**
     * remove a destination
     * 
     * @param context
     * @param destination
     * @throws Exception
     */
    @Override
    public synchronized List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception {
        PendingMessageCursor tsp = topics.remove(destination);
        if (tsp != null) {
            storePrefetches.remove(tsp);
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * @return true if there are no pending messages
     */
    @Override
    public synchronized boolean isEmpty() {
        for (PendingMessageCursor tsp : storePrefetches) {
            if( !tsp.isEmpty() )
                return false;
        }
        return true;
    }

    @Override
    public synchronized boolean isEmpty(Destination destination) {
        boolean result = true;
        TopicStorePrefetch tsp = topics.get(destination);
        if (tsp != null) {
            result = tsp.isEmpty();
        }
        return result;
    }

    /**
     * Informs the Broker if the subscription needs to intervention to recover
     * it's state e.g. DurableTopicSubscriber may do
     * 
     * @see org.apache.activemq.broker.region.cursors.AbstractPendingMessageCursor
     * @return true if recovery required
     */
    @Override
    public boolean isRecoveryRequired() {
        return false;
    }

    @Override
    public synchronized void addMessageLast(MessageReference node) throws Exception {
        if (node != null) {
            Message msg = node.getMessage();
            if (isStarted()) {
                if (!msg.isPersistent()) {
                    nonPersistent.addMessageLast(node);
                }
            }
            if (msg.isPersistent()) {
                Destination dest = msg.getRegionDestination();
                TopicStorePrefetch tsp = topics.get(dest);
                if (tsp != null) {
                    tsp.addMessageLast(node);
                    if (prioritizedMessages && immediatePriorityDispatch && tsp.isPaging()) {
                        if (msg.getPriority() > tsp.getLastRecoveredPriority()) {
                            tsp.recoverMessage(node.getMessage(), true);
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("cached high priority (" + msg.getPriority() + ") message:" +
                                    msg.getMessageId() + ", current paged batch priority: " +
                                    tsp.getLastRecoveredPriority() + ", cache size:" + tsp.batchList.size());
                            }
                        }
                    }
                }
            }

        }
    }

    @Override
    public boolean isTransient() {
        return subscription.isKeepDurableSubsActive();
    }

    @Override
    public void addMessageFirst(MessageReference node) throws Exception {
        // for keep durable subs active, need to deal with redispatch
        if (node != null) {
            Message msg = node.getMessage();
            if (!msg.isPersistent()) {
                nonPersistent.addMessageFirst(node);
            } else {
                Destination dest = msg.getRegionDestination();
                TopicStorePrefetch tsp = topics.get(dest);
                if (tsp != null) {
                    tsp.addMessageFirst(node);
                }
            }
        }
    }

    @Override
    public synchronized void addRecoveredMessage(MessageReference node) throws Exception {
        nonPersistent.addMessageLast(node);
    }

    @Override
    public synchronized void clear() {
        for (PendingMessageCursor tsp : storePrefetches) {
            tsp.clear();
        }
    }

    @Override
    public synchronized boolean hasNext() {
        boolean result = true;
        if (result) {
            try {
                currentCursor = getNextCursor();
            } catch (Exception e) {
                LOG.error("Failed to get current cursor ", e);
                throw new RuntimeException(e);
            }
            result = currentCursor != null ? currentCursor.hasNext() : false;
        }
        return result;
    }

    @Override
    public synchronized MessageReference next() {
        MessageReference result = currentCursor != null ? currentCursor.next() : null;
        return result;
    }

    @Override
    public synchronized void remove() {
        if (currentCursor != null) {
            currentCursor.remove();
        }
    }

    @Override
    public synchronized void remove(MessageReference node) {
        for (PendingMessageCursor tsp : storePrefetches) {
            tsp.remove(node);
        }
    }

    @Override
    public synchronized void reset() {
        for (PendingMessageCursor storePrefetch : storePrefetches) {
            storePrefetch.reset();
        }
    }

    @Override
    public synchronized void release() {
        for (PendingMessageCursor storePrefetch : storePrefetches) {
            storePrefetch.release();
        }
    }

    @Override
    public synchronized int size() {
        int pendingCount=0;
        for (PendingMessageCursor tsp : storePrefetches) {
            pendingCount += tsp.size();
        }
        return pendingCount;
    }

    @Override
    public void setMaxBatchSize(int newMaxBatchSize) {
        for (PendingMessageCursor storePrefetch : storePrefetches) {
            storePrefetch.setMaxBatchSize(newMaxBatchSize);
        }
        super.setMaxBatchSize(newMaxBatchSize);
    }

    @Override
    public synchronized void gc() {
        for (PendingMessageCursor tsp : storePrefetches) {
            tsp.gc();
        }
    }

    @Override
    public void setSystemUsage(SystemUsage usageManager) {
        super.setSystemUsage(usageManager);
        for (PendingMessageCursor tsp : storePrefetches) {
            tsp.setSystemUsage(usageManager);
        }
    }
    
    @Override
    public void setMemoryUsageHighWaterMark(int memoryUsageHighWaterMark) {
        super.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
        for (PendingMessageCursor cursor : storePrefetches) {
            cursor.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
        }
    }
    
    @Override
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        super.setMaxProducersToAudit(maxProducersToAudit);
        for (PendingMessageCursor cursor : storePrefetches) {
            cursor.setMaxAuditDepth(maxAuditDepth);
        }
    }

    @Override
    public void setMaxAuditDepth(int maxAuditDepth) {
        super.setMaxAuditDepth(maxAuditDepth);
        for (PendingMessageCursor cursor : storePrefetches) {
            cursor.setMaxAuditDepth(maxAuditDepth);
        }
    }
    
    @Override
    public void setEnableAudit(boolean enableAudit) {
        super.setEnableAudit(enableAudit);
        for (PendingMessageCursor cursor : storePrefetches) {
            cursor.setEnableAudit(enableAudit);
        }
    }
    
    @Override
    public  void setUseCache(boolean useCache) {
        super.setUseCache(useCache);
        for (PendingMessageCursor cursor : storePrefetches) {
            cursor.setUseCache(useCache);
        }
    }
    
    protected synchronized PendingMessageCursor getNextCursor() throws Exception {
        if (currentCursor == null || currentCursor.isEmpty()) {
            currentCursor = null;
            for (PendingMessageCursor tsp : storePrefetches) {
                if (tsp.hasNext()) {
                    currentCursor = tsp;
                    break;
                }
            }
            // round-robin
            if (storePrefetches.size()>1) {
                PendingMessageCursor first = storePrefetches.remove(0);
                storePrefetches.add(first);
            }
        }
        return currentCursor;
    }
    
    @Override
    public String toString() {
        return "StoreDurableSubscriber(" + clientId + ":" + subscriberName + ")";
    }

    public boolean isImmediatePriorityDispatch() {
        return immediatePriorityDispatch;
    }

    public void setImmediatePriorityDispatch(boolean immediatePriorityDispatch) {
        this.immediatePriorityDispatch = immediatePriorityDispatch;
    }

}
