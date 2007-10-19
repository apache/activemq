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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * persist pending messages pending message (messages awaiting dispatch to a
 * consumer) cursor
 * 
 * @version $Revision$
 */
public class StoreDurableSubscriberCursor extends AbstractPendingMessageCursor {

    private static final Log LOG = LogFactory.getLog(StoreDurableSubscriberCursor.class);
    private String clientId;
    private String subscriberName;
    private Map<Destination, TopicStorePrefetch> topics = new HashMap<Destination, TopicStorePrefetch>();
    private LinkedList<PendingMessageCursor> storePrefetches = new LinkedList<PendingMessageCursor>();
    private boolean started;
    private PendingMessageCursor nonPersistent;
    private PendingMessageCursor currentCursor;
    private final Subscription subscription;

    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     * @throws IOException
     */
    public StoreDurableSubscriberCursor(String clientId, String subscriberName, Store store, int maxBatchSize, Subscription subscription) {
        this.clientId = clientId;
        this.subscriberName = subscriberName;
        this.subscription = subscription;
        this.nonPersistent = new FilePendingMessageCursor(clientId + subscriberName, store);
        storePrefetches.add(nonPersistent);
    }

    public synchronized void start() throws Exception {
        if (!started) {
            started = true;
            super.start();
            for (PendingMessageCursor tsp : storePrefetches) {
                tsp.start();
            }
        }
    }

    public synchronized void stop() throws Exception {
        if (started) {
            started = false;
            super.stop();
            for (PendingMessageCursor tsp : storePrefetches) {
                tsp.stop();
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
    public synchronized void add(ConnectionContext context, Destination destination) throws Exception {
        if (destination != null && !AdvisorySupport.isAdvisoryTopic(destination.getActiveMQDestination())) {
            TopicStorePrefetch tsp = new TopicStorePrefetch((Topic)destination, clientId, subscriberName, subscription);
            tsp.setMaxBatchSize(getMaxBatchSize());
            tsp.setSystemUsage(systemUsage);
            tsp.setEnableAudit(isEnableAudit());
            tsp.setMaxAuditDepth(getMaxAuditDepth());
            tsp.setMaxProducersToAudit(getMaxProducersToAudit());
            topics.put(destination, tsp);
            storePrefetches.add(tsp);
            if (started) {
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
    public synchronized void remove(ConnectionContext context, Destination destination) throws Exception {
        Object tsp = topics.remove(destination);
        if (tsp != null) {
            storePrefetches.remove(tsp);
        }
    }

    /**
     * @return true if there are no pending messages
     */
    public synchronized boolean isEmpty() {
        for (PendingMessageCursor tsp : storePrefetches) {
            if( !tsp.isEmpty() )
                return false;
        }
        return true;
    }

    public boolean isEmpty(Destination destination) {
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
     * @see org.apache.activemq.region.cursors.PendingMessageCursor
     * @return true if recovery required
     */
    public boolean isRecoveryRequired() {
        return false;
    }

    public synchronized void addMessageLast(MessageReference node) throws Exception {
        if (node != null) {
            Message msg = node.getMessage();
            if (started) {
                if (!msg.isPersistent()) {
                    nonPersistent.addMessageLast(node);
                }
            }
            if (msg.isPersistent()) {
                Destination dest = msg.getRegionDestination();
                TopicStorePrefetch tsp = topics.get(dest);
                if (tsp != null) {
                    tsp.addMessageLast(node);
                }
            }
        }
    }

    public void addRecoveredMessage(MessageReference node) throws Exception {
        nonPersistent.addMessageLast(node);
    }

    public synchronized void clear() {
        nonPersistent.clear();
        for (PendingMessageCursor tsp : storePrefetches) {
            tsp.clear();
        }
    }

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

    public synchronized MessageReference next() {
        MessageReference result = currentCursor != null ? currentCursor.next() : null;
        return result;
    }

    public synchronized void remove() {
        if (currentCursor != null) {
            currentCursor.remove();
        }
    }

    public synchronized void remove(MessageReference node) {
        if (currentCursor != null) {
            currentCursor.remove(node);
        }
    }

    public synchronized void reset() {
        for (Iterator<PendingMessageCursor> i = storePrefetches.iterator(); i.hasNext();) {
            AbstractPendingMessageCursor tsp = (AbstractPendingMessageCursor)i.next();
            tsp.reset();
        }
    }

    public synchronized void release() {
        for (Iterator<PendingMessageCursor> i = storePrefetches.iterator(); i.hasNext();) {
            AbstractPendingMessageCursor tsp = (AbstractPendingMessageCursor)i.next();
            tsp.release();
        }
    }

    public int size() {
        int pendingCount=0;
        for (PendingMessageCursor tsp : storePrefetches) {
            pendingCount += tsp.size();
        }
        return pendingCount;
    }

    public synchronized void setMaxBatchSize(int maxBatchSize) {
        for (Iterator<PendingMessageCursor> i = storePrefetches.iterator(); i.hasNext();) {
            AbstractPendingMessageCursor tsp = (AbstractPendingMessageCursor)i.next();
            tsp.setMaxBatchSize(maxBatchSize);
        }
        super.setMaxBatchSize(maxBatchSize);
    }

    public synchronized void gc() {
        for (Iterator<PendingMessageCursor> i = storePrefetches.iterator(); i.hasNext();) {
            PendingMessageCursor tsp = i.next();
            tsp.gc();
        }
    }

    public synchronized void setSystemUsage(SystemUsage usageManager) {
        super.setSystemUsage(usageManager);
        for (Iterator<PendingMessageCursor> i = storePrefetches.iterator(); i.hasNext();) {
            PendingMessageCursor tsp = i.next();
            tsp.setSystemUsage(usageManager);
        }
    }
    
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        super.setMaxProducersToAudit(maxProducersToAudit);
        for (PendingMessageCursor cursor : storePrefetches) {
            cursor.setMaxAuditDepth(maxAuditDepth);
        }
        if (nonPersistent != null) {
            nonPersistent.setMaxProducersToAudit(maxProducersToAudit);
        }
    }

    public void setMaxAuditDepth(int maxAuditDepth) {
        super.setMaxAuditDepth(maxAuditDepth);
        for (PendingMessageCursor cursor : storePrefetches) {
            cursor.setMaxAuditDepth(maxAuditDepth);
        }
        if (nonPersistent != null) {
            nonPersistent.setMaxAuditDepth(maxAuditDepth);
        }
    }
    
    public synchronized void setEnableAudit(boolean enableAudit) {
        super.setEnableAudit(enableAudit);
        for (PendingMessageCursor cursor : storePrefetches) {
            cursor.setEnableAudit(enableAudit);
        }
        if (nonPersistent != null) {
            nonPersistent.setEnableAudit(enableAudit);
        }
    }

    protected synchronized PendingMessageCursor getNextCursor() throws Exception {
        if (currentCursor == null || currentCursor.isEmpty()) {
            currentCursor = null;
            for (Iterator<PendingMessageCursor> i = storePrefetches.iterator(); i.hasNext();) {
                AbstractPendingMessageCursor tsp = (AbstractPendingMessageCursor)i.next();
                if (tsp.hasNext()) {
                    currentCursor = tsp;
                    break;
                }
            }
            // round-robin
            storePrefetches.addLast(storePrefetches.removeFirst());
        }
        return currentCursor;
    }

    public String toString() {
        return "StoreDurableSubscriber(" + clientId + ":" + subscriberName + ")";
    }
}
