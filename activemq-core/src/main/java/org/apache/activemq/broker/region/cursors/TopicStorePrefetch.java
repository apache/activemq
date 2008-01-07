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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * perist pendingCount messages pendingCount message (messages awaiting disptach
 * to a consumer) cursor
 * 
 * @version $Revision$
 */
class TopicStorePrefetch extends AbstractPendingMessageCursor implements MessageRecoveryListener, UsageListener {

    private static final Log LOG = LogFactory.getLog(TopicStorePrefetch.class);
    private TopicMessageStore store;
    private final LinkedHashMap<MessageId,Message> batchList = new LinkedHashMap<MessageId,Message> ();
    private String clientId;
    private String subscriberName;
    private Destination regionDestination;
    private boolean batchResetNeeded = true;
    private boolean storeMayHaveMoreMessages = true;
    private boolean started;
    private final Subscription subscription;
   
    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     */
    public TopicStorePrefetch(Topic topic, String clientId, String subscriberName, Subscription subscription) {
        this.regionDestination = topic;
        this.subscription = subscription;
        this.store = (TopicMessageStore)topic.getMessageStore();
        this.clientId = clientId;
        this.subscriberName = subscriberName;
        this.maxProducersToAudit=32;
        this.maxAuditDepth=10000;
    }

    public synchronized void start() throws Exception {
        if (!started) {
            started = true;
            super.start();
            getSystemUsage().getMemoryUsage().addUsageListener(this);
            safeFillBatch();
        }
    }

    public synchronized void stop() throws Exception {
        if (started) {
            started = false;
            getSystemUsage().getMemoryUsage().removeUsageListener(this);
            super.stop();
            store.resetBatching(clientId, subscriberName);
            gc();
        }
    }

    /**
     * @return true if there are no pendingCount messages
     */
    public synchronized boolean isEmpty() {
        safeFillBatch();
        return batchList.isEmpty();
    }

    public synchronized int size() {
        safeFillBatch();
        return batchList.size();
    }

    public synchronized void addMessageLast(MessageReference node) throws Exception {
        if (node != null) {
            storeMayHaveMoreMessages=true;
            node.decrementReferenceCount();
        }
    }

    public synchronized void addMessageFirst(MessageReference node) throws Exception {
        if (node != null) {
            storeMayHaveMoreMessages=true;
            node.decrementReferenceCount();
            rollback(node.getMessageId());
        }
    }

    public synchronized void remove() {
    }

    public synchronized void remove(MessageReference node) {
    }

    public synchronized void clear() {
        gc();
    }

    public synchronized boolean hasNext() {
        boolean result =  !isEmpty();
        return result;
    }

    public synchronized MessageReference next() {
        Message result = null;
        safeFillBatch();
        if (batchList.isEmpty()) {
            return null;
        } else {
            Iterator i = batchList.entrySet().iterator();
            result = (Message) ((Map.Entry)i.next()).getValue();
            i.remove();
            result.setRegionDestination(regionDestination);
            result.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
        }
        return result;
    }

    public void reset() {
    }

    // MessageRecoveryListener implementation
    public void finished() {
    }

    public synchronized boolean recoverMessage(Message message)
            throws Exception {
        MessageEvaluationContext messageEvaluationContext = new MessageEvaluationContext();
        messageEvaluationContext.setMessageReference(message);
        if (subscription.matches(message, messageEvaluationContext)) {
            message.setRegionDestination(regionDestination);
            if (!isDuplicate(message.getMessageId())) {
                // only increment if count is zero (could have been cached)
                if (message.getReferenceCount() == 0) {
                    message.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
                    message.incrementReferenceCount();
                   
                }
                batchList.put(message.getMessageId(), message);
            }else {
                this.storeMayHaveMoreMessages=true;
            }
        }
        return true;
    }

    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
        // shouldn't get called
        throw new RuntimeException("Not supported");
    }
    
    /**
     * Mark a message as already dispatched
     * @param message
     */  
    public synchronized void dispatched(MessageReference message) {
        if (this.audit != null) {
            isDuplicate(message.getMessageId());
            Message removed = this.batchList.remove(message.getMessageId());
            if (removed != null) {
                removed.decrementReferenceCount();
            }
        }
    }

    // implementation
    protected synchronized void safeFillBatch() {
        try {
            fillBatch();
        } catch (Exception e) {
            LOG.error("Failed to fill batch", e);
            throw new RuntimeException(e);
        }
    }

    protected synchronized void fillBatch() throws Exception {
        if (batchResetNeeded) {
            this.store.resetBatching(clientId, subscriberName);
            this.batchResetNeeded = false;
            this.storeMayHaveMoreMessages = true;
        }
        while (this.batchList.isEmpty() && this.storeMayHaveMoreMessages) {
            this.storeMayHaveMoreMessages = false;
            this.store.recoverNextMessages(clientId, subscriberName,
                    maxBatchSize, this);
            if (!this.batchList.isEmpty()) {
                this.storeMayHaveMoreMessages=true;
            }
        }
    }

    protected synchronized int getStoreSize() {
        try {
            return store.getMessageCount(clientId, subscriberName);
        } catch (IOException e) {
            LOG.error(this + " Failed to get the outstanding message count from the store", e);
            throw new RuntimeException(e);
        }
    }

    public synchronized void gc() {
        for (Message msg : batchList.values()) {
            rollback(msg.getMessageId());
            msg.decrementReferenceCount();
        }
        batchList.clear();
        batchResetNeeded = true;
    }
    
    public void onUsageChanged(Usage usage, int oldPercentUsage,int newPercentUsage) {
        if (oldPercentUsage > newPercentUsage && oldPercentUsage >= 90) {
            storeMayHaveMoreMessages = true;
            try {
                fillBatch();
            } catch (Exception e) {
                LOG.error("Failed to fill batch ", e);
            }
        }
    }

    public String toString() {
        return "TopicStorePrefetch" + System.identityHashCode(this) + "(" + clientId + "," + subscriberName + ")";
    }
}
