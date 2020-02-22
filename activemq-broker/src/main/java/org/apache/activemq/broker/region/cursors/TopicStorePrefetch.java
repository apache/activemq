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

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.store.TopicMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * persist pendingCount messages pendingCount message (messages awaiting dispatch
 * to a consumer) cursor
 *
 *
 */
class TopicStorePrefetch extends AbstractStoreCursor {
    private static final Logger LOG = LoggerFactory.getLogger(TopicStorePrefetch.class);
    private final TopicMessageStore store;
    private final String clientId;
    private final String subscriberName;
    private final Subscription subscription;
    private byte lastRecoveredPriority = 9;
    private boolean storeHasMessages = false;

    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     */
    public TopicStorePrefetch(Subscription subscription,Topic topic, String clientId, String subscriberName) {
        super(topic);
        this.subscription=subscription;
        this.store = (TopicMessageStore)topic.getMessageStore();
        this.clientId = clientId;
        this.subscriberName = subscriberName;
        this.maxProducersToAudit=32;
        this.maxAuditDepth=10000;
        resetSize();
        this.storeHasMessages=this.size > 0;
    }

    @Override
    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
        // shouldn't get called
        throw new RuntimeException("Not supported");
    }

    @Override
    public synchronized void addMessageFirst(MessageReference node) throws Exception {
        batchList.addMessageFirst(node);
        size++;
        node.incrementReferenceCount();
    }

    @Override
    public final synchronized boolean addMessageLast(MessageReference node) throws Exception {
        this.storeHasMessages = super.addMessageLast(node);
        return this.storeHasMessages;
    }

    @Override
    public synchronized boolean recoverMessage(Message message, boolean cached) throws Exception {
        LOG.trace("{} recover: {}, priority: {}", this, message.getMessageId(), message.getPriority());
        boolean recovered = false;
        MessageEvaluationContext messageEvaluationContext = new NonCachedMessageEvaluationContext();
        messageEvaluationContext.setMessageReference(message);
        if (this.subscription.matches(message, messageEvaluationContext)) {
            recovered = super.recoverMessage(message, cached);
            if (recovered && !cached) {
                lastRecoveredPriority = message.getPriority();
            }
            storeHasMessages = true;
        }
        return recovered;
    }

    @Override
    protected boolean duplicateFromStoreExcepted(Message message) {
        // setBatch is not implemented - sequence order not reliable with concurrent transactions
        // on cache exhaustion - first pageIn starts from last ack location which may replay what
        // cursor has dispatched
        return true;
    }

    @Override
    protected synchronized int getStoreSize() {
        try {
            return store.getMessageCount(clientId, subscriberName);
        } catch (Exception e) {
            LOG.error("{} Failed to get the outstanding message count from the store", this, e);
            throw new RuntimeException(e);
        }
    }


    @Override
    protected synchronized long getStoreMessageSize() {
        try {
            return store.getMessageSize(clientId, subscriberName);
        } catch (Exception e) {
            LOG.error("{} Failed to get the outstanding message count from the store", this, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected synchronized boolean isStoreEmpty() {
        try {
            return this.store.isEmpty();
        } catch (Exception e) {
            LOG.error("Failed to determine if store is empty", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    protected void resetBatch() {
        this.store.resetBatching(clientId, subscriberName);
    }

    @Override
    protected void doFillBatch() throws Exception {
        // avoid repeated  trips to the store if there is nothing of interest
        this.storeHasMessages = false;
        this.store.recoverNextMessages(clientId, subscriberName,
                maxBatchSize, this);
        dealWithDuplicates();
        if (!this.storeHasMessages && (!this.batchList.isEmpty() || !hadSpace)) {
            this.storeHasMessages = true;
        }
    }

    public byte getLastRecoveredPriority() {
        return lastRecoveredPriority;
    }

    public final boolean isPaging() {
        return !isCacheEnabled() && !batchList.isEmpty();
    }

    @Override
    public Subscription getSubscription() {
        return subscription;
    }

    @Override
    public String toString() {
        return "TopicStorePrefetch(" + clientId + "," + subscriberName + ",storeHasMessages=" + this.storeHasMessages +") " + this.subscription.getConsumerInfo().getConsumerId() + " - " + super.toString();
    }
}
