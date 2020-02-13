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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.SystemUsage;

/**
 * Abstract method holder for pending message (messages awaiting disptach to a
 * consumer) cursor
 *
 *
 */
public abstract class AbstractPendingMessageCursor implements PendingMessageCursor {
    protected int memoryUsageHighWaterMark = 70;
    protected int maxBatchSize = BaseDestination.MAX_PAGE_SIZE;
    protected SystemUsage systemUsage;
    protected int maxProducersToAudit = BaseDestination.MAX_PRODUCERS_TO_AUDIT;
    protected int maxAuditDepth = BaseDestination.MAX_AUDIT_DEPTH;
    protected boolean enableAudit=true;
    protected ActiveMQMessageAudit audit;
    protected boolean useCache=true;
    protected boolean cacheEnabled=true;
    protected boolean started=false;
    protected MessageReference last = null;
    protected final boolean prioritizedMessages;

    public AbstractPendingMessageCursor(boolean prioritizedMessages) {
        this.prioritizedMessages=prioritizedMessages;
    }


    @Override
    public synchronized void start() throws Exception  {
        if (!started && enableAudit && audit==null) {
            audit= new ActiveMQMessageAudit(maxAuditDepth,maxProducersToAudit);
        }
        started=true;
    }

    @Override
    public synchronized void stop() throws Exception  {
        started=false;
        gc();
    }

    @Override
    public void add(ConnectionContext context, Destination destination) throws Exception {
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception {
        return Collections.EMPTY_LIST;
    }

    @Override
    public boolean isRecoveryRequired() {
        return true;
    }

    @Override
    public void addMessageFirst(MessageReference node) throws Exception {
    }

    @Override
    public boolean addMessageLast(MessageReference node) throws Exception {
        return tryAddMessageLast(node, INFINITE_WAIT);
    }

    @Override
    public boolean tryAddMessageLast(MessageReference node, long maxWaitTime) throws Exception {
        return true;
    }

    @Override
    public void addRecoveredMessage(MessageReference node) throws Exception {
        addMessageLast(node);
    }

    @Override
    public void clear() {
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean isEmpty(Destination destination) {
        return isEmpty();
    }

    @Override
    public MessageReference next() {
        return null;
    }

    @Override
    public void remove() {
    }

    @Override
    public void reset() {
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    @Override
    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    protected void fillBatch() throws Exception {
    }

    @Override
    public void resetForGC() {
        reset();
    }

    @Override
    public void remove(MessageReference node) {
    }

    @Override
    public void gc() {
    }

    @Override
    public void setSystemUsage(SystemUsage usageManager) {
        this.systemUsage = usageManager;
    }

    @Override
    public boolean hasSpace() {
        // allow isFull to verify parent usage and otherwise enforce local memoryUsageHighWaterMark
        return systemUsage != null ? (!isParentFull() && systemUsage.getMemoryUsage().getPercentUsage() < memoryUsageHighWaterMark) : true;
    }

    boolean parentHasSpace(int waterMark) {
        boolean result = true;
        if (systemUsage != null) {
            if (systemUsage.getMemoryUsage().getParent() != null) {
                return systemUsage.getMemoryUsage().getParent().getPercentUsage() <= waterMark;
            }
        }
        return result;
    }

    private boolean isParentFull() {
        boolean result = false;
        if (systemUsage != null) {
            if (systemUsage.getMemoryUsage().getParent() != null) {
                return systemUsage.getMemoryUsage().getParent().getPercentUsage() >= 100;
            }
        }
        return result;
    }

    @Override
    public boolean isFull() {
        return systemUsage != null ? systemUsage.getMemoryUsage().isFull() : false;
    }

    @Override
    public void release() {
    }

    @Override
    public boolean hasMessagesBufferedToDeliver() {
        return false;
    }

    /**
     * @return the memoryUsageHighWaterMark
     */
    @Override
    public int getMemoryUsageHighWaterMark() {
        return memoryUsageHighWaterMark;
    }

    /**
     * @param memoryUsageHighWaterMark the memoryUsageHighWaterMark to set
     */
    @Override
    public void setMemoryUsageHighWaterMark(int memoryUsageHighWaterMark) {
        this.memoryUsageHighWaterMark = memoryUsageHighWaterMark;
    }

    /**
     * @return the usageManager
     */
    @Override
    public SystemUsage getSystemUsage() {
        return this.systemUsage;
    }

    /**
     * destroy the cursor
     *
     * @throws Exception
     */
    @Override
    public void destroy() throws Exception {
        stop();
    }

    /**
     * Page in a restricted number of messages
     *
     * @param maxItems maximum number of messages to return
     * @return a list of paged in messages
     */
    @Override
    public LinkedList<MessageReference> pageInList(int maxItems) {
        throw new RuntimeException("Not supported");
    }

    /**
     * @return the maxProducersToAudit
     */
    @Override
    public synchronized int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    /**
     * @param maxProducersToAudit the maxProducersToAudit to set
     */
    @Override
    public synchronized void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
        if (audit != null) {
            audit.setMaximumNumberOfProducersToTrack(maxProducersToAudit);
        }
    }

    /**
     * @return the maxAuditDepth
     */
    @Override
    public synchronized int getMaxAuditDepth() {
        return maxAuditDepth;
    }


    /**
     * @param maxAuditDepth the maxAuditDepth to set
     */
    @Override
    public synchronized void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
        if (audit != null) {
            audit.setAuditDepth(maxAuditDepth);
        }
    }


    /**
     * @return the enableAudit
     */
    @Override
    public boolean isEnableAudit() {
        return enableAudit;
    }

    /**
     * @param enableAudit the enableAudit to set
     */
    @Override
    public synchronized void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
        if (enableAudit && started && audit==null) {
            audit= new ActiveMQMessageAudit(maxAuditDepth,maxProducersToAudit);
        }
    }

    @Override
    public boolean isTransient() {
        return false;
    }


    /**
     * set the audit
     * @param audit new audit component
     */
    @Override
    public void setMessageAudit(ActiveMQMessageAudit audit) {
    	this.audit=audit;
    }


    /**
     * @return the audit
     */
    @Override
    public ActiveMQMessageAudit getMessageAudit() {
    	return audit;
    }

    @Override
    public boolean isUseCache() {
        return useCache;
    }

    @Override
    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public synchronized boolean isDuplicate(MessageId messageId) {
        boolean unique = recordUniqueId(messageId);
        rollback(messageId);
        return !unique;
    }

    /**
     * records a message id and checks if it is a duplicate
     * @param messageId
     * @return true if id is unique, false otherwise.
     */
    public synchronized boolean recordUniqueId(MessageId messageId) {
        if (!enableAudit || audit==null) {
            return true;
        }
        return !audit.isDuplicate(messageId);
    }

    @Override
    public synchronized void rollback(MessageId id) {
        if (audit != null) {
            audit.rollback(id);
        }
    }

    public synchronized boolean isStarted() {
        return started;
    }

    public static boolean isPrioritizedMessageSubscriber(Broker broker,Subscription sub) {
        boolean result = false;
        Set<Destination> destinations = broker.getDestinations(sub.getActiveMQDestination());
        if (destinations != null) {
            for (Destination dest:destinations) {
                if (dest.isPrioritizedMessages()) {
                    result = true;
                    break;
                }
            }
        }
        return result;

    }

    @Override
    public synchronized boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public synchronized void setCacheEnabled(boolean val) {
        cacheEnabled = val;
    }

    @Override
    public void rebase() {
    }
}
