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
    private boolean cacheEnabled=true;
    private boolean started=false;
    protected MessageReference last = null;
    protected final boolean prioritizedMessages;
    
    public AbstractPendingMessageCursor(boolean prioritizedMessages) {
        this.prioritizedMessages=prioritizedMessages;
    }
  

    public synchronized void start() throws Exception  {
        if (!started && enableAudit && audit==null) {
            audit= new ActiveMQMessageAudit(maxAuditDepth,maxProducersToAudit);
        }
        started=true;
    }

    public synchronized void stop() throws Exception  {
        started=false;
        gc();
    }

    public void add(ConnectionContext context, Destination destination) throws Exception {
    }

    @SuppressWarnings("unchecked")
    public List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception {
        return Collections.EMPTY_LIST;
    }

    public boolean isRecoveryRequired() {
        return true;
    }

    public void addMessageFirst(MessageReference node) throws Exception {
    }

    public void addMessageLast(MessageReference node) throws Exception {
    }
    
    public boolean tryAddMessageLast(MessageReference node, long maxWaitTime) throws Exception {
        addMessageLast(node);
        return true;
    }

    public void addRecoveredMessage(MessageReference node) throws Exception {
        addMessageLast(node);
    }

    public void clear() {
    }

    public boolean hasNext() {
        return false;
    }

    public boolean isEmpty() {
        return false;
    }

    public boolean isEmpty(Destination destination) {
        return isEmpty();
    }

    public MessageReference next() {
        return null;
    }

    public void remove() {
    }

    public void reset() {
    }

    public int size() {
        return 0;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    protected void fillBatch() throws Exception {
    }

    public void resetForGC() {
        reset();
    }

    public void remove(MessageReference node) {
    }

    public void gc() {
    }

    public void setSystemUsage(SystemUsage usageManager) {
        this.systemUsage = usageManager;
    }

    public boolean hasSpace() {
        return systemUsage != null ? (!systemUsage.getMemoryUsage().isFull(memoryUsageHighWaterMark)) : true;
    }

    public boolean isFull() {
        return systemUsage != null ? systemUsage.getMemoryUsage().isFull() : false;
    }

    public void release() {
    }

    public boolean hasMessagesBufferedToDeliver() {
        return false;
    }

    /**
     * @return the memoryUsageHighWaterMark
     */
    public int getMemoryUsageHighWaterMark() {
        return memoryUsageHighWaterMark;
    }

    /**
     * @param memoryUsageHighWaterMark the memoryUsageHighWaterMark to set
     */
    public void setMemoryUsageHighWaterMark(int memoryUsageHighWaterMark) {
        this.memoryUsageHighWaterMark = memoryUsageHighWaterMark;
    }

    /**
     * @return the usageManager
     */
    public SystemUsage getSystemUsage() {
        return this.systemUsage;
    }

    /**
     * destroy the cursor
     * 
     * @throws Exception
     */
    public void destroy() throws Exception {
        stop();
    }

    /**
     * Page in a restricted number of messages
     * 
     * @param maxItems maximum number of messages to return
     * @return a list of paged in messages
     */
    public LinkedList<MessageReference> pageInList(int maxItems) {
        throw new RuntimeException("Not supported");
    }

    /**
     * @return the maxProducersToAudit
     */
    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    /**
     * @param maxProducersToAudit the maxProducersToAudit to set
     */
    public synchronized void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
        if (audit != null) {
            audit.setMaximumNumberOfProducersToTrack(maxProducersToAudit);
        }
    }

    /**
     * @return the maxAuditDepth
     */
    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }
    

    /**
     * @param maxAuditDepth the maxAuditDepth to set
     */
    public synchronized void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
        if (audit != null) {
            audit.setAuditDepth(maxAuditDepth);
        }
    }
    
    
    /**
     * @return the enableAudit
     */
    public boolean isEnableAudit() {
        return enableAudit;
    }

    /**
     * @param enableAudit the enableAudit to set
     */
    public synchronized void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
        if (enableAudit && started && audit==null) {
            audit= new ActiveMQMessageAudit(maxAuditDepth,maxProducersToAudit);
        }
    }
    
    public boolean isTransient() {
        return false;
    }
    
       
    /**
     * set the audit
     * @param audit new audit component
     */
    public void setMessageAudit(ActiveMQMessageAudit audit) {
    	this.audit=audit;
    }
    
    
    /**
     * @return the audit
     */
    public ActiveMQMessageAudit getMessageAudit() {
    	return audit;
    }
    
    public boolean isUseCache() {
        return useCache;
    }

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

    public synchronized boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public synchronized void setCacheEnabled(boolean val) {
        cacheEnabled = val;
    }

    public void rebase() {
    }
}
