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

import java.util.LinkedList;

import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.SystemUsage;

/**
 * Abstract method holder for pending message (messages awaiting disptach to a
 * consumer) cursor
 * 
 * @version $Revision$
 */
public class AbstractPendingMessageCursor implements PendingMessageCursor {
    protected int memoryUsageHighWaterMark = 90;
    protected int maxBatchSize = 100;
    protected SystemUsage systemUsage;
    protected int maxProducersToAudit=1024;
    protected int maxAuditDepth=1;
    protected boolean enableAudit=true;
    protected ActiveMQMessageAudit audit;
    private boolean started=false;
  

    public synchronized void start() throws Exception  {
        if (!started && enableAudit && audit==null) {
            audit= new ActiveMQMessageAudit(maxAuditDepth,maxProducersToAudit);
        }
        started=true;
    }

    public synchronized void stop() throws Exception  {
        started=false;
        audit=null;
        gc();
    }

    public void add(ConnectionContext context, Destination destination) throws Exception {
    }

    public void remove(ConnectionContext context, Destination destination) throws Exception {
    }

    public boolean isRecoveryRequired() {
        return true;
    }

    public void addMessageFirst(MessageReference node) throws Exception {
    }

    public void addMessageLast(MessageReference node) throws Exception {
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
        return systemUsage != null ? (systemUsage.getMemoryUsage().getPercentUsage() < memoryUsageHighWaterMark) : true;
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
        return this.memoryUsageHighWaterMark;
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
     * @param maxItems
     * @return a list of paged in messages
     */
    public LinkedList pageInList(int maxItems) {
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
            this.audit.setMaximumNumberOfProducersToTrack(maxProducersToAudit);
        }
    }

    /**
     * @return the maxAuditDepth
     */
    public int getMaxAuditDepth() {
        return this.maxAuditDepth;
    }
    

    /**
     * @param maxAuditDepth the maxAuditDepth to set
     */
    public synchronized void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
        if (audit != null) {
            this.audit.setAuditDepth(maxAuditDepth);
        }
    }
    
    
    /**
     * @return the enableAudit
     */
    public boolean isEnableAudit() {
        return this.enableAudit;
    }

    /**
     * @param enableAudit the enableAudit to set
     */
    public synchronized void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
        if (this.enableAudit && started && audit==null) {
            audit= new ActiveMQMessageAudit(maxAuditDepth,maxProducersToAudit);
        }
    }
    
    public boolean isTransient() {
        return false;
    }
    
    /**
     * Mark a message as already dispatched
     * @param message
     */
    public void dispatched(MessageReference message) {   
    }


    protected synchronized boolean  isDuplicate(MessageId messageId) {
        if (!this.enableAudit || this.audit==null) {
            return false;
        }
        return this.audit.isDuplicate(messageId);
    }
    
    protected synchronized void rollback(MessageId id) {
        if (this.audit != null) {
            audit.rollback(id);
        }
    }
    
    protected synchronized boolean isStarted() {
        return started;
    }
  
   
}
