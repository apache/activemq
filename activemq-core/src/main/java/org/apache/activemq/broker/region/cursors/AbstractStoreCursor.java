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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *  Store based cursor
 *
 */
public abstract class AbstractStoreCursor extends AbstractPendingMessageCursor implements MessageRecoveryListener, UsageListener {
    private static final Log LOG = LogFactory.getLog(AbstractStoreCursor.class);
    protected final Destination regionDestination;
    protected final LinkedHashMap<MessageId,Message> batchList = new LinkedHashMap<MessageId,Message> ();
    protected boolean cacheEnabled=false;
    protected boolean batchResetNeeded = true;
    protected boolean storeHasMessages = false;
    protected int size;
    
    protected AbstractStoreCursor(Destination destination) {
        this.regionDestination=destination;
    }
    
    public final synchronized void start() throws Exception{
        if (!isStarted()) {
            super.start();
            clear();
            resetBatch();
            this.size = getStoreSize();
            this.storeHasMessages=this.size > 0;
            if (!this.storeHasMessages&&useCache) {
                cacheEnabled=true;
            }
        } 
        getSystemUsage().getMemoryUsage().addUsageListener(this);
    }
    
    public final synchronized void stop() throws Exception {
        getSystemUsage().getMemoryUsage().removeUsageListener(this);
        resetBatch();
        gc();
        super.stop();
    }

    
    public final boolean recoverMessage(Message message) throws Exception {
        return recoverMessage(message,false);
    }
    
    public synchronized boolean recoverMessage(Message message, boolean cached)throws Exception {
        if (!isDuplicate(message.getMessageId())) {
            if (!cached) {
                message.setRegionDestination(regionDestination);
                message.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
            }
            message.incrementReferenceCount();
            batchList.put(message.getMessageId(), message);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring batched duplicated from store: " + message);
            }
            storeHasMessages = true;
        }
        return true;
    }
    
    public final void reset() {
    }

    public final void finished() {
    }
        
    public final synchronized boolean hasNext() {
        if (batchList.isEmpty()) {
            try {
                fillBatch();
            } catch (Exception e) {
                LOG.error("Failed to fill batch", e);
                throw new RuntimeException(e);
            }
        }
        boolean result= !batchList.isEmpty();
        return result;
    }
    
    public final synchronized MessageReference next() {
        Message result = null;
        if (!this.batchList.isEmpty()) {
            Iterator<Entry<MessageId, Message>> i = this.batchList.entrySet().iterator();
            result = i.next().getValue();
            i.remove();
        }
        return result;
    }
    
    public final synchronized void addMessageLast(MessageReference node) throws Exception {
        if (cacheEnabled && hasSpace()) {
            recoverMessage(node.getMessage(),true);
        }else {
            cacheEnabled=false;
        }
        size++;
    }

    public final synchronized void addMessageFirst(MessageReference node) throws Exception {
        cacheEnabled=false;
        size++;
    }

    public final synchronized void remove() {
        size--;
        if (size==0 && isStarted() && cacheEnabled) {
            cacheEnabled=true;
        }
    }

    public final synchronized void remove(MessageReference node) {
        size--;
        cacheEnabled=false;
    }
    
           
    public final synchronized void onUsageChanged(Usage usage, int oldPercentUsage,
            int newPercentUsage) {
        if (oldPercentUsage > newPercentUsage && oldPercentUsage >= memoryUsageHighWaterMark) {
            storeHasMessages = true;
            try {
                fillBatch();
            } catch (Exception e) {
                LOG.error("Failed to fill batch ", e);
            }
        }
        
    }
    
    public final synchronized void clear() {
        gc();
    }
    
    public final synchronized void gc() {
        for (Message msg : batchList.values()) {
            rollback(msg.getMessageId());
            msg.decrementReferenceCount();
        }
        batchList.clear();
        batchResetNeeded = true;
        this.cacheEnabled=false;
    }
    
    protected final synchronized void fillBatch() {
        if (batchResetNeeded) {
            resetBatch();
            this.batchResetNeeded = false;
        }
        
        if( this.batchList.isEmpty() && (this.storeHasMessages ||this.size >0)) {
            this.storeHasMessages = false;
            try {
                doFillBatch();
            } catch (Exception e) {
                LOG.error("Failed to fill batch", e);
                throw new RuntimeException(e);
            }
            if (!this.batchList.isEmpty()) {
                this.storeHasMessages=true;
            }
        }
    }
    
    public final synchronized boolean isEmpty() {
        return size <= 0;
    }

    public final synchronized boolean hasMessagesBufferedToDeliver() {
        return !batchList.isEmpty();
    }

    public final synchronized int size() {
        if (isStarted()) {
            return size;
        }
        this.size = getStoreSize();
        return size;
        
    }
    
    
    protected abstract void doFillBatch() throws Exception;
    
    protected abstract void resetBatch();
    
    protected abstract int getStoreSize();
}
