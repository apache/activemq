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
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *  Store based cursor
 *
 */
public abstract class AbstractStoreCursor extends AbstractPendingMessageCursor implements MessageRecoveryListener {
    private static final Log LOG = LogFactory.getLog(AbstractStoreCursor.class);
    protected final Destination regionDestination;
    private final PendingList batchList;
    private Iterator<MessageReference> iterator = null;
    protected boolean cacheEnabled=false;
    protected boolean batchResetNeeded = true;
    private boolean storeHasMessages = false;
    protected int size;
    private MessageId lastCachedId;
    private boolean hadSpace = false;

    protected AbstractStoreCursor(Destination destination) {
        super((destination != null ? destination.isPrioritizedMessages():false));
        this.regionDestination=destination;
        if (this.prioritizedMessages) {
            this.batchList= new PrioritizedPendingList();
        }else {
            this.batchList = new OrderedPendingList();
        }
    }
    
    
    public final synchronized void start() throws Exception{
        if (!isStarted()) {
            clear();
            super.start();      
            resetBatch();
            this.size = getStoreSize();
            this.storeHasMessages=this.size > 0;
            if (!this.storeHasMessages&&useCache) {
                cacheEnabled=true;
            }
        } 
    }
    
    
    public final synchronized void stop() throws Exception {
        resetBatch();
        super.stop();
        gc();
    }

    
    public final boolean recoverMessage(Message message) throws Exception {
        return recoverMessage(message,false);
    }
    
    public synchronized boolean recoverMessage(Message message, boolean cached) throws Exception {
        boolean recovered = false;
        if (recordUniqueId(message.getMessageId())) {
            if (!cached) {
                message.setRegionDestination(regionDestination);
                if( message.getMemoryUsage()==null ) {
                    message.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
                }
            }
            message.incrementReferenceCount();
            batchList.addMessageLast(message);
            clearIterator(true);
            recovered = true;
            storeHasMessages = true;
        } else {
            /*
             * we should expect to get these - as the message is recorded as it before it goes into
             * the cache. If subsequently, we pull out that message from the store (before its deleted)
             * it will be a duplicate - but should be ignored
             */
            if (LOG.isTraceEnabled()) {
                LOG.trace(regionDestination.getActiveMQDestination().getPhysicalName()
                        + " cursor got duplicate: " + message.getMessageId() + ", " + message.getPriority());
            }
        }
        return recovered;
    }
    
    
    public final void reset() {
        if (batchList.isEmpty()) {
            try {
                fillBatch();
            } catch (Exception e) {
                LOG.error("Failed to fill batch", e);
                throw new RuntimeException(e);
            }
        }
        clearIterator(true);
        size();
    }
    
    
    public synchronized void release() {
        clearIterator(false);
    }
    
    private synchronized void clearIterator(boolean ensureIterator) {
        boolean haveIterator = this.iterator != null;
        this.iterator=null;
        if(haveIterator&&ensureIterator) {
            ensureIterator();
        }
    }
    
    private synchronized void ensureIterator() {
        if(this.iterator==null) {
            this.iterator=this.batchList.iterator();
        }
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
        ensureIterator();
        return this.iterator.hasNext();
    }
    
    
    public final synchronized MessageReference next() {
        MessageReference result = null;
        if (!this.batchList.isEmpty()&&this.iterator.hasNext()) {
            result = this.iterator.next();
        }
        last = result;
        if (result != null) {
            result.incrementReferenceCount();
        }
        return result;
    }
    
    
    public final synchronized void addMessageLast(MessageReference node) throws Exception {
        if (cacheEnabled && hasSpace()) {
            recoverMessage(node.getMessage(),true);
            lastCachedId = node.getMessageId();
        } else {
            if (cacheEnabled) {
                cacheEnabled=false;
                if (LOG.isTraceEnabled()) {
                    LOG.trace(regionDestination.getActiveMQDestination().getPhysicalName() + " disabling cache on size:" + size
                            + ", lastCachedIdSeq: " + (lastCachedId == null ? -1 : lastCachedId.getBrokerSequenceId())
                            + " current node seqId: " + node.getMessageId().getBrokerSequenceId());
                }
                // sync with store on disabling the cache
                if (lastCachedId != null) {
                    setBatch(lastCachedId);
                }
            }
        }
        this.storeHasMessages = true;
        size++;
    }

    protected void setBatch(MessageId messageId) throws Exception {
    }

    
    public final synchronized void addMessageFirst(MessageReference node) throws Exception {
        cacheEnabled=false;
        size++;
    }

    
    public final synchronized void remove() {
        size--;
        if (iterator!=null) {
            iterator.remove();
        }
        if (last != null) {
            last.decrementReferenceCount();
        }
        if (size==0 && isStarted() && useCache && hasSpace() && isStoreEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(regionDestination.getActiveMQDestination().getPhysicalName() + " enabling cache on last remove");
            }
            cacheEnabled=true;
        }
    }

    
    public final synchronized void remove(MessageReference node) {
        size--;
        cacheEnabled=false;
        batchList.remove(node);
    }
    
    
    public final synchronized void clear() {
        gc();
    }
    
    
    public synchronized void gc() {
        for (Iterator<MessageReference>i = batchList.iterator();i.hasNext();) {
            MessageReference msg = i.next();
            rollback(msg.getMessageId());
            msg.decrementReferenceCount();
        }
        batchList.clear();
        clearIterator(false);
        batchResetNeeded = true;
        this.cacheEnabled=false;
    }

    @Override
    public boolean hasSpace() {
        hadSpace = super.hasSpace();
        return hadSpace;
    }

    protected final synchronized void fillBatch() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("fillBatch - batchResetNeeded=" + batchResetNeeded
                    + ", hasMessages=" + this.storeHasMessages + ", size=" + this.size + ", cacheEnabled=" + this.cacheEnabled);
        }
        if (batchResetNeeded) {
            resetBatch();
            this.batchResetNeeded = false;
        }
        if (this.batchList.isEmpty() && this.storeHasMessages && this.size >0) {
            this.storeHasMessages = false;
            try {
                doFillBatch();
            } catch (Exception e) {
                LOG.error("Failed to fill batch", e);
                throw new RuntimeException(e);
            }
            if (!this.batchList.isEmpty() || !hadSpace) {
                this.storeHasMessages=true;
            }
        }
    }
    
    
    public final synchronized boolean isEmpty() {
        // negative means more messages added to store through queue.send since last reset
        return size == 0;
    }

    
    public final synchronized boolean hasMessagesBufferedToDeliver() {
        return !batchList.isEmpty();
    }

    
    public final synchronized int size() {
        if (size < 0) {
            this.size = getStoreSize();
        }
        return size;
    }
    
    
    protected abstract void doFillBatch() throws Exception;
    
    protected abstract void resetBatch();
    
    protected abstract int getStoreSize();
    
    protected abstract boolean isStoreEmpty();
}
