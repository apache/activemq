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
import java.util.LinkedList;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Store based cursor
 *
 */
public abstract class AbstractStoreCursor extends AbstractPendingMessageCursor implements MessageRecoveryListener {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStoreCursor.class);
    protected final Destination regionDestination;
    protected final PendingList batchList;
    private Iterator<MessageReference> iterator = null;
    protected boolean batchResetNeeded = false;
    private boolean storeHasMessages = false;
    protected int size;
    private MessageId lastCachedId;
    private TransactionId lastTx;
    protected boolean hadSpace = false;

    protected AbstractStoreCursor(Destination destination) {
        super((destination != null ? destination.isPrioritizedMessages():false));
        this.regionDestination=destination;
        if (this.prioritizedMessages) {
            this.batchList= new PrioritizedPendingList();
        } else {
            this.batchList = new OrderedPendingList();
        }
    }
    
    
    public final synchronized void start() throws Exception{
        if (!isStarted()) {
            super.start();
            resetBatch();
            resetSize();
            setCacheEnabled(!this.storeHasMessages&&useCache);
        } 
    }

    protected void resetSize() {
        this.size = getStoreSize();
        this.storeHasMessages=this.size > 0;
    }

    @Override
    public void rebase() {
        resetSize();
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
            if (LOG.isDebugEnabled()) {
                LOG.debug(this + " - cursor got duplicate: " + message.getMessageId() + "," + message.getPriority() + ", cached=" + cached, new Throwable("duplicate message detected"));
            } else {
                LOG.warn("{} - cursor got duplicate {}", regionDestination.getActiveMQDestination(), message.getMessageId());
            }
            if (!cached ||  message.getMessageId().getEntryLocator() != null) {
                // came from the store or was added to the jdbc store
                duplicate(message);
            }
        }
        return recovered;
    }

    // track for processing outside of store index lock so we can dlq
    final LinkedList<Message> duplicatesFromStore = new LinkedList<Message>();
    private void duplicate(Message message) {
        duplicatesFromStore.add(message);
    }

    void dealWithDuplicates() {
        for (Message message : duplicatesFromStore) {
            regionDestination.duplicateFromStore(message, getSubscription());
        }
        duplicatesFromStore.clear();
    }

    public final synchronized void reset() {
        if (batchList.isEmpty()) {
            try {
                fillBatch();
            } catch (Exception e) {
                LOG.error("{} - Failed to fill batch", this, e);
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
                LOG.error("{} - Failed to fill batch", this, e);
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
        boolean disableCache = false;
        if (hasSpace()) {
            if (!isCacheEnabled() && size==0 && isStarted() && useCache) {
                LOG.trace("{} - enabling cache for empty store {}", this, node.getMessageId());
                setCacheEnabled(true);
            }
            if (isCacheEnabled()) {
                if (recoverMessage(node.getMessage(),true)) {
                    lastCachedId = node.getMessageId();
                    lastTx = node.getMessage().getTransactionId();
                } else {
                    dealWithDuplicates();
                    return;
                }
            }
        } else {
            disableCache = true;
        }

        if (disableCache && isCacheEnabled()) {
            setCacheEnabled(false);
            // sync with store on disabling the cache
            if (lastCachedId != null) {
                LOG.debug("{} - disabling cache, lastCachedId: {} last-tx: {} current node Id: {} node-tx: {} batchList size: {}",
                        new Object[]{ this, lastCachedId, lastTx, node.getMessageId(), node.getMessage().getTransactionId(), batchList.size() });
                setBatch(lastCachedId);
                lastCachedId = null;
                lastTx = null;
            }
        }
        this.storeHasMessages = true;
        size++;
    }

    protected void setBatch(MessageId messageId) throws Exception {
    }

    
    public synchronized void addMessageFirst(MessageReference node) throws Exception {
        setCacheEnabled(false);
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
    }

    
    public final synchronized void remove(MessageReference node) {
        if (batchList.remove(node) != null) {
            size--;
            setCacheEnabled(false);
        }
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
        setCacheEnabled(false);
    }

    protected final synchronized void fillBatch() {
        //LOG.trace("{} - fillBatch", this);
        if (batchResetNeeded) {
            resetSize();
            setMaxBatchSize(Math.min(regionDestination.getMaxPageSize(), size));
            resetBatch();
            this.batchResetNeeded = false;
        }
        if (this.batchList.isEmpty() && this.storeHasMessages && this.size >0) {
            try {
                doFillBatch();
            } catch (Exception e) {
                LOG.error("{} - Failed to fill batch", this, e);
                throw new RuntimeException(e);
            }
            this.storeHasMessages = !this.batchList.isEmpty() || !hadSpace;
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

    @Override
    public String toString() {
        return super.toString() + ":" + regionDestination.getActiveMQDestination().getPhysicalName() + ",batchResetNeeded=" + batchResetNeeded
                    + ",storeHasMessages=" + this.storeHasMessages + ",size=" + this.size + ",cacheEnabled=" + isCacheEnabled()
                    + ",maxBatchSize:" + maxBatchSize + ",hasSpace:" + hasSpace();
    }
    
    protected abstract void doFillBatch() throws Exception;
    
    protected abstract void resetBatch();

    protected abstract int getStoreSize();
    
    protected abstract boolean isStoreEmpty();

    public Subscription getSubscription() {
        return null;
    }
}
