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
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
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
    private LinkedList<MessageId> pendingCachedIds = new LinkedList<>();
    MessageId lastCachedId = null;
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
            LOG.warn("{} - cursor got duplicate {} seq: {}", this, message.getMessageId(), message.getMessageId().getFutureOrSequenceLong());

            // a duplicate from the store - needs to be removed/acked - otherwise it will get redispatched on restart
            // jdbc store will store duplicates and will set entry locator to sequence long.
            // REVISIT - this seems too hacky - see use case AMQ4952Test
            if (!cached || message.getMessageId().getEntryLocator() instanceof Long) {
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
    
    public final synchronized boolean addMessageLast(MessageReference node) throws Exception {
        boolean disableCache = false;
        if (hasSpace()) {
            if (!isCacheEnabled() && size==0 && isStarted() && useCache) {
                LOG.trace("{} - enabling cache for empty store {} {}", this, node.getMessageId(), node.getMessageId().getFutureOrSequenceLong());
                setCacheEnabled(true);
            }
            if (isCacheEnabled()) {
                if (recoverMessage(node.getMessage(),true)) {
                    if (node.getMessageId().getFutureOrSequenceLong() instanceof Future) {
                        pruneLastCached();
                        pendingCachedIds.add(node.getMessageId());
                    } else {
                        setLastCachedId(node.getMessageId());
                    }
                } else {
                    dealWithDuplicates();
                    return false;
                }
            }
        } else {
            disableCache = true;
        }

        if (disableCache && isCacheEnabled()) {
            setCacheEnabled(false);
            // sync with store on disabling the cache
            if (!pendingCachedIds.isEmpty() || lastCachedId != null) {
                LOG.trace("{} - disabling cache. current Id: {} seq: {}, batchList size: {}",
                            new Object[]{this, node.getMessageId(), node.getMessageId().getFutureOrSequenceLong(), batchList.size()});
                collapseLastCachedIds();
                if (lastCachedId != null) {
                    setBatch(lastCachedId);
                    lastCachedId = null;
                }
            }
        }
        this.storeHasMessages = true;
        size++;
        return true;
    }


    private void pruneLastCached() {
        for (Iterator<MessageId> it = pendingCachedIds.iterator(); it.hasNext(); ) {
            MessageId candidate = it.next();
            final Object futureOrLong = candidate.getFutureOrSequenceLong();
            if (futureOrLong instanceof Future) {
                Future future = (Future) futureOrLong;
                if (future.isCancelled()) {
                    it.remove();
                }
            } else {
                // store complete - track via lastCachedId
                setLastCachedId(candidate);
                it.remove();
            }
        }
    }

    private void collapseLastCachedIds() throws Exception {
        for (MessageId candidate : pendingCachedIds) {
            final Object futureOrLong = candidate.getFutureOrSequenceLong();
            if (futureOrLong instanceof Future) {
                Future future = (Future) futureOrLong;
                try {
                    future.get();
                    // future should be replaced with sequence by this time
                } catch (CancellationException ignored) {
                    continue;
                }
            }
            setLastCachedId(candidate);
        }
        pendingCachedIds.clear();
    }

    private void setLastCachedId(MessageId candidate) {
        if (lastCachedId == null) {
            lastCachedId = candidate;
        } else if (Long.compare(((Long) candidate.getFutureOrSequenceLong()), ((Long) lastCachedId.getFutureOrSequenceLong())) > 0) {
            lastCachedId = candidate;
        }
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
        for (MessageReference msg : batchList) {
            rollback(msg.getMessageId());
            msg.decrementReferenceCount();
        }
        batchList.clear();
        clearIterator(false);
        batchResetNeeded = true;
        setCacheEnabled(false);
    }

    protected final synchronized void fillBatch() {
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
                    + ",maxBatchSize:" + maxBatchSize + ",hasSpace:" + hasSpace() + ",pendingCachedIds.size:" + pendingCachedIds.size() + ",lastCachedId:" + lastCachedId;
    }
    
    protected abstract void doFillBatch() throws Exception;
    
    protected abstract void resetBatch();

    protected abstract int getStoreSize();
    
    protected abstract boolean isStoreEmpty();

    public Subscription getSubscription() {
        return null;
    }
}
