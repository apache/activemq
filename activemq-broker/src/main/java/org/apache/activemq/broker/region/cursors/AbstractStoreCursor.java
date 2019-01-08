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
import java.util.ListIterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    protected int size;
    private final LinkedList<MessageId> pendingCachedIds = new LinkedList<>();
    private static int SYNC_ADD = 0;
    private static int ASYNC_ADD = 1;
    final MessageId[] lastCachedIds = new MessageId[2];
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


    @Override
    public final synchronized void start() throws Exception{
        if (!isStarted()) {
            super.start();
            resetBatch();
            resetSize();
            setCacheEnabled(size==0&&useCache);
        }
    }

    protected void resetSize() {
        this.size = getStoreSize();
    }

    @Override
    public void rebase() {
        MessageId lastAdded = lastCachedIds[SYNC_ADD];
        if (lastAdded != null) {
            try {
                setBatch(lastAdded);
            } catch (Exception e) {
                LOG.error("{} - Failed to set batch on rebase", this, e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public final synchronized void stop() throws Exception {
        resetBatch();
        super.stop();
        gc();
    }


    @Override
    public final boolean recoverMessage(Message message) throws Exception {
        return recoverMessage(message,false);
    }

    public synchronized boolean recoverMessage(Message message, boolean cached) throws Exception {
        boolean recovered = false;
        message.setRegionDestination(regionDestination);
        if (recordUniqueId(message.getMessageId())) {
            if (!cached) {
                if( message.getMemoryUsage()==null ) {
                    message.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
                }
            }
            message.incrementReferenceCount();
            batchList.addMessageLast(message);
            clearIterator(true);
            recovered = true;
        } else if (!cached) {
            // a duplicate from the store (!cached) - needs to be removed/acked - otherwise it will get re dispatched on restart
            if (duplicateFromStoreExcepted(message)) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{} store replayed pending message due to concurrentStoreAndDispatchQueues {} seq: {}", this, message.getMessageId(), message.getMessageId().getFutureOrSequenceLong());
                }
            } else {
                LOG.warn("{} - cursor got duplicate from store {} seq: {}", this, message.getMessageId(), message.getMessageId().getFutureOrSequenceLong());
                duplicate(message);
            }
        } else {
            LOG.warn("{} - cursor got duplicate send {} seq: {}", this, message.getMessageId(), message.getMessageId().getFutureOrSequenceLong());
            if (gotToTheStore(message)) {
                duplicate(message);
            }
        }
        return recovered;
    }

    protected boolean duplicateFromStoreExcepted(Message message) {
        // expected for messages pending acks with kahadb.concurrentStoreAndDispatchQueues=true for
        // which this existing unused flag has been repurposed
        return message.isRecievedByDFBridge();
    }

    public static boolean gotToTheStore(Message message) throws Exception {
        if (message.isRecievedByDFBridge()) {
            // concurrent store and dispatch - wait to see if the message gets to the store to see
            // if the index suppressed it (original still present), or whether it was stored and needs to be removed
            Object possibleFuture = message.getMessageId().getFutureOrSequenceLong();
            if (possibleFuture instanceof Future) {
                try {
                    ((Future) possibleFuture).get();
                } catch (Exception okToErrorOrCancelStoreOp) {}
            }
            // need to access again after wait on future
            Object sequence = message.getMessageId().getFutureOrSequenceLong();
            return (sequence != null && sequence instanceof Long && Long.compare((Long) sequence, -1l) != 0);
        }
        return true;
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

    @Override
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


    @Override
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


    @Override
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


    @Override
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

    @Override
    public synchronized boolean tryAddMessageLast(MessageReference node, long wait) throws Exception {
        boolean disableCache = false;
        if (hasSpace()) {
            if (isCacheEnabled()) {
                if (recoverMessage(node.getMessage(),true)) {
                    trackLastCached(node);
                } else {
                    dealWithDuplicates();
                    return false;
                }
            }
        } else {
            disableCache = true;
        }

        if (disableCache && isCacheEnabled()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} - disabling cache on add {} {}", this, node.getMessageId(), node.getMessageId().getFutureOrSequenceLong());
            }
            syncWithStore(node.getMessage());
            setCacheEnabled(false);
        }
        size++;
        return true;
    }

    @Override
    public synchronized boolean isCacheEnabled() {
        return super.isCacheEnabled() || enableCacheNow();
    }

    protected boolean enableCacheNow() {
        boolean result = false;
        if (canEnableCash()) {
            setCacheEnabled(true);
            result = true;
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} enabling cache on empty store", this);
            }
        }
        return result;
    }

    protected boolean canEnableCash() {
        return useCache && size==0 && hasSpace() && isStarted();
    }

    @Override
    public boolean canRecoveryNextMessage() {
        // Should be safe to recovery messages if the overall memory usage if < 90%
        return parentHasSpace(90);
    }

    private void syncWithStore(Message currentAdd) throws Exception {
        pruneLastCached();
        for (ListIterator<MessageId> it = pendingCachedIds.listIterator(pendingCachedIds.size()); it.hasPrevious(); ) {
            MessageId lastPending = it.previous();
            Object futureOrLong = lastPending.getFutureOrSequenceLong();
            if (futureOrLong instanceof Future) {
                Future future = (Future) futureOrLong;
                if (future.isCancelled()) {
                    continue;
                }
                try {
                    future.get(5, TimeUnit.SECONDS);
                    setLastCachedId(ASYNC_ADD, lastPending);
                } catch (CancellationException ok) {
                    continue;
                } catch (TimeoutException potentialDeadlock) {
                    LOG.debug("{} timed out waiting for async add", this, potentialDeadlock);
                } catch (Exception worstCaseWeReplay) {
                    LOG.debug("{} exception waiting for async add", this, worstCaseWeReplay);
                }
            } else {
                setLastCachedId(ASYNC_ADD, lastPending);
            }
            break;
        }

        MessageId candidate = lastCachedIds[ASYNC_ADD];
        if (candidate != null) {
            // ensure we don't skip current possibly sync add b/c we waited on the future
            if (!isAsync(currentAdd) && Long.compare(((Long) currentAdd.getMessageId().getFutureOrSequenceLong()), ((Long) lastCachedIds[ASYNC_ADD].getFutureOrSequenceLong())) < 0) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("no set batch from async:" + candidate.getFutureOrSequenceLong() + " >= than current: " + currentAdd.getMessageId().getFutureOrSequenceLong() + ", " + this);
                }
                candidate = null;
            }
        }
        if (candidate == null) {
            candidate = lastCachedIds[SYNC_ADD];
        }
        if (candidate != null) {
            setBatch(candidate);
        }
        // cleanup
        lastCachedIds[SYNC_ADD] = lastCachedIds[ASYNC_ADD] = null;
        pendingCachedIds.clear();
    }

    private void trackLastCached(MessageReference node) {
        if (isAsync(node.getMessage())) {
            pruneLastCached();
            pendingCachedIds.add(node.getMessageId());
        } else {
            setLastCachedId(SYNC_ADD, node.getMessageId());
        }
    }

    private static final boolean isAsync(Message message) {
        return message.isRecievedByDFBridge() || message.getMessageId().getFutureOrSequenceLong() instanceof Future;
    }

    private void pruneLastCached() {
        for (Iterator<MessageId> it = pendingCachedIds.iterator(); it.hasNext(); ) {
            MessageId candidate = it.next();
            final Object futureOrLong = candidate.getFutureOrSequenceLong();
            if (futureOrLong instanceof Future) {
                Future future = (Future) futureOrLong;
                if (future.isDone()) {
                    if (future.isCancelled()) {
                        it.remove();
                    } else {
                        // check for exception, we may be seeing old state
                        try {
                            future.get(0, TimeUnit.SECONDS);
                            // stale; if we get a result next prune will see Long
                        } catch (ExecutionException expected) {
                            it.remove();
                        } catch (Exception unexpected) {
                            LOG.debug("{} unexpected exception verifying exception state of future", this, unexpected);
                        }
                    }
                } else {
                    // we don't want to wait for work to complete
                    break;
                }
            } else {
                // complete
                setLastCachedId(ASYNC_ADD, candidate);

                // keep lock step with sync adds while order is preserved
                if (lastCachedIds[SYNC_ADD] != null) {
                    long next = 1 + (Long)lastCachedIds[SYNC_ADD].getFutureOrSequenceLong();
                    if (Long.compare((Long)futureOrLong, next) == 0) {
                        setLastCachedId(SYNC_ADD, candidate);
                    }
                }
                it.remove();
            }
        }
    }

    private void setLastCachedId(final int index, MessageId candidate) {
        MessageId lastCacheId = lastCachedIds[index];
        if (lastCacheId == null) {
            lastCachedIds[index] = candidate;
        } else {
            Object lastCacheFutureOrSequenceLong = lastCacheId.getFutureOrSequenceLong();
            Object candidateOrSequenceLong = candidate.getFutureOrSequenceLong();
            if (lastCacheFutureOrSequenceLong == null) { // possibly null for topics
                lastCachedIds[index] = candidate;
            } else if (candidateOrSequenceLong != null &&
                    Long.compare(((Long) candidateOrSequenceLong), ((Long) lastCacheFutureOrSequenceLong)) > 0) {
                lastCachedIds[index] = candidate;
            } else if (LOG.isTraceEnabled()) {
                LOG.trace("no set last cached[" + index + "] current:" + lastCacheFutureOrSequenceLong + " <= than candidate: " + candidateOrSequenceLong+ ", " + this);
            }
        }
    }

    protected void setBatch(MessageId messageId) throws Exception {
    }


    @Override
    public synchronized void addMessageFirst(MessageReference node) throws Exception {
        size++;
    }


    @Override
    public final synchronized void remove() {
        size--;
        if (iterator!=null) {
            iterator.remove();
        }
        if (last != null) {
            last.decrementReferenceCount();
        }
    }


    @Override
    public final synchronized void remove(MessageReference node) {
        if (batchList.remove(node) != null) {
            size--;
            setCacheEnabled(false);
        }
    }


    @Override
    public final synchronized void clear() {
        gc();
    }


    @Override
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

    @Override
    protected final synchronized void fillBatch() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} fillBatch", this);
        }
        if (batchResetNeeded) {
            resetSize();
            setMaxBatchSize(Math.min(regionDestination.getMaxPageSize(), size));
            resetBatch();
            this.batchResetNeeded = false;
        }
        if (this.batchList.isEmpty() && this.size >0) {
            try {
                doFillBatch();
            } catch (Exception e) {
                LOG.error("{} - Failed to fill batch", this, e);
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    public final synchronized boolean isEmpty() {
        // negative means more messages added to store through queue.send since last reset
        return size == 0;
    }


    @Override
    public final synchronized boolean hasMessagesBufferedToDeliver() {
        return !batchList.isEmpty();
    }


    @Override
    public final synchronized int size() {
        if (size < 0) {
            this.size = getStoreSize();
        }
        return size;
    }

    @Override
    public final synchronized long messageSize() {
        return getStoreMessageSize();
    }

    @Override
    public String toString() {
        return super.toString() + ":" + regionDestination.getActiveMQDestination().getPhysicalName() + ",batchResetNeeded=" + batchResetNeeded
                    + ",size=" + this.size + ",cacheEnabled=" + cacheEnabled
                    + ",maxBatchSize:" + maxBatchSize + ",hasSpace:" + hasSpace() + ",pendingCachedIds.size:" + pendingCachedIds.size()
                    + ",lastSyncCachedId:" + lastCachedIds[SYNC_ADD] + ",lastSyncCachedId-seq:" + (lastCachedIds[SYNC_ADD] != null ? lastCachedIds[SYNC_ADD].getFutureOrSequenceLong() : "null")
                    + ",lastAsyncCachedId:" + lastCachedIds[ASYNC_ADD] + ",lastAsyncCachedId-seq:" + (lastCachedIds[ASYNC_ADD] != null ? lastCachedIds[ASYNC_ADD].getFutureOrSequenceLong() : "null");
    }

    protected abstract void doFillBatch() throws Exception;

    protected abstract void resetBatch();

    protected abstract int getStoreSize();

    protected abstract long getStoreMessageSize();

    protected abstract boolean isStoreEmpty();

    public Subscription getSubscription() {
        return null;
    }
}
