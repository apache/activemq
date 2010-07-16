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
package org.apache.activemq.store.amq;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.JournalQueueAck;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.kaha.MessageAckWithLocation;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ReferenceStore;
import org.apache.activemq.store.ReferenceStore.ReferenceData;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.TransactionTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A MessageStore that uses a Journal to store it's messages.
 * 
 * @version $Revision: 1.14 $
 */
public class AMQMessageStore extends AbstractMessageStore {
    private static final Log LOG = LogFactory.getLog(AMQMessageStore.class);
    protected final AMQPersistenceAdapter peristenceAdapter;
    protected final AMQTransactionStore transactionStore;
    protected final ReferenceStore referenceStore;
    protected final TransactionTemplate transactionTemplate;
    protected Location lastLocation;
    protected Location lastWrittenLocation;
    protected Set<Location> inFlightTxLocations = new HashSet<Location>();
    protected final TaskRunner asyncWriteTask;
    protected CountDownLatch flushLatch;
    private Map<MessageId, ReferenceData> messages = new LinkedHashMap<MessageId, ReferenceData>();
    private List<MessageAckWithLocation> messageAcks = new ArrayList<MessageAckWithLocation>();
    /** A MessageStore that we can use to retrieve messages quickly. */
    private Map<MessageId, ReferenceData> cpAddedMessageIds;
    private final boolean debug = LOG.isDebugEnabled();
    private final AtomicReference<Location> mark = new AtomicReference<Location>();
    protected final Lock lock;

    public AMQMessageStore(AMQPersistenceAdapter adapter, ReferenceStore referenceStore, ActiveMQDestination destination) {
        super(destination);
        this.peristenceAdapter = adapter;
        this.lock = referenceStore.getStoreLock();
        this.transactionStore = adapter.getTransactionStore();
        this.referenceStore = referenceStore;
        this.transactionTemplate = new TransactionTemplate(adapter, new ConnectionContext(
                new NonCachedMessageEvaluationContext()));
        asyncWriteTask = adapter.getTaskRunnerFactory().createTaskRunner(new Task() {
            public boolean iterate() {
                asyncWrite();
                return false;
            }
        }, "Checkpoint: " + destination);
    }

    public void setMemoryUsage(MemoryUsage memoryUsage) {
        referenceStore.setMemoryUsage(memoryUsage);
    }

    /**
     * Not synchronize since the Journal has better throughput if you increase the number of concurrent writes that it
     * is doing.
     */
    public final void addMessage(ConnectionContext context, final Message message) throws IOException {
        final MessageId id = message.getMessageId();
        final Location location = peristenceAdapter.writeCommand(message, message.isResponseRequired());
        if (!context.isInTransaction()) {
            if (debug) {
                LOG.debug("Journalled message add for: " + id + ", at: " + location);
            }
            this.peristenceAdapter.addInProgressDataFile(this, location.getDataFileId());
            addMessage(message, location);
        } else {
            if (debug) {
                LOG.debug("Journalled transacted message add for: " + id + ", at: " + location);
            }
            lock.lock();
            try {
                inFlightTxLocations.add(location);
            } finally {
                lock.unlock();
            }
            transactionStore.addMessage(this, message, location);
            context.getTransaction().addSynchronization(new Synchronization() {
                public void afterCommit() throws Exception {
                    if (debug) {
                        LOG.debug("Transacted message add commit for: " + id + ", at: " + location);
                    }
                    lock.lock();
                    try {
                        inFlightTxLocations.remove(location);
                    } finally {
                        lock.unlock();
                    }
                    addMessage(message, location);
                }

                public void afterRollback() throws Exception {
                    if (debug) {
                        LOG.debug("Transacted message add rollback for: " + id + ", at: " + location);
                    }
                    lock.lock();
                    try {
                        inFlightTxLocations.remove(location);
                    } finally {
                        lock.unlock();
                    }
                }
            });
        }
    }

    final void addMessage(final Message message, final Location location) throws InterruptedIOException {
        ReferenceData data = new ReferenceData();
        data.setExpiration(message.getExpiration());
        data.setFileId(location.getDataFileId());
        data.setOffset(location.getOffset());
        lock.lock();
        try {
            lastLocation = location;
            messages.put(message.getMessageId(), data);
        } finally {
            lock.unlock();
        }
        if (messages.size() > this.peristenceAdapter.getMaxCheckpointMessageAddSize()) {
            flush();
        } else {
            try {
                asyncWriteTask.wakeup();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }
    }

    public boolean replayAddMessage(ConnectionContext context, Message message, Location location) {
        MessageId id = message.getMessageId();
        try {
            // Only add the message if it has not already been added.
            ReferenceData data = referenceStore.getMessageReference(id);
            if (data == null) {
                data = new ReferenceData();
                data.setExpiration(message.getExpiration());
                data.setFileId(location.getDataFileId());
                data.setOffset(location.getOffset());
                referenceStore.addMessageReference(context, id, data);
                return true;
            }
        } catch (Throwable e) {
            LOG.warn("Could not replay add for message '" + id + "'.  Message may have already been added. reason: "
                    + e, e);
        }
        return false;
    }

    /**
     */
    public void removeMessage(final ConnectionContext context, final MessageAck ack) throws IOException {
        JournalQueueAck remove = new JournalQueueAck();
        remove.setDestination(destination);
        remove.setMessageAck(ack);
        final Location location = peristenceAdapter.writeCommand(remove, ack.isResponseRequired());
        if (!context.isInTransaction()) {
            if (debug) {
                LOG.debug("Journalled message remove for: " + ack.getLastMessageId() + ", at: " + location);
            }
            removeMessage(ack, location);
        } else {
            if (debug) {
                LOG.debug("Journalled transacted message remove for: " + ack.getLastMessageId() + ", at: " + location);
            }
            lock.lock();
            try {
                inFlightTxLocations.add(location);
            } finally {
                lock.unlock();
            }
            transactionStore.removeMessage(this, ack, location);
            context.getTransaction().addSynchronization(new Synchronization() {
                public void afterCommit() throws Exception {
                    if (debug) {
                        LOG.debug("Transacted message remove commit for: " + ack.getLastMessageId() + ", at: "
                                + location);
                    }
                    lock.lock();
                    try {
                        inFlightTxLocations.remove(location);
                    } finally {
                        lock.unlock();
                    }
                    removeMessage(ack, location);
                }

                public void afterRollback() throws Exception {
                    if (debug) {
                        LOG.debug("Transacted message remove rollback for: " + ack.getLastMessageId() + ", at: "
                                + location);
                    }
                    lock.lock();
                    try {
                        inFlightTxLocations.remove(location);
                    } finally {
                        lock.unlock();
                    }
                }
            });
        }
    }

    final void removeMessage(final MessageAck ack, final Location location) throws InterruptedIOException {
        ReferenceData data;
        lock.lock();
        try {
            lastLocation = location;
            MessageId id = ack.getLastMessageId();
            data = messages.remove(id);
            if (data == null) {
                messageAcks.add(new MessageAckWithLocation(ack, location));
            } else {
                // message never got written so datafileReference will still exist
                AMQMessageStore.this.peristenceAdapter.removeInProgressDataFile(AMQMessageStore.this, data.getFileId());
            }
        } finally {
            lock.unlock();
        }
        if (messageAcks.size() > this.peristenceAdapter.getMaxCheckpointMessageAddSize()) {
            flush();
        } else if (data == null) {
            try {
                asyncWriteTask.wakeup();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }
    }

    public boolean replayRemoveMessage(ConnectionContext context, MessageAck messageAck) {
        try {
            // Only remove the message if it has not already been removed.
            ReferenceData t = referenceStore.getMessageReference(messageAck.getLastMessageId());
            if (t != null) {
                referenceStore.removeMessage(context, messageAck);
                return true;
            }
        } catch (Throwable e) {
            LOG.warn("Could not replay acknowledge for message '" + messageAck.getLastMessageId()
                    + "'.  Message may have already been acknowledged. reason: " + e);
        }
        return false;
    }

    /**
     * Waits till the lastest data has landed on the referenceStore
     * 
     * @throws InterruptedIOException
     */
    public void flush() throws InterruptedIOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("flush starting ...");
        }
        CountDownLatch countDown;
        lock.lock();
        try {
            if (lastWrittenLocation == lastLocation) {
                return;
            }
            if (flushLatch == null) {
                flushLatch = new CountDownLatch(1);
            }
            countDown = flushLatch;
        } finally {
            lock.unlock();
        }
        try {
            asyncWriteTask.wakeup();
            countDown.await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("flush finished");
        }
    }

    /**
     * @return
     * @throws IOException
     */
    synchronized void asyncWrite() {
        try {
            CountDownLatch countDown;
            lock.lock();
            try {
                countDown = flushLatch;
                flushLatch = null;
            } finally {
                lock.unlock();
            }
            mark.set(doAsyncWrite());
            if (countDown != null) {
                countDown.countDown();
            }
        } catch (IOException e) {
            LOG.error("Checkpoint failed: " + e, e);
        }
    }

    /**
     * @return
     * @throws IOException
     */
    protected Location doAsyncWrite() throws IOException {
        final List<MessageAckWithLocation> cpRemovedMessageLocations;
        final List<Location> cpActiveJournalLocations;
        final int maxCheckpointMessageAddSize = peristenceAdapter.getMaxCheckpointMessageAddSize();
        final Location lastLocation;
        // swap out the message hash maps..
        lock.lock();
        try {
            cpAddedMessageIds = this.messages;
            cpRemovedMessageLocations = this.messageAcks;
            cpActiveJournalLocations = new ArrayList<Location>(inFlightTxLocations);
            this.messages = new LinkedHashMap<MessageId, ReferenceData>();
            this.messageAcks = new ArrayList<MessageAckWithLocation>();
            lastLocation = this.lastLocation;
        } finally {
            lock.unlock();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Doing batch update... adding: " + cpAddedMessageIds.size() + " removing: "
                    + cpRemovedMessageLocations.size() + " ");
        }
        transactionTemplate.run(new Callback() {
            public void execute() throws Exception {
                int size = 0;
                PersistenceAdapter persitanceAdapter = transactionTemplate.getPersistenceAdapter();
                ConnectionContext context = transactionTemplate.getContext();
                // Checkpoint the added messages.
                Iterator<Entry<MessageId, ReferenceData>> iterator = cpAddedMessageIds.entrySet().iterator();
                while (iterator.hasNext()) {
                    Entry<MessageId, ReferenceData> entry = iterator.next();
                    try {
                        if (referenceStore.addMessageReference(context, entry.getKey(), entry.getValue())) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("adding message ref:" + entry.getKey());
                            }
                            size++;
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("not adding duplicate reference: " + entry.getKey() + ", " + entry.getValue());
                            }
                        }
                        AMQMessageStore.this.peristenceAdapter.removeInProgressDataFile(AMQMessageStore.this, entry
                                .getValue().getFileId());
                    } catch (Throwable e) {
                        LOG.warn("Message could not be added to long term store: " + e.getMessage(), e);
                    }
                    
                    // Commit the batch if it's getting too big
                    if (size >= maxCheckpointMessageAddSize) {
                        persitanceAdapter.commitTransaction(context);
                        persitanceAdapter.beginTransaction(context);
                        size = 0;
                    }
                }
                persitanceAdapter.commitTransaction(context);
                persitanceAdapter.beginTransaction(context);
                // Checkpoint the removed messages.
                for (MessageAckWithLocation ack : cpRemovedMessageLocations) {
                    try {
                        referenceStore.removeMessage(transactionTemplate.getContext(), ack);
                    } catch (Throwable e) {
                        LOG.warn("Message could not be removed from long term store: " + e.getMessage(), e);
                    }
                }
            }
        });
        LOG.debug("Batch update done. lastLocation:" + lastLocation);
        lock.lock();
        try {
            cpAddedMessageIds = null;
            lastWrittenLocation = lastLocation;
        } finally {
            lock.unlock();
        }
        if (cpActiveJournalLocations.size() > 0) {
            Collections.sort(cpActiveJournalLocations);
            return cpActiveJournalLocations.get(0);
        } else {
            return lastLocation;
        }
    }

    /**
     * 
     */
    public Message getMessage(MessageId identity) throws IOException {
        Location location = getLocation(identity);
        if (location != null) {
            DataStructure rc = peristenceAdapter.readCommand(location);
            try {
                return (Message) rc;
            } catch (ClassCastException e) {
                throw new IOException("Could not read message " + identity + " at location " + location
                        + ", expected a message, but got: " + rc);
            }
        }
        return null;
    }

    protected Location getLocation(MessageId messageId) throws IOException {
        ReferenceData data = null;
        lock.lock();
        try {
            // Is it still in flight???
            data = messages.get(messageId);
            if (data == null && cpAddedMessageIds != null) {
                data = cpAddedMessageIds.get(messageId);
            }
        } finally {
            lock.unlock();
        }
        if (data == null) {
            data = referenceStore.getMessageReference(messageId);
            if (data == null) {
                return null;
            }
        }
        Location location = new Location();
        location.setDataFileId(data.getFileId());
        location.setOffset(data.getOffset());
        return location;
    }

    /**
     * Replays the referenceStore first as those messages are the oldest ones, then messages are replayed from the
     * transaction log and then the cache is updated.
     * 
     * @param listener
     * @throws Exception
     */
    public void recover(final MessageRecoveryListener listener) throws Exception {
        flush();
        referenceStore.recover(new RecoveryListenerAdapter(this, listener));
    }

    public void start() throws Exception {
        referenceStore.start();
    }

    public void stop() throws Exception {
        flush();
        asyncWriteTask.shutdown();
        referenceStore.stop();
    }

    /**
     * @return Returns the longTermStore.
     */
    public ReferenceStore getReferenceStore() {
        return referenceStore;
    }

    /**
     * @see org.apache.activemq.store.MessageStore#removeAllMessages(ConnectionContext)
     */
    public void removeAllMessages(ConnectionContext context) throws IOException {
        flush();
        referenceStore.removeAllMessages(context);
    }

    public void addMessageReference(ConnectionContext context, MessageId messageId, long expirationTime,
            String messageRef) throws IOException {
        throw new IOException("The journal does not support message references.");
    }

    public String getMessageReference(MessageId identity) throws IOException {
        throw new IOException("The journal does not support message references.");
    }

    /**
     * @return
     * @throws IOException
     * @see org.apache.activemq.store.MessageStore#getMessageCount()
     */
    public int getMessageCount() throws IOException {
        flush();
        return referenceStore.getMessageCount();
    }

    public void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {
        RecoveryListenerAdapter recoveryListener = new RecoveryListenerAdapter(this, listener);
        referenceStore.recoverNextMessages(maxReturned, recoveryListener);
        if (recoveryListener.size() == 0 && recoveryListener.hasSpace()) {
            flush();
            referenceStore.recoverNextMessages(maxReturned, recoveryListener);
        }
    }

    Message getMessage(ReferenceData data) throws IOException {
        Location location = new Location();
        location.setDataFileId(data.getFileId());
        location.setOffset(data.getOffset());
        DataStructure rc = peristenceAdapter.readCommand(location);
        try {
            return (Message) rc;
        } catch (ClassCastException e) {
            throw new IOException("Could not read message  at location " + location + ", expected a message, but got: "
                    + rc);
        }
    }

    public void resetBatching() {
        referenceStore.resetBatching();
    }

    public Location getMark() {
        return mark.get();
    }

    public void dispose(ConnectionContext context) {
        try {
            flush();
        } catch (InterruptedIOException e) {
            Thread.currentThread().interrupt();
        }
        referenceStore.dispose(context);
        super.dispose(context);
    }

    public void setBatch(MessageId messageId) {
        try {
            flush();
        } catch (InterruptedIOException e) {
            LOG.debug("flush on setBatch resulted in exception", e);
        }
        getReferenceStore().setBatch(messageId);
    }

}
