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
package org.apache.activemq.store.kahadb;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.IndexListener;
import org.apache.activemq.store.ListenableFuture;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.MessageStoreStatistics;
import org.apache.activemq.store.MessageStoreSubscriptionStatistics;
import org.apache.activemq.store.NoLocalSubscriptionAware;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionIdTransformer;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaDestination.DestinationType;
import org.apache.activemq.store.kahadb.data.KahaLocation;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.data.KahaUpdateMessageCommand;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.SequenceSet;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ThreadPoolUtils;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KahaDBStore extends MessageDatabase implements PersistenceAdapter, NoLocalSubscriptionAware {
    static final Logger LOG = LoggerFactory.getLogger(KahaDBStore.class);
    private static final int MAX_ASYNC_JOBS = BaseDestination.MAX_AUDIT_DEPTH;

    public static final String PROPERTY_CANCELED_TASK_MOD_METRIC = "org.apache.activemq.store.kahadb.CANCELED_TASK_MOD_METRIC";
    public static final int cancelledTaskModMetric = Integer.parseInt(System.getProperty(
            PROPERTY_CANCELED_TASK_MOD_METRIC, "0"), 10);
    public static final String PROPERTY_ASYNC_EXECUTOR_MAX_THREADS = "org.apache.activemq.store.kahadb.ASYNC_EXECUTOR_MAX_THREADS";
    private static final int asyncExecutorMaxThreads = Integer.parseInt(System.getProperty(
            PROPERTY_ASYNC_EXECUTOR_MAX_THREADS, "1"), 10);;

    protected ExecutorService queueExecutor;
    protected ExecutorService topicExecutor;
    protected final List<Map<AsyncJobKey, StoreTask>> asyncQueueMaps = new LinkedList<Map<AsyncJobKey, StoreTask>>();
    protected final List<Map<AsyncJobKey, StoreTask>> asyncTopicMaps = new LinkedList<Map<AsyncJobKey, StoreTask>>();
    final WireFormat wireFormat = new OpenWireFormat();
    private SystemUsage usageManager;
    private LinkedBlockingQueue<Runnable> asyncQueueJobQueue;
    private LinkedBlockingQueue<Runnable> asyncTopicJobQueue;
    Semaphore globalQueueSemaphore;
    Semaphore globalTopicSemaphore;
    private boolean concurrentStoreAndDispatchQueues = true;
    // when true, message order may be compromised when cache is exhausted if store is out
    // or order w.r.t cache
    private boolean concurrentStoreAndDispatchTopics = false;
    private final boolean concurrentStoreAndDispatchTransactions = false;
    private int maxAsyncJobs = MAX_ASYNC_JOBS;
    private final KahaDBTransactionStore transactionStore;
    private TransactionIdTransformer transactionIdTransformer;

    public KahaDBStore() {
        this.transactionStore = new KahaDBTransactionStore(this);
        this.transactionIdTransformer = new TransactionIdTransformer() {
            @Override
            public TransactionId transform(TransactionId txid) {
                return txid;
            }
        };
    }

    @Override
    public String toString() {
        return "KahaDB:[" + directory.getAbsolutePath() + "]";
    }

    @Override
    public void setBrokerName(String brokerName) {
    }

    @Override
    public void setUsageManager(SystemUsage usageManager) {
        this.usageManager = usageManager;
    }

    public SystemUsage getUsageManager() {
        return this.usageManager;
    }

    /**
     * @return the concurrentStoreAndDispatch
     */
    public boolean isConcurrentStoreAndDispatchQueues() {
        return this.concurrentStoreAndDispatchQueues;
    }

    /**
     * @param concurrentStoreAndDispatch
     *            the concurrentStoreAndDispatch to set
     */
    public void setConcurrentStoreAndDispatchQueues(boolean concurrentStoreAndDispatch) {
        this.concurrentStoreAndDispatchQueues = concurrentStoreAndDispatch;
    }

    /**
     * @return the concurrentStoreAndDispatch
     */
    public boolean isConcurrentStoreAndDispatchTopics() {
        return this.concurrentStoreAndDispatchTopics;
    }

    /**
     * @param concurrentStoreAndDispatch
     *            the concurrentStoreAndDispatch to set
     */
    public void setConcurrentStoreAndDispatchTopics(boolean concurrentStoreAndDispatch) {
        this.concurrentStoreAndDispatchTopics = concurrentStoreAndDispatch;
    }

    public boolean isConcurrentStoreAndDispatchTransactions() {
        return this.concurrentStoreAndDispatchTransactions;
    }

    /**
     * @return the maxAsyncJobs
     */
    public int getMaxAsyncJobs() {
        return this.maxAsyncJobs;
    }

    /**
     * @param maxAsyncJobs
     *            the maxAsyncJobs to set
     */
    public void setMaxAsyncJobs(int maxAsyncJobs) {
        this.maxAsyncJobs = maxAsyncJobs;
    }


    @Override
    protected void configureMetadata() {
        if (brokerService != null) {
            metadata.openwireVersion = brokerService.getStoreOpenWireVersion();
            wireFormat.setVersion(metadata.openwireVersion);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Store OpenWire version configured as: {}", metadata.openwireVersion);
            }

        }
    }

    @Override
    public void doStart() throws Exception {
        //configure the metadata before start, right now
        //this is just the open wire version
        configureMetadata();

        super.doStart();

        if (brokerService != null) {
            // In case the recovered store used a different OpenWire version log a warning
            // to assist in determining why journal reads fail.
            if (metadata.openwireVersion != brokerService.getStoreOpenWireVersion()) {
                LOG.warn("Existing Store uses a different OpenWire version[{}] " +
                         "than the version configured[{}] reverting to the version " +
                         "used by this store, some newer broker features may not work" +
                         "as expected.",
                         metadata.openwireVersion, brokerService.getStoreOpenWireVersion());

                // Update the broker service instance to the actual version in use.
                wireFormat.setVersion(metadata.openwireVersion);
                brokerService.setStoreOpenWireVersion(metadata.openwireVersion);
            }
        }

        this.globalQueueSemaphore = new Semaphore(getMaxAsyncJobs());
        this.globalTopicSemaphore = new Semaphore(getMaxAsyncJobs());
        this.asyncQueueJobQueue = new LinkedBlockingQueue<Runnable>(getMaxAsyncJobs());
        this.asyncTopicJobQueue = new LinkedBlockingQueue<Runnable>(getMaxAsyncJobs());
        this.queueExecutor = new StoreTaskExecutor(1, asyncExecutorMaxThreads, 0L, TimeUnit.MILLISECONDS,
            asyncQueueJobQueue, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "ConcurrentQueueStoreAndDispatch");
                    thread.setDaemon(true);
                    return thread;
                }
            });
        this.topicExecutor = new StoreTaskExecutor(1, asyncExecutorMaxThreads, 0L, TimeUnit.MILLISECONDS,
            asyncTopicJobQueue, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "ConcurrentTopicStoreAndDispatch");
                    thread.setDaemon(true);
                    return thread;
                }
            });
    }

    @Override
    public void doStop(ServiceStopper stopper) throws Exception {
        // drain down async jobs
        LOG.info("Stopping async queue tasks");
        if (this.globalQueueSemaphore != null) {
            this.globalQueueSemaphore.tryAcquire(this.maxAsyncJobs, 60, TimeUnit.SECONDS);
        }
        synchronized (this.asyncQueueMaps) {
            for (Map<AsyncJobKey, StoreTask> m : asyncQueueMaps) {
                synchronized (m) {
                    for (StoreTask task : m.values()) {
                        task.cancel();
                    }
                }
            }
            this.asyncQueueMaps.clear();
        }
        LOG.info("Stopping async topic tasks");
        if (this.globalTopicSemaphore != null) {
            this.globalTopicSemaphore.tryAcquire(this.maxAsyncJobs, 60, TimeUnit.SECONDS);
        }
        synchronized (this.asyncTopicMaps) {
            for (Map<AsyncJobKey, StoreTask> m : asyncTopicMaps) {
                synchronized (m) {
                    for (StoreTask task : m.values()) {
                        task.cancel();
                    }
                }
            }
            this.asyncTopicMaps.clear();
        }
        if (this.globalQueueSemaphore != null) {
            this.globalQueueSemaphore.drainPermits();
        }
        if (this.globalTopicSemaphore != null) {
            this.globalTopicSemaphore.drainPermits();
        }
        if (this.queueExecutor != null) {
            ThreadPoolUtils.shutdownNow(queueExecutor);
            queueExecutor = null;
        }
        if (this.topicExecutor != null) {
            ThreadPoolUtils.shutdownNow(topicExecutor);
            topicExecutor = null;
        }
        LOG.info("Stopped KahaDB");
        super.doStop(stopper);
    }

    private Location findMessageLocation(final String key, final KahaDestination destination) throws IOException {
        return pageFile.tx().execute(new Transaction.CallableClosure<Location, IOException>() {
            @Override
            public Location execute(Transaction tx) throws IOException {
                StoredDestination sd = getStoredDestination(destination, tx);
                Long sequence = sd.messageIdIndex.get(tx, key);
                if (sequence == null) {
                    return null;
                }
                return sd.orderIndex.get(tx, sequence).location;
            }
        });
    }

    protected StoreQueueTask removeQueueTask(KahaDBMessageStore store, MessageId id) {
        StoreQueueTask task = null;
        synchronized (store.asyncTaskMap) {
            task = (StoreQueueTask) store.asyncTaskMap.remove(new AsyncJobKey(id, store.getDestination()));
        }
        return task;
    }

    // with asyncTaskMap locked
    protected void addQueueTask(KahaDBMessageStore store, StoreQueueTask task) throws IOException {
        store.asyncTaskMap.put(new AsyncJobKey(task.getMessage().getMessageId(), store.getDestination()), task);
        this.queueExecutor.execute(task);
    }

    protected StoreTopicTask removeTopicTask(KahaDBTopicMessageStore store, MessageId id) {
        StoreTopicTask task = null;
        synchronized (store.asyncTaskMap) {
            task = (StoreTopicTask) store.asyncTaskMap.remove(new AsyncJobKey(id, store.getDestination()));
        }
        return task;
    }

    protected void addTopicTask(KahaDBTopicMessageStore store, StoreTopicTask task) throws IOException {
        synchronized (store.asyncTaskMap) {
            store.asyncTaskMap.put(new AsyncJobKey(task.getMessage().getMessageId(), store.getDestination()), task);
        }
        this.topicExecutor.execute(task);
    }

    @Override
    public TransactionStore createTransactionStore() throws IOException {
        return this.transactionStore;
    }

    public boolean getForceRecoverIndex() {
        return this.forceRecoverIndex;
    }

    public void setForceRecoverIndex(boolean forceRecoverIndex) {
        this.forceRecoverIndex = forceRecoverIndex;
    }

    public void forgetRecoveredAcks(ArrayList<MessageAck> preparedAcks, boolean isRollback) throws IOException {
        if (preparedAcks != null) {
            Map<ActiveMQDestination, KahaDBMessageStore> stores = new HashMap<>();
            for (MessageAck ack : preparedAcks) {
                stores.put(ack.getDestination(), findMatchingStore(ack.getDestination()));
            }
            ArrayList<MessageAck> perStoreAcks = new ArrayList<>();
            for (Entry<ActiveMQDestination, KahaDBMessageStore> entry : stores.entrySet()) {
                for (MessageAck ack : preparedAcks) {
                    if (entry.getKey().equals(ack.getDestination())) {
                        perStoreAcks.add(ack);
                    }
                }
                entry.getValue().forgetRecoveredAcks(perStoreAcks, isRollback);
                perStoreAcks.clear();
            }
        }
    }

    public void trackRecoveredAcks(ArrayList<MessageAck> preparedAcks) throws IOException {
        Map<ActiveMQDestination, KahaDBMessageStore> stores = new HashMap<>();
        for (MessageAck ack : preparedAcks) {
            stores.put(ack.getDestination(), findMatchingStore(ack.getDestination()));
        }
        ArrayList<MessageAck> perStoreAcks = new ArrayList<>();
        for (Entry<ActiveMQDestination, KahaDBMessageStore> entry : stores.entrySet()) {
            for (MessageAck ack : preparedAcks) {
                if (entry.getKey().equals(ack.getDestination())) {
                    perStoreAcks.add(ack);
                }
            }
            entry.getValue().trackRecoveredAcks(perStoreAcks);
            perStoreAcks.clear();
        }
    }

    private KahaDBMessageStore findMatchingStore(ActiveMQDestination activeMQDestination) throws IOException {
        ProxyMessageStore store = (ProxyMessageStore) storeCache.get(key(convert(activeMQDestination)));
        if (store == null) {
            if (activeMQDestination.isQueue()) {
                store = (ProxyMessageStore) createQueueMessageStore((ActiveMQQueue) activeMQDestination);
            } else {
                store = (ProxyMessageStore) createTopicMessageStore((ActiveMQTopic) activeMQDestination);
            }
        }
        return (KahaDBMessageStore) store.getDelegate();
    }

    public class KahaDBMessageStore extends AbstractMessageStore {
        protected final Map<AsyncJobKey, StoreTask> asyncTaskMap = new HashMap<AsyncJobKey, StoreTask>();
        protected KahaDestination dest;
        private final int maxAsyncJobs;
        private final Semaphore localDestinationSemaphore;
        protected final HashMap<String, Set<String>> ackedAndPreparedMap = new HashMap<String, Set<String>>();
        protected final HashMap<String, Set<String>> rolledBackAcksMap = new HashMap<String, Set<String>>();

        double doneTasks, canceledTasks = 0;

        public KahaDBMessageStore(ActiveMQDestination destination) {
            super(destination);
            this.dest = convert(destination);
            this.maxAsyncJobs = getMaxAsyncJobs();
            this.localDestinationSemaphore = new Semaphore(this.maxAsyncJobs);
        }

        @Override
        public ActiveMQDestination getDestination() {
            return destination;
        }


        private final String recoveredTxStateMapKey(ActiveMQDestination destination, MessageAck ack) {
            return destination.isQueue() ? destination.getPhysicalName() : ack.getConsumerId().getConnectionId();
        }

        // messages that have prepared (pending) acks cannot be re-dispatched unless the outcome is rollback,
        // till then they are skipped by the store.
        // 'at most once' XA guarantee
        public void trackRecoveredAcks(ArrayList<MessageAck> acks) {
            indexLock.writeLock().lock();
            try {
                for (MessageAck ack : acks) {
                    final String key = recoveredTxStateMapKey(destination, ack);
                    Set ackedAndPrepared = ackedAndPreparedMap.get(key);
                    if (ackedAndPrepared == null) {
                        ackedAndPrepared = new LinkedHashSet<String>();
                        ackedAndPreparedMap.put(key, ackedAndPrepared);
                    }
                    ackedAndPrepared.add(ack.getLastMessageId().toProducerKey());
                }
            } finally {
                indexLock.writeLock().unlock();
            }
        }

        public void forgetRecoveredAcks(ArrayList<MessageAck> acks, boolean rollback) throws IOException {
            if (acks != null) {
                indexLock.writeLock().lock();
                try {
                    for (MessageAck ack : acks) {
                        final String id = ack.getLastMessageId().toProducerKey();
                        final String key = recoveredTxStateMapKey(destination, ack);
                        Set ackedAndPrepared = ackedAndPreparedMap.get(key);
                        if (ackedAndPrepared != null) {
                            ackedAndPrepared.remove(id);
                            if (ackedAndPreparedMap.isEmpty()) {
                                ackedAndPreparedMap.remove(key);
                            }
                        }
                        if (rollback) {
                            Set rolledBackAcks = rolledBackAcksMap.get(key);
                            if (rolledBackAcks == null) {
                                rolledBackAcks = new LinkedHashSet<String>();
                                rolledBackAcksMap.put(key, rolledBackAcks);
                            }
                            rolledBackAcks.add(id);
                            pageFile.tx().execute(tx -> {
                                incrementAndAddSizeToStoreStat(tx, dest, 0);
                            });
                        }
                    }
                } finally {
                    indexLock.writeLock().unlock();
                }
            }
        }

        @Override
        public ListenableFuture<Object> asyncAddQueueMessage(final ConnectionContext context, final Message message)
                throws IOException {
            if (isConcurrentStoreAndDispatchQueues()) {
                message.beforeMarshall(wireFormat);
                StoreQueueTask result = new StoreQueueTask(this, context, message);
                ListenableFuture<Object> future = result.getFuture();
                message.getMessageId().setFutureOrSequenceLong(future);
                message.setRecievedByDFBridge(true); // flag message as concurrentStoreAndDispatch
                result.aquireLocks();
                synchronized (asyncTaskMap) {
                    addQueueTask(this, result);
                    if (indexListener != null) {
                        indexListener.onAdd(new IndexListener.MessageContext(context, message, null));
                    }
                }
                return future;
            } else {
                return super.asyncAddQueueMessage(context, message);
            }
        }

        @Override
        public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
            if (isConcurrentStoreAndDispatchQueues()) {
                AsyncJobKey key = new AsyncJobKey(ack.getLastMessageId(), getDestination());
                StoreQueueTask task = null;
                synchronized (asyncTaskMap) {
                    task = (StoreQueueTask) asyncTaskMap.get(key);
                }
                if (task != null) {
                    if (ack.isInTransaction() || !task.cancel()) {
                        try {
                            task.future.get();
                        } catch (InterruptedException e) {
                            throw new InterruptedIOException(e.toString());
                        } catch (Exception ignored) {
                            LOG.debug("removeAsync: cannot cancel, waiting for add resulted in ex", ignored);
                        }
                        removeMessage(context, ack);
                    } else {
                        indexLock.writeLock().lock();
                        try {
                            metadata.producerSequenceIdTracker.isDuplicate(ack.getLastMessageId());
                        } finally {
                            indexLock.writeLock().unlock();
                        }
                        synchronized (asyncTaskMap) {
                            asyncTaskMap.remove(key);
                        }
                    }
                } else {
                    removeMessage(context, ack);
                }
            } else {
                removeMessage(context, ack);
            }
        }

        @Override
        public void addMessage(final ConnectionContext context, final Message message) throws IOException {
            final KahaAddMessageCommand command = new KahaAddMessageCommand();
            command.setDestination(dest);
            command.setMessageId(message.getMessageId().toProducerKey());
            command.setTransactionInfo(TransactionIdConversion.convert(transactionIdTransformer.transform(message.getTransactionId())));
            command.setPriority(message.getPriority());
            command.setPrioritySupported(isPrioritizedMessages());
            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(message);
            command.setMessage(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            store(command, isEnableJournalDiskSyncs() && message.isResponseRequired(), new IndexAware() {
                // sync add? (for async, future present from getFutureOrSequenceLong)
                Object possibleFuture = message.getMessageId().getFutureOrSequenceLong();

                @Override
                public void sequenceAssignedWithIndexLocked(final long sequence) {
                    message.getMessageId().setFutureOrSequenceLong(sequence);
                    if (indexListener != null) {
                        if (possibleFuture == null) {
                            trackPendingAdd(dest, sequence);
                            indexListener.onAdd(new IndexListener.MessageContext(context, message, new Runnable() {
                                @Override
                                public void run() {
                                    trackPendingAddComplete(dest, sequence);
                                }
                            }));
                        }
                    }
                }
            }, null);
        }

        @Override
        public void updateMessage(Message message) throws IOException {
            if (LOG.isTraceEnabled()) {
                LOG.trace("updating: " + message.getMessageId() + " with deliveryCount: " + message.getRedeliveryCounter());
            }
            KahaUpdateMessageCommand updateMessageCommand = new KahaUpdateMessageCommand();
            KahaAddMessageCommand command = new KahaAddMessageCommand();
            command.setDestination(dest);
            command.setMessageId(message.getMessageId().toProducerKey());
            command.setPriority(message.getPriority());
            command.setPrioritySupported(prioritizedMessages);
            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(message);
            command.setMessage(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            updateMessageCommand.setMessage(command);
            store(updateMessageCommand, isEnableJournalDiskSyncs(), null, null);
        }

        @Override
        public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
            KahaRemoveMessageCommand command = new KahaRemoveMessageCommand();
            command.setDestination(dest);
            command.setMessageId(ack.getLastMessageId().toProducerKey());
            command.setTransactionInfo(TransactionIdConversion.convert(transactionIdTransformer.transform(ack.getTransactionId())));

            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(ack);
            command.setAck(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            store(command, isEnableJournalDiskSyncs() && ack.isResponseRequired(), null, null);
        }

        @Override
        public void removeAllMessages(ConnectionContext context) throws IOException {
            KahaRemoveDestinationCommand command = new KahaRemoveDestinationCommand();
            command.setDestination(dest);
            store(command, true, null, null);
        }

        @Override
        public Message getMessage(MessageId identity) throws IOException {
            final String key = identity.toProducerKey();

            // Hopefully one day the page file supports concurrent read
            // operations... but for now we must
            // externally synchronize...
            Location location;
            indexLock.writeLock().lock();
            try {
                location = findMessageLocation(key, dest);
            } finally {
                indexLock.writeLock().unlock();
            }
            if (location == null) {
                return null;
            }

            return loadMessage(location);
        }

        @Override
        public boolean isEmpty() throws IOException {
            indexLock.writeLock().lock();
            try {
                return pageFile.tx().execute(new Transaction.CallableClosure<Boolean, IOException>() {
                    @Override
                    public Boolean execute(Transaction tx) throws IOException {
                        // Iterate through all index entries to get a count of
                        // messages in the destination.
                        StoredDestination sd = getStoredDestination(dest, tx);
                        return sd.locationIndex.isEmpty(tx);
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
        }

        @Override
        public void recover(final MessageRecoveryListener listener) throws Exception {
            // recovery may involve expiry which will modify
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    @Override
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        recoverRolledBackAcks(destination.getPhysicalName(), sd, tx, Integer.MAX_VALUE, listener);
                        sd.orderIndex.resetCursorPosition();
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); listener.hasSpace() && iterator
                                .hasNext(); ) {
                            Entry<Long, MessageKeys> entry = iterator.next();
                            Set ackedAndPrepared = ackedAndPreparedMap.get(destination.getPhysicalName());
                            if (ackedAndPrepared != null && ackedAndPrepared.contains(entry.getValue().messageId)) {
                                continue;
                            }
                            Message msg = loadMessage(entry.getValue().location);
                            listener.recoverMessage(msg);
                        }
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
        }

        @Override
        public void recoverNextMessages(final int maxReturned, final MessageRecoveryListener listener) throws Exception {
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    @Override
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Entry<Long, MessageKeys> entry = null;
                        int counter = recoverRolledBackAcks(destination.getPhysicalName(), sd, tx, maxReturned, listener);
                        Set ackedAndPrepared = ackedAndPreparedMap.get(destination.getPhysicalName());
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator.hasNext(); ) {
                            entry = iterator.next();
                            if (ackedAndPrepared != null && ackedAndPrepared.contains(entry.getValue().messageId)) {
                                continue;
                            }
                            Message msg = loadMessage(entry.getValue().location);
                            msg.getMessageId().setFutureOrSequenceLong(entry.getKey());
                            listener.recoverMessage(msg);
                            counter++;
                            if (counter >= maxReturned || !listener.canRecoveryNextMessage()) {
                                break;
                            }
                        }
                        sd.orderIndex.stoppedIterating();
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
        }

        protected int recoverRolledBackAcks(String recoveredTxStateMapKey, StoredDestination sd, Transaction tx, int maxReturned, MessageRecoveryListener listener) throws Exception {
            int counter = 0;
            String id;

            Set rolledBackAcks = rolledBackAcksMap.get(recoveredTxStateMapKey);
            if (rolledBackAcks == null) {
                return counter;
            }
            for (Iterator<String> iterator = rolledBackAcks.iterator(); iterator.hasNext(); ) {
                id = iterator.next();
                iterator.remove();
                Long sequence = sd.messageIdIndex.get(tx, id);
                if (sequence != null) {
                    if (sd.orderIndex.alreadyDispatched(sequence)) {
                        listener.recoverMessage(loadMessage(sd.orderIndex.get(tx, sequence).location));
                        counter++;
                        if (counter >= maxReturned) {
                            break;
                        }
                    } else {
                        LOG.debug("rolledback ack message {} with seq {} will be picked up in future batch {}", id, sequence, sd.orderIndex.cursor);
                    }
                } else {
                    LOG.warn("Failed to locate rolled back ack message {} in {}", id, sd);
                }
            }
            if (rolledBackAcks.isEmpty()) {
                rolledBackAcksMap.remove(recoveredTxStateMapKey);
            }
            return counter;
        }


        @Override
        public void resetBatching() {
            if (pageFile.isLoaded()) {
                indexLock.writeLock().lock();
                try {
                    pageFile.tx().execute(new Transaction.Closure<Exception>() {
                        @Override
                        public void execute(Transaction tx) throws Exception {
                            StoredDestination sd = getExistingStoredDestination(dest, tx);
                            if (sd != null) {
                                sd.orderIndex.resetCursorPosition();}
                            }
                        });
                } catch (Exception e) {
                    LOG.error("Failed to reset batching",e);
                } finally {
                    indexLock.writeLock().unlock();
                }
            }
        }

        @Override
        public void setBatch(final MessageId identity) throws IOException {
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long location = (Long) identity.getFutureOrSequenceLong();
                        Long pending = sd.orderIndex.minPendingAdd();
                        if (pending != null) {
                            location = Math.min(location, pending-1);
                        }
                        sd.orderIndex.setBatch(tx, location);
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
        }

        @Override
        public void setMemoryUsage(MemoryUsage memoryUsage) {
        }
        @Override
        public void start() throws Exception {
            super.start();
        }
        @Override
        public void stop() throws Exception {
            super.stop();
        }

        protected void lockAsyncJobQueue() {
            try {
                if (!this.localDestinationSemaphore.tryAcquire(this.maxAsyncJobs, 60, TimeUnit.SECONDS)) {
                    throw new TimeoutException(this +" timeout waiting for localDestSem:" + this.localDestinationSemaphore);
                }
            } catch (Exception e) {
                LOG.error("Failed to lock async jobs for " + this.destination, e);
            }
        }

        protected void unlockAsyncJobQueue() {
            this.localDestinationSemaphore.release(this.maxAsyncJobs);
        }

        protected void acquireLocalAsyncLock() {
            try {
                this.localDestinationSemaphore.acquire();
            } catch (InterruptedException e) {
                LOG.error("Failed to aquire async lock for " + this.destination, e);
            }
        }

        protected void releaseLocalAsyncLock() {
            this.localDestinationSemaphore.release();
        }

        @Override
        public String toString(){
            return "permits:" + this.localDestinationSemaphore.availablePermits() + ",sd=" + storedDestinations.get(key(dest));
        }

        @Override
        protected void recoverMessageStoreStatistics() throws IOException {
            try {
                MessageStoreStatistics recoveredStatistics;
                lockAsyncJobQueue();
                indexLock.writeLock().lock();
                try {
                    recoveredStatistics = pageFile.tx().execute(new Transaction.CallableClosure<MessageStoreStatistics, IOException>() {
                        @Override
                        public MessageStoreStatistics execute(Transaction tx) throws IOException {
                            MessageStoreStatistics statistics = getStoredMessageStoreStatistics(dest, tx);

                            // Iterate through all index entries to get the size of each message
                            if (statistics == null) {
                                StoredDestination sd = getStoredDestination(dest, tx);
                                statistics = new MessageStoreStatistics();
                                for (Iterator<Entry<Location, Long>> iterator = sd.locationIndex.iterator(tx); iterator.hasNext(); ) {
                                    int locationSize = iterator.next().getKey().getSize();
                                    statistics.getMessageCount().increment();
                                    statistics.getMessageSize().addSize(locationSize > 0 ? locationSize : 0);
                                }
                            }
                            return statistics;
                        }
                    });
                    Set ackedAndPrepared = ackedAndPreparedMap.get(destination.getPhysicalName());
                    if (ackedAndPrepared != null) {
                        recoveredStatistics.getMessageCount().subtract(ackedAndPrepared.size());
                    }
                    getMessageStoreStatistics().getMessageCount().setCount(recoveredStatistics.getMessageCount().getCount());
                    getMessageStoreStatistics().getMessageSize().setTotalSize(recoveredStatistics.getMessageSize().getTotalSize());
                } finally {
                    indexLock.writeLock().unlock();
                }
            } finally {
                unlockAsyncJobQueue();
            }
        }
    }

    class KahaDBTopicMessageStore extends KahaDBMessageStore implements TopicMessageStore {
        private final AtomicInteger subscriptionCount = new AtomicInteger();
        protected final MessageStoreSubscriptionStatistics messageStoreSubStats =
                new MessageStoreSubscriptionStatistics(isEnableSubscriptionStatistics());

        public KahaDBTopicMessageStore(ActiveMQTopic destination) throws IOException {
            super(destination);
            this.subscriptionCount.set(getAllSubscriptions().length);
            if (isConcurrentStoreAndDispatchTopics()) {
                asyncTopicMaps.add(asyncTaskMap);
            }
        }

        @Override
        protected void recoverMessageStoreStatistics() throws IOException {
            super.recoverMessageStoreStatistics();
            this.recoverMessageStoreSubMetrics();
        }

        @Override
        public ListenableFuture<Object> asyncAddTopicMessage(final ConnectionContext context, final Message message)
                throws IOException {
            if (isConcurrentStoreAndDispatchTopics()) {
                message.beforeMarshall(wireFormat);
                StoreTopicTask result = new StoreTopicTask(this, context, message, subscriptionCount.get());
                result.aquireLocks();
                addTopicTask(this, result);
                return result.getFuture();
            } else {
                return super.asyncAddTopicMessage(context, message);
            }
        }

        @Override
        public void acknowledge(ConnectionContext context, String clientId, String subscriptionName,
                                MessageId messageId, MessageAck ack) throws IOException {
            String subscriptionKey = subscriptionKey(clientId, subscriptionName).toString();
            if (isConcurrentStoreAndDispatchTopics()) {
                AsyncJobKey key = new AsyncJobKey(messageId, getDestination());
                StoreTopicTask task = null;
                synchronized (asyncTaskMap) {
                    task = (StoreTopicTask) asyncTaskMap.get(key);
                }
                if (task != null) {
                    if (task.addSubscriptionKey(subscriptionKey)) {
                        removeTopicTask(this, messageId);
                        if (task.cancel()) {
                            synchronized (asyncTaskMap) {
                                asyncTaskMap.remove(key);
                            }
                        }
                    }
                } else {
                    doAcknowledge(context, subscriptionKey, messageId, ack);
                }
            } else {
                doAcknowledge(context, subscriptionKey, messageId, ack);
            }
        }

        protected void doAcknowledge(ConnectionContext context, String subscriptionKey, MessageId messageId, MessageAck ack)
                throws IOException {
            KahaRemoveMessageCommand command = new KahaRemoveMessageCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey);
            command.setMessageId(messageId.toProducerKey());
            command.setTransactionInfo(ack != null ? TransactionIdConversion.convert(transactionIdTransformer.transform(ack.getTransactionId())) : null);
            if (ack != null && ack.isUnmatchedAck()) {
                command.setAck(UNMATCHED);
            } else {
                org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(ack);
                command.setAck(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            }
            store(command, false, null, null);
        }

        @Override
        public void addSubscription(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
            String subscriptionKey = subscriptionKey(subscriptionInfo.getClientId(), subscriptionInfo
                    .getSubscriptionName());
            KahaSubscriptionCommand command = new KahaSubscriptionCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey.toString());
            command.setRetroactive(retroactive);
            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(subscriptionInfo);
            command.setSubscriptionInfo(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            store(command, isEnableJournalDiskSyncs() && true, null, null);
            this.subscriptionCount.incrementAndGet();
        }

        @Override
        public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
            KahaSubscriptionCommand command = new KahaSubscriptionCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey(clientId, subscriptionName).toString());
            store(command, isEnableJournalDiskSyncs() && true, null, null);
            this.subscriptionCount.decrementAndGet();
        }

        @Override
        public SubscriptionInfo[] getAllSubscriptions() throws IOException {

            final ArrayList<SubscriptionInfo> subscriptions = new ArrayList<SubscriptionInfo>();
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        for (Iterator<Entry<String, KahaSubscriptionCommand>> iterator = sd.subscriptions.iterator(tx); iterator
                                .hasNext();) {
                            Entry<String, KahaSubscriptionCommand> entry = iterator.next();
                            SubscriptionInfo info = (SubscriptionInfo) wireFormat.unmarshal(new DataInputStream(entry
                                    .getValue().getSubscriptionInfo().newInput()));
                            subscriptions.add(info);

                        }
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }

            SubscriptionInfo[] rc = new SubscriptionInfo[subscriptions.size()];
            subscriptions.toArray(rc);
            return rc;
        }

        @Override
        public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            indexLock.writeLock().lock();
            try {
                return pageFile.tx().execute(new Transaction.CallableClosure<SubscriptionInfo, IOException>() {
                    @Override
                    public SubscriptionInfo execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        KahaSubscriptionCommand command = sd.subscriptions.get(tx, subscriptionKey);
                        if (command == null) {
                            return null;
                        }
                        return (SubscriptionInfo) wireFormat.unmarshal(new DataInputStream(command
                                .getSubscriptionInfo().newInput()));
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
        }

        @Override
        public int getMessageCount(String clientId, String subscriptionName) throws IOException {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);

            if (isEnableSubscriptionStatistics()) {
                return (int)this.messageStoreSubStats.getMessageCount(subscriptionKey).getCount();
            } else {

                indexLock.writeLock().lock();
                try {
                    return pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>() {
                        @Override
                        public Integer execute(Transaction tx) throws IOException {
                            StoredDestination sd = getStoredDestination(dest, tx);
                            LastAck cursorPos = getLastAck(tx, sd, subscriptionKey);
                            if (cursorPos == null) {
                                // The subscription might not exist.
                                return 0;
                            }

                            return (int) getStoredMessageCount(tx, sd, subscriptionKey);
                        }
                    });
                } finally {
                    indexLock.writeLock().unlock();
                }
            }
        }


        @Override
        public long getMessageSize(String clientId, String subscriptionName) throws IOException {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            if (isEnableSubscriptionStatistics()) {
                return this.messageStoreSubStats.getMessageSize(subscriptionKey).getTotalSize();
            } else {
                indexLock.writeLock().lock();
                try {
                    return pageFile.tx().execute(new Transaction.CallableClosure<Long, IOException>() {
                        @Override
                        public Long execute(Transaction tx) throws IOException {
                            StoredDestination sd = getStoredDestination(dest, tx);
                            LastAck cursorPos = getLastAck(tx, sd, subscriptionKey);
                            if (cursorPos == null) {
                                // The subscription might not exist.
                                return 0l;
                            }

                            return getStoredMessageSize(tx, sd, subscriptionKey);
                        }
                    });
                } finally {
                    indexLock.writeLock().unlock();
                }
            }
        }

        protected void recoverMessageStoreSubMetrics() throws IOException {
            if (isEnableSubscriptionStatistics()) {
                final MessageStoreSubscriptionStatistics statistics = getMessageStoreSubStatistics();
                indexLock.writeLock().lock();
                try {
                    pageFile.tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            StoredDestination sd = getStoredDestination(dest, tx);

                            List<String> subscriptionKeys = new ArrayList<>();
                            for (Iterator<Entry<String, KahaSubscriptionCommand>> iterator = sd.subscriptions
                                    .iterator(tx); iterator.hasNext();) {
                                Entry<String, KahaSubscriptionCommand> entry = iterator.next();

                                final String subscriptionKey = entry.getKey();
                                final LastAck cursorPos = getLastAck(tx, sd, subscriptionKey);
                                if (cursorPos != null) {
                                    //add the subscriptions to a list for recovering pending sizes below
                                    subscriptionKeys.add(subscriptionKey);
                                    //recover just the count here as that is fast
                                    statistics.getMessageCount(subscriptionKey)
                                            .setCount(getStoredMessageCount(tx, sd, subscriptionKey));
                                }
                            }

                            //Recover the message sizes for each subscription by iterating only 1 time over the order index
                            //to speed up recovery
                            final Map<String, AtomicLong> subPendingMessageSizes = getStoredMessageSize(tx, sd, subscriptionKeys);
                            subPendingMessageSizes.forEach((k,v) -> {
                                statistics.getMessageSize(k).addSize(v.get() > 0 ? v.get() : 0);
                            });
                        }
                    });
                } finally {
                    indexLock.writeLock().unlock();
                }
            }
        }

        @Override
        public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener)
                throws Exception {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            @SuppressWarnings("unused")
            final SubscriptionInfo info = lookupSubscription(clientId, subscriptionName);
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    @Override
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        LastAck cursorPos = getLastAck(tx, sd, subscriptionKey);
                        SequenceSet subAckPositions = getSequenceSet(tx, sd, subscriptionKey);
                        //If we have ackPositions tracked then compare the first one as individual acknowledge mode
                        //may have bumped lastAck even though there are earlier messages to still consume
                        if (subAckPositions != null && !subAckPositions.isEmpty()
                                && subAckPositions.getHead().getFirst() < cursorPos.lastAckedSequence) {
                            //we have messages to ack before lastAckedSequence
                            sd.orderIndex.setBatch(tx, subAckPositions.getHead().getFirst() - 1);
                        } else {
                            subAckPositions = null;
                            sd.orderIndex.setBatch(tx, cursorPos);
                        }
                        recoverRolledBackAcks(subscriptionKey, sd, tx, Integer.MAX_VALUE, listener);
                        Set ackedAndPrepared = ackedAndPreparedMap.get(subscriptionKey);
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator
                                .hasNext();) {
                            Entry<Long, MessageKeys> entry = iterator.next();
                            if (ackedAndPrepared != null && ackedAndPrepared.contains(entry.getValue().messageId)) {
                                continue;
                            }
                            //If subAckPositions is set then verify the sequence set contains the message still
                            //and if it doesn't skip it
                            if (subAckPositions != null && !subAckPositions.contains(entry.getKey())) {
                                continue;
                            }
                            listener.recoverMessage(loadMessage(entry.getValue().location));
                        }
                        sd.orderIndex.resetCursorPosition();
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
        }

        @Override
        public void recoverNextMessages(String clientId, String subscriptionName, final int maxReturned,
                final MessageRecoveryListener listener) throws Exception {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            @SuppressWarnings("unused")
            final SubscriptionInfo info = lookupSubscription(clientId, subscriptionName);
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    @Override
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        sd.orderIndex.resetCursorPosition();
                        MessageOrderCursor moc = sd.subscriptionCursors.get(subscriptionKey);
                        SequenceSet subAckPositions = null;
                        if (moc == null) {
                            LastAck pos = getLastAck(tx, sd, subscriptionKey);
                            if (pos == null) {
                                // sub deleted
                                return;
                            }
                            subAckPositions = getSequenceSet(tx, sd, subscriptionKey);
                            //If we have ackPositions tracked then compare the first one as individual acknowledge mode
                            //may have bumped lastAck even though there are earlier messages to still consume
                            if (subAckPositions != null && !subAckPositions.isEmpty()
                                    && subAckPositions.getHead().getFirst() < pos.lastAckedSequence) {
                                //we have messages to ack before lastAckedSequence
                                sd.orderIndex.setBatch(tx, subAckPositions.getHead().getFirst() - 1);
                            } else {
                                subAckPositions = null;
                                sd.orderIndex.setBatch(tx, pos);
                            }
                            moc = sd.orderIndex.cursor;
                        } else {
                            sd.orderIndex.cursor.sync(moc);
                        }

                        Entry<Long, MessageKeys> entry = null;
                        int counter = recoverRolledBackAcks(subscriptionKey, sd, tx, maxReturned, listener);
                        Set ackedAndPrepared = ackedAndPreparedMap.get(subscriptionKey);
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx, moc); iterator
                                .hasNext();) {
                            entry = iterator.next();
                            if (ackedAndPrepared != null && ackedAndPrepared.contains(entry.getValue().messageId)) {
                                continue;
                            }
                            //If subAckPositions is set then verify the sequence set contains the message still
                            //and if it doesn't skip it
                            if (subAckPositions != null && !subAckPositions.contains(entry.getKey())) {
                                continue;
                            }
                            if (listener.recoverMessage(loadMessage(entry.getValue().location))) {
                                counter++;
                            }
                            if (counter >= maxReturned || listener.hasSpace() == false) {
                                break;
                            }
                        }
                        sd.orderIndex.stoppedIterating();
                        if (entry != null) {
                            MessageOrderCursor copy = sd.orderIndex.cursor.copy();
                            sd.subscriptionCursors.put(subscriptionKey, copy);
                        }
                    }
                });
            } finally {
                indexLock.writeLock().unlock();
            }
        }

        @Override
        public void resetBatching(String clientId, String subscriptionName) {
            try {
                final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
                indexLock.writeLock().lock();
                try {
                    pageFile.tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            StoredDestination sd = getStoredDestination(dest, tx);
                            sd.subscriptionCursors.remove(subscriptionKey);
                        }
                    });
                }finally {
                    indexLock.writeLock().unlock();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public MessageStoreSubscriptionStatistics getMessageStoreSubStatistics() {
            return messageStoreSubStats;
        }
    }

    String subscriptionKey(String clientId, String subscriptionName) {
        return clientId + ":" + subscriptionName;
    }

    @Override
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        String key = key(convert(destination));
        MessageStore store = storeCache.get(key(convert(destination)));
        if (store == null) {
            final MessageStore queueStore = this.transactionStore.proxy(new KahaDBMessageStore(destination));
            store = storeCache.putIfAbsent(key, queueStore);
            if (store == null) {
                store = queueStore;
            }
        }

        return store;
    }

    @Override
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        String key = key(convert(destination));
        MessageStore store = storeCache.get(key(convert(destination)));
        if (store == null) {
            final TopicMessageStore topicStore = this.transactionStore.proxy(new KahaDBTopicMessageStore(destination));
            store = storeCache.putIfAbsent(key, topicStore);
            if (store == null) {
                store = topicStore;
            }
        }

        return (TopicMessageStore) store;
    }

    /**
     * Cleanup method to remove any state associated with the given destination.
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination
     *            Destination to forget
     */
    @Override
    public void removeQueueMessageStore(ActiveMQQueue destination) {
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination
     *            Destination to forget
     */
    @Override
    public void removeTopicMessageStore(ActiveMQTopic destination) {
    }

    @Override
    public void deleteAllMessages() throws IOException {
        deleteAllMessages = true;
    }

    @Override
    public Set<ActiveMQDestination> getDestinations() {
        try {
            final HashSet<ActiveMQDestination> rc = new HashSet<ActiveMQDestination>();
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        for (Iterator<Entry<String, StoredDestination>> iterator = metadata.destinations.iterator(tx); iterator
                                .hasNext();) {
                            Entry<String, StoredDestination> entry = iterator.next();
                            //Removing isEmpty topic check - see AMQ-5875
                            rc.add(convert(entry.getKey()));
                        }
                    }
                });
            }finally {
                indexLock.writeLock().unlock();
            }
            return rc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getLastMessageBrokerSequenceId() throws IOException {
        return 0;
    }

    @Override
    public long getLastProducerSequenceId(ProducerId id) {
        indexLock.writeLock().lock();
        try {
            return metadata.producerSequenceIdTracker.getLastSeqId(id);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public long size() {
        try {
            return journalSize.get() + getPageFile().getDiskSize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beginTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    @Override
    public void commitTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    @Override
    public void rollbackTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }

    @Override
    public void checkpoint(boolean sync) throws IOException {
        super.checkpointCleanup(sync);
    }

    // /////////////////////////////////////////////////////////////////
    // Internal helper methods.
    // /////////////////////////////////////////////////////////////////

    /**
     * @param location
     * @return
     * @throws IOException
     */
    Message loadMessage(Location location) throws IOException {
        try {
            JournalCommand<?> command = load(location);
            KahaAddMessageCommand addMessage = null;
            switch (command.type()) {
                case KAHA_UPDATE_MESSAGE_COMMAND:
                    addMessage = ((KahaUpdateMessageCommand) command).getMessage();
                    break;
                case KAHA_ADD_MESSAGE_COMMAND:
                    addMessage = (KahaAddMessageCommand) command;
                    break;
                default:
                    throw new IOException("Could not load journal record, unexpected command type: " + command.type() + " at location: " + location);
            }
            if (!addMessage.hasMessage()) {
                throw new IOException("Could not load journal record, null message content at location: " + location);
            }
            Message msg = (Message) wireFormat.unmarshal(new DataInputStream(addMessage.getMessage().newInput()));
            return msg;
        } catch (Throwable t) {
            IOException ioe = IOExceptionSupport.create("Unexpected error on journal read at: " + location , t);
            LOG.error("Failed to load message at: {}", location , ioe);
            brokerService.handleIOException(ioe);
            throw ioe;
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Internal conversion methods.
    // /////////////////////////////////////////////////////////////////

    KahaLocation convert(Location location) {
        KahaLocation rc = new KahaLocation();
        rc.setLogId(location.getDataFileId());
        rc.setOffset(location.getOffset());
        return rc;
    }

    KahaDestination convert(ActiveMQDestination dest) {
        KahaDestination rc = new KahaDestination();
        rc.setName(dest.getPhysicalName());
        switch (dest.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            rc.setType(DestinationType.QUEUE);
            return rc;
        case ActiveMQDestination.TOPIC_TYPE:
            rc.setType(DestinationType.TOPIC);
            return rc;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            rc.setType(DestinationType.TEMP_QUEUE);
            return rc;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            rc.setType(DestinationType.TEMP_TOPIC);
            return rc;
        default:
            return null;
        }
    }

    ActiveMQDestination convert(String dest) {
        int p = dest.indexOf(":");
        if (p < 0) {
            throw new IllegalArgumentException("Not in the valid destination format");
        }
        int type = Integer.parseInt(dest.substring(0, p));
        String name = dest.substring(p + 1);
        return convert(type, name);
    }

    private ActiveMQDestination convert(KahaDestination commandDestination) {
        return convert(commandDestination.getType().getNumber(), commandDestination.getName());
    }

    private ActiveMQDestination convert(int type, String name) {
        switch (KahaDestination.DestinationType.valueOf(type)) {
        case QUEUE:
            return new ActiveMQQueue(name);
        case TOPIC:
            return new ActiveMQTopic(name);
        case TEMP_QUEUE:
            return new ActiveMQTempQueue(name);
        case TEMP_TOPIC:
            return new ActiveMQTempTopic(name);
        default:
            throw new IllegalArgumentException("Not in the valid destination format");
        }
    }

    public TransactionIdTransformer getTransactionIdTransformer() {
        return transactionIdTransformer;
    }

    public void setTransactionIdTransformer(TransactionIdTransformer transactionIdTransformer) {
        this.transactionIdTransformer = transactionIdTransformer;
    }

    static class AsyncJobKey {
        MessageId id;
        ActiveMQDestination destination;

        AsyncJobKey(MessageId id, ActiveMQDestination destination) {
            this.id = id;
            this.destination = destination;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            return obj instanceof AsyncJobKey && id.equals(((AsyncJobKey) obj).id)
                    && destination.equals(((AsyncJobKey) obj).destination);
        }

        @Override
        public int hashCode() {
            return id.hashCode() + destination.hashCode();
        }

        @Override
        public String toString() {
            return destination.getPhysicalName() + "-" + id;
        }
    }

    public interface StoreTask {
        public boolean cancel();

        public void aquireLocks();

        public void releaseLocks();
    }

    class StoreQueueTask implements Runnable, StoreTask {
        protected final Message message;
        protected final ConnectionContext context;
        protected final KahaDBMessageStore store;
        protected final InnerFutureTask future;
        protected final AtomicBoolean done = new AtomicBoolean();
        protected final AtomicBoolean locked = new AtomicBoolean();

        public StoreQueueTask(KahaDBMessageStore store, ConnectionContext context, Message message) {
            this.store = store;
            this.context = context;
            this.message = message;
            this.future = new InnerFutureTask(this);
        }

        public ListenableFuture<Object> getFuture() {
            return this.future;
        }

        @Override
        public boolean cancel() {
            if (this.done.compareAndSet(false, true)) {
                return this.future.cancel(false);
            }
            return false;
        }

        @Override
        public void aquireLocks() {
            if (this.locked.compareAndSet(false, true)) {
                try {
                    globalQueueSemaphore.acquire();
                    store.acquireLocalAsyncLock();
                    message.incrementReferenceCount();
                } catch (InterruptedException e) {
                    LOG.warn("Failed to aquire lock", e);
                }
            }

        }

        @Override
        public void releaseLocks() {
            if (this.locked.compareAndSet(true, false)) {
                store.releaseLocalAsyncLock();
                globalQueueSemaphore.release();
                message.decrementReferenceCount();
            }
        }

        @Override
        public void run() {
            this.store.doneTasks++;
            try {
                if (this.done.compareAndSet(false, true)) {
                    this.store.addMessage(context, message);
                    removeQueueTask(this.store, this.message.getMessageId());
                    this.future.complete();
                } else if (cancelledTaskModMetric > 0 && (++this.store.canceledTasks) % cancelledTaskModMetric == 0) {
                    System.err.println(this.store.dest.getName() + " cancelled: "
                            + (this.store.canceledTasks / this.store.doneTasks) * 100);
                    this.store.canceledTasks = this.store.doneTasks = 0;
                }
            } catch (Throwable t) {
                this.future.setException(t);
                removeQueueTask(this.store, this.message.getMessageId());
            }
        }

        protected Message getMessage() {
            return this.message;
        }

        private class InnerFutureTask extends FutureTask<Object> implements ListenableFuture<Object>  {

            private final AtomicReference<Runnable> listenerRef = new AtomicReference<>();

            public InnerFutureTask(Runnable runnable) {
                super(runnable, null);
            }

            @Override
            public void setException(final Throwable e) {
                super.setException(e);
            }

            public void complete() {
                super.set(null);
            }

            @Override
            public void done() {
                fireListener();
            }

            @Override
            public void addListener(Runnable listener) {
                this.listenerRef.set(listener);
                if (isDone()) {
                    fireListener();
                }
            }

            private void fireListener() {
                Runnable listener = listenerRef.getAndSet(null);
                if (listener != null) {
                    try {
                        listener.run();
                    } catch (Exception ignored) {
                        LOG.warn("Unexpected exception from future {} listener callback {}", this, listener, ignored);
                    }
                }
            }
        }
    }

    class StoreTopicTask extends StoreQueueTask {
        private final int subscriptionCount;
        private final List<String> subscriptionKeys = new ArrayList<String>(1);
        private final KahaDBTopicMessageStore topicStore;
        public StoreTopicTask(KahaDBTopicMessageStore store, ConnectionContext context, Message message,
                int subscriptionCount) {
            super(store, context, message);
            this.topicStore = store;
            this.subscriptionCount = subscriptionCount;

        }

        @Override
        public void aquireLocks() {
            if (this.locked.compareAndSet(false, true)) {
                try {
                    globalTopicSemaphore.acquire();
                    store.acquireLocalAsyncLock();
                    message.incrementReferenceCount();
                } catch (InterruptedException e) {
                    LOG.warn("Failed to aquire lock", e);
                }
            }
        }

        @Override
        public void releaseLocks() {
            if (this.locked.compareAndSet(true, false)) {
                message.decrementReferenceCount();
                store.releaseLocalAsyncLock();
                globalTopicSemaphore.release();
            }
        }

        /**
         * add a key
         *
         * @param key
         * @return true if all acknowledgements received
         */
        public boolean addSubscriptionKey(String key) {
            synchronized (this.subscriptionKeys) {
                this.subscriptionKeys.add(key);
            }
            return this.subscriptionKeys.size() >= this.subscriptionCount;
        }

        @Override
        public void run() {
            this.store.doneTasks++;
            try {
                if (this.done.compareAndSet(false, true)) {
                    this.topicStore.addMessage(context, message);
                    // apply any acks we have
                    synchronized (this.subscriptionKeys) {
                        for (String key : this.subscriptionKeys) {
                            this.topicStore.doAcknowledge(context, key, this.message.getMessageId(), null);

                        }
                    }
                    removeTopicTask(this.topicStore, this.message.getMessageId());
                    this.future.complete();
                } else if (cancelledTaskModMetric > 0 && this.store.canceledTasks++ % cancelledTaskModMetric == 0) {
                    System.err.println(this.store.dest.getName() + " cancelled: "
                            + (this.store.canceledTasks / this.store.doneTasks) * 100);
                    this.store.canceledTasks = this.store.doneTasks = 0;
                }
            } catch (Throwable t) {
                this.future.setException(t);
                removeTopicTask(this.topicStore, this.message.getMessageId());
            }
        }
    }

    public class StoreTaskExecutor extends ThreadPoolExecutor {

        public StoreTaskExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit timeUnit, BlockingQueue<Runnable> queue, ThreadFactory threadFactory) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, timeUnit, queue, threadFactory);
        }

        @Override
        protected void afterExecute(Runnable runnable, Throwable throwable) {
            super.afterExecute(runnable, throwable);

            if (runnable instanceof StoreTask) {
               ((StoreTask)runnable).releaseLocks();
            }
        }
    }

    @Override
    public JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException {
        return new JobSchedulerStoreImpl();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.store.NoLocalSubscriptionAware#isPersistNoLocal()
     */
    @Override
    public boolean isPersistNoLocal() {
        // Prior to v11 the broker did not store the noLocal value for durable subs.
        return brokerService.getStoreOpenWireVersion() >= 11;
    }
}
