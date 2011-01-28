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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaLocalTransactionId;
import org.apache.activemq.store.kahadb.data.KahaLocation;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.store.kahadb.data.KahaXATransactionId;
import org.apache.activemq.store.kahadb.data.KahaDestination.DestinationType;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Transaction;

public class KahaDBStore extends MessageDatabase implements PersistenceAdapter {
    static final Log LOG = LogFactory.getLog(KahaDBStore.class);
    private static final int MAX_ASYNC_JOBS = 10000;

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
    private boolean concurrentStoreAndDispatchTransactions = false;
    private int maxAsyncJobs = MAX_ASYNC_JOBS;
    private final KahaDBTransactionStore transactionStore;

    public KahaDBStore() {
        this.transactionStore = new KahaDBTransactionStore(this);
    }

    public void setBrokerName(String brokerName) {
    }

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
    public void doStart() throws Exception {
        super.doStart();
        this.globalQueueSemaphore = new Semaphore(getMaxAsyncJobs());
        this.globalTopicSemaphore = new Semaphore(getMaxAsyncJobs());
        this.asyncQueueJobQueue = new LinkedBlockingQueue<Runnable>(getMaxAsyncJobs());
        this.asyncTopicJobQueue = new LinkedBlockingQueue<Runnable>(getMaxAsyncJobs());
        this.queueExecutor = new ThreadPoolExecutor(1, asyncExecutorMaxThreads, 0L, TimeUnit.MILLISECONDS,
                asyncQueueJobQueue, new ThreadFactory() {
                    public Thread newThread(Runnable runnable) {
                        Thread thread = new Thread(runnable, "ConcurrentQueueStoreAndDispatch");
                        thread.setDaemon(true);
                        return thread;
                    }
                });
        this.topicExecutor = new ThreadPoolExecutor(1, asyncExecutorMaxThreads, 0L, TimeUnit.MILLISECONDS,
                asyncTopicJobQueue, new ThreadFactory() {
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
            this.queueExecutor.shutdownNow();
        }
        if (this.topicExecutor != null) {
            this.topicExecutor.shutdownNow();
        }
        LOG.info("Stopped KahaDB");
        super.doStop(stopper);
    }

    protected StoreQueueTask removeQueueTask(KahaDBMessageStore store, MessageId id) {
        StoreQueueTask task = null;
        synchronized (store.asyncTaskMap) {
            task = (StoreQueueTask) store.asyncTaskMap.remove(new AsyncJobKey(id, store.getDestination()));
        }
        return task;
    }

    protected void addQueueTask(KahaDBMessageStore store, StoreQueueTask task) throws IOException {
        synchronized (store.asyncTaskMap) {
            store.asyncTaskMap.put(new AsyncJobKey(task.getMessage().getMessageId(), store.getDestination()), task);
        }
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

    public TransactionStore createTransactionStore() throws IOException {
        return this.transactionStore;
    }

    public boolean getForceRecoverIndex() {
        return this.forceRecoverIndex;
    }

    public void setForceRecoverIndex(boolean forceRecoverIndex) {
        this.forceRecoverIndex = forceRecoverIndex;
    }

    public class KahaDBMessageStore extends AbstractMessageStore {
        protected final Map<AsyncJobKey, StoreTask> asyncTaskMap = new HashMap<AsyncJobKey, StoreTask>();
        protected KahaDestination dest;
        private final int maxAsyncJobs;
        private final Semaphore localDestinationSemaphore;

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

        @Override
        public Future<Object> asyncAddQueueMessage(final ConnectionContext context, final Message message)
                throws IOException {
            if (isConcurrentStoreAndDispatchQueues()) {
                StoreQueueTask result = new StoreQueueTask(this, context, message);
                result.aquireLocks();
                addQueueTask(this, result);
                return result.getFuture();
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
                    if (!task.cancel()) {
                        try {

                            task.future.get();
                        } catch (InterruptedException e) {
                            throw new InterruptedIOException(e.toString());
                        } catch (Exception ignored) {
                            LOG.debug("removeAsync: cannot cancel, waiting for add resulted in ex", ignored);
                        }
                        removeMessage(context, ack);
                    } else {
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

        public void addMessage(ConnectionContext context, Message message) throws IOException {
            KahaAddMessageCommand command = new KahaAddMessageCommand();
            command.setDestination(dest);
            command.setMessageId(message.getMessageId().toString());
            command.setTransactionInfo(createTransactionInfo(message.getTransactionId()));
            command.setPriority(message.getPriority());
            command.setPrioritySupported(isPrioritizedMessages());
            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(message);
            command.setMessage(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            store(command, isEnableJournalDiskSyncs() && message.isResponseRequired(), null, null);
            
        }

        public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
            KahaRemoveMessageCommand command = new KahaRemoveMessageCommand();
            command.setDestination(dest);
            command.setMessageId(ack.getLastMessageId().toString());
            command.setTransactionInfo(createTransactionInfo(ack.getTransactionId()));

            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(ack);
            command.setAck(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            store(command, isEnableJournalDiskSyncs() && ack.isResponseRequired(), null, null);
        }

        public void removeAllMessages(ConnectionContext context) throws IOException {
            KahaRemoveDestinationCommand command = new KahaRemoveDestinationCommand();
            command.setDestination(dest);
            store(command, true, null, null);
        }

        public Message getMessage(MessageId identity) throws IOException {
            final String key = identity.toString();

            // Hopefully one day the page file supports concurrent read
            // operations... but for now we must
            // externally synchronize...
            Location location;
            indexLock.readLock().lock();
            try {
                location = pageFile.tx().execute(new Transaction.CallableClosure<Location, IOException>() {
                    public Location execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long sequence = sd.messageIdIndex.get(tx, key);
                        if (sequence == null) {
                            return null;
                        }
                        return sd.orderIndex.get(tx, sequence).location;
                    }
                });
            }finally {
                indexLock.readLock().unlock();
            }
            if (location == null) {
                return null;
            }

            return loadMessage(location);
        }

        public int getMessageCount() throws IOException {
            try {
                lockAsyncJobQueue();
                indexLock.readLock().lock();
                try {
                    return pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>() {
                        public Integer execute(Transaction tx) throws IOException {
                            // Iterate through all index entries to get a count
                            // of
                            // messages in the destination.
                            StoredDestination sd = getStoredDestination(dest, tx);
                            int rc = 0;
                            for (Iterator<Entry<Location, Long>> iterator = sd.locationIndex.iterator(tx); iterator
                                    .hasNext();) {
                                iterator.next();
                                rc++;
                            }
                            return rc;
                        }
                    });
                }finally {
                    indexLock.readLock().unlock();
                }
            } finally {
                unlockAsyncJobQueue();
            }
        }

        @Override
        public boolean isEmpty() throws IOException {
            indexLock.readLock().lock();
            try {
                return pageFile.tx().execute(new Transaction.CallableClosure<Boolean, IOException>() {
                    public Boolean execute(Transaction tx) throws IOException {
                        // Iterate through all index entries to get a count of
                        // messages in the destination.
                        StoredDestination sd = getStoredDestination(dest, tx);
                        return sd.locationIndex.isEmpty(tx);
                    }
                });
            }finally {
                indexLock.readLock().unlock();
            }
        }

        public void recover(final MessageRecoveryListener listener) throws Exception {
            indexLock.readLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        sd.orderIndex.resetCursorPosition();
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator
                                .hasNext();) {
                            Entry<Long, MessageKeys> entry = iterator.next();
                            Message msg = loadMessage(entry.getValue().location);
                            listener.recoverMessage(msg);
                        }
                    }
                });
            }finally {
                indexLock.readLock().unlock();
            }
        }

        
        public void recoverNextMessages(final int maxReturned, final MessageRecoveryListener listener) throws Exception {
            indexLock.readLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Entry<Long, MessageKeys> entry = null;
                        int counter = 0;
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx);
                             listener.hasSpace() && iterator.hasNext(); ) {
                            entry = iterator.next();
                            Message msg = loadMessage(entry.getValue().location);
                            listener.recoverMessage(msg);
                            counter++;
                            if (counter >= maxReturned) {
                                break;
                            }
                        }
                        sd.orderIndex.stoppedIterating();
                    }
                });
            }finally {
                indexLock.readLock().unlock();
            }
        }

        public void resetBatching() {
            try {
                pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getExistingStoredDestination(dest, tx);
                        if (sd != null) {
                            sd.orderIndex.resetCursorPosition();}
                        }
                    });
            } catch (Exception e) {
                LOG.error("Failed to reset batching",e);
            }
        }

        @Override
        public void setBatch(MessageId identity) throws IOException {
            try {
                final String key = identity.toString();
                lockAsyncJobQueue();

                // Hopefully one day the page file supports concurrent read
                // operations... but for now we must
                // externally synchronize...
               
                indexLock.writeLock().lock();
                try {
                        pageFile.tx().execute(new Transaction.Closure<IOException>() {
                        public void execute(Transaction tx) throws IOException {
                            StoredDestination sd = getStoredDestination(dest, tx);
                            Long location = sd.messageIdIndex.get(tx, key);
                            if (location != null) {
                                sd.orderIndex.setBatch(tx, location);
                            }
                        }
                    });
                }finally {
                    indexLock.writeLock().unlock();
                }
                
            } finally {
                unlockAsyncJobQueue();
            }

        }

        @Override
        public void setMemoryUsage(MemoryUsage memoeyUSage) {
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
                this.localDestinationSemaphore.tryAcquire(this.maxAsyncJobs, 60, TimeUnit.SECONDS);
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

    }

    class KahaDBTopicMessageStore extends KahaDBMessageStore implements TopicMessageStore {
        private final AtomicInteger subscriptionCount = new AtomicInteger();
        public KahaDBTopicMessageStore(ActiveMQTopic destination) throws IOException {
            super(destination);
            this.subscriptionCount.set(getAllSubscriptions().length);
            asyncTopicMaps.add(asyncTaskMap);
        }

        @Override
        public Future<Object> asyncAddTopicMessage(final ConnectionContext context, final Message message)
                throws IOException {
            if (isConcurrentStoreAndDispatchTopics()) {
                StoreTopicTask result = new StoreTopicTask(this, context, message, subscriptionCount.get());
                result.aquireLocks();
                addTopicTask(this, result);
                return result.getFuture();
            } else {
                return super.asyncAddTopicMessage(context, message);
            }
        }

        public void acknowledge(ConnectionContext context, String clientId, String subscriptionName,
                                MessageId messageId, MessageAck ack)
                throws IOException {
            String subscriptionKey = subscriptionKey(clientId, subscriptionName);
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
            command.setMessageId(messageId.toString());
            if (ack != null && ack.isUnmatchedAck()) {
                command.setAck(UNMATCHED);
            }
            store(command, false, null, null);
        }

        public void addSubsciption(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
            String subscriptionKey = subscriptionKey(subscriptionInfo.getClientId(), subscriptionInfo
                    .getSubscriptionName());
            KahaSubscriptionCommand command = new KahaSubscriptionCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey);
            command.setRetroactive(retroactive);
            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(subscriptionInfo);
            command.setSubscriptionInfo(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            store(command, isEnableJournalDiskSyncs() && true, null, null);
            this.subscriptionCount.incrementAndGet();
        }

        public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
            KahaSubscriptionCommand command = new KahaSubscriptionCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey(clientId, subscriptionName));
            store(command, isEnableJournalDiskSyncs() && true, null, null);
            this.subscriptionCount.decrementAndGet();
        }

        public SubscriptionInfo[] getAllSubscriptions() throws IOException {

            final ArrayList<SubscriptionInfo> subscriptions = new ArrayList<SubscriptionInfo>();
            indexLock.readLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
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
            }finally {
                indexLock.readLock().unlock();
            }

            SubscriptionInfo[] rc = new SubscriptionInfo[subscriptions.size()];
            subscriptions.toArray(rc);
            return rc;
        }

        public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            indexLock.readLock().lock();
            try {
                return pageFile.tx().execute(new Transaction.CallableClosure<SubscriptionInfo, IOException>() {
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
            }finally {
                indexLock.readLock().unlock();
            }
        }

        public int getMessageCount(String clientId, String subscriptionName) throws IOException {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            indexLock.writeLock().lock();
            try {
                return pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>() {
                    public Integer execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        LastAck cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                        if (cursorPos == null) {
                            // The subscription might not exist.
                            return 0;
                        }

                        int counter = 0;
                        for (Iterator<Entry<Long, HashSet<String>>> iterator =
                                sd.ackPositions.iterator(tx, cursorPos.lastAckedSequence); iterator.hasNext();) {
                            Entry<Long, HashSet<String>> entry = iterator.next();
                            if (entry.getValue().contains(subscriptionKey)) {
                                counter++;
                            }
                        }
                        return counter;
                    }
                });
            }finally {
                indexLock.writeLock().unlock();
            }
        }

        public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener)
                throws Exception {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            final SubscriptionInfo info = lookupSubscription(clientId, subscriptionName);
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        LastAck cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                        sd.orderIndex.setBatch(tx, cursorPos);
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator
                                .hasNext();) {
                            Entry<Long, MessageKeys> entry = iterator.next();
                            listener.recoverMessage(loadMessage(entry.getValue().location));
                        }
                        sd.orderIndex.resetCursorPosition();
                    }
                });
            }finally {
                indexLock.writeLock().unlock();
            }
        }

        public void recoverNextMessages(String clientId, String subscriptionName, final int maxReturned,
                final MessageRecoveryListener listener) throws Exception {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            final SubscriptionInfo info = lookupSubscription(clientId, subscriptionName);
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        sd.orderIndex.resetCursorPosition();
                        MessageOrderCursor moc = sd.subscriptionCursors.get(subscriptionKey);
                        if (moc == null) {
                            LastAck pos = sd.subscriptionAcks.get(tx, subscriptionKey);
                            if (pos == null) {
                                // sub deleted
                                return;
                            }
                            sd.orderIndex.setBatch(tx, pos);
                            moc = sd.orderIndex.cursor;
                        } else {
                            sd.orderIndex.cursor.sync(moc);
                        }

                        Entry<Long, MessageKeys> entry = null;
                        int counter = 0;
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx, moc); iterator
                                .hasNext();) {
                            entry = iterator.next();
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
            }finally {
                indexLock.writeLock().unlock();
            }
        }

        public void resetBatching(String clientId, String subscriptionName) {
            try {
                final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
                indexLock.writeLock().lock();
                try {
                    pageFile.tx().execute(new Transaction.Closure<IOException>() {
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
    }

    String subscriptionKey(String clientId, String subscriptionName) {
        return clientId + ":" + subscriptionName;
    }

    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        return this.transactionStore.proxy(new KahaDBMessageStore(destination));
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        return this.transactionStore.proxy(new KahaDBTopicMessageStore(destination));
    }

    /**
     * Cleanup method to remove any state associated with the given destination.
     * This method does not stop the message store (it might not be cached).
     * 
     * @param destination
     *            Destination to forget
     */
    public void removeQueueMessageStore(ActiveMQQueue destination) {
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     * This method does not stop the message store (it might not be cached).
     * 
     * @param destination
     *            Destination to forget
     */
    public void removeTopicMessageStore(ActiveMQTopic destination) {
    }

    public void deleteAllMessages() throws IOException {
        deleteAllMessages = true;
    }

    public Set<ActiveMQDestination> getDestinations() {
        try {
            final HashSet<ActiveMQDestination> rc = new HashSet<ActiveMQDestination>();
            indexLock.readLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        for (Iterator<Entry<String, StoredDestination>> iterator = metadata.destinations.iterator(tx); iterator
                                .hasNext();) {
                            Entry<String, StoredDestination> entry = iterator.next();
                            if (!isEmptyTopic(entry, tx)) {
                                rc.add(convert(entry.getKey()));
                            }
                        }
                    }

                    private boolean isEmptyTopic(Entry<String, StoredDestination> entry, Transaction tx)
                            throws IOException {
                        boolean isEmptyTopic = false;
                        ActiveMQDestination dest = convert(entry.getKey());
                        if (dest.isTopic()) {
                            StoredDestination loadedStore = getStoredDestination(convert(dest), tx);
                            if (loadedStore.subscriptionAcks.isEmpty(tx)) {
                                isEmptyTopic = true;
                            }
                        }
                        return isEmptyTopic;
                    }
                });
            }finally {
                indexLock.readLock().unlock();
            }
            return rc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getLastMessageBrokerSequenceId() throws IOException {
        return 0;
    }
    
    public long getLastProducerSequenceId(ProducerId id) {
        indexLock.readLock().lock();
        try {
            return metadata.producerSequenceIdTracker.getLastSeqId(id);
        } finally {
            indexLock.readLock().unlock();
        }
    }

    public long size() {
        return storeSize.get();
    }

    public void beginTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    public void commitTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    public void rollbackTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }

    public void checkpoint(boolean sync) throws IOException {
        super.checkpointCleanup(false);
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
        KahaAddMessageCommand addMessage = (KahaAddMessageCommand) load(location);
        Message msg = (Message) wireFormat.unmarshal(new DataInputStream(addMessage.getMessage().newInput()));
        return msg;
    }

    // /////////////////////////////////////////////////////////////////
    // Internal conversion methods.
    // /////////////////////////////////////////////////////////////////

    KahaTransactionInfo createTransactionInfo(TransactionId txid) {
        if (txid == null) {
            return null;
        }
        KahaTransactionInfo rc = new KahaTransactionInfo();

        if (txid.isLocalTransaction()) {
            LocalTransactionId t = (LocalTransactionId) txid;
            KahaLocalTransactionId kahaTxId = new KahaLocalTransactionId();
            kahaTxId.setConnectionId(t.getConnectionId().getValue());
            kahaTxId.setTransacitonId(t.getValue());
            rc.setLocalTransacitonId(kahaTxId);
        } else {
            XATransactionId t = (XATransactionId) txid;
            KahaXATransactionId kahaTxId = new KahaXATransactionId();
            kahaTxId.setBranchQualifier(new Buffer(t.getBranchQualifier()));
            kahaTxId.setGlobalTransactionId(new Buffer(t.getGlobalTransactionId()));
            kahaTxId.setFormatId(t.getFormatId());
            rc.setXaTransacitonId(kahaTxId);
        }
        return rc;
    }

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

    interface StoreTask {
        public boolean cancel();
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

        public Future<Object> getFuture() {
            return this.future;
        }

        public boolean cancel() {
            releaseLocks();
            if (this.done.compareAndSet(false, true)) {
                return this.future.cancel(false);
            }
            return false;
        }

        void aquireLocks() {
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

        void releaseLocks() {
            if (this.locked.compareAndSet(true, false)) {
                store.releaseLocalAsyncLock();
                globalQueueSemaphore.release();
                message.decrementReferenceCount();
            }
        }

        public void run() {
            this.store.doneTasks++;
            try {
                if (this.done.compareAndSet(false, true)) {
                    this.store.addMessage(context, message);
                    removeQueueTask(this.store, this.message.getMessageId());
                    this.future.complete();
                } else if (cancelledTaskModMetric > 0 && this.store.canceledTasks++ % cancelledTaskModMetric == 0) {
                    System.err.println(this.store.dest.getName() + " cancelled: "
                            + (this.store.canceledTasks / this.store.doneTasks) * 100);
                    this.store.canceledTasks = this.store.doneTasks = 0;
                }
            } catch (Exception e) {
                this.future.setException(e);
            } finally {
                releaseLocks();
            }
        }

        protected Message getMessage() {
            return this.message;
        }

        private class InnerFutureTask extends FutureTask<Object> {

            public InnerFutureTask(Runnable runnable) {
                super(runnable, null);

            }

            public void setException(final Exception e) {
                super.setException(e);
            }

            public void complete() {
                super.set(null);
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
        void aquireLocks() {
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
        void releaseLocks() {
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
            } catch (Exception e) {
                this.future.setException(e);
            } finally {
                releaseLocks();
            }
        }
    }
}
