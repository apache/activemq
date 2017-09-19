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

import static org.apache.activemq.store.kahadb.disk.journal.Location.NOT_SET;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.ActiveMQMessageAuditNoSync;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.MessageStoreStatistics;
import org.apache.activemq.store.MessageStoreSubscriptionStatistics;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.kahadb.data.KahaAckMessageFileMapCommand;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaCommitCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.data.KahaPrepareCommand;
import org.apache.activemq.store.kahadb.data.KahaProducerAuditCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaRewrittenDataFileCommand;
import org.apache.activemq.store.kahadb.data.KahaRollbackCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.data.KahaTraceCommand;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.store.kahadb.data.KahaUpdateMessageCommand;
import org.apache.activemq.store.kahadb.disk.index.BTreeIndex;
import org.apache.activemq.store.kahadb.disk.index.BTreeVisitor;
import org.apache.activemq.store.kahadb.disk.index.ListIndex;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.journal.TargetedDataFileAppender;
import org.apache.activemq.store.kahadb.disk.page.Page;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.LocationMarshaller;
import org.apache.activemq.store.kahadb.disk.util.LongMarshaller;
import org.apache.activemq.store.kahadb.disk.util.Marshaller;
import org.apache.activemq.store.kahadb.disk.util.Sequence;
import org.apache.activemq.store.kahadb.disk.util.SequenceSet;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.store.kahadb.disk.util.VariableMarshaller;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public abstract class MessageDatabase extends ServiceSupport implements BrokerServiceAware {

    protected BrokerService brokerService;

    public static final String PROPERTY_LOG_SLOW_ACCESS_TIME = "org.apache.activemq.store.kahadb.LOG_SLOW_ACCESS_TIME";
    public static final int LOG_SLOW_ACCESS_TIME = Integer.getInteger(PROPERTY_LOG_SLOW_ACCESS_TIME, 0);
    public static final File DEFAULT_DIRECTORY = new File("KahaDB");
    protected static final Buffer UNMATCHED;
    static {
        UNMATCHED = new Buffer(new byte[]{});
    }
    private static final Logger LOG = LoggerFactory.getLogger(MessageDatabase.class);

    static final int CLOSED_STATE = 1;
    static final int OPEN_STATE = 2;
    static final long NOT_ACKED = -1;

    static final int VERSION = 6;

    static final byte COMPACTED_JOURNAL_FILE = DataFile.STANDARD_LOG_FILE + 1;

    protected class Metadata {
        protected Page<Metadata> page;
        protected int state;
        protected BTreeIndex<String, StoredDestination> destinations;
        protected Location lastUpdate;
        protected Location firstInProgressTransactionLocation;
        protected Location producerSequenceIdTrackerLocation = null;
        protected Location ackMessageFileMapLocation = null;
        protected transient ActiveMQMessageAuditNoSync producerSequenceIdTracker = new ActiveMQMessageAuditNoSync();
        protected transient Map<Integer, Set<Integer>> ackMessageFileMap = new HashMap<>();
        protected int version = VERSION;
        protected int openwireVersion = OpenWireFormat.DEFAULT_STORE_VERSION;

        public void read(DataInput is) throws IOException {
            state = is.readInt();
            destinations = new BTreeIndex<>(pageFile, is.readLong());
            if (is.readBoolean()) {
                lastUpdate = LocationMarshaller.INSTANCE.readPayload(is);
            } else {
                lastUpdate = null;
            }
            if (is.readBoolean()) {
                firstInProgressTransactionLocation = LocationMarshaller.INSTANCE.readPayload(is);
            } else {
                firstInProgressTransactionLocation = null;
            }
            try {
                if (is.readBoolean()) {
                    producerSequenceIdTrackerLocation = LocationMarshaller.INSTANCE.readPayload(is);
                } else {
                    producerSequenceIdTrackerLocation = null;
                }
            } catch (EOFException expectedOnUpgrade) {
            }
            try {
                version = is.readInt();
            } catch (EOFException expectedOnUpgrade) {
                version = 1;
            }
            if (version >= 5 && is.readBoolean()) {
                ackMessageFileMapLocation = LocationMarshaller.INSTANCE.readPayload(is);
            } else {
                ackMessageFileMapLocation = null;
            }
            try {
                openwireVersion = is.readInt();
            } catch (EOFException expectedOnUpgrade) {
                openwireVersion = OpenWireFormat.DEFAULT_LEGACY_VERSION;
            }
            LOG.info("KahaDB is version " + version);
        }

        public void write(DataOutput os) throws IOException {
            os.writeInt(state);
            os.writeLong(destinations.getPageId());

            if (lastUpdate != null) {
                os.writeBoolean(true);
                LocationMarshaller.INSTANCE.writePayload(lastUpdate, os);
            } else {
                os.writeBoolean(false);
            }

            if (firstInProgressTransactionLocation != null) {
                os.writeBoolean(true);
                LocationMarshaller.INSTANCE.writePayload(firstInProgressTransactionLocation, os);
            } else {
                os.writeBoolean(false);
            }

            if (producerSequenceIdTrackerLocation != null) {
                os.writeBoolean(true);
                LocationMarshaller.INSTANCE.writePayload(producerSequenceIdTrackerLocation, os);
            } else {
                os.writeBoolean(false);
            }
            os.writeInt(VERSION);
            if (ackMessageFileMapLocation != null) {
                os.writeBoolean(true);
                LocationMarshaller.INSTANCE.writePayload(ackMessageFileMapLocation, os);
            } else {
                os.writeBoolean(false);
            }
            os.writeInt(this.openwireVersion);
        }
    }

    class MetadataMarshaller extends VariableMarshaller<Metadata> {
        @Override
        public Metadata readPayload(DataInput dataIn) throws IOException {
            Metadata rc = createMetadata();
            rc.read(dataIn);
            return rc;
        }

        @Override
        public void writePayload(Metadata object, DataOutput dataOut) throws IOException {
            object.write(dataOut);
        }
    }

    protected PageFile pageFile;
    protected Journal journal;
    protected Metadata metadata = new Metadata();

    protected MetadataMarshaller metadataMarshaller = new MetadataMarshaller();

    protected boolean failIfDatabaseIsLocked;

    protected boolean deleteAllMessages;
    protected File directory = DEFAULT_DIRECTORY;
    protected File indexDirectory = null;
    protected ScheduledExecutorService scheduler;
    private final Object schedulerLock = new Object();

    protected JournalDiskSyncStrategy journalDiskSyncStrategy = JournalDiskSyncStrategy.ALWAYS;
    protected boolean archiveDataLogs;
    protected File directoryArchive;
    protected AtomicLong journalSize = new AtomicLong(0);
    long journalDiskSyncInterval = 1000;
    long checkpointInterval = 5*1000;
    long cleanupInterval = 30*1000;
    int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    int journalMaxWriteBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    boolean enableIndexWriteAsync = false;
    int setIndexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE;
    private String preallocationScope = Journal.PreallocationScope.ENTIRE_JOURNAL.name();
    private String preallocationStrategy = Journal.PreallocationStrategy.SPARSE_FILE.name();

    protected AtomicBoolean opened = new AtomicBoolean();
    private boolean ignoreMissingJournalfiles = false;
    private int indexCacheSize = 10000;
    private boolean checkForCorruptJournalFiles = false;
    private boolean checksumJournalFiles = true;
    protected boolean forceRecoverIndex = false;
    private boolean archiveCorruptedIndex = false;
    private boolean useIndexLFRUEviction = false;
    private float indexLFUEvictionFactor = 0.2f;
    private boolean enableIndexDiskSyncs = true;
    private boolean enableIndexRecoveryFile = true;
    private boolean enableIndexPageCaching = true;
    ReentrantReadWriteLock checkpointLock = new ReentrantReadWriteLock();

    private boolean enableAckCompaction = true;
    private int compactAcksAfterNoGC = 10;
    private boolean compactAcksIgnoresStoreGrowth = false;
    private int checkPointCyclesWithNoGC;
    private int journalLogOnLastCompactionCheck;
    private boolean enableSubscriptionStatistics = false;

    //only set when using JournalDiskSyncStrategy.PERIODIC
    protected final AtomicReference<Location> lastAsyncJournalUpdate = new AtomicReference<>();

    @Override
    public void doStart() throws Exception {
        load();
    }

    @Override
    public void doStop(ServiceStopper stopper) throws Exception {
        unload();
    }

    public void allowIOResumption() {
        if (pageFile != null) {
            pageFile.allowIOResumption();
        }
        if (journal != null) {
            journal.allowIOResumption();
        }
    }

    private void loadPageFile() throws IOException {
        this.indexLock.writeLock().lock();
        try {
            final PageFile pageFile = getPageFile();
            pageFile.load();
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    if (pageFile.getPageCount() == 0) {
                        // First time this is created.. Initialize the metadata
                        Page<Metadata> page = tx.allocate();
                        assert page.getPageId() == 0;
                        page.set(metadata);
                        metadata.page = page;
                        metadata.state = CLOSED_STATE;
                        metadata.destinations = new BTreeIndex<>(pageFile, tx.allocate().getPageId());

                        tx.store(metadata.page, metadataMarshaller, true);
                    } else {
                        Page<Metadata> page = tx.load(0, metadataMarshaller);
                        metadata = page.get();
                        metadata.page = page;
                    }
                    metadata.destinations.setKeyMarshaller(StringMarshaller.INSTANCE);
                    metadata.destinations.setValueMarshaller(new StoredDestinationMarshaller());
                    metadata.destinations.load(tx);
                }
            });
            // Load up all the destinations since we need to scan all the indexes to figure out which journal files can be deleted.
            // Perhaps we should just keep an index of file
            storedDestinations.clear();
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    for (Iterator<Entry<String, StoredDestination>> iterator = metadata.destinations.iterator(tx); iterator.hasNext();) {
                        Entry<String, StoredDestination> entry = iterator.next();
                        StoredDestination sd = loadStoredDestination(tx, entry.getKey(), entry.getValue().subscriptions!=null);
                        storedDestinations.put(entry.getKey(), sd);

                        if (checkForCorruptJournalFiles) {
                            // sanity check the index also
                            if (!entry.getValue().locationIndex.isEmpty(tx)) {
                                if (entry.getValue().orderIndex.nextMessageId <= 0) {
                                    throw new IOException("Detected uninitialized orderIndex nextMessageId with pending messages for " + entry.getKey());
                                }
                            }
                        }
                    }
                }
            });
            pageFile.flush();
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    private void startCheckpoint() {
        if (checkpointInterval == 0 && cleanupInterval == 0) {
            LOG.info("periodic checkpoint/cleanup disabled, will ocurr on clean shutdown/restart");
            return;
        }
        synchronized (schedulerLock) {
            if (scheduler == null || scheduler.isShutdown()) {
                scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread schedulerThread = new Thread(r);

                        schedulerThread.setName("ActiveMQ Journal Checkpoint Worker");
                        schedulerThread.setDaemon(true);

                        return schedulerThread;
                    }
                });

                // Short intervals for check-point and cleanups
                long delay;
                if (journal.isJournalDiskSyncPeriodic()) {
                    delay = Math.min(journalDiskSyncInterval > 0 ? journalDiskSyncInterval : checkpointInterval, 500);
                } else {
                    delay = Math.min(checkpointInterval > 0 ? checkpointInterval : cleanupInterval, 500);
                }

                scheduler.scheduleWithFixedDelay(new CheckpointRunner(), 0, delay, TimeUnit.MILLISECONDS);
            }
        }
    }

    private final class CheckpointRunner implements Runnable {

        private long lastCheckpoint = System.currentTimeMillis();
        private long lastCleanup = System.currentTimeMillis();
        private long lastSync = System.currentTimeMillis();
        private Location lastAsyncUpdate = null;

        @Override
        public void run() {
            try {
                // Decide on cleanup vs full checkpoint here.
                if (opened.get()) {
                    long now = System.currentTimeMillis();
                    if (journal.isJournalDiskSyncPeriodic() &&
                            journalDiskSyncInterval > 0 && (now - lastSync >= journalDiskSyncInterval)) {
                        Location currentUpdate = lastAsyncJournalUpdate.get();
                        if (currentUpdate != null && !currentUpdate.equals(lastAsyncUpdate)) {
                            lastAsyncUpdate = currentUpdate;
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Writing trace command to trigger journal sync");
                            }
                            store(new KahaTraceCommand(), true, null, null);
                        }
                        lastSync = now;
                    }
                    if (cleanupInterval > 0 && (now - lastCleanup >= cleanupInterval)) {
                        checkpointCleanup(true);
                        lastCleanup = now;
                        lastCheckpoint = now;
                    } else if (checkpointInterval > 0 && (now - lastCheckpoint >= checkpointInterval)) {
                        checkpointCleanup(false);
                        lastCheckpoint = now;
                    }
                }
            } catch (IOException ioe) {
                LOG.error("Checkpoint failed", ioe);
                brokerService.handleIOException(ioe);
            } catch (Throwable e) {
                LOG.error("Checkpoint failed", e);
                brokerService.handleIOException(IOExceptionSupport.create(e));
            }
        }
    }

    public void open() throws IOException {
        if( opened.compareAndSet(false, true) ) {
            getJournal().start();
            try {
                loadPageFile();
            } catch (Throwable t) {
                LOG.warn("Index corrupted. Recovering the index through journal replay. Cause:" + t);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Index load failure", t);
                }
                // try to recover index
                try {
                    pageFile.unload();
                } catch (Exception ignore) {}
                if (archiveCorruptedIndex) {
                    pageFile.archive();
                } else {
                    pageFile.delete();
                }
                metadata = createMetadata();
                //The metadata was recreated after a detect corruption so we need to
                //reconfigure anything that was configured on the old metadata on startup
                configureMetadata();
                pageFile = null;
                loadPageFile();
            }
            recover();
            startCheckpoint();
        }
    }

    public void load() throws IOException {
        this.indexLock.writeLock().lock();
        try {
            IOHelper.mkdirs(directory);
            if (deleteAllMessages) {
                getJournal().setCheckForCorruptionOnStartup(false);
                getJournal().start();
                getJournal().delete();
                getJournal().close();
                journal = null;
                getPageFile().delete();
                LOG.info("Persistence store purged.");
                deleteAllMessages = false;
            }

            open();
            store(new KahaTraceCommand().setMessage("LOADED " + new Date()));
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    public void close() throws IOException, InterruptedException {
        if (opened.compareAndSet(true, false)) {
            checkpointLock.writeLock().lock();
            try {
                if (metadata.page != null) {
                    checkpointUpdate(true);
                }
                pageFile.unload();
                metadata = createMetadata();
            } finally {
                checkpointLock.writeLock().unlock();
            }
            journal.close();
            synchronized(schedulerLock) {
                if (scheduler != null) {
                    ThreadPoolUtils.shutdownGraceful(scheduler, -1);
                    scheduler = null;
                }
            }
            // clear the cache and journalSize on shutdown of the store
            storeCache.clear();
            journalSize.set(0);
        }
    }

    public void unload() throws IOException, InterruptedException {
        this.indexLock.writeLock().lock();
        try {
            if( pageFile != null && pageFile.isLoaded() ) {
                metadata.state = CLOSED_STATE;
                metadata.firstInProgressTransactionLocation = getInProgressTxLocationRange()[0];

                if (metadata.page != null) {
                    pageFile.tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            tx.store(metadata.page, metadataMarshaller, true);
                        }
                    });
                }
            }
        } finally {
            this.indexLock.writeLock().unlock();
        }
        close();
    }

    // public for testing
    @SuppressWarnings("rawtypes")
    public Location[] getInProgressTxLocationRange() {
        Location[] range = new Location[]{null, null};
        synchronized (inflightTransactions) {
            if (!inflightTransactions.isEmpty()) {
                for (List<Operation> ops : inflightTransactions.values()) {
                    if (!ops.isEmpty()) {
                        trackMaxAndMin(range, ops);
                    }
                }
            }
            if (!preparedTransactions.isEmpty()) {
                for (List<Operation> ops : preparedTransactions.values()) {
                    if (!ops.isEmpty()) {
                        trackMaxAndMin(range, ops);
                    }
                }
            }
        }
        return range;
    }

    @SuppressWarnings("rawtypes")
    private void trackMaxAndMin(Location[] range, List<Operation> ops) {
        Location t = ops.get(0).getLocation();
        if (range[0] == null || t.compareTo(range[0]) <= 0) {
            range[0] = t;
        }
        t = ops.get(ops.size() -1).getLocation();
        if (range[1] == null || t.compareTo(range[1]) >= 0) {
            range[1] = t;
        }
    }

    class TranInfo {
        TransactionId id;
        Location location;

        class opCount {
            int add;
            int remove;
        }
        HashMap<KahaDestination, opCount> destinationOpCount = new HashMap<>();

        @SuppressWarnings("rawtypes")
        public void track(Operation operation) {
            if (location == null ) {
                location = operation.getLocation();
            }
            KahaDestination destination;
            boolean isAdd = false;
            if (operation instanceof AddOperation) {
                AddOperation add = (AddOperation) operation;
                destination = add.getCommand().getDestination();
                isAdd = true;
            } else {
                RemoveOperation removeOpperation = (RemoveOperation) operation;
                destination = removeOpperation.getCommand().getDestination();
            }
            opCount opCount = destinationOpCount.get(destination);
            if (opCount == null) {
                opCount = new opCount();
                destinationOpCount.put(destination, opCount);
            }
            if (isAdd) {
                opCount.add++;
            } else {
                opCount.remove++;
            }
        }

        @Override
        public String toString() {
           StringBuffer buffer = new StringBuffer();
           buffer.append(location).append(";").append(id).append(";\n");
           for (Entry<KahaDestination, opCount> op : destinationOpCount.entrySet()) {
               buffer.append(op.getKey()).append('+').append(op.getValue().add).append(',').append('-').append(op.getValue().remove).append(';');
           }
           return buffer.toString();
        }
    }

    @SuppressWarnings("rawtypes")
    public String getTransactions() {

        ArrayList<TranInfo> infos = new ArrayList<>();
        synchronized (inflightTransactions) {
            if (!inflightTransactions.isEmpty()) {
                for (Entry<TransactionId, List<Operation>> entry : inflightTransactions.entrySet()) {
                    TranInfo info = new TranInfo();
                    info.id = entry.getKey();
                    for (Operation operation : entry.getValue()) {
                        info.track(operation);
                    }
                    infos.add(info);
                }
            }
        }
        synchronized (preparedTransactions) {
            if (!preparedTransactions.isEmpty()) {
                for (Entry<TransactionId, List<Operation>> entry : preparedTransactions.entrySet()) {
                    TranInfo info = new TranInfo();
                    info.id = entry.getKey();
                    for (Operation operation : entry.getValue()) {
                        info.track(operation);
                    }
                    infos.add(info);
                }
            }
        }
        return infos.toString();
    }

    /**
     * Move all the messages that were in the journal into long term storage. We
     * just replay and do a checkpoint.
     *
     * @throws IOException
     * @throws IOException
     * @throws IllegalStateException
     */
    private void recover() throws IllegalStateException, IOException {
        this.indexLock.writeLock().lock();
        try {

            long start = System.currentTimeMillis();
            boolean requiresJournalReplay = recoverProducerAudit();
            requiresJournalReplay |= recoverAckMessageFileMap();
            Location lastIndoubtPosition = getRecoveryPosition();
            Location recoveryPosition = requiresJournalReplay ? journal.getNextLocation(null) : lastIndoubtPosition;
            if (recoveryPosition != null) {
                int redoCounter = 0;
                int dataFileRotationTracker = recoveryPosition.getDataFileId();
                LOG.info("Recovering from the journal @" + recoveryPosition);
                while (recoveryPosition != null) {
                    try {
                        JournalCommand<?> message = load(recoveryPosition);
                        metadata.lastUpdate = recoveryPosition;
                        process(message, recoveryPosition, lastIndoubtPosition);
                        redoCounter++;
                    } catch (IOException failedRecovery) {
                        if (isIgnoreMissingJournalfiles()) {
                            LOG.debug("Failed to recover data at position:" + recoveryPosition, failedRecovery);
                            // track this dud location
                            journal.corruptRecoveryLocation(recoveryPosition);
                        } else {
                            throw new IOException("Failed to recover data at position:" + recoveryPosition, failedRecovery);
                        }
                    }
                    recoveryPosition = journal.getNextLocation(recoveryPosition);
                    // hold on to the minimum number of open files during recovery
                    if (recoveryPosition != null && dataFileRotationTracker != recoveryPosition.getDataFileId()) {
                        dataFileRotationTracker = recoveryPosition.getDataFileId();
                        journal.cleanup();
                    }
                    if (LOG.isInfoEnabled() && redoCounter % 100000 == 0) {
                        LOG.info("@" + recoveryPosition + ", " + redoCounter + " entries recovered ..");
                    }
                }
                if (LOG.isInfoEnabled()) {
                    long end = System.currentTimeMillis();
                    LOG.info("Recovery replayed " + redoCounter + " operations from the journal in " + ((end - start) / 1000.0f) + " seconds.");
                }
            }

            // We may have to undo some index updates.
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    recoverIndex(tx);
                }
            });

            // rollback any recovered inflight local transactions, and discard any inflight XA transactions.
            Set<TransactionId> toRollback = new HashSet<>();
            Set<TransactionId> toDiscard = new HashSet<>();
            synchronized (inflightTransactions) {
                for (Iterator<TransactionId> it = inflightTransactions.keySet().iterator(); it.hasNext(); ) {
                    TransactionId id = it.next();
                    if (id.isLocalTransaction()) {
                        toRollback.add(id);
                    } else {
                        toDiscard.add(id);
                    }
                }
                for (TransactionId tx: toRollback) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("rolling back recovered indoubt local transaction " + tx);
                    }
                    store(new KahaRollbackCommand().setTransactionInfo(TransactionIdConversion.convertToLocal(tx)), false, null, null);
                }
                for (TransactionId tx: toDiscard) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("discarding recovered in-flight XA transaction " + tx);
                    }
                    inflightTransactions.remove(tx);
                }
            }

            synchronized (preparedTransactions) {
                for (TransactionId txId : preparedTransactions.keySet()) {
                    LOG.warn("Recovered prepared XA TX: [{}]", txId);
                }
            }

        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unused")
    private KahaTransactionInfo createLocalTransactionInfo(TransactionId tx) {
        return TransactionIdConversion.convertToLocal(tx);
    }

    private Location minimum(Location x,
                             Location y) {
        Location min = null;
        if (x != null) {
            min = x;
            if (y != null) {
                int compare = y.compareTo(x);
                if (compare < 0) {
                    min = y;
                }
            }
        } else {
            min = y;
        }
        return min;
    }

    private boolean recoverProducerAudit() throws IOException {
        boolean requiresReplay = true;
        if (metadata.producerSequenceIdTrackerLocation != null) {
            try {
                KahaProducerAuditCommand audit = (KahaProducerAuditCommand) load(metadata.producerSequenceIdTrackerLocation);
                ObjectInputStream objectIn = new ObjectInputStream(audit.getAudit().newInput());
                int maxNumProducers = getMaxFailoverProducersToTrack();
                int maxAuditDepth = getFailoverProducersAuditDepth();
                metadata.producerSequenceIdTracker = (ActiveMQMessageAuditNoSync) objectIn.readObject();
                metadata.producerSequenceIdTracker.setAuditDepth(maxAuditDepth);
                metadata.producerSequenceIdTracker.setMaximumNumberOfProducersToTrack(maxNumProducers);
                requiresReplay = false;
            } catch (Exception e) {
                LOG.warn("Cannot recover message audit", e);
            }
        }
        // got no audit stored so got to recreate via replay from start of the journal
        return requiresReplay;
    }

    @SuppressWarnings("unchecked")
    private boolean recoverAckMessageFileMap() throws IOException {
        boolean requiresReplay = true;
        if (metadata.ackMessageFileMapLocation != null) {
            try {
                KahaAckMessageFileMapCommand audit = (KahaAckMessageFileMapCommand) load(metadata.ackMessageFileMapLocation);
                ObjectInputStream objectIn = new ObjectInputStream(audit.getAckMessageFileMap().newInput());
                metadata.ackMessageFileMap = (Map<Integer, Set<Integer>>) objectIn.readObject();
                requiresReplay = false;
            } catch (Exception e) {
                LOG.warn("Cannot recover ackMessageFileMap", e);
            }
        }
        // got no ackMessageFileMap stored so got to recreate via replay from start of the journal
        return requiresReplay;
    }

    protected void recoverIndex(Transaction tx) throws IOException {
        long start = System.currentTimeMillis();
        // It is possible index updates got applied before the journal updates..
        // in that case we need to removed references to messages that are not in the journal
        final Location lastAppendLocation = journal.getLastAppendLocation();
        long undoCounter=0;

        // Go through all the destinations to see if they have messages past the lastAppendLocation
        for (String key : storedDestinations.keySet()) {
            StoredDestination sd = storedDestinations.get(key);

            final ArrayList<Long> matches = new ArrayList<>();
            // Find all the Locations that are >= than the last Append Location.
            sd.locationIndex.visit(tx, new BTreeVisitor.GTEVisitor<Location, Long>(lastAppendLocation) {
                @Override
                protected void matched(Location key, Long value) {
                    matches.add(value);
                }
            });

            for (Long sequenceId : matches) {
                MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
                if (keys != null) {
                    sd.locationIndex.remove(tx, keys.location);
                    sd.messageIdIndex.remove(tx, keys.messageId);
                    metadata.producerSequenceIdTracker.rollback(keys.messageId);
                    undoCounter++;
                    decrementAndSubSizeToStoreStat(key, keys.location.getSize());
                    // TODO: do we need to modify the ack positions for the pub sub case?
                }
            }
        }

        if (undoCounter > 0) {
            // The rolledback operations are basically in flight journal writes.  To avoid getting
            // these the end user should do sync writes to the journal.
            if (LOG.isInfoEnabled()) {
                long end = System.currentTimeMillis();
                LOG.info("Rolled back " + undoCounter + " messages from the index in " + ((end - start) / 1000.0f) + " seconds.");
            }
        }

        undoCounter = 0;
        start = System.currentTimeMillis();

        // Lets be extra paranoid here and verify that all the datafiles being referenced
        // by the indexes still exists.

        final SequenceSet ss = new SequenceSet();
        for (StoredDestination sd : storedDestinations.values()) {
            // Use a visitor to cut down the number of pages that we load
            sd.locationIndex.visit(tx, new BTreeVisitor<Location, Long>() {
                int last=-1;

                @Override
                public boolean isInterestedInKeysBetween(Location first, Location second) {
                    if( first==null ) {
                        return !ss.contains(0, second.getDataFileId());
                    } else if( second==null ) {
                        return true;
                    } else {
                        return !ss.contains(first.getDataFileId(), second.getDataFileId());
                    }
                }

                @Override
                public void visit(List<Location> keys, List<Long> values) {
                    for (Location l : keys) {
                        int fileId = l.getDataFileId();
                        if( last != fileId ) {
                            ss.add(fileId);
                            last = fileId;
                        }
                    }
                }

            });
        }
        HashSet<Integer> missingJournalFiles = new HashSet<>();
        while (!ss.isEmpty()) {
            missingJournalFiles.add((int) ss.removeFirst());
        }

        for (Entry<Integer, Set<Integer>> entry : metadata.ackMessageFileMap.entrySet()) {
            missingJournalFiles.add(entry.getKey());
            for (Integer i : entry.getValue()) {
                missingJournalFiles.add(i);
            }
        }

        missingJournalFiles.removeAll(journal.getFileMap().keySet());

        if (!missingJournalFiles.isEmpty()) {
            LOG.warn("Some journal files are missing: " + missingJournalFiles);
        }

        ArrayList<BTreeVisitor.Predicate<Location>> knownCorruption = new ArrayList<>();
        ArrayList<BTreeVisitor.Predicate<Location>> missingPredicates = new ArrayList<>();
        for (Integer missing : missingJournalFiles) {
            missingPredicates.add(new BTreeVisitor.BetweenVisitor<Location, Long>(new Location(missing, 0), new Location(missing + 1, 0)));
        }

        if (checkForCorruptJournalFiles) {
            Collection<DataFile> dataFiles = journal.getFileMap().values();
            for (DataFile dataFile : dataFiles) {
                int id = dataFile.getDataFileId();
                // eof to next file id
                missingPredicates.add(new BTreeVisitor.BetweenVisitor<Location, Long>(new Location(id, dataFile.getLength()), new Location(id + 1, 0)));
                Sequence seq = dataFile.getCorruptedBlocks().getHead();
                while (seq != null) {
                    BTreeVisitor.BetweenVisitor<Location, Long> visitor =
                        new BTreeVisitor.BetweenVisitor<>(new Location(id, (int) seq.getFirst()), new Location(id, (int) seq.getLast() + 1));
                    missingPredicates.add(visitor);
                    knownCorruption.add(visitor);
                    seq = seq.getNext();
                }
            }
        }

        if (!missingPredicates.isEmpty()) {
            for (Entry<String, StoredDestination> sdEntry : storedDestinations.entrySet()) {
                final StoredDestination sd = sdEntry.getValue();
                final LinkedHashMap<Long, Location> matches = new LinkedHashMap<>();
                sd.locationIndex.visit(tx, new BTreeVisitor.OrVisitor<Location, Long>(missingPredicates) {
                    @Override
                    protected void matched(Location key, Long value) {
                        matches.put(value, key);
                    }
                });

                // If some message references are affected by the missing data files...
                if (!matches.isEmpty()) {

                    // We either 'gracefully' recover dropping the missing messages or
                    // we error out.
                    if( ignoreMissingJournalfiles ) {
                        // Update the index to remove the references to the missing data
                        for (Long sequenceId : matches.keySet()) {
                            MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
                            sd.locationIndex.remove(tx, keys.location);
                            sd.messageIdIndex.remove(tx, keys.messageId);
                            LOG.info("[" + sdEntry.getKey() + "] dropped: " + keys.messageId + " at corrupt location: " + keys.location);
                            undoCounter++;
                            decrementAndSubSizeToStoreStat(sdEntry.getKey(), keys.location.getSize());
                            // TODO: do we need to modify the ack positions for the pub sub case?
                        }
                    } else {
                        LOG.error("[" + sdEntry.getKey() + "] references corrupt locations: " + matches);
                        throw new IOException("Detected missing/corrupt journal files referenced by:[" + sdEntry.getKey() + "] " +matches.size()+" messages affected.");
                    }
                }
            }
        }

        if (!ignoreMissingJournalfiles) {
            if (!knownCorruption.isEmpty()) {
                LOG.error("Detected corrupt journal files. " + knownCorruption);
                throw new IOException("Detected corrupt journal files. " + knownCorruption);
            }

            if (!missingJournalFiles.isEmpty()) {
                LOG.error("Detected missing journal files. " + missingJournalFiles);
                throw new IOException("Detected missing journal files. " + missingJournalFiles);
            }
        }

        if (undoCounter > 0) {
            // The rolledback operations are basically in flight journal writes.  To avoid getting these the end user
            // should do sync writes to the journal.
            if (LOG.isInfoEnabled()) {
                long end = System.currentTimeMillis();
                LOG.info("Detected missing/corrupt journal files.  Dropped " + undoCounter + " messages from the index in " + ((end - start) / 1000.0f) + " seconds.");
            }
        }
    }

    private Location nextRecoveryPosition;
    private Location lastRecoveryPosition;

    public void incrementalRecover() throws IOException {
        this.indexLock.writeLock().lock();
        try {
            if( nextRecoveryPosition == null ) {
                if( lastRecoveryPosition==null ) {
                    nextRecoveryPosition = getRecoveryPosition();
                } else {
                    nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition);
                }
            }
            while (nextRecoveryPosition != null) {
                lastRecoveryPosition = nextRecoveryPosition;
                metadata.lastUpdate = lastRecoveryPosition;
                JournalCommand<?> message = load(lastRecoveryPosition);
                process(message, lastRecoveryPosition, (IndexAware) null);
                nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition);
            }
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    public Location getLastUpdatePosition() throws IOException {
        return metadata.lastUpdate;
    }

    private Location getRecoveryPosition() throws IOException {

        if (!this.forceRecoverIndex) {

            // If we need to recover the transactions..
            if (metadata.firstInProgressTransactionLocation != null) {
                return metadata.firstInProgressTransactionLocation;
            }

            // Perhaps there were no transactions...
            if( metadata.lastUpdate!=null) {
                // Start replay at the record after the last one recorded in the index file.
                return getNextInitializedLocation(metadata.lastUpdate);
            }
        }
        // This loads the first position.
        return journal.getNextLocation(null);
    }

    private Location getNextInitializedLocation(Location location) throws IOException {
        Location mayNotBeInitialized = journal.getNextLocation(location);
        if (location.getSize() == NOT_SET && mayNotBeInitialized != null && mayNotBeInitialized.getSize() != NOT_SET) {
            // need to init size and type to skip
            return journal.getNextLocation(mayNotBeInitialized);
        } else {
            return mayNotBeInitialized;
        }
    }

    protected void checkpointCleanup(final boolean cleanup) throws IOException {
        long start;
        this.indexLock.writeLock().lock();
        try {
            start = System.currentTimeMillis();
            if( !opened.get() ) {
                return;
            }
        } finally {
            this.indexLock.writeLock().unlock();
        }
        checkpointUpdate(cleanup);
        long end = System.currentTimeMillis();
        if (LOG_SLOW_ACCESS_TIME > 0 && end - start > LOG_SLOW_ACCESS_TIME) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Slow KahaDB access: cleanup took " + (end - start));
            }
        }
    }

    public ByteSequence toByteSequence(JournalCommand<?> data) throws IOException {
        int size = data.serializedSizeFramed();
        DataByteArrayOutputStream os = new DataByteArrayOutputStream(size + 1);
        os.writeByte(data.type().getNumber());
        data.writeFramed(os);
        return os.toByteSequence();
    }

    // /////////////////////////////////////////////////////////////////
    // Methods call by the broker to update and query the store.
    // /////////////////////////////////////////////////////////////////
    public Location store(JournalCommand<?> data) throws IOException {
        return store(data, false, null,null);
    }

    public Location store(JournalCommand<?> data, Runnable onJournalStoreComplete) throws IOException {
        return store(data, false, null, null, onJournalStoreComplete);
    }

    public Location store(JournalCommand<?> data, boolean sync, IndexAware before,Runnable after) throws IOException {
        return store(data, sync, before, after, null);
    }

    /**
     * All updated are are funneled through this method. The updates are converted
     * to a JournalMessage which is logged to the journal and then the data from
     * the JournalMessage is used to update the index just like it would be done
     * during a recovery process.
     */
    public Location store(JournalCommand<?> data, boolean sync, IndexAware before, Runnable after, Runnable onJournalStoreComplete) throws IOException {
        try {
            ByteSequence sequence = toByteSequence(data);
            Location location;

            checkpointLock.readLock().lock();
            try {

                long start = System.currentTimeMillis();
                location = onJournalStoreComplete == null ? journal.write(sequence, sync) : journal.write(sequence, onJournalStoreComplete) ;
                long start2 = System.currentTimeMillis();
                //Track the last async update so we know if we need to sync at the next checkpoint
                if (!sync && journal.isJournalDiskSyncPeriodic()) {
                    lastAsyncJournalUpdate.set(location);
                }
                process(data, location, before);

                long end = System.currentTimeMillis();
                if (LOG_SLOW_ACCESS_TIME > 0 && end - start > LOG_SLOW_ACCESS_TIME) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Slow KahaDB access: Journal append took: "+(start2-start)+" ms, Index update took "+(end-start2)+" ms");
                    }
                }
            } finally {
                checkpointLock.readLock().unlock();
            }

            if (after != null) {
                after.run();
            }

            if (scheduler == null && opened.get()) {
                startCheckpoint();
            }
            return location;
        } catch (IOException ioe) {
            LOG.error("KahaDB failed to store to Journal, command of type: " + data.type(), ioe);
            brokerService.handleIOException(ioe);
            throw ioe;
        }
    }

    /**
     * Loads a previously stored JournalMessage
     *
     * @param location
     * @return
     * @throws IOException
     */
    public JournalCommand<?> load(Location location) throws IOException {
        long start = System.currentTimeMillis();
        ByteSequence data = journal.read(location);
        long end = System.currentTimeMillis();
        if( LOG_SLOW_ACCESS_TIME>0 && end-start > LOG_SLOW_ACCESS_TIME) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Slow KahaDB access: Journal read took: "+(end-start)+" ms");
            }
        }
        DataByteArrayInputStream is = new DataByteArrayInputStream(data);
        byte readByte = is.readByte();
        KahaEntryType type = KahaEntryType.valueOf(readByte);
        if( type == null ) {
            try {
                is.close();
            } catch (IOException e) {}
            throw new IOException("Could not load journal record, null type information from: " + readByte + " at location: "+location);
        }
        JournalCommand<?> message = (JournalCommand<?>)type.createMessage();
        message.mergeFramed(is);
        return message;
    }

    /**
     * do minimal recovery till we reach the last inDoubtLocation
     * @param data
     * @param location
     * @param inDoubtlocation
     * @throws IOException
     */
    void process(JournalCommand<?> data, final Location location, final Location inDoubtlocation) throws IOException {
        if (inDoubtlocation != null && location.compareTo(inDoubtlocation) >= 0) {
            process(data, location, (IndexAware) null);
        } else {
            // just recover producer audit
            data.visit(new Visitor() {
                @Override
                public void visit(KahaAddMessageCommand command) throws IOException {
                    metadata.producerSequenceIdTracker.isDuplicate(command.getMessageId());
                }
            });
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Journaled record processing methods. Once the record is journaled,
    // these methods handle applying the index updates. These may be called
    // from the recovery method too so they need to be idempotent
    // /////////////////////////////////////////////////////////////////

    void process(JournalCommand<?> data, final Location location, final IndexAware onSequenceAssignedCallback) throws IOException {
        data.visit(new Visitor() {
            @Override
            public void visit(KahaAddMessageCommand command) throws IOException {
                process(command, location, onSequenceAssignedCallback);
            }

            @Override
            public void visit(KahaRemoveMessageCommand command) throws IOException {
                process(command, location);
            }

            @Override
            public void visit(KahaPrepareCommand command) throws IOException {
                process(command, location);
            }

            @Override
            public void visit(KahaCommitCommand command) throws IOException {
                process(command, location, onSequenceAssignedCallback);
            }

            @Override
            public void visit(KahaRollbackCommand command) throws IOException {
                process(command, location);
            }

            @Override
            public void visit(KahaRemoveDestinationCommand command) throws IOException {
                process(command, location);
            }

            @Override
            public void visit(KahaSubscriptionCommand command) throws IOException {
                process(command, location);
            }

            @Override
            public void visit(KahaProducerAuditCommand command) throws IOException {
                processLocation(location);
            }

            @Override
            public void visit(KahaAckMessageFileMapCommand command) throws IOException {
                processLocation(location);
            }

            @Override
            public void visit(KahaTraceCommand command) {
                processLocation(location);
            }

            @Override
            public void visit(KahaUpdateMessageCommand command) throws IOException {
                process(command, location);
            }

            @Override
            public void visit(KahaRewrittenDataFileCommand command) throws IOException {
                process(command, location);
            }
        });
    }

    @SuppressWarnings("rawtypes")
    protected void process(final KahaAddMessageCommand command, final Location location, final IndexAware runWithIndexLock) throws IOException {
        if (command.hasTransactionInfo()) {
            List<Operation> inflightTx = getInflightTx(command.getTransactionInfo());
            inflightTx.add(new AddOperation(command, location, runWithIndexLock));
        } else {
            this.indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        long assignedIndex = updateIndex(tx, command, location);
                        if (runWithIndexLock != null) {
                            runWithIndexLock.sequenceAssignedWithIndexLocked(assignedIndex);
                        }
                    }
                });

            } finally {
                this.indexLock.writeLock().unlock();
            }
        }
    }

    protected void process(final KahaUpdateMessageCommand command, final Location location) throws IOException {
        this.indexLock.writeLock().lock();
        try {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, command, location);
                }
            });
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("rawtypes")
    protected void process(final KahaRemoveMessageCommand command, final Location location) throws IOException {
        if (command.hasTransactionInfo()) {
           List<Operation> inflightTx = getInflightTx(command.getTransactionInfo());
           inflightTx.add(new RemoveOperation(command, location));
        } else {
            this.indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        updateIndex(tx, command, location);
                    }
                });
            } finally {
                this.indexLock.writeLock().unlock();
            }
        }
    }

    protected void process(final KahaRemoveDestinationCommand command, final Location location) throws IOException {
        this.indexLock.writeLock().lock();
        try {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, command, location);
                }
            });
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    protected void process(final KahaSubscriptionCommand command, final Location location) throws IOException {
        this.indexLock.writeLock().lock();
        try {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, command, location);
                }
            });
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    protected void processLocation(final Location location) {
        this.indexLock.writeLock().lock();
        try {
            metadata.lastUpdate = location;
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("rawtypes")
    protected void process(KahaCommitCommand command, final Location location, final IndexAware before) throws IOException {
        TransactionId key = TransactionIdConversion.convert(command.getTransactionInfo());
        List<Operation> inflightTx;
        synchronized (inflightTransactions) {
            inflightTx = inflightTransactions.remove(key);
            if (inflightTx == null) {
                inflightTx = preparedTransactions.remove(key);
            }
        }
        if (inflightTx == null) {
            // only non persistent messages in this tx
            if (before != null) {
                before.sequenceAssignedWithIndexLocked(-1);
            }
            return;
        }

        final List<Operation> messagingTx = inflightTx;
        indexLock.writeLock().lock();
        try {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    for (Operation op : messagingTx) {
                        op.execute(tx);
                    }
                }
            });
            metadata.lastUpdate = location;
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("rawtypes")
    protected void process(KahaPrepareCommand command, Location location) {
        TransactionId key = TransactionIdConversion.convert(command.getTransactionInfo());
        synchronized (inflightTransactions) {
            List<Operation> tx = inflightTransactions.remove(key);
            if (tx != null) {
                preparedTransactions.put(key, tx);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    protected void process(KahaRollbackCommand command, Location location)  throws IOException {
        TransactionId key = TransactionIdConversion.convert(command.getTransactionInfo());
        List<Operation> updates = null;
        synchronized (inflightTransactions) {
            updates = inflightTransactions.remove(key);
            if (updates == null) {
                updates = preparedTransactions.remove(key);
            }
        }
    }

    protected void process(KahaRewrittenDataFileCommand command, Location location)  throws IOException {
        final TreeSet<Integer> completeFileSet = new TreeSet<>(journal.getFileMap().keySet());

        // Mark the current journal file as a compacted file so that gc checks can skip
        // over logs that are smaller compaction type logs.
        DataFile current = journal.getDataFileById(location.getDataFileId());
        current.setTypeCode(command.getRewriteType());

        if (completeFileSet.contains(command.getSourceDataFileId()) && command.getSkipIfSourceExists()) {
            // Move offset so that next location read jumps to next file.
            location.setOffset(journalMaxFileLength);
        }
    }

    // /////////////////////////////////////////////////////////////////
    // These methods do the actual index updates.
    // /////////////////////////////////////////////////////////////////

    protected final ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();
    private final HashSet<Integer> journalFilesBeingReplicated = new HashSet<>();

    long updateIndex(Transaction tx, KahaAddMessageCommand command, Location location) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);

        // Skip adding the message to the index if this is a topic and there are
        // no subscriptions.
        if (sd.subscriptions != null && sd.subscriptions.isEmpty(tx)) {
            return -1;
        }

        // Add the message.
        int priority = command.getPrioritySupported() ? command.getPriority() : javax.jms.Message.DEFAULT_PRIORITY;
        long id = sd.orderIndex.getNextMessageId();
        Long previous = sd.locationIndex.put(tx, location, id);
        if (previous == null) {
            previous = sd.messageIdIndex.put(tx, command.getMessageId(), id);
            if (previous == null) {
                incrementAndAddSizeToStoreStat(command.getDestination(), location.getSize());
                sd.orderIndex.put(tx, priority, id, new MessageKeys(command.getMessageId(), location));
                if (sd.subscriptions != null && !sd.subscriptions.isEmpty(tx)) {
                    addAckLocationForNewMessage(tx, command.getDestination(), sd, id);
                }
                metadata.lastUpdate = location;
            } else {

                MessageKeys messageKeys = sd.orderIndex.get(tx, previous);
                if (messageKeys != null && messageKeys.location.compareTo(location) < 0) {
                    // If the message ID is indexed, then the broker asked us to store a duplicate before the message was dispatched and acked, we ignore this add attempt
                    LOG.warn("Duplicate message add attempt rejected. Destination: {}://{}, Message id: {}", command.getDestination().getType(), command.getDestination().getName(), command.getMessageId());
                }
                sd.messageIdIndex.put(tx, command.getMessageId(), previous);
                sd.locationIndex.remove(tx, location);
                id = -1;
            }
        } else {
            // restore the previous value.. Looks like this was a redo of a previously
            // added message. We don't want to assign it a new id as the other indexes would
            // be wrong..
            sd.locationIndex.put(tx, location, previous);
            // ensure sequence is not broken
            sd.orderIndex.revertNextMessageId();
            metadata.lastUpdate = location;
        }
        // record this id in any event, initial send or recovery
        metadata.producerSequenceIdTracker.isDuplicate(command.getMessageId());

       return id;
    }

    void trackPendingAdd(KahaDestination destination, Long seq) {
        StoredDestination sd = storedDestinations.get(key(destination));
        if (sd != null) {
            sd.trackPendingAdd(seq);
        }
    }

    void trackPendingAddComplete(KahaDestination destination, Long seq) {
        StoredDestination sd = storedDestinations.get(key(destination));
        if (sd != null) {
            sd.trackPendingAddComplete(seq);
        }
    }

    void updateIndex(Transaction tx, KahaUpdateMessageCommand updateMessageCommand, Location location) throws IOException {
        KahaAddMessageCommand command = updateMessageCommand.getMessage();
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);

        Long id = sd.messageIdIndex.get(tx, command.getMessageId());
        if (id != null) {
            MessageKeys previousKeys = sd.orderIndex.put(
                    tx,
                    command.getPrioritySupported() ? command.getPriority() : javax.jms.Message.DEFAULT_PRIORITY,
                    id,
                    new MessageKeys(command.getMessageId(), location)
            );
            sd.locationIndex.put(tx, location, id);
            incrementAndAddSizeToStoreStat(command.getDestination(), location.getSize());

            if (previousKeys != null) {
                //Remove the existing from the size
                decrementAndSubSizeToStoreStat(command.getDestination(), previousKeys.location.getSize());

                //update all the subscription metrics
                if (enableSubscriptionStatistics && sd.ackPositions != null && location.getSize() != previousKeys.location.getSize()) {
                    Iterator<Entry<String, SequenceSet>> iter = sd.ackPositions.iterator(tx);
                    while (iter.hasNext()) {
                        Entry<String, SequenceSet> e = iter.next();
                        if (e.getValue().contains(id)) {
                            incrementAndAddSizeToStoreStat(key(command.getDestination()), e.getKey(), location.getSize());
                            decrementAndSubSizeToStoreStat(key(command.getDestination()), e.getKey(), previousKeys.location.getSize());
                        }
                    }
                }

                // on first update previous is original location, on recovery/replay it may be the updated location
                if(!previousKeys.location.equals(location)) {
                    sd.locationIndex.remove(tx, previousKeys.location);
                }
            }
            metadata.lastUpdate = location;
        } else {
            //Add the message if it can't be found
            this.updateIndex(tx, command, location);
        }
    }

    void updateIndex(Transaction tx, KahaRemoveMessageCommand command, Location ackLocation) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);
        if (!command.hasSubscriptionKey()) {

            // In the queue case we just remove the message from the index..
            Long sequenceId = sd.messageIdIndex.remove(tx, command.getMessageId());
            if (sequenceId != null) {
                MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
                if (keys != null) {
                    sd.locationIndex.remove(tx, keys.location);
                    decrementAndSubSizeToStoreStat(command.getDestination(), keys.location.getSize());
                    recordAckMessageReferenceLocation(ackLocation, keys.location);
                    metadata.lastUpdate = ackLocation;
                }  else if (LOG.isDebugEnabled()) {
                    LOG.debug("message not found in order index: " + sequenceId  + " for: " + command.getMessageId());
                }
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("message not found in sequence id index: " + command.getMessageId());
            }
        } else {
            // In the topic case we need remove the message once it's been acked
            // by all the subs
            Long sequence = sd.messageIdIndex.get(tx, command.getMessageId());

            // Make sure it's a valid message id...
            if (sequence != null) {
                String subscriptionKey = command.getSubscriptionKey();
                if (command.getAck() != UNMATCHED) {
                    sd.orderIndex.get(tx, sequence);
                    byte priority = sd.orderIndex.lastGetPriority();
                    sd.subscriptionAcks.put(tx, subscriptionKey, new LastAck(sequence, priority));
                }

                MessageKeys keys = sd.orderIndex.get(tx, sequence);
                if (keys != null) {
                    recordAckMessageReferenceLocation(ackLocation, keys.location);
                }
                // The following method handles deleting un-referenced messages.
                removeAckLocation(command, tx, sd, subscriptionKey, sequence);
                metadata.lastUpdate = ackLocation;
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("on ack, no message sequence exists for id: " + command.getMessageId() + " and sub: " + command.getSubscriptionKey());
            }

        }
    }

    private void recordAckMessageReferenceLocation(Location ackLocation, Location messageLocation) {
        Set<Integer> referenceFileIds = metadata.ackMessageFileMap.get(Integer.valueOf(ackLocation.getDataFileId()));
        if (referenceFileIds == null) {
            referenceFileIds = new HashSet<>();
            referenceFileIds.add(messageLocation.getDataFileId());
            metadata.ackMessageFileMap.put(ackLocation.getDataFileId(), referenceFileIds);
        } else {
            Integer id = Integer.valueOf(messageLocation.getDataFileId());
            if (!referenceFileIds.contains(id)) {
                referenceFileIds.add(id);
            }
        }
    }

    void updateIndex(Transaction tx, KahaRemoveDestinationCommand command, Location location) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);
        sd.orderIndex.remove(tx);

        sd.locationIndex.clear(tx);
        sd.locationIndex.unload(tx);
        tx.free(sd.locationIndex.getPageId());

        sd.messageIdIndex.clear(tx);
        sd.messageIdIndex.unload(tx);
        tx.free(sd.messageIdIndex.getPageId());

        if (sd.subscriptions != null) {
            sd.subscriptions.clear(tx);
            sd.subscriptions.unload(tx);
            tx.free(sd.subscriptions.getPageId());

            sd.subscriptionAcks.clear(tx);
            sd.subscriptionAcks.unload(tx);
            tx.free(sd.subscriptionAcks.getPageId());

            sd.ackPositions.clear(tx);
            sd.ackPositions.unload(tx);
            tx.free(sd.ackPositions.getHeadPageId());

            sd.subLocations.clear(tx);
            sd.subLocations.unload(tx);
            tx.free(sd.subLocations.getHeadPageId());
        }

        String key = key(command.getDestination());
        storedDestinations.remove(key);
        metadata.destinations.remove(tx, key);
        clearStoreStats(command.getDestination());
        storeCache.remove(key(command.getDestination()));
    }

    void updateIndex(Transaction tx, KahaSubscriptionCommand command, Location location) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);
        final String subscriptionKey = command.getSubscriptionKey();

        // If set then we are creating it.. otherwise we are destroying the sub
        if (command.hasSubscriptionInfo()) {
            Location existing = sd.subLocations.get(tx, subscriptionKey);
            if (existing != null && existing.compareTo(location) == 0) {
                // replay on recovery, ignore
                LOG.trace("ignoring journal replay of replay of sub from: " + location);
                return;
            }

            sd.subscriptions.put(tx, subscriptionKey, command);
            sd.subLocations.put(tx, subscriptionKey, location);
            long ackLocation=NOT_ACKED;
            if (!command.getRetroactive()) {
                ackLocation = sd.orderIndex.nextMessageId-1;
            } else {
                addAckLocationForRetroactiveSub(tx, sd, subscriptionKey);
            }
            sd.subscriptionAcks.put(tx, subscriptionKey, new LastAck(ackLocation));
            sd.subscriptionCache.add(subscriptionKey);
        } else {
            // delete the sub...
            sd.subscriptions.remove(tx, subscriptionKey);
            sd.subLocations.remove(tx, subscriptionKey);
            sd.subscriptionAcks.remove(tx, subscriptionKey);
            sd.subscriptionCache.remove(subscriptionKey);
            removeAckLocationsForSub(command, tx, sd, subscriptionKey);
            MessageStoreSubscriptionStatistics subStats = getSubStats(key(command.getDestination()));
            if (subStats != null) {
                subStats.removeSubscription(subscriptionKey);
            }

            if (sd.subscriptions.isEmpty(tx)) {
                // remove the stored destination
                KahaRemoveDestinationCommand removeDestinationCommand = new KahaRemoveDestinationCommand();
                removeDestinationCommand.setDestination(command.getDestination());
                updateIndex(tx, removeDestinationCommand, null);
                clearStoreStats(command.getDestination());
            }
        }
    }

    private void checkpointUpdate(final boolean cleanup) throws IOException {
        checkpointLock.writeLock().lock();
        try {
            this.indexLock.writeLock().lock();
            try {
                Set<Integer> filesToGc = pageFile.tx().execute(new Transaction.CallableClosure<Set<Integer>, IOException>() {
                    @Override
                    public Set<Integer> execute(Transaction tx) throws IOException {
                        return checkpointUpdate(tx, cleanup);
                    }
                });
                pageFile.flush();
                // after the index update such that partial removal does not leave dangling references in the index.
                journal.removeDataFiles(filesToGc);
            } finally {
                this.indexLock.writeLock().unlock();
            }

        } finally {
            checkpointLock.writeLock().unlock();
        }
    }

    /**
     * @param tx
     * @throws IOException
     */
    Set<Integer> checkpointUpdate(Transaction tx, boolean cleanup) throws IOException {
        MDC.put("activemq.persistenceDir", getDirectory().getName());
        LOG.debug("Checkpoint started.");

        // reflect last update exclusive of current checkpoint
        Location lastUpdate = metadata.lastUpdate;

        metadata.state = OPEN_STATE;
        metadata.producerSequenceIdTrackerLocation = checkpointProducerAudit();
        metadata.ackMessageFileMapLocation = checkpointAckMessageFileMap();
        Location[] inProgressTxRange = getInProgressTxLocationRange();
        metadata.firstInProgressTransactionLocation = inProgressTxRange[0];
        tx.store(metadata.page, metadataMarshaller, true);

        final TreeSet<Integer> gcCandidateSet = new TreeSet<>();
        if (cleanup) {

            final TreeSet<Integer> completeFileSet = new TreeSet<>(journal.getFileMap().keySet());
            gcCandidateSet.addAll(completeFileSet);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Last update: " + lastUpdate + ", full gc candidates set: " + gcCandidateSet);
            }

            if (lastUpdate != null) {
                // we won't delete past the last update, ackCompaction journal can be a candidate in error
                gcCandidateSet.removeAll(new TreeSet<Integer>(gcCandidateSet.tailSet(lastUpdate.getDataFileId())));
            }

            // Don't GC files under replication
            if( journalFilesBeingReplicated!=null ) {
                gcCandidateSet.removeAll(journalFilesBeingReplicated);
            }

            if (metadata.producerSequenceIdTrackerLocation != null) {
                int dataFileId = metadata.producerSequenceIdTrackerLocation.getDataFileId();
                if (gcCandidateSet.contains(dataFileId) && gcCandidateSet.first() == dataFileId) {
                    // rewrite so we don't prevent gc
                    metadata.producerSequenceIdTracker.setModified(true);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("rewriting producerSequenceIdTracker:" + metadata.producerSequenceIdTrackerLocation);
                    }
                }
                gcCandidateSet.remove(dataFileId);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("gc candidates after producerSequenceIdTrackerLocation:" + metadata.producerSequenceIdTrackerLocation + ", " + gcCandidateSet);
                }
            }

            if (metadata.ackMessageFileMapLocation != null) {
                int dataFileId = metadata.ackMessageFileMapLocation.getDataFileId();
                gcCandidateSet.remove(dataFileId);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("gc candidates after ackMessageFileMapLocation:" + metadata.ackMessageFileMapLocation + ", " + gcCandidateSet);
                }
            }

            // Don't GC files referenced by in-progress tx
            if (inProgressTxRange[0] != null) {
                for (int pendingTx=inProgressTxRange[0].getDataFileId(); pendingTx <= inProgressTxRange[1].getDataFileId(); pendingTx++) {
                    gcCandidateSet.remove(pendingTx);
                }
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("gc candidates after in progress tx range:" + Arrays.asList(inProgressTxRange) + ", " + gcCandidateSet);
            }

            // Go through all the destinations to see if any of them can remove GC candidates.
            for (Entry<String, StoredDestination> entry : storedDestinations.entrySet()) {
                if( gcCandidateSet.isEmpty() ) {
                    break;
                }

                // Use a visitor to cut down the number of pages that we load
                entry.getValue().locationIndex.visit(tx, new BTreeVisitor<Location, Long>() {
                    int last=-1;
                    @Override
                    public boolean isInterestedInKeysBetween(Location first, Location second) {
                        if( first==null ) {
                            SortedSet<Integer> subset = gcCandidateSet.headSet(second.getDataFileId()+1);
                            if( !subset.isEmpty() && subset.last() == second.getDataFileId() ) {
                                subset.remove(second.getDataFileId());
                            }
                            return !subset.isEmpty();
                        } else if( second==null ) {
                            SortedSet<Integer> subset = gcCandidateSet.tailSet(first.getDataFileId());
                            if( !subset.isEmpty() && subset.first() == first.getDataFileId() ) {
                                subset.remove(first.getDataFileId());
                            }
                            return !subset.isEmpty();
                        } else {
                            SortedSet<Integer> subset = gcCandidateSet.subSet(first.getDataFileId(), second.getDataFileId()+1);
                            if( !subset.isEmpty() && subset.first() == first.getDataFileId() ) {
                                subset.remove(first.getDataFileId());
                            }
                            if( !subset.isEmpty() && subset.last() == second.getDataFileId() ) {
                                subset.remove(second.getDataFileId());
                            }
                            return !subset.isEmpty();
                        }
                    }

                    @Override
                    public void visit(List<Location> keys, List<Long> values) {
                        for (Location l : keys) {
                            int fileId = l.getDataFileId();
                            if( last != fileId ) {
                                gcCandidateSet.remove(fileId);
                                last = fileId;
                            }
                        }
                    }
                });

                // Durable Subscription
                if (entry.getValue().subLocations != null) {
                    Iterator<Entry<String, Location>> iter = entry.getValue().subLocations.iterator(tx);
                    while (iter.hasNext()) {
                        Entry<String, Location> subscription = iter.next();
                        int dataFileId = subscription.getValue().getDataFileId();

                        // Move subscription along if it has no outstanding messages that need ack'd
                        // and its in the last log file in the journal.
                        if (!gcCandidateSet.isEmpty() && gcCandidateSet.first() == dataFileId) {
                            final StoredDestination destination = entry.getValue();
                            final String subscriptionKey = subscription.getKey();
                            SequenceSet pendingAcks = destination.ackPositions.get(tx, subscriptionKey);

                            // When pending is size one that is the next message Id meaning there
                            // are no pending messages currently.
                            if (pendingAcks == null || pendingAcks.isEmpty() ||
                                (pendingAcks.size() == 1 && pendingAcks.getTail().range() == 1)) {

                                if (LOG.isTraceEnabled()) {
                                    LOG.trace("Found candidate for rewrite: {} from file {}", entry.getKey(), dataFileId);
                                }

                                final KahaSubscriptionCommand kahaSub =
                                    destination.subscriptions.get(tx, subscriptionKey);
                                destination.subLocations.put(
                                    tx, subscriptionKey, checkpointSubscriptionCommand(kahaSub));

                                // Skips the remove from candidates if we rewrote the subscription
                                // in order to prevent duplicate subscription commands on recover.
                                // If another subscription is on the same file and isn't rewritten
                                // than it will remove the file from the set.
                                continue;
                            }
                        }

                        gcCandidateSet.remove(dataFileId);
                    }
                }

                if (LOG.isTraceEnabled()) {
                    LOG.trace("gc candidates after dest:" + entry.getKey() + ", " + gcCandidateSet);
                }
            }

            // check we are not deleting file with ack for in-use journal files
            if (LOG.isTraceEnabled()) {
                LOG.trace("gc candidates: " + gcCandidateSet);
                LOG.trace("ackMessageFileMap: " +  metadata.ackMessageFileMap);
            }

            boolean ackMessageFileMapMod = false;
            Iterator<Integer> candidates = gcCandidateSet.iterator();
            while (candidates.hasNext()) {
                Integer candidate = candidates.next();
                Set<Integer> referencedFileIds = metadata.ackMessageFileMap.get(candidate);
                if (referencedFileIds != null) {
                    for (Integer referencedFileId : referencedFileIds) {
                        if (completeFileSet.contains(referencedFileId) && !gcCandidateSet.contains(referencedFileId)) {
                            // active file that is not targeted for deletion is referenced so don't delete
                            candidates.remove();
                            break;
                        }
                    }
                    if (gcCandidateSet.contains(candidate)) {
                        ackMessageFileMapMod |= (metadata.ackMessageFileMap.remove(candidate) != null);
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("not removing data file: " + candidate
                                    + " as contained ack(s) refer to referenced file: " + referencedFileIds);
                        }
                    }
                }
            }

            if (!gcCandidateSet.isEmpty()) {
                LOG.debug("Cleanup removing the data files: {}", gcCandidateSet);
                for (Integer candidate : gcCandidateSet) {
                    for (Set<Integer> ackFiles : metadata.ackMessageFileMap.values()) {
                        ackMessageFileMapMod |= ackFiles.remove(candidate);
                    }
                }
                if (ackMessageFileMapMod) {
                    checkpointUpdate(tx, false);
                }
            } else if (isEnableAckCompaction()) {
                if (++checkPointCyclesWithNoGC >= getCompactAcksAfterNoGC()) {
                    // First check length of journal to make sure it makes sense to even try.
                    //
                    // If there is only one journal file with Acks in it we don't need to move
                    // it since it won't be chained to any later logs.
                    //
                    // If the logs haven't grown since the last time then we need to compact
                    // otherwise there seems to still be room for growth and we don't need to incur
                    // the overhead.  Depending on configuration this check can be avoided and
                    // Ack compaction will run any time the store has not GC'd a journal file in
                    // the configured amount of cycles.
                    if (metadata.ackMessageFileMap.size() > 1 &&
                        (journalLogOnLastCompactionCheck == journal.getCurrentDataFileId() || isCompactAcksIgnoresStoreGrowth())) {

                        LOG.trace("No files GC'd checking if threshold to ACK compaction has been met.");
                        try {
                            scheduler.execute(new AckCompactionRunner());
                        } catch (Exception ex) {
                            LOG.warn("Error on queueing the Ack Compactor", ex);
                        }
                    } else {
                        LOG.trace("Journal activity detected, no Ack compaction scheduled.");
                    }

                    checkPointCyclesWithNoGC = 0;
                } else {
                    LOG.trace("Not yet time to check for compaction: {} of {} cycles",
                              checkPointCyclesWithNoGC, getCompactAcksAfterNoGC());
                }

                journalLogOnLastCompactionCheck = journal.getCurrentDataFileId();
            }
        }
        MDC.remove("activemq.persistenceDir");

        LOG.debug("Checkpoint done.");
        return gcCandidateSet;
    }

    private final class AckCompactionRunner implements Runnable {

        @Override
        public void run() {

            int journalToAdvance = -1;
            Set<Integer> journalLogsReferenced = new HashSet<>();

            //flag to know whether the ack forwarding completed without an exception
            boolean forwarded = false;

            try {
                //acquire the checkpoint lock to prevent other threads from
                //running a checkpoint while this is running
                //
                //Normally this task runs on the same executor as the checkpoint task
                //so this ack compaction runner wouldn't run at the same time as the checkpoint task.
                //
                //However, there are two cases where this isn't always true.
                //First, the checkpoint() method is public and can be called through the
                //PersistenceAdapter interface by someone at the same time this is running.
                //Second, a checkpoint is called during shutdown without using the executor.
                //
                //In the future it might be better to just remove the checkpointLock entirely
                //and only use the executor but this would need to be examined for any unintended
                //consequences
                checkpointLock.readLock().lock();

                try {

                    // Lock index to capture the ackMessageFileMap data
                    indexLock.writeLock().lock();

                    // Map keys might not be sorted, find the earliest log file to forward acks
                    // from and move only those, future cycles can chip away at more as needed.
                    // We won't move files that are themselves rewritten on a previous compaction.
                    List<Integer> journalFileIds = new ArrayList<>(metadata.ackMessageFileMap.keySet());
                    Collections.sort(journalFileIds);
                    for (Integer journalFileId : journalFileIds) {
                        DataFile current = journal.getDataFileById(journalFileId);
                        if (current != null && current.getTypeCode() != COMPACTED_JOURNAL_FILE) {
                            journalToAdvance = journalFileId;
                            break;
                        }
                    }

                    // Check if we found one, or if we only found the current file being written to.
                    if (journalToAdvance == -1 || journalToAdvance == journal.getCurrentDataFileId()) {
                        return;
                    }

                    journalLogsReferenced.addAll(metadata.ackMessageFileMap.get(journalToAdvance));

                } finally {
                    indexLock.writeLock().unlock();
                }

                try {
                    // Background rewrite of the old acks
                    forwardAllAcks(journalToAdvance, journalLogsReferenced);
                    forwarded = true;
                } catch (IOException ioe) {
                    LOG.error("Forwarding of acks failed", ioe);
                    brokerService.handleIOException(ioe);
                } catch (Throwable e) {
                    LOG.error("Forwarding of acks failed", e);
                    brokerService.handleIOException(IOExceptionSupport.create(e));
                }
            } finally {
                checkpointLock.readLock().unlock();
            }

            try {
                if (forwarded) {
                    // Checkpoint with changes from the ackMessageFileMap
                    checkpointUpdate(false);
                }
            } catch (IOException ioe) {
                LOG.error("Checkpoint failed", ioe);
                brokerService.handleIOException(ioe);
            } catch (Throwable e) {
                LOG.error("Checkpoint failed", e);
                brokerService.handleIOException(IOExceptionSupport.create(e));
            }
        }
    }

    private void forwardAllAcks(Integer journalToRead, Set<Integer> journalLogsReferenced) throws IllegalStateException, IOException {
        LOG.trace("Attempting to move all acks in journal:{} to the front.", journalToRead);

        DataFile forwardsFile = journal.reserveDataFile();
        forwardsFile.setTypeCode(COMPACTED_JOURNAL_FILE);
        LOG.trace("Reserved file for forwarded acks: {}", forwardsFile);

        Map<Integer, Set<Integer>> updatedAckLocations = new HashMap<>();

        try (TargetedDataFileAppender appender = new TargetedDataFileAppender(journal, forwardsFile);) {
            KahaRewrittenDataFileCommand compactionMarker = new KahaRewrittenDataFileCommand();
            compactionMarker.setSourceDataFileId(journalToRead);
            compactionMarker.setRewriteType(forwardsFile.getTypeCode());

            ByteSequence payload = toByteSequence(compactionMarker);
            appender.storeItem(payload, Journal.USER_RECORD_TYPE, false);
            LOG.trace("Marked ack rewrites file as replacing file: {}", journalToRead);

            final Location limit = new Location(journalToRead + 1, 0);
            Location nextLocation = getNextLocationForAckForward(new Location(journalToRead, 0), limit);
            while (nextLocation != null) {
                JournalCommand<?> command = null;
                try {
                    command = load(nextLocation);
                } catch (IOException ex) {
                    LOG.trace("Error loading command during ack forward: {}", nextLocation);
                }

                if (command != null && command instanceof KahaRemoveMessageCommand) {
                    payload = toByteSequence(command);
                    Location location = appender.storeItem(payload, Journal.USER_RECORD_TYPE, false);
                    updatedAckLocations.put(location.getDataFileId(), journalLogsReferenced);
                }

                nextLocation = getNextLocationForAckForward(nextLocation, limit);
            }
        }

        LOG.trace("ACKS forwarded, updates for ack locations: {}", updatedAckLocations);

        // Lock index while we update the ackMessageFileMap.
        indexLock.writeLock().lock();

        // Update the ack map with the new locations of the acks
        for (Entry<Integer, Set<Integer>> entry : updatedAckLocations.entrySet()) {
            Set<Integer> referenceFileIds = metadata.ackMessageFileMap.get(entry.getKey());
            if (referenceFileIds == null) {
                referenceFileIds = new HashSet<>();
                referenceFileIds.addAll(entry.getValue());
                metadata.ackMessageFileMap.put(entry.getKey(), referenceFileIds);
            } else {
                referenceFileIds.addAll(entry.getValue());
            }
        }

        // remove the old location data from the ack map so that the old journal log file can
        // be removed on next GC.
        metadata.ackMessageFileMap.remove(journalToRead);

        indexLock.writeLock().unlock();

        LOG.trace("ACK File Map following updates: {}", metadata.ackMessageFileMap);
    }

    private Location getNextLocationForAckForward(final Location nextLocation, final Location limit) {
        //getNextLocation() can throw an IOException, we should handle it and set
        //nextLocation to null and abort gracefully
        //Should not happen in the normal case
        Location location = null;
        try {
            location = journal.getNextLocation(nextLocation, limit);
        } catch (IOException e) {
            LOG.warn("Failed to load next journal location after: {}, reason: {}", nextLocation, e);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to load next journal location after: {}", nextLocation, e);
            }
        }
        return location;
    }

    final Runnable nullCompletionCallback = new Runnable() {
        @Override
        public void run() {
        }
    };

    private Location checkpointProducerAudit() throws IOException {
        if (metadata.producerSequenceIdTracker == null || metadata.producerSequenceIdTracker.modified()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oout = new ObjectOutputStream(baos);
            oout.writeObject(metadata.producerSequenceIdTracker);
            oout.flush();
            oout.close();
            // using completion callback allows a disk sync to be avoided when enableJournalDiskSyncs = false
            Location location = store(new KahaProducerAuditCommand().setAudit(new Buffer(baos.toByteArray())), nullCompletionCallback);
            try {
                location.getLatch().await();
                if (location.getBatch().exception.get() != null) {
                    throw location.getBatch().exception.get();
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException(e.toString());
            }
            return location;
        }
        return metadata.producerSequenceIdTrackerLocation;
    }

    private Location checkpointAckMessageFileMap() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oout = new ObjectOutputStream(baos);
        oout.writeObject(metadata.ackMessageFileMap);
        oout.flush();
        oout.close();
        // using completion callback allows a disk sync to be avoided when enableJournalDiskSyncs = false
        Location location = store(new KahaAckMessageFileMapCommand().setAckMessageFileMap(new Buffer(baos.toByteArray())), nullCompletionCallback);
        try {
            location.getLatch().await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.toString());
        }
        return location;
    }

    private Location checkpointSubscriptionCommand(KahaSubscriptionCommand subscription) throws IOException {

        ByteSequence sequence = toByteSequence(subscription);
        Location location = journal.write(sequence, nullCompletionCallback) ;

        try {
            location.getLatch().await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.toString());
        }
        return location;
    }

    public HashSet<Integer> getJournalFilesBeingReplicated() {
        return journalFilesBeingReplicated;
    }

    // /////////////////////////////////////////////////////////////////
    // StoredDestination related implementation methods.
    // /////////////////////////////////////////////////////////////////

    protected final HashMap<String, StoredDestination> storedDestinations = new HashMap<>();

    static class MessageKeys {
        final String messageId;
        final Location location;

        public MessageKeys(String messageId, Location location) {
            this.messageId=messageId;
            this.location=location;
        }

        @Override
        public String toString() {
            return "["+messageId+","+location+"]";
        }
    }

    protected class MessageKeysMarshaller extends VariableMarshaller<MessageKeys> {
        final LocationSizeMarshaller locationSizeMarshaller = new LocationSizeMarshaller();

        @Override
        public MessageKeys readPayload(DataInput dataIn) throws IOException {
            return new MessageKeys(dataIn.readUTF(), locationSizeMarshaller.readPayload(dataIn));
        }

        @Override
        public void writePayload(MessageKeys object, DataOutput dataOut) throws IOException {
            dataOut.writeUTF(object.messageId);
            locationSizeMarshaller.writePayload(object.location, dataOut);
        }
    }

    class LastAck {
        long lastAckedSequence;
        byte priority;

        public LastAck(LastAck source) {
            this.lastAckedSequence = source.lastAckedSequence;
            this.priority = source.priority;
        }

        public LastAck() {
            this.priority = MessageOrderIndex.HI;
        }

        public LastAck(long ackLocation) {
            this.lastAckedSequence = ackLocation;
            this.priority = MessageOrderIndex.LO;
        }

        public LastAck(long ackLocation, byte priority) {
            this.lastAckedSequence = ackLocation;
            this.priority = priority;
        }

        @Override
        public String toString() {
            return "[" + lastAckedSequence + ":" + priority + "]";
        }
    }

    protected class LastAckMarshaller implements Marshaller<LastAck> {

        @Override
        public void writePayload(LastAck object, DataOutput dataOut) throws IOException {
            dataOut.writeLong(object.lastAckedSequence);
            dataOut.writeByte(object.priority);
        }

        @Override
        public LastAck readPayload(DataInput dataIn) throws IOException {
            LastAck lastAcked = new LastAck();
            lastAcked.lastAckedSequence = dataIn.readLong();
            if (metadata.version >= 3) {
                lastAcked.priority = dataIn.readByte();
            }
            return lastAcked;
        }

        @Override
        public int getFixedSize() {
            return 9;
        }

        @Override
        public LastAck deepCopy(LastAck source) {
            return new LastAck(source);
        }

        @Override
        public boolean isDeepCopySupported() {
            return true;
        }
    }

    class StoredDestination {

        MessageOrderIndex orderIndex = new MessageOrderIndex();
        BTreeIndex<Location, Long> locationIndex;
        BTreeIndex<String, Long> messageIdIndex;

        // These bits are only set for Topics
        BTreeIndex<String, KahaSubscriptionCommand> subscriptions;
        BTreeIndex<String, LastAck> subscriptionAcks;
        HashMap<String, MessageOrderCursor> subscriptionCursors;
        ListIndex<String, SequenceSet> ackPositions;
        ListIndex<String, Location> subLocations;

        // Transient data used to track which Messages are no longer needed.
        final TreeMap<Long, Long> messageReferences = new TreeMap<>();
        final HashSet<String> subscriptionCache = new LinkedHashSet<>();

        public void trackPendingAdd(Long seq) {
            orderIndex.trackPendingAdd(seq);
        }

        public void trackPendingAddComplete(Long seq) {
            orderIndex.trackPendingAddComplete(seq);
        }

        @Override
        public String toString() {
            return "nextSeq:" + orderIndex.nextMessageId + ",lastRet:" + orderIndex.cursor + ",pending:" + orderIndex.pendingAdditions.size();
        }
    }

    protected class StoredDestinationMarshaller extends VariableMarshaller<StoredDestination> {

        final MessageKeysMarshaller messageKeysMarshaller = new MessageKeysMarshaller();

        @Override
        public StoredDestination readPayload(final DataInput dataIn) throws IOException {
            final StoredDestination value = new StoredDestination();
            value.orderIndex.defaultPriorityIndex = new BTreeIndex<>(pageFile, dataIn.readLong());
            value.locationIndex = new BTreeIndex<>(pageFile, dataIn.readLong());
            value.messageIdIndex = new BTreeIndex<>(pageFile, dataIn.readLong());

            if (dataIn.readBoolean()) {
                value.subscriptions = new BTreeIndex<>(pageFile, dataIn.readLong());
                value.subscriptionAcks = new BTreeIndex<>(pageFile, dataIn.readLong());
                if (metadata.version >= 4) {
                    value.ackPositions = new ListIndex<>(pageFile, dataIn.readLong());
                } else {
                    // upgrade
                    pageFile.tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            LinkedHashMap<String, SequenceSet> temp = new LinkedHashMap<>();

                            if (metadata.version >= 3) {
                                // migrate
                                BTreeIndex<Long, HashSet<String>> oldAckPositions =
                                        new BTreeIndex<>(pageFile, dataIn.readLong());
                                oldAckPositions.setKeyMarshaller(LongMarshaller.INSTANCE);
                                oldAckPositions.setValueMarshaller(HashSetStringMarshaller.INSTANCE);
                                oldAckPositions.load(tx);


                                // Do the initial build of the data in memory before writing into the store
                                // based Ack Positions List to avoid a lot of disk thrashing.
                                Iterator<Entry<Long, HashSet<String>>> iterator = oldAckPositions.iterator(tx);
                                while (iterator.hasNext()) {
                                    Entry<Long, HashSet<String>> entry = iterator.next();

                                    for(String subKey : entry.getValue()) {
                                        SequenceSet pendingAcks = temp.get(subKey);
                                        if (pendingAcks == null) {
                                            pendingAcks = new SequenceSet();
                                            temp.put(subKey, pendingAcks);
                                        }

                                        pendingAcks.add(entry.getKey());
                                    }
                                }
                            }
                            // Now move the pending messages to ack data into the store backed
                            // structure.
                            value.ackPositions = new ListIndex<>(pageFile, tx.allocate());
                            value.ackPositions.setKeyMarshaller(StringMarshaller.INSTANCE);
                            value.ackPositions.setValueMarshaller(SequenceSet.Marshaller.INSTANCE);
                            value.ackPositions.load(tx);
                            for(String subscriptionKey : temp.keySet()) {
                                value.ackPositions.put(tx, subscriptionKey, temp.get(subscriptionKey));
                            }

                        }
                    });
                }

                if (metadata.version >= 5) {
                    value.subLocations = new ListIndex<>(pageFile, dataIn.readLong());
                } else {
                    // upgrade
                    pageFile.tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            value.subLocations = new ListIndex<>(pageFile, tx.allocate());
                            value.subLocations.setKeyMarshaller(StringMarshaller.INSTANCE);
                            value.subLocations.setValueMarshaller(LocationMarshaller.INSTANCE);
                            value.subLocations.load(tx);
                        }
                    });
                }
            }
            if (metadata.version >= 2) {
                value.orderIndex.lowPriorityIndex = new BTreeIndex<>(pageFile, dataIn.readLong());
                value.orderIndex.highPriorityIndex = new BTreeIndex<>(pageFile, dataIn.readLong());
            } else {
                // upgrade
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        value.orderIndex.lowPriorityIndex = new BTreeIndex<>(pageFile, tx.allocate());
                        value.orderIndex.lowPriorityIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
                        value.orderIndex.lowPriorityIndex.setValueMarshaller(messageKeysMarshaller);
                        value.orderIndex.lowPriorityIndex.load(tx);

                        value.orderIndex.highPriorityIndex = new BTreeIndex<>(pageFile, tx.allocate());
                        value.orderIndex.highPriorityIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
                        value.orderIndex.highPriorityIndex.setValueMarshaller(messageKeysMarshaller);
                        value.orderIndex.highPriorityIndex.load(tx);
                    }
                });
            }

            return value;
        }

        @Override
        public void writePayload(StoredDestination value, DataOutput dataOut) throws IOException {
            dataOut.writeLong(value.orderIndex.defaultPriorityIndex.getPageId());
            dataOut.writeLong(value.locationIndex.getPageId());
            dataOut.writeLong(value.messageIdIndex.getPageId());
            if (value.subscriptions != null) {
                dataOut.writeBoolean(true);
                dataOut.writeLong(value.subscriptions.getPageId());
                dataOut.writeLong(value.subscriptionAcks.getPageId());
                dataOut.writeLong(value.ackPositions.getHeadPageId());
                dataOut.writeLong(value.subLocations.getHeadPageId());
            } else {
                dataOut.writeBoolean(false);
            }
            dataOut.writeLong(value.orderIndex.lowPriorityIndex.getPageId());
            dataOut.writeLong(value.orderIndex.highPriorityIndex.getPageId());
        }
    }

    static class KahaSubscriptionCommandMarshaller extends VariableMarshaller<KahaSubscriptionCommand> {
        final static KahaSubscriptionCommandMarshaller INSTANCE = new KahaSubscriptionCommandMarshaller();

        @Override
        public KahaSubscriptionCommand readPayload(DataInput dataIn) throws IOException {
            KahaSubscriptionCommand rc = new KahaSubscriptionCommand();
            rc.mergeFramed((InputStream)dataIn);
            return rc;
        }

        @Override
        public void writePayload(KahaSubscriptionCommand object, DataOutput dataOut) throws IOException {
            object.writeFramed((OutputStream)dataOut);
        }
    }

    protected StoredDestination getStoredDestination(KahaDestination destination, Transaction tx) throws IOException {
        String key = key(destination);
        StoredDestination rc = storedDestinations.get(key);
        if (rc == null) {
            boolean topic = destination.getType() == KahaDestination.DestinationType.TOPIC || destination.getType() == KahaDestination.DestinationType.TEMP_TOPIC;
            rc = loadStoredDestination(tx, key, topic);
            // Cache it. We may want to remove/unload destinations from the
            // cache that are not used for a while
            // to reduce memory usage.
            storedDestinations.put(key, rc);
        }
        return rc;
    }

    protected StoredDestination getExistingStoredDestination(KahaDestination destination, Transaction tx) throws IOException {
        String key = key(destination);
        StoredDestination rc = storedDestinations.get(key);
        if (rc == null && metadata.destinations.containsKey(tx, key)) {
            rc = getStoredDestination(destination, tx);
        }
        return rc;
    }

    /**
     * @param tx
     * @param key
     * @param topic
     * @return
     * @throws IOException
     */
    private StoredDestination loadStoredDestination(Transaction tx, String key, boolean topic) throws IOException {
        // Try to load the existing indexes..
        StoredDestination rc = metadata.destinations.get(tx, key);
        if (rc == null) {
            // Brand new destination.. allocate indexes for it.
            rc = new StoredDestination();
            rc.orderIndex.allocate(tx);
            rc.locationIndex = new BTreeIndex<>(pageFile, tx.allocate());
            rc.messageIdIndex = new BTreeIndex<>(pageFile, tx.allocate());

            if (topic) {
                rc.subscriptions = new BTreeIndex<>(pageFile, tx.allocate());
                rc.subscriptionAcks = new BTreeIndex<>(pageFile, tx.allocate());
                rc.ackPositions = new ListIndex<>(pageFile, tx.allocate());
                rc.subLocations = new ListIndex<>(pageFile, tx.allocate());
            }
            metadata.destinations.put(tx, key, rc);
        }

        // Configure the marshalers and load.
        rc.orderIndex.load(tx);

        // Figure out the next key using the last entry in the destination.
        rc.orderIndex.configureLast(tx);

        rc.locationIndex.setKeyMarshaller(new LocationSizeMarshaller());
        rc.locationIndex.setValueMarshaller(LongMarshaller.INSTANCE);
        rc.locationIndex.load(tx);

        rc.messageIdIndex.setKeyMarshaller(StringMarshaller.INSTANCE);
        rc.messageIdIndex.setValueMarshaller(LongMarshaller.INSTANCE);
        rc.messageIdIndex.load(tx);

        //go through an upgrade old index if older than version 6
        if (metadata.version < 6) {
            for (Iterator<Entry<Location, Long>> iterator = rc.locationIndex.iterator(tx); iterator.hasNext(); ) {
                Entry<Location, Long> entry = iterator.next();
                // modify so it is upgraded
                rc.locationIndex.put(tx, entry.getKey(), entry.getValue());
            }
            //upgrade the order index
            for (Iterator<Entry<Long, MessageKeys>> iterator = rc.orderIndex.iterator(tx); iterator.hasNext(); ) {
                Entry<Long, MessageKeys> entry = iterator.next();
                //call get so that the last priority is updated
                rc.orderIndex.get(tx, entry.getKey());
                rc.orderIndex.put(tx, rc.orderIndex.lastGetPriority(), entry.getKey(), entry.getValue());
            }
        }

        // If it was a topic...
        if (topic) {

            rc.subscriptions.setKeyMarshaller(StringMarshaller.INSTANCE);
            rc.subscriptions.setValueMarshaller(KahaSubscriptionCommandMarshaller.INSTANCE);
            rc.subscriptions.load(tx);

            rc.subscriptionAcks.setKeyMarshaller(StringMarshaller.INSTANCE);
            rc.subscriptionAcks.setValueMarshaller(new LastAckMarshaller());
            rc.subscriptionAcks.load(tx);

            rc.ackPositions.setKeyMarshaller(StringMarshaller.INSTANCE);
            rc.ackPositions.setValueMarshaller(SequenceSet.Marshaller.INSTANCE);
            rc.ackPositions.load(tx);

            rc.subLocations.setKeyMarshaller(StringMarshaller.INSTANCE);
            rc.subLocations.setValueMarshaller(LocationMarshaller.INSTANCE);
            rc.subLocations.load(tx);

            rc.subscriptionCursors = new HashMap<>();

            if (metadata.version < 3) {

                // on upgrade need to fill ackLocation with available messages past last ack
                for (Iterator<Entry<String, LastAck>> iterator = rc.subscriptionAcks.iterator(tx); iterator.hasNext(); ) {
                    Entry<String, LastAck> entry = iterator.next();
                    for (Iterator<Entry<Long, MessageKeys>> orderIterator =
                            rc.orderIndex.iterator(tx, new MessageOrderCursor(entry.getValue().lastAckedSequence)); orderIterator.hasNext(); ) {
                        Long sequence = orderIterator.next().getKey();
                        addAckLocation(tx, rc, sequence, entry.getKey());
                    }
                    // modify so it is upgraded
                    rc.subscriptionAcks.put(tx, entry.getKey(), entry.getValue());
                }
            }

            // Configure the message references index
            Iterator<Entry<String, SequenceSet>> subscriptions = rc.ackPositions.iterator(tx);
            while (subscriptions.hasNext()) {
                Entry<String, SequenceSet> subscription = subscriptions.next();
                SequenceSet pendingAcks = subscription.getValue();
                if (pendingAcks != null && !pendingAcks.isEmpty()) {
                    Long lastPendingAck = pendingAcks.getTail().getLast();
                    for (Long sequenceId : pendingAcks) {
                        Long current = rc.messageReferences.get(sequenceId);
                        if (current == null) {
                            current = new Long(0);
                        }

                        // We always add a trailing empty entry for the next position to start from
                        // so we need to ensure we don't count that as a message reference on reload.
                        if (!sequenceId.equals(lastPendingAck)) {
                            current = current.longValue() + 1;
                        } else {
                            current = Long.valueOf(0L);
                        }

                        rc.messageReferences.put(sequenceId, current);
                    }
                }
            }

            // Configure the subscription cache
            for (Iterator<Entry<String, LastAck>> iterator = rc.subscriptionAcks.iterator(tx); iterator.hasNext(); ) {
                Entry<String, LastAck> entry = iterator.next();
                rc.subscriptionCache.add(entry.getKey());
            }

            if (rc.orderIndex.nextMessageId == 0) {
                // check for existing durable sub all acked out - pull next seq from acks as messages are gone
                if (!rc.subscriptionAcks.isEmpty(tx)) {
                    for (Iterator<Entry<String, LastAck>> iterator = rc.subscriptionAcks.iterator(tx); iterator.hasNext();) {
                        Entry<String, LastAck> entry = iterator.next();
                        rc.orderIndex.nextMessageId =
                                Math.max(rc.orderIndex.nextMessageId, entry.getValue().lastAckedSequence +1);
                    }
                }
            } else {
                // update based on ackPositions for unmatched, last entry is always the next
                if (!rc.messageReferences.isEmpty()) {
                    Long nextMessageId = (Long) rc.messageReferences.keySet().toArray()[rc.messageReferences.size() - 1];
                    rc.orderIndex.nextMessageId =
                            Math.max(rc.orderIndex.nextMessageId, nextMessageId);
                }
            }
        }

        if (metadata.version < VERSION) {
            // store again after upgrade
            metadata.destinations.put(tx, key, rc);
        }
        return rc;
    }

    /**
     * Clear the counter for the destination, if one exists.
     *
     * @param kahaDestination
     */
    protected void clearStoreStats(KahaDestination kahaDestination) {
        String key = key(kahaDestination);
        MessageStoreStatistics storeStats = getStoreStats(key);
        MessageStoreSubscriptionStatistics subStats = getSubStats(key);
        if (storeStats != null) {
            storeStats.reset();
        }
        if (subStats != null) {
            subStats.reset();
        }
    }

    /**
     * Update MessageStoreStatistics
     *
     * @param kahaDestination
     * @param size
     */
    protected void incrementAndAddSizeToStoreStat(KahaDestination kahaDestination, long size) {
        incrementAndAddSizeToStoreStat(key(kahaDestination), size);
    }

    protected void incrementAndAddSizeToStoreStat(String kahaDestKey, long size) {
        MessageStoreStatistics storeStats = getStoreStats(kahaDestKey);
        if (storeStats != null) {
            storeStats.getMessageCount().increment();
            if (size > 0) {
                storeStats.getMessageSize().addSize(size);
            }
        }
    }

    protected void decrementAndSubSizeToStoreStat(KahaDestination kahaDestination, long size) {
        decrementAndSubSizeToStoreStat(key(kahaDestination), size);
    }

    protected void decrementAndSubSizeToStoreStat(String kahaDestKey, long size) {
        MessageStoreStatistics storeStats = getStoreStats(kahaDestKey);
        if (storeStats != null) {
            storeStats.getMessageCount().decrement();
            if (size > 0) {
                storeStats.getMessageSize().addSize(-size);
            }
        }
    }

    protected void incrementAndAddSizeToStoreStat(KahaDestination kahaDestination, String subKey, long size) {
        incrementAndAddSizeToStoreStat(key(kahaDestination), subKey, size);
    }

    protected void incrementAndAddSizeToStoreStat(String kahaDestKey, String subKey, long size) {
        if (enableSubscriptionStatistics) {
            MessageStoreSubscriptionStatistics subStats = getSubStats(kahaDestKey);
            if (subStats != null && subKey != null) {
                subStats.getMessageCount(subKey).increment();
                if (size > 0) {
                    subStats.getMessageSize(subKey).addSize(size);
                }
            }
        }
    }


    protected void decrementAndSubSizeToStoreStat(String kahaDestKey, String subKey, long size) {
        if (enableSubscriptionStatistics) {
            MessageStoreSubscriptionStatistics subStats = getSubStats(kahaDestKey);
            if (subStats != null && subKey != null) {
                subStats.getMessageCount(subKey).decrement();
                if (size > 0) {
                    subStats.getMessageSize(subKey).addSize(-size);
                }
            }
        }
    }

    protected void decrementAndSubSizeToStoreStat(KahaDestination kahaDestination, String subKey, long size) {
        decrementAndSubSizeToStoreStat(key(kahaDestination), subKey, size);
    }

    /**
     * This is a map to cache MessageStores for a specific
     * KahaDestination key
     */
    protected final ConcurrentMap<String, MessageStore> storeCache =
            new ConcurrentHashMap<>();

    /**
     * Locate the storeMessageSize counter for this KahaDestination
     */
    protected MessageStoreStatistics getStoreStats(String kahaDestKey) {
        MessageStoreStatistics storeStats = null;
        try {
            MessageStore messageStore = storeCache.get(kahaDestKey);
            if (messageStore != null) {
                storeStats = messageStore.getMessageStoreStatistics();
            }
        } catch (Exception e1) {
             LOG.error("Getting size counter of destination failed", e1);
        }

        return storeStats;
    }

    protected MessageStoreSubscriptionStatistics getSubStats(String kahaDestKey) {
        MessageStoreSubscriptionStatistics subStats = null;
        try {
            MessageStore messageStore = storeCache.get(kahaDestKey);
            if (messageStore instanceof TopicMessageStore) {
                subStats = ((TopicMessageStore)messageStore).getMessageStoreSubStatistics();
            }
        } catch (Exception e1) {
             LOG.error("Getting size counter of destination failed", e1);
        }

        return subStats;
    }

    /**
     * Determine whether this Destination matches the DestinationType
     *
     * @param destination
     * @param type
     * @return
     */
    protected boolean matchType(Destination destination,
            KahaDestination.DestinationType type) {
        if (destination instanceof Topic
                && type.equals(KahaDestination.DestinationType.TOPIC)) {
            return true;
        } else if (destination instanceof Queue
                && type.equals(KahaDestination.DestinationType.QUEUE)) {
            return true;
        }
        return false;
    }

    class LocationSizeMarshaller implements Marshaller<Location> {

        public LocationSizeMarshaller() {

        }

        @Override
        public Location readPayload(DataInput dataIn) throws IOException {
            Location rc = new Location();
            rc.setDataFileId(dataIn.readInt());
            rc.setOffset(dataIn.readInt());
            if (metadata.version >= 6) {
                rc.setSize(dataIn.readInt());
            }
            return rc;
        }

        @Override
        public void writePayload(Location object, DataOutput dataOut)
                throws IOException {
            dataOut.writeInt(object.getDataFileId());
            dataOut.writeInt(object.getOffset());
            dataOut.writeInt(object.getSize());
        }

        @Override
        public int getFixedSize() {
            return 12;
        }

        @Override
        public Location deepCopy(Location source) {
            return new Location(source);
        }

        @Override
        public boolean isDeepCopySupported() {
            return true;
        }
    }

    private void addAckLocation(Transaction tx, StoredDestination sd, Long messageSequence, String subscriptionKey) throws IOException {
        SequenceSet sequences = sd.ackPositions.get(tx, subscriptionKey);
        if (sequences == null) {
            sequences = new SequenceSet();
            sequences.add(messageSequence);
            sd.ackPositions.add(tx, subscriptionKey, sequences);
        } else {
            sequences.add(messageSequence);
            sd.ackPositions.put(tx, subscriptionKey, sequences);
        }

        Long count = sd.messageReferences.get(messageSequence);
        if (count == null) {
            count = Long.valueOf(0L);
        }
        count = count.longValue() + 1;
        sd.messageReferences.put(messageSequence, count);
    }

    // new sub is interested in potentially all existing messages
    private void addAckLocationForRetroactiveSub(Transaction tx, StoredDestination sd, String subscriptionKey) throws IOException {
        SequenceSet allOutstanding = new SequenceSet();
        Iterator<Map.Entry<String, SequenceSet>> iterator = sd.ackPositions.iterator(tx);
        while (iterator.hasNext()) {
            SequenceSet set = iterator.next().getValue();
            for (Long entry : set) {
                allOutstanding.add(entry);
            }
        }
        sd.ackPositions.put(tx, subscriptionKey, allOutstanding);

        for (Long ackPosition : allOutstanding) {
            Long count = sd.messageReferences.get(ackPosition);

            // There might not be a reference if the ackLocation was the last
            // one which is a placeholder for the next incoming message and
            // no value was added to the message references table.
            if (count != null) {
                count = count.longValue() + 1;
                sd.messageReferences.put(ackPosition, count);
            }
        }
    }

    // on a new message add, all existing subs are interested in this message
    private void addAckLocationForNewMessage(Transaction tx, KahaDestination kahaDest,
            StoredDestination sd, Long messageSequence) throws IOException {
        for(String subscriptionKey : sd.subscriptionCache) {
            SequenceSet sequences = sd.ackPositions.get(tx, subscriptionKey);
            if (sequences == null) {
                sequences = new SequenceSet();
                sequences.add(new Sequence(messageSequence, messageSequence + 1));
                sd.ackPositions.add(tx, subscriptionKey, sequences);
            } else {
                sequences.add(new Sequence(messageSequence, messageSequence + 1));
                sd.ackPositions.put(tx, subscriptionKey, sequences);
            }

            MessageKeys key = sd.orderIndex.get(tx, messageSequence);
            incrementAndAddSizeToStoreStat(kahaDest, subscriptionKey,
                    key.location.getSize());

            Long count = sd.messageReferences.get(messageSequence);
            if (count == null) {
                count = Long.valueOf(0L);
            }
            count = count.longValue() + 1;
            sd.messageReferences.put(messageSequence, count);
            sd.messageReferences.put(messageSequence + 1, Long.valueOf(0L));
        }
    }

    private void removeAckLocationsForSub(KahaSubscriptionCommand command,
            Transaction tx, StoredDestination sd, String subscriptionKey) throws IOException {
        if (!sd.ackPositions.isEmpty(tx)) {
            SequenceSet sequences = sd.ackPositions.remove(tx, subscriptionKey);
            if (sequences == null || sequences.isEmpty()) {
                return;
            }

            ArrayList<Long> unreferenced = new ArrayList<>();

            for(Long sequenceId : sequences) {
                Long references = sd.messageReferences.get(sequenceId);
                if (references != null) {
                    references = references.longValue() - 1;

                    if (references.longValue() > 0) {
                        sd.messageReferences.put(sequenceId, references);
                    } else {
                        sd.messageReferences.remove(sequenceId);
                        unreferenced.add(sequenceId);
                    }
                }
            }

            for(Long sequenceId : unreferenced) {
                // Find all the entries that need to get deleted.
                ArrayList<Entry<Long, MessageKeys>> deletes = new ArrayList<>();
                sd.orderIndex.getDeleteList(tx, deletes, sequenceId);

                // Do the actual deletes.
                for (Entry<Long, MessageKeys> entry : deletes) {
                    sd.locationIndex.remove(tx, entry.getValue().location);
                    sd.messageIdIndex.remove(tx, entry.getValue().messageId);
                    sd.orderIndex.remove(tx, entry.getKey());
                    decrementAndSubSizeToStoreStat(command.getDestination(), entry.getValue().location.getSize());
                }
            }
        }
    }

    /**
     * @param tx
     * @param sd
     * @param subscriptionKey
     * @param messageSequence
     * @throws IOException
     */
    private void removeAckLocation(KahaRemoveMessageCommand command,
            Transaction tx, StoredDestination sd, String subscriptionKey,
            Long messageSequence) throws IOException {
        // Remove the sub from the previous location set..
        if (messageSequence != null) {
            SequenceSet range = sd.ackPositions.get(tx, subscriptionKey);
            if (range != null && !range.isEmpty()) {
                range.remove(messageSequence);
                if (!range.isEmpty()) {
                    sd.ackPositions.put(tx, subscriptionKey, range);
                } else {
                    sd.ackPositions.remove(tx, subscriptionKey);
                }

                MessageKeys key = sd.orderIndex.get(tx, messageSequence);
                decrementAndSubSizeToStoreStat(command.getDestination(), subscriptionKey,
                        key.location.getSize());

                // Check if the message is reference by any other subscription.
                Long count = sd.messageReferences.get(messageSequence);
                if (count != null) {
                    long references = count.longValue() - 1;
                    if (references > 0) {
                        sd.messageReferences.put(messageSequence, Long.valueOf(references));
                        return;
                    } else {
                        sd.messageReferences.remove(messageSequence);
                    }
                }

                // Find all the entries that need to get deleted.
                ArrayList<Entry<Long, MessageKeys>> deletes = new ArrayList<>();
                sd.orderIndex.getDeleteList(tx, deletes, messageSequence);

                // Do the actual deletes.
                for (Entry<Long, MessageKeys> entry : deletes) {
                    sd.locationIndex.remove(tx, entry.getValue().location);
                    sd.messageIdIndex.remove(tx, entry.getValue().messageId);
                    sd.orderIndex.remove(tx, entry.getKey());
                    decrementAndSubSizeToStoreStat(command.getDestination(), entry.getValue().location.getSize());
                }
            }
        }
    }

    public LastAck getLastAck(Transaction tx, StoredDestination sd, String subscriptionKey) throws IOException {
        return sd.subscriptionAcks.get(tx, subscriptionKey);
    }

    protected long getStoredMessageCount(Transaction tx, StoredDestination sd, String subscriptionKey) throws IOException {
        if (sd.ackPositions != null) {
            SequenceSet messageSequences = sd.ackPositions.get(tx, subscriptionKey);
            if (messageSequences != null) {
                long result = messageSequences.rangeSize();
                // if there's anything in the range the last value is always the nextMessage marker, so remove 1.
                return result > 0 ? result - 1 : 0;
            }
        }

        return 0;
    }

    protected long getStoredMessageSize(Transaction tx, StoredDestination sd, String subscriptionKey) throws IOException {
        long locationSize = 0;

        if (sd.ackPositions != null) {
            //grab the messages attached to this subscription
            SequenceSet messageSequences = sd.ackPositions.get(tx, subscriptionKey);

            if (messageSequences != null) {
                Sequence head = messageSequences.getHead();
                if (head != null) {
                    //get an iterator over the order index starting at the first unacked message
                    //and go over each message to add up the size
                    Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx,
                            new MessageOrderCursor(head.getFirst()));

                    while (iterator.hasNext()) {
                        Entry<Long, MessageKeys> entry = iterator.next();
                        locationSize += entry.getValue().location.getSize();
                    }
                }
            }
        }

        return locationSize;
    }

    protected String key(KahaDestination destination) {
        return destination.getType().getNumber() + ":" + destination.getName();
    }

    // /////////////////////////////////////////////////////////////////
    // Transaction related implementation methods.
    // /////////////////////////////////////////////////////////////////
    @SuppressWarnings("rawtypes")
    private final LinkedHashMap<TransactionId, List<Operation>> inflightTransactions = new LinkedHashMap<>();
    @SuppressWarnings("rawtypes")
    protected final LinkedHashMap<TransactionId, List<Operation>> preparedTransactions = new LinkedHashMap<>();
    protected final Set<String> ackedAndPrepared = new HashSet<>();
    protected final Set<String> rolledBackAcks = new HashSet<>();

    // messages that have prepared (pending) acks cannot be re-dispatched unless the outcome is rollback,
    // till then they are skipped by the store.
    // 'at most once' XA guarantee
    public void trackRecoveredAcks(ArrayList<MessageAck> acks) {
        this.indexLock.writeLock().lock();
        try {
            for (MessageAck ack : acks) {
                ackedAndPrepared.add(ack.getLastMessageId().toProducerKey());
            }
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    public void forgetRecoveredAcks(ArrayList<MessageAck> acks, boolean rollback) throws IOException {
        if (acks != null) {
            this.indexLock.writeLock().lock();
            try {
                for (MessageAck ack : acks) {
                    final String id = ack.getLastMessageId().toProducerKey();
                    ackedAndPrepared.remove(id);
                    if (rollback) {
                        rolledBackAcks.add(id);
                    }
                }
            } finally {
                this.indexLock.writeLock().unlock();
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private List<Operation> getInflightTx(KahaTransactionInfo info) {
        TransactionId key = TransactionIdConversion.convert(info);
        List<Operation> tx;
        synchronized (inflightTransactions) {
            tx = inflightTransactions.get(key);
            if (tx == null) {
                tx = Collections.synchronizedList(new ArrayList<Operation>());
                inflightTransactions.put(key, tx);
            }
        }
        return tx;
    }

    @SuppressWarnings("unused")
    private TransactionId key(KahaTransactionInfo transactionInfo) {
        return TransactionIdConversion.convert(transactionInfo);
    }

    abstract class Operation <T extends JournalCommand<T>> {
        final T command;
        final Location location;

        public Operation(T command, Location location) {
            this.command = command;
            this.location = location;
        }

        public Location getLocation() {
            return location;
        }

        public T getCommand() {
            return command;
        }

        abstract public void execute(Transaction tx) throws IOException;
    }

    class AddOperation extends Operation<KahaAddMessageCommand> {
        final IndexAware runWithIndexLock;
        public AddOperation(KahaAddMessageCommand command, Location location, IndexAware runWithIndexLock) {
            super(command, location);
            this.runWithIndexLock = runWithIndexLock;
        }

        @Override
        public void execute(Transaction tx) throws IOException {
            long seq = updateIndex(tx, command, location);
            if (runWithIndexLock != null) {
                runWithIndexLock.sequenceAssignedWithIndexLocked(seq);
            }
        }
    }

    class RemoveOperation extends Operation<KahaRemoveMessageCommand> {

        public RemoveOperation(KahaRemoveMessageCommand command, Location location) {
            super(command, location);
        }

        @Override
        public void execute(Transaction tx) throws IOException {
            updateIndex(tx, command, location);
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Initialization related implementation methods.
    // /////////////////////////////////////////////////////////////////

    private PageFile createPageFile() throws IOException {
        if (indexDirectory == null) {
            indexDirectory = directory;
        }
        IOHelper.mkdirs(indexDirectory);
        PageFile index = new PageFile(indexDirectory, "db");
        index.setEnableWriteThread(isEnableIndexWriteAsync());
        index.setWriteBatchSize(getIndexWriteBatchSize());
        index.setPageCacheSize(indexCacheSize);
        index.setUseLFRUEviction(isUseIndexLFRUEviction());
        index.setLFUEvictionFactor(getIndexLFUEvictionFactor());
        index.setEnableDiskSyncs(isEnableIndexDiskSyncs());
        index.setEnableRecoveryFile(isEnableIndexRecoveryFile());
        index.setEnablePageCaching(isEnableIndexPageCaching());
        return index;
    }

    protected Journal createJournal() throws IOException {
        Journal manager = new Journal();
        manager.setDirectory(directory);
        manager.setMaxFileLength(getJournalMaxFileLength());
        manager.setCheckForCorruptionOnStartup(checkForCorruptJournalFiles);
        manager.setChecksum(checksumJournalFiles || checkForCorruptJournalFiles);
        manager.setWriteBatchSize(getJournalMaxWriteBatchSize());
        manager.setArchiveDataLogs(isArchiveDataLogs());
        manager.setSizeAccumulator(journalSize);
        manager.setEnableAsyncDiskSync(isEnableJournalDiskSyncs());
        manager.setPreallocationScope(Journal.PreallocationScope.valueOf(preallocationScope.trim().toUpperCase()));
        manager.setPreallocationStrategy(
                Journal.PreallocationStrategy.valueOf(preallocationStrategy.trim().toUpperCase()));
        manager.setJournalDiskSyncStrategy(journalDiskSyncStrategy);
        if (getDirectoryArchive() != null) {
            IOHelper.mkdirs(getDirectoryArchive());
            manager.setDirectoryArchive(getDirectoryArchive());
        }
        return manager;
    }

    private Metadata createMetadata() {
        Metadata md = new Metadata();
        md.producerSequenceIdTracker.setAuditDepth(getFailoverProducersAuditDepth());
        md.producerSequenceIdTracker.setMaximumNumberOfProducersToTrack(getMaxFailoverProducersToTrack());
        return md;
    }

    protected abstract void configureMetadata();

    public int getJournalMaxWriteBatchSize() {
        return journalMaxWriteBatchSize;
    }

    public void setJournalMaxWriteBatchSize(int journalMaxWriteBatchSize) {
        this.journalMaxWriteBatchSize = journalMaxWriteBatchSize;
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public boolean isDeleteAllMessages() {
        return deleteAllMessages;
    }

    public void setDeleteAllMessages(boolean deleteAllMessages) {
        this.deleteAllMessages = deleteAllMessages;
    }

    public void setIndexWriteBatchSize(int setIndexWriteBatchSize) {
        this.setIndexWriteBatchSize = setIndexWriteBatchSize;
    }

    public int getIndexWriteBatchSize() {
        return setIndexWriteBatchSize;
    }

    public void setEnableIndexWriteAsync(boolean enableIndexWriteAsync) {
        this.enableIndexWriteAsync = enableIndexWriteAsync;
    }

    boolean isEnableIndexWriteAsync() {
        return enableIndexWriteAsync;
    }

    /**
     * @deprecated use {@link #getJournalDiskSyncStrategyEnum} or {@link #getJournalDiskSyncStrategy} instead
     * @return
     */
    @Deprecated
    public boolean isEnableJournalDiskSyncs() {
        return journalDiskSyncStrategy == JournalDiskSyncStrategy.ALWAYS;
    }

    /**
     * @deprecated use {@link #setEnableJournalDiskSyncs} instead
     * @param syncWrites
     */
    @Deprecated
    public void setEnableJournalDiskSyncs(boolean syncWrites) {
        if (syncWrites) {
            journalDiskSyncStrategy = JournalDiskSyncStrategy.ALWAYS;
        } else {
            journalDiskSyncStrategy = JournalDiskSyncStrategy.NEVER;
        }
    }

    public JournalDiskSyncStrategy getJournalDiskSyncStrategyEnum() {
        return journalDiskSyncStrategy;
    }

    public String getJournalDiskSyncStrategy() {
        return journalDiskSyncStrategy.name();
    }

    public void setJournalDiskSyncStrategy(String journalDiskSyncStrategy) {
        this.journalDiskSyncStrategy = JournalDiskSyncStrategy.valueOf(journalDiskSyncStrategy.trim().toUpperCase());
    }

    public long getJournalDiskSyncInterval() {
        return journalDiskSyncInterval;
    }

    public void setJournalDiskSyncInterval(long journalDiskSyncInterval) {
        this.journalDiskSyncInterval = journalDiskSyncInterval;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public long getCleanupInterval() {
        return cleanupInterval;
    }

    public void setCleanupInterval(long cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    public void setJournalMaxFileLength(int journalMaxFileLength) {
        this.journalMaxFileLength = journalMaxFileLength;
    }

    public int getJournalMaxFileLength() {
        return journalMaxFileLength;
    }

    public void setMaxFailoverProducersToTrack(int maxFailoverProducersToTrack) {
        this.metadata.producerSequenceIdTracker.setMaximumNumberOfProducersToTrack(maxFailoverProducersToTrack);
    }

    public int getMaxFailoverProducersToTrack() {
        return this.metadata.producerSequenceIdTracker.getMaximumNumberOfProducersToTrack();
    }

    public void setFailoverProducersAuditDepth(int failoverProducersAuditDepth) {
        this.metadata.producerSequenceIdTracker.setAuditDepth(failoverProducersAuditDepth);
    }

    public int getFailoverProducersAuditDepth() {
        return this.metadata.producerSequenceIdTracker.getAuditDepth();
    }

    public PageFile getPageFile() throws IOException {
        if (pageFile == null) {
            pageFile = createPageFile();
        }
        return pageFile;
    }

    public Journal getJournal() throws IOException {
        if (journal == null) {
            journal = createJournal();
        }
        return journal;
    }

    protected Metadata getMetadata() {
        return metadata;
    }

    public boolean isFailIfDatabaseIsLocked() {
        return failIfDatabaseIsLocked;
    }

    public void setFailIfDatabaseIsLocked(boolean failIfDatabaseIsLocked) {
        this.failIfDatabaseIsLocked = failIfDatabaseIsLocked;
    }

    public boolean isIgnoreMissingJournalfiles() {
        return ignoreMissingJournalfiles;
    }

    public void setIgnoreMissingJournalfiles(boolean ignoreMissingJournalfiles) {
        this.ignoreMissingJournalfiles = ignoreMissingJournalfiles;
    }

    public int getIndexCacheSize() {
        return indexCacheSize;
    }

    public void setIndexCacheSize(int indexCacheSize) {
        this.indexCacheSize = indexCacheSize;
    }

    public boolean isCheckForCorruptJournalFiles() {
        return checkForCorruptJournalFiles;
    }

    public void setCheckForCorruptJournalFiles(boolean checkForCorruptJournalFiles) {
        this.checkForCorruptJournalFiles = checkForCorruptJournalFiles;
    }

    public boolean isChecksumJournalFiles() {
        return checksumJournalFiles;
    }

    public void setChecksumJournalFiles(boolean checksumJournalFiles) {
        this.checksumJournalFiles = checksumJournalFiles;
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    /**
     * @return the archiveDataLogs
     */
    public boolean isArchiveDataLogs() {
        return this.archiveDataLogs;
    }

    /**
     * @param archiveDataLogs the archiveDataLogs to set
     */
    public void setArchiveDataLogs(boolean archiveDataLogs) {
        this.archiveDataLogs = archiveDataLogs;
    }

    /**
     * @return the directoryArchive
     */
    public File getDirectoryArchive() {
        return this.directoryArchive;
    }

    /**
     * @param directoryArchive the directoryArchive to set
     */
    public void setDirectoryArchive(File directoryArchive) {
        this.directoryArchive = directoryArchive;
    }

    public boolean isArchiveCorruptedIndex() {
        return archiveCorruptedIndex;
    }

    public void setArchiveCorruptedIndex(boolean archiveCorruptedIndex) {
        this.archiveCorruptedIndex = archiveCorruptedIndex;
    }

    public float getIndexLFUEvictionFactor() {
        return indexLFUEvictionFactor;
    }

    public void setIndexLFUEvictionFactor(float indexLFUEvictionFactor) {
        this.indexLFUEvictionFactor = indexLFUEvictionFactor;
    }

    public boolean isUseIndexLFRUEviction() {
        return useIndexLFRUEviction;
    }

    public void setUseIndexLFRUEviction(boolean useIndexLFRUEviction) {
        this.useIndexLFRUEviction = useIndexLFRUEviction;
    }

    public void setEnableIndexDiskSyncs(boolean enableIndexDiskSyncs) {
        this.enableIndexDiskSyncs = enableIndexDiskSyncs;
    }

    public void setEnableIndexRecoveryFile(boolean enableIndexRecoveryFile) {
        this.enableIndexRecoveryFile = enableIndexRecoveryFile;
    }

    public void setEnableIndexPageCaching(boolean enableIndexPageCaching) {
        this.enableIndexPageCaching = enableIndexPageCaching;
    }

    public boolean isEnableIndexDiskSyncs() {
        return enableIndexDiskSyncs;
    }

    public boolean isEnableIndexRecoveryFile() {
        return enableIndexRecoveryFile;
    }

    public boolean isEnableIndexPageCaching() {
        return enableIndexPageCaching;
    }

    // /////////////////////////////////////////////////////////////////
    // Internal conversion methods.
    // /////////////////////////////////////////////////////////////////

    class MessageOrderCursor{
        long defaultCursorPosition;
        long lowPriorityCursorPosition;
        long highPriorityCursorPosition;
        MessageOrderCursor(){
        }

        MessageOrderCursor(long position){
            this.defaultCursorPosition=position;
            this.lowPriorityCursorPosition=position;
            this.highPriorityCursorPosition=position;
        }

        MessageOrderCursor(MessageOrderCursor other){
            this.defaultCursorPosition=other.defaultCursorPosition;
            this.lowPriorityCursorPosition=other.lowPriorityCursorPosition;
            this.highPriorityCursorPosition=other.highPriorityCursorPosition;
        }

        MessageOrderCursor copy() {
            return new MessageOrderCursor(this);
        }

        void reset() {
            this.defaultCursorPosition=0;
            this.highPriorityCursorPosition=0;
            this.lowPriorityCursorPosition=0;
        }

        void increment() {
            if (defaultCursorPosition!=0) {
                defaultCursorPosition++;
            }
            if (highPriorityCursorPosition!=0) {
                highPriorityCursorPosition++;
            }
            if (lowPriorityCursorPosition!=0) {
                lowPriorityCursorPosition++;
            }
        }

        @Override
        public String toString() {
           return "MessageOrderCursor:[def:" + defaultCursorPosition
                   + ", low:" + lowPriorityCursorPosition
                   + ", high:" +  highPriorityCursorPosition + "]";
        }

        public void sync(MessageOrderCursor other) {
            this.defaultCursorPosition=other.defaultCursorPosition;
            this.lowPriorityCursorPosition=other.lowPriorityCursorPosition;
            this.highPriorityCursorPosition=other.highPriorityCursorPosition;
        }
    }

    class MessageOrderIndex {
        static final byte HI = 9;
        static final byte LO = 0;
        static final byte DEF = 4;

        long nextMessageId;
        BTreeIndex<Long, MessageKeys> defaultPriorityIndex;
        BTreeIndex<Long, MessageKeys> lowPriorityIndex;
        BTreeIndex<Long, MessageKeys> highPriorityIndex;
        final MessageOrderCursor cursor = new MessageOrderCursor();
        Long lastDefaultKey;
        Long lastHighKey;
        Long lastLowKey;
        byte lastGetPriority;
        final List<Long> pendingAdditions = new LinkedList<>();
        final MessageKeysMarshaller messageKeysMarshaller = new MessageKeysMarshaller();

        MessageKeys remove(Transaction tx, Long key) throws IOException {
            MessageKeys result = defaultPriorityIndex.remove(tx, key);
            if (result == null && highPriorityIndex!=null) {
                result = highPriorityIndex.remove(tx, key);
                if (result ==null && lowPriorityIndex!=null) {
                    result = lowPriorityIndex.remove(tx, key);
                }
            }
            return result;
        }

        void load(Transaction tx) throws IOException {
            defaultPriorityIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
            defaultPriorityIndex.setValueMarshaller(messageKeysMarshaller);
            defaultPriorityIndex.load(tx);
            lowPriorityIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
            lowPriorityIndex.setValueMarshaller(messageKeysMarshaller);
            lowPriorityIndex.load(tx);
            highPriorityIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
            highPriorityIndex.setValueMarshaller(messageKeysMarshaller);
            highPriorityIndex.load(tx);
        }

        void allocate(Transaction tx) throws IOException {
            defaultPriorityIndex = new BTreeIndex<>(pageFile, tx.allocate());
            if (metadata.version >= 2) {
                lowPriorityIndex = new BTreeIndex<>(pageFile, tx.allocate());
                highPriorityIndex = new BTreeIndex<>(pageFile, tx.allocate());
            }
        }

        void configureLast(Transaction tx) throws IOException {
            // Figure out the next key using the last entry in the destination.
            TreeSet<Long> orderedSet = new TreeSet<>();

            addLast(orderedSet, highPriorityIndex, tx);
            addLast(orderedSet, defaultPriorityIndex, tx);
            addLast(orderedSet, lowPriorityIndex, tx);

            if (!orderedSet.isEmpty()) {
                nextMessageId = orderedSet.last() + 1;
            }
        }

        private void addLast(TreeSet<Long> orderedSet, BTreeIndex<Long, MessageKeys> index, Transaction tx) throws IOException {
            if (index != null) {
                Entry<Long, MessageKeys> lastEntry = index.getLast(tx);
                if (lastEntry != null) {
                    orderedSet.add(lastEntry.getKey());
                }
            }
        }

        void clear(Transaction tx) throws IOException {
            this.remove(tx);
            this.resetCursorPosition();
            this.allocate(tx);
            this.load(tx);
            this.configureLast(tx);
        }

        void remove(Transaction tx) throws IOException {
            defaultPriorityIndex.clear(tx);
            defaultPriorityIndex.unload(tx);
            tx.free(defaultPriorityIndex.getPageId());
            if (lowPriorityIndex != null) {
                lowPriorityIndex.clear(tx);
                lowPriorityIndex.unload(tx);

                tx.free(lowPriorityIndex.getPageId());
            }
            if (highPriorityIndex != null) {
                highPriorityIndex.clear(tx);
                highPriorityIndex.unload(tx);
                tx.free(highPriorityIndex.getPageId());
            }
        }

        void resetCursorPosition() {
            this.cursor.reset();
            lastDefaultKey = null;
            lastHighKey = null;
            lastLowKey = null;
        }

        void setBatch(Transaction tx, Long sequence) throws IOException {
            if (sequence != null) {
                Long nextPosition = new Long(sequence.longValue() + 1);
                lastDefaultKey = sequence;
                cursor.defaultCursorPosition = nextPosition.longValue();
                lastHighKey = sequence;
                cursor.highPriorityCursorPosition = nextPosition.longValue();
                lastLowKey = sequence;
                cursor.lowPriorityCursorPosition = nextPosition.longValue();
            }
        }

        void setBatch(Transaction tx, LastAck last) throws IOException {
            setBatch(tx, last.lastAckedSequence);
            if (cursor.defaultCursorPosition == 0
                    && cursor.highPriorityCursorPosition == 0
                    && cursor.lowPriorityCursorPosition == 0) {
                long next = last.lastAckedSequence + 1;
                switch (last.priority) {
                    case DEF:
                        cursor.defaultCursorPosition = next;
                        cursor.highPriorityCursorPosition = next;
                        break;
                    case HI:
                        cursor.highPriorityCursorPosition = next;
                        break;
                    case LO:
                        cursor.lowPriorityCursorPosition = next;
                        cursor.defaultCursorPosition = next;
                        cursor.highPriorityCursorPosition = next;
                        break;
                }
            }
        }

        void stoppedIterating() {
            if (lastDefaultKey!=null) {
                cursor.defaultCursorPosition=lastDefaultKey.longValue()+1;
            }
            if (lastHighKey!=null) {
                cursor.highPriorityCursorPosition=lastHighKey.longValue()+1;
            }
            if (lastLowKey!=null) {
                cursor.lowPriorityCursorPosition=lastLowKey.longValue()+1;
            }
            lastDefaultKey = null;
            lastHighKey = null;
            lastLowKey = null;
        }

        void getDeleteList(Transaction tx, ArrayList<Entry<Long, MessageKeys>> deletes, Long sequenceId)
                throws IOException {
            if (defaultPriorityIndex.containsKey(tx, sequenceId)) {
                getDeleteList(tx, deletes, defaultPriorityIndex, sequenceId);
            } else if (highPriorityIndex != null && highPriorityIndex.containsKey(tx, sequenceId)) {
                getDeleteList(tx, deletes, highPriorityIndex, sequenceId);
            } else if (lowPriorityIndex != null && lowPriorityIndex.containsKey(tx, sequenceId)) {
                getDeleteList(tx, deletes, lowPriorityIndex, sequenceId);
            }
        }

        void getDeleteList(Transaction tx, ArrayList<Entry<Long, MessageKeys>> deletes,
                BTreeIndex<Long, MessageKeys> index, Long sequenceId) throws IOException {

            Iterator<Entry<Long, MessageKeys>> iterator = index.iterator(tx, sequenceId, null);
            deletes.add(iterator.next());
        }

        long getNextMessageId() {
            return nextMessageId++;
        }

        void revertNextMessageId() {
            nextMessageId--;
        }

        MessageKeys get(Transaction tx, Long key) throws IOException {
            MessageKeys result = defaultPriorityIndex.get(tx, key);
            if (result == null) {
                result = highPriorityIndex.get(tx, key);
                if (result == null) {
                    result = lowPriorityIndex.get(tx, key);
                    lastGetPriority = LO;
                } else {
                    lastGetPriority = HI;
                }
            } else {
                lastGetPriority = DEF;
            }
            return result;
        }

        MessageKeys put(Transaction tx, int priority, Long key, MessageKeys value) throws IOException {
            if (priority == javax.jms.Message.DEFAULT_PRIORITY) {
                return defaultPriorityIndex.put(tx, key, value);
            } else if (priority > javax.jms.Message.DEFAULT_PRIORITY) {
                return highPriorityIndex.put(tx, key, value);
            } else {
                return lowPriorityIndex.put(tx, key, value);
            }
        }

        Iterator<Entry<Long, MessageKeys>> iterator(Transaction tx) throws IOException{
            return new MessageOrderIterator(tx,cursor,this);
        }

        Iterator<Entry<Long, MessageKeys>> iterator(Transaction tx, MessageOrderCursor m) throws IOException{
            return new MessageOrderIterator(tx,m,this);
        }

        public byte lastGetPriority() {
            return lastGetPriority;
        }

        public boolean alreadyDispatched(Long sequence) {
            return (cursor.highPriorityCursorPosition > 0 && cursor.highPriorityCursorPosition >= sequence) ||
                    (cursor.defaultCursorPosition > 0 && cursor.defaultCursorPosition >= sequence) ||
                    (cursor.lowPriorityCursorPosition > 0 && cursor.lowPriorityCursorPosition >= sequence);
        }

        public void trackPendingAdd(Long seq) {
            synchronized (pendingAdditions) {
                pendingAdditions.add(seq);
            }
        }

        public void trackPendingAddComplete(Long seq) {
            synchronized (pendingAdditions) {
                pendingAdditions.remove(seq);
            }
        }

        public Long minPendingAdd() {
            synchronized (pendingAdditions) {
                if (!pendingAdditions.isEmpty()) {
                    return pendingAdditions.get(0);
                } else {
                    return null;
                }
            }
        }

        class MessageOrderIterator implements Iterator<Entry<Long, MessageKeys>>{
            Iterator<Entry<Long, MessageKeys>>currentIterator;
            final Iterator<Entry<Long, MessageKeys>>highIterator;
            final Iterator<Entry<Long, MessageKeys>>defaultIterator;
            final Iterator<Entry<Long, MessageKeys>>lowIterator;

            MessageOrderIterator(Transaction tx, MessageOrderCursor m, MessageOrderIndex messageOrderIndex) throws IOException {
                Long pendingAddLimiter = messageOrderIndex.minPendingAdd();
                this.defaultIterator = defaultPriorityIndex.iterator(tx, m.defaultCursorPosition, pendingAddLimiter);
                if (highPriorityIndex != null) {
                    this.highIterator = highPriorityIndex.iterator(tx, m.highPriorityCursorPosition, pendingAddLimiter);
                } else {
                    this.highIterator = null;
                }
                if (lowPriorityIndex != null) {
                    this.lowIterator = lowPriorityIndex.iterator(tx, m.lowPriorityCursorPosition, pendingAddLimiter);
                } else {
                    this.lowIterator = null;
                }
            }

            @Override
            public boolean hasNext() {
                if (currentIterator == null) {
                    if (highIterator != null) {
                        if (highIterator.hasNext()) {
                            currentIterator = highIterator;
                            return currentIterator.hasNext();
                        }
                        if (defaultIterator.hasNext()) {
                            currentIterator = defaultIterator;
                            return currentIterator.hasNext();
                        }
                        if (lowIterator.hasNext()) {
                            currentIterator = lowIterator;
                            return currentIterator.hasNext();
                        }
                        return false;
                    } else {
                        currentIterator = defaultIterator;
                        return currentIterator.hasNext();
                    }
                }
                if (highIterator != null) {
                    if (currentIterator.hasNext()) {
                        return true;
                    }
                    if (currentIterator == highIterator) {
                        if (defaultIterator.hasNext()) {
                            currentIterator = defaultIterator;
                            return currentIterator.hasNext();
                        }
                        if (lowIterator.hasNext()) {
                            currentIterator = lowIterator;
                            return currentIterator.hasNext();
                        }
                        return false;
                    }

                    if (currentIterator == defaultIterator) {
                        if (lowIterator.hasNext()) {
                            currentIterator = lowIterator;
                            return currentIterator.hasNext();
                        }
                        return false;
                    }
                }
                return currentIterator.hasNext();
            }

            @Override
            public Entry<Long, MessageKeys> next() {
                Entry<Long, MessageKeys> result = currentIterator.next();
                if (result != null) {
                    Long key = result.getKey();
                    if (highIterator != null) {
                        if (currentIterator == defaultIterator) {
                            lastDefaultKey = key;
                        } else if (currentIterator == highIterator) {
                            lastHighKey = key;
                        } else {
                            lastLowKey = key;
                        }
                    } else {
                        lastDefaultKey = key;
                    }
                }
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static class HashSetStringMarshaller extends VariableMarshaller<HashSet<String>> {
        final static HashSetStringMarshaller INSTANCE = new HashSetStringMarshaller();

        @Override
        public void writePayload(HashSet<String> object, DataOutput dataOut) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oout = new ObjectOutputStream(baos);
            oout.writeObject(object);
            oout.flush();
            oout.close();
            byte[] data = baos.toByteArray();
            dataOut.writeInt(data.length);
            dataOut.write(data);
        }

        @Override
        @SuppressWarnings("unchecked")
        public HashSet<String> readPayload(DataInput dataIn) throws IOException {
            int dataLen = dataIn.readInt();
            byte[] data = new byte[dataLen];
            dataIn.readFully(data);
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ObjectInputStream oin = new ObjectInputStream(bais);
            try {
                return (HashSet<String>) oin.readObject();
            } catch (ClassNotFoundException cfe) {
                IOException ioe = new IOException("Failed to read HashSet<String>: " + cfe);
                ioe.initCause(cfe);
                throw ioe;
            }
        }
    }

    public File getIndexDirectory() {
        return indexDirectory;
    }

    public void setIndexDirectory(File indexDirectory) {
        this.indexDirectory = indexDirectory;
    }

    interface IndexAware {
        public void sequenceAssignedWithIndexLocked(long index);
    }

    public String getPreallocationScope() {
        return preallocationScope;
    }

    public void setPreallocationScope(String preallocationScope) {
        this.preallocationScope = preallocationScope;
    }

    public String getPreallocationStrategy() {
        return preallocationStrategy;
    }

    public void setPreallocationStrategy(String preallocationStrategy) {
        this.preallocationStrategy = preallocationStrategy;
    }

    public int getCompactAcksAfterNoGC() {
        return compactAcksAfterNoGC;
    }

    /**
     * Sets the number of GC cycles where no journal logs were removed before an attempt to
     * move forward all the acks in the last log that contains them and is otherwise unreferenced.
     * <p>
     * A value of -1 will disable this feature.
     *
     * @param compactAcksAfterNoGC
     *      Number of empty GC cycles before we rewrite old ACKS.
     */
    public void setCompactAcksAfterNoGC(int compactAcksAfterNoGC) {
        this.compactAcksAfterNoGC = compactAcksAfterNoGC;
    }

    /**
     * Returns whether Ack compaction will ignore that the store is still growing
     * and run more often.
     *
     * @return the compactAcksIgnoresStoreGrowth current value.
     */
    public boolean isCompactAcksIgnoresStoreGrowth() {
        return compactAcksIgnoresStoreGrowth;
    }

    /**
     * Configure if Ack compaction will occur regardless of continued growth of the
     * journal logs meaning that the store has not run out of space yet.  Because the
     * compaction operation can be costly this value is defaulted to off and the Ack
     * compaction is only done when it seems that the store cannot grow and larger.
     *
     * @param compactAcksIgnoresStoreGrowth the compactAcksIgnoresStoreGrowth to set
     */
    public void setCompactAcksIgnoresStoreGrowth(boolean compactAcksIgnoresStoreGrowth) {
        this.compactAcksIgnoresStoreGrowth = compactAcksIgnoresStoreGrowth;
    }

    /**
     * Returns whether Ack compaction is enabled
     *
     * @return enableAckCompaction
     */
    public boolean isEnableAckCompaction() {
        return enableAckCompaction;
    }

    /**
     * Configure if the Ack compaction task should be enabled to run
     *
     * @param enableAckCompaction
     */
    public void setEnableAckCompaction(boolean enableAckCompaction) {
        this.enableAckCompaction = enableAckCompaction;
    }

    /**
     * @return
     */
    public boolean isEnableSubscriptionStatistics() {
        return enableSubscriptionStatistics;
    }

    /**
     * Enable caching statistics for each subscription to allow non-blocking
     * retrieval of metrics.  This could incur some overhead to compute if there are a lot
     * of subscriptions.
     *
     * @param enableSubscriptionStatistics
     */
    public void setEnableSubscriptionStatistics(boolean enableSubscriptionStatistics) {
        this.enableSubscriptionStatistics = enableSubscriptionStatistics;
    }
}
