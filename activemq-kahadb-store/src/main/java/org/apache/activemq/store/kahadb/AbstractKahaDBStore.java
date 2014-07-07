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

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.broker.LockableServiceSupport;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.store.SharedFileLocker;
import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.data.KahaTraceCommand;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractKahaDBStore extends LockableServiceSupport {

    static final Logger LOG = LoggerFactory.getLogger(AbstractKahaDBStore.class);

    public static final String PROPERTY_LOG_SLOW_ACCESS_TIME = "org.apache.activemq.store.kahadb.LOG_SLOW_ACCESS_TIME";
    public static final int LOG_SLOW_ACCESS_TIME = Integer.getInteger(PROPERTY_LOG_SLOW_ACCESS_TIME, 0);

    protected File directory;
    protected PageFile pageFile;
    protected Journal journal;
    protected AtomicLong journalSize = new AtomicLong(0);
    protected boolean failIfDatabaseIsLocked;
    protected long checkpointInterval = 5*1000;
    protected long cleanupInterval = 30*1000;
    protected boolean checkForCorruptJournalFiles = false;
    protected boolean checksumJournalFiles = true;
    protected boolean forceRecoverIndex = false;
    protected int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    protected int journalMaxWriteBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    protected boolean archiveCorruptedIndex = false;
    protected boolean enableIndexWriteAsync = false;
    protected boolean enableJournalDiskSyncs = false;
    protected boolean deleteAllJobs = false;
    protected int indexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE;
    protected boolean useIndexLFRUEviction = false;
    protected float indexLFUEvictionFactor = 0.2f;
    protected boolean ignoreMissingJournalfiles = false;
    protected int indexCacheSize = 1000;
    protected boolean enableIndexDiskSyncs = true;
    protected boolean enableIndexRecoveryFile = true;
    protected boolean enableIndexPageCaching = true;
    protected boolean archiveDataLogs;
    protected boolean purgeStoreOnStartup;
    protected File directoryArchive;

    protected AtomicBoolean opened = new AtomicBoolean();
    protected Thread checkpointThread;
    protected final Object checkpointThreadLock = new Object();
    protected ReentrantReadWriteLock checkpointLock = new ReentrantReadWriteLock();
    protected ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();

    /**
     * @return the name to give this store's PageFile instance.
     */
    protected abstract String getPageFileName();

    /**
     * @return the location of the data directory if no set by configuration.
     */
    protected abstract File getDefaultDataDirectory();

    /**
     * Loads the store from disk.
     *
     * Based on configuration this method can either load an existing store or it can purge
     * an existing store and start in a clean state.
     *
     * @throws IOException if an error occurs during the load.
     */
    public abstract void load() throws IOException;

    /**
     * Unload the state of the Store to disk and shuts down all resources assigned to this
     * KahaDB store implementation.
     *
     * @throws IOException if an error occurs during the store unload.
     */
    public abstract void unload() throws IOException;

    @Override
    protected void doStart() throws Exception {
        this.indexLock.writeLock().lock();
        if (getDirectory() == null) {
            setDirectory(getDefaultDataDirectory());
        }
        IOHelper.mkdirs(getDirectory());
        try {
            if (isPurgeStoreOnStartup()) {
                getJournal().start();
                getJournal().delete();
                getJournal().close();
                journal = null;
                getPageFile().delete();
                LOG.info("{} Persistence store purged.", this);
                setPurgeStoreOnStartup(false);
            }

            load();
            store(new KahaTraceCommand().setMessage("LOADED " + new Date()));
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        unload();
    }

    public PageFile getPageFile() {
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

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public boolean isArchiveCorruptedIndex() {
        return archiveCorruptedIndex;
    }

    public void setArchiveCorruptedIndex(boolean archiveCorruptedIndex) {
        this.archiveCorruptedIndex = archiveCorruptedIndex;
    }

    public boolean isFailIfDatabaseIsLocked() {
        return failIfDatabaseIsLocked;
    }

    public void setFailIfDatabaseIsLocked(boolean failIfDatabaseIsLocked) {
        this.failIfDatabaseIsLocked = failIfDatabaseIsLocked;
    }

    public boolean isCheckForCorruptJournalFiles() {
        return checkForCorruptJournalFiles;
    }

    public void setCheckForCorruptJournalFiles(boolean checkForCorruptJournalFiles) {
        this.checkForCorruptJournalFiles = checkForCorruptJournalFiles;
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

    public boolean isChecksumJournalFiles() {
        return checksumJournalFiles;
    }

    public void setChecksumJournalFiles(boolean checksumJournalFiles) {
        this.checksumJournalFiles = checksumJournalFiles;
    }

    public boolean isForceRecoverIndex() {
        return forceRecoverIndex;
    }

    public void setForceRecoverIndex(boolean forceRecoverIndex) {
        this.forceRecoverIndex = forceRecoverIndex;
    }

    public int getJournalMaxFileLength() {
        return journalMaxFileLength;
    }

    public void setJournalMaxFileLength(int journalMaxFileLength) {
        this.journalMaxFileLength = journalMaxFileLength;
    }

    public int getJournalMaxWriteBatchSize() {
        return journalMaxWriteBatchSize;
    }

    public void setJournalMaxWriteBatchSize(int journalMaxWriteBatchSize) {
        this.journalMaxWriteBatchSize = journalMaxWriteBatchSize;
    }

    public boolean isEnableIndexWriteAsync() {
        return enableIndexWriteAsync;
    }

    public void setEnableIndexWriteAsync(boolean enableIndexWriteAsync) {
        this.enableIndexWriteAsync = enableIndexWriteAsync;
    }

    public boolean isEnableJournalDiskSyncs() {
        return enableJournalDiskSyncs;
    }

    public void setEnableJournalDiskSyncs(boolean syncWrites) {
        this.enableJournalDiskSyncs = syncWrites;
    }

    public boolean isDeleteAllJobs() {
        return deleteAllJobs;
    }

    public void setDeleteAllJobs(boolean deleteAllJobs) {
        this.deleteAllJobs = deleteAllJobs;
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

    public int getIndexCacheSize() {
        return indexCacheSize;
    }

    public void setIndexCacheSize(int indexCacheSize) {
        this.indexCacheSize = indexCacheSize;
    }

    public int getIndexWriteBatchSize() {
        return indexWriteBatchSize;
    }

    public void setIndexWriteBatchSize(int indexWriteBatchSize) {
        this.indexWriteBatchSize = indexWriteBatchSize;
    }

    public boolean isUseIndexLFRUEviction() {
        return useIndexLFRUEviction;
    }

    public void setUseIndexLFRUEviction(boolean useIndexLFRUEviction) {
        this.useIndexLFRUEviction = useIndexLFRUEviction;
    }

    public float getIndexLFUEvictionFactor() {
        return indexLFUEvictionFactor;
    }

    public void setIndexLFUEvictionFactor(float indexLFUEvictionFactor) {
        this.indexLFUEvictionFactor = indexLFUEvictionFactor;
    }

    public boolean isEnableIndexDiskSyncs() {
        return enableIndexDiskSyncs;
    }

    public void setEnableIndexDiskSyncs(boolean enableIndexDiskSyncs) {
        this.enableIndexDiskSyncs = enableIndexDiskSyncs;
    }

    public boolean isEnableIndexRecoveryFile() {
        return enableIndexRecoveryFile;
    }

    public void setEnableIndexRecoveryFile(boolean enableIndexRecoveryFile) {
        this.enableIndexRecoveryFile = enableIndexRecoveryFile;
    }

    public boolean isEnableIndexPageCaching() {
        return enableIndexPageCaching;
    }

    public void setEnableIndexPageCaching(boolean enableIndexPageCaching) {
        this.enableIndexPageCaching = enableIndexPageCaching;
    }

    public boolean isPurgeStoreOnStartup() {
        return this.purgeStoreOnStartup;
    }

    public void setPurgeStoreOnStartup(boolean purge) {
        this.purgeStoreOnStartup = purge;
    }

    public boolean isIgnoreMissingJournalfiles() {
        return ignoreMissingJournalfiles;
    }

    public void setIgnoreMissingJournalfiles(boolean ignoreMissingJournalfiles) {
        this.ignoreMissingJournalfiles = ignoreMissingJournalfiles;
    }

    public long size() {
        if (!isStarted()) {
            return 0;
        }
        try {
            return journalSize.get() + pageFile.getDiskSize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Locker createDefaultLocker() throws IOException {
        SharedFileLocker locker = new SharedFileLocker();
        locker.setDirectory(this.getDirectory());
        return locker;
    }

    @Override
    public void init() throws Exception {
    }

    /**
     * Store a command in the Journal and process to update the Store index.
     *
     * @param command
     *      The specific JournalCommand to store and process.
     *
     * @returns the Location where the data was written in the Journal.
     *
     * @throws IOException if an error occurs storing or processing the command.
     */
    public Location store(JournalCommand<?> command) throws IOException {
        return store(command, isEnableIndexDiskSyncs(), null, null, null);
    }

    /**
     * Store a command in the Journal and process to update the Store index.
     *
     * @param command
     *      The specific JournalCommand to store and process.
     * @param sync
     *      Should the store operation be done synchronously. (ignored if completion passed).
     *
     * @returns the Location where the data was written in the Journal.
     *
     * @throws IOException if an error occurs storing or processing the command.
     */
    public Location store(JournalCommand<?> command, boolean sync) throws IOException {
        return store(command, sync, null, null, null);
    }

    /**
     * Store a command in the Journal and process to update the Store index.
     *
     * @param command
     *      The specific JournalCommand to store and process.
     * @param onJournalStoreComplete
     *      The Runnable to call when the Journal write operation completes.
     *
     * @returns the Location where the data was written in the Journal.
     *
     * @throws IOException if an error occurs storing or processing the command.
     */
    public Location store(JournalCommand<?> command, Runnable onJournalStoreComplete) throws IOException {
        return store(command, isEnableIndexDiskSyncs(), null, null, onJournalStoreComplete);
    }

    /**
     * Store a command in the Journal and process to update the Store index.
     *
     * @param command
     *      The specific JournalCommand to store and process.
     * @param sync
     *      Should the store operation be done synchronously. (ignored if completion passed).
     * @param before
     *      The Runnable instance to execute before performing the store and process operation.
     * @param after
     *      The Runnable instance to execute after performing the store and process operation.
     *
     * @returns the Location where the data was written in the Journal.
     *
     * @throws IOException if an error occurs storing or processing the command.
     */
    public Location store(JournalCommand<?> command, boolean sync, Runnable before, Runnable after) throws IOException {
        return store(command, sync, before, after, null);
    }

    /**
     * All updated are are funneled through this method. The updates are converted to a
     * JournalMessage which is logged to the journal and then the data from the JournalMessage
     * is used to update the index just like it would be done during a recovery process.
     *
     * @param command
     *      The specific JournalCommand to store and process.
     * @param sync
     *      Should the store operation be done synchronously. (ignored if completion passed).
     * @param before
     *      The Runnable instance to execute before performing the store and process operation.
     * @param after
     *      The Runnable instance to execute after performing the store and process operation.
     * @param onJournalStoreComplete
     *      Callback to be run when the journal write operation is complete.
     *
     * @returns the Location where the data was written in the Journal.
     *
     * @throws IOException if an error occurs storing or processing the command.
     */
    public Location store(JournalCommand<?> command, boolean sync, Runnable before, Runnable after, Runnable onJournalStoreComplete) throws IOException {
        try {

            if (before != null) {
                before.run();
            }

            ByteSequence sequence = toByteSequence(command);
            Location location;
            checkpointLock.readLock().lock();
            try {

                long start = System.currentTimeMillis();
                location = onJournalStoreComplete == null ? journal.write(sequence, sync) :
                                                            journal.write(sequence, onJournalStoreComplete);
                long start2 = System.currentTimeMillis();

                process(command, location);

                long end = System.currentTimeMillis();
                if (LOG_SLOW_ACCESS_TIME > 0 && end - start > LOG_SLOW_ACCESS_TIME) {
                    LOG.info("Slow KahaDB access: Journal append took: {} ms, Index update took {} ms",
                             (start2-start), (end-start2));
                }
            } finally {
                checkpointLock.readLock().unlock();
            }

            if (after != null) {
                after.run();
            }

            if (checkpointThread != null && !checkpointThread.isAlive()) {
                startCheckpoint();
            }
            return location;
        } catch (IOException ioe) {
            LOG.error("KahaDB failed to store to Journal", ioe);
            if (brokerService != null) {
                brokerService.handleIOException(ioe);
            }
            throw ioe;
        }
    }

    /**
     * Loads a previously stored JournalMessage
     *
     * @param location
     *      The location of the journal command to read.
     *
     * @return a new un-marshaled JournalCommand instance.
     *
     * @throws IOException if an error occurs reading the stored command.
     */
    protected JournalCommand<?> load(Location location) throws IOException {
        ByteSequence data = journal.read(location);
        DataByteArrayInputStream is = new DataByteArrayInputStream(data);
        byte readByte = is.readByte();
        KahaEntryType type = KahaEntryType.valueOf(readByte);
        if (type == null) {
            try {
                is.close();
            } catch (IOException e) {
            }
            throw new IOException("Could not load journal record. Invalid location: " + location);
        }
        JournalCommand<?> message = (JournalCommand<?>)type.createMessage();
        message.mergeFramed(is);
        return message;
    }

    /**
     * Process a stored or recovered JournalCommand instance and update the DB Index with the
     * state changes that this command produces.  This can be called either as a new DB operation
     * or as a replay during recovery operations.
     *
     * @param command
     *      The JournalCommand to process.
     * @param location
     *      The location in the Journal where the command was written or read from.
     */
    protected abstract void process(JournalCommand<?> command, Location location) throws IOException;

    /**
     * Perform a checkpoint operation with optional cleanup.
     *
     * Called by the checkpoint background thread periodically to initiate a checkpoint operation
     * and if the cleanup flag is set a cleanup sweep should be done to allow for release of no
     * longer needed journal log files etc.
     *
     * @param cleanup
     *      Should the method do a simple checkpoint or also perform a journal cleanup.
     *
     * @throws IOException if an error occurs during the checkpoint operation.
     */
    protected void checkpointUpdate(final boolean cleanup) throws IOException {
        checkpointLock.writeLock().lock();
        try {
            this.indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        checkpointUpdate(tx, cleanup);
                    }
                });
            } finally {
                this.indexLock.writeLock().unlock();
            }

        } finally {
            checkpointLock.writeLock().unlock();
        }
    }

    /**
     * Perform the checkpoint update operation.  If the cleanup flag is true then the
     * operation should also purge any unused Journal log files.
     *
     * This method must always be called with the checkpoint and index write locks held.
     *
     * @param tx
     *      The TX under which to perform the checkpoint update.
     * @param cleanup
     *      Should the checkpoint also do unused Journal file cleanup.
     *
     * @throws IOException if an error occurs while performing the checkpoint.
     */
    protected abstract void checkpointUpdate(Transaction tx, boolean cleanup) throws IOException;

    /**
     * Creates a new ByteSequence that represents the marshaled form of the given Journal Command.
     *
     * @param command
     *      The Journal Command that should be marshaled to bytes for writing.
     *
     * @return the byte representation of the given journal command.
     *
     * @throws IOException if an error occurs while serializing the command.
     */
    protected ByteSequence toByteSequence(JournalCommand<?> data) throws IOException {
        int size = data.serializedSizeFramed();
        DataByteArrayOutputStream os = new DataByteArrayOutputStream(size + 1);
        os.writeByte(data.type().getNumber());
        data.writeFramed(os);
        return os.toByteSequence();
    }

    /**
     * Create the PageFile instance and configure it using the configuration options
     * currently set.
     *
     * @return the newly created and configured PageFile instance.
     */
    protected PageFile createPageFile() {
        PageFile index = new PageFile(getDirectory(), getPageFileName());
        index.setEnableWriteThread(isEnableIndexWriteAsync());
        index.setWriteBatchSize(getIndexWriteBatchSize());
        index.setPageCacheSize(getIndexCacheSize());
        index.setUseLFRUEviction(isUseIndexLFRUEviction());
        index.setLFUEvictionFactor(getIndexLFUEvictionFactor());
        index.setEnableDiskSyncs(isEnableIndexDiskSyncs());
        index.setEnableRecoveryFile(isEnableIndexRecoveryFile());
        index.setEnablePageCaching(isEnableIndexPageCaching());
        return index;
    }

    /**
     * Create a new Journal instance and configure it using the currently set configuration
     * options.  If an archive directory is configured than this method will attempt to create
     * that directory if it does not already exist.
     *
     * @return the newly created an configured Journal instance.
     *
     * @throws IOException if an error occurs while creating the Journal object.
     */
    protected Journal createJournal() throws IOException {
        Journal manager = new Journal();
        manager.setDirectory(getDirectory());
        manager.setMaxFileLength(getJournalMaxFileLength());
        manager.setCheckForCorruptionOnStartup(isCheckForCorruptJournalFiles());
        manager.setChecksum(isChecksumJournalFiles() || isCheckForCorruptJournalFiles());
        manager.setWriteBatchSize(getJournalMaxWriteBatchSize());
        manager.setArchiveDataLogs(isArchiveDataLogs());
        manager.setSizeAccumulator(journalSize);
        manager.setEnableAsyncDiskSync(isEnableJournalDiskSyncs());
        if (getDirectoryArchive() != null) {
            IOHelper.mkdirs(getDirectoryArchive());
            manager.setDirectoryArchive(getDirectoryArchive());
        }
        return manager;
    }

    /**
     * Starts the checkpoint Thread instance if not already running and not disabled
     * by configuration.
     */
    protected void startCheckpoint() {
        if (checkpointInterval == 0 && cleanupInterval == 0) {
            LOG.info("periodic checkpoint/cleanup disabled, will ocurr on clean shutdown/restart");
            return;
        }
        synchronized (checkpointThreadLock) {
            boolean start = false;
            if (checkpointThread == null) {
                start = true;
            } else if (!checkpointThread.isAlive()) {
                start = true;
                LOG.info("KahaDB: Recovering checkpoint thread after death");
            }
            if (start) {
                checkpointThread = new Thread("ActiveMQ Journal Checkpoint Worker") {
                    @Override
                    public void run() {
                        try {
                            long lastCleanup = System.currentTimeMillis();
                            long lastCheckpoint = System.currentTimeMillis();
                            // Sleep for a short time so we can periodically check
                            // to see if we need to exit this thread.
                            long sleepTime = Math.min(checkpointInterval > 0 ? checkpointInterval : cleanupInterval, 500);
                            while (opened.get()) {
                                Thread.sleep(sleepTime);
                                long now = System.currentTimeMillis();
                                if( cleanupInterval > 0 && (now - lastCleanup >= cleanupInterval) ) {
                                    checkpointCleanup(true);
                                    lastCleanup = now;
                                    lastCheckpoint = now;
                                } else if( checkpointInterval > 0 && (now - lastCheckpoint >= checkpointInterval )) {
                                    checkpointCleanup(false);
                                    lastCheckpoint = now;
                                }
                            }
                        } catch (InterruptedException e) {
                            // Looks like someone really wants us to exit this thread...
                        } catch (IOException ioe) {
                            LOG.error("Checkpoint failed", ioe);
                            brokerService.handleIOException(ioe);
                        }
                    }
                };

                checkpointThread.setDaemon(true);
                checkpointThread.start();
            }
        }
    }

    /**
     * Called from the worker thread to start a checkpoint.
     *
     * This method ensure that the store is in an opened state and optionaly logs information
     * related to slow store access times.
     *
     * @param cleanup
     *      Should a cleanup of the journal occur during the checkpoint operation.
     *
     * @throws IOException if an error occurs during the checkpoint operation.
     */
    protected void checkpointCleanup(final boolean cleanup) throws IOException {
        long start;
        this.indexLock.writeLock().lock();
        try {
            start = System.currentTimeMillis();
            if (!opened.get()) {
                return;
            }
        } finally {
            this.indexLock.writeLock().unlock();
        }
        checkpointUpdate(cleanup);
        long end = System.currentTimeMillis();
        if (LOG_SLOW_ACCESS_TIME > 0 && end - start > LOG_SLOW_ACCESS_TIME) {
            LOG.info("Slow KahaDB access: cleanup took {}", (end - start));
        }
    }
}
