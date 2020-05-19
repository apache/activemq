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
package org.apache.activemq.store.kahadb.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.activemq.broker.scheduler.JobScheduler;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.kahadb.AbstractKahaDBStore;
import org.apache.activemq.store.kahadb.JournalCommand;
import org.apache.activemq.store.kahadb.KahaDBMetaData;
import org.apache.activemq.store.kahadb.Visitor;
import org.apache.activemq.store.kahadb.data.KahaAddScheduledJobCommand;
import org.apache.activemq.store.kahadb.data.KahaDestroySchedulerCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveScheduledJobCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveScheduledJobsCommand;
import org.apache.activemq.store.kahadb.data.KahaRescheduleJobCommand;
import org.apache.activemq.store.kahadb.data.KahaTraceCommand;
import org.apache.activemq.store.kahadb.disk.index.BTreeVisitor;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Page;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.VariableMarshaller;
import org.apache.activemq.store.kahadb.scheduler.legacy.LegacyStoreReplayer;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * @org.apache.xbean.XBean element="kahaDBJobScheduler"
 */

public class JobSchedulerStoreImpl extends AbstractKahaDBStore implements JobSchedulerStore {

    private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerStoreImpl.class);

    private JobSchedulerKahaDBMetaData metaData = new JobSchedulerKahaDBMetaData(this);
    private final MetaDataMarshaller metaDataMarshaller = new MetaDataMarshaller(this);
    private final Map<String, JobSchedulerImpl> schedulers = new HashMap<String, JobSchedulerImpl>();
    private File legacyStoreArchiveDirectory;

    /**
     * The Scheduler Token is used to identify base revisions of the Scheduler store.  A store
     * based on the initial scheduler design will not have this tag in it's meta-data and will
     * indicate an update is needed.  Later versions of the scheduler can also change this value
     * to indicate incompatible store bases which require complete meta-data and journal rewrites
     * instead of simpler meta-data updates.
     */
    static final UUID SCHEDULER_STORE_TOKEN = UUID.fromString("57ed642b-1ee3-47b3-be6d-b7297d500409");

    /**
     * The default scheduler store version.  All new store instance will be given this version and
     * earlier versions will be updated to this version.
     */
    static final int CURRENT_VERSION = 1;

    @Override
    public JobScheduler getJobScheduler(final String name) throws Exception {
        this.indexLock.writeLock().lock();
        try {
            JobSchedulerImpl result = this.schedulers.get(name);
            if (result == null) {
                final JobSchedulerImpl js = new JobSchedulerImpl(this);
                js.setName(name);
                getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        js.createIndexes(tx);
                        js.load(tx);
                        metaData.getJobSchedulers().put(tx, name, js);
                    }
                });
                result = js;
                this.schedulers.put(name, js);
                if (isStarted()) {
                    result.start();
                }
                this.pageFile.flush();
            }
            return result;
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    @Override
    public boolean removeJobScheduler(final String name) throws Exception {
        boolean result = false;

        this.indexLock.writeLock().lock();
        try {
            final JobSchedulerImpl js = this.schedulers.remove(name);
            result = js != null;
            if (result) {
                js.stop();
                getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        metaData.getJobSchedulers().remove(tx, name);
                        js.removeAll(tx);
                    }
                });
            }
        } finally {
            this.indexLock.writeLock().unlock();
        }
        return result;
    }

    /**
     * Sets the directory where the legacy scheduler store files are archived before an
     * update attempt is made.  Both the legacy index files and the journal files are moved
     * to this folder prior to an upgrade attempt.
     *
     * @param directory
     *      The directory to move the legacy Scheduler Store files to.
     */
    public void setLegacyStoreArchiveDirectory(File directory) {
        this.legacyStoreArchiveDirectory = directory;
    }

    /**
     * Gets the directory where the legacy Scheduler Store files will be archived if the
     * broker is started and an existing Job Scheduler Store from an old version is detected.
     *
     * @return the directory where scheduler store legacy files are archived on upgrade.
     */
    public File getLegacyStoreArchiveDirectory() {
        if (this.legacyStoreArchiveDirectory == null) {
            this.legacyStoreArchiveDirectory = new File(getDirectory(), "legacySchedulerStore");
        }

        return this.legacyStoreArchiveDirectory.getAbsoluteFile();
    }

    @Override
    public void load() throws IOException {
        if (opened.compareAndSet(false, true)) {
            getJournal().start();
            try {
                loadPageFile();
            } catch (UnknownStoreVersionException ex) {
                LOG.info("Can't start until store update is performed.");
                upgradeFromLegacy();
                // Restart with the updated store
                getJournal().start();
                loadPageFile();
                LOG.info("Update from legacy Scheduler store completed successfully.");
            } catch (Throwable t) {
                LOG.warn("Index corrupted. Recovering the index through journal replay. Cause: {}", t.toString());
                LOG.debug("Index load failure", t);

                // try to recover index
                try {
                    pageFile.unload();
                } catch (Exception ignore) {
                }
                if (isArchiveCorruptedIndex()) {
                    pageFile.archive();
                } else {
                    pageFile.delete();
                }
                metaData = new JobSchedulerKahaDBMetaData(this);
                pageFile = null;
                loadPageFile();
            }
            startCheckpoint();
            recover();
        }
        LOG.info("{} started.", this);
    }

    @Override
    public void unload() throws IOException {
        if (opened.compareAndSet(true, false)) {
            for (JobSchedulerImpl js : this.schedulers.values()) {
                try {
                    js.stop();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
            this.indexLock.writeLock().lock();
            try {
                if (pageFile != null && pageFile.isLoaded()) {
                    metaData.setState(KahaDBMetaData.CLOSED_STATE);

                    if (metaData.getPage() != null) {
                        pageFile.tx().execute(new Transaction.Closure<IOException>() {
                            @Override
                            public void execute(Transaction tx) throws IOException {
                                tx.store(metaData.getPage(), metaDataMarshaller, true);
                            }
                        });
                    }
                }
            } finally {
                this.indexLock.writeLock().unlock();
            }

            checkpointLock.writeLock().lock();
            try {
                if (metaData.getPage() != null) {
                    checkpointUpdate(getCleanupOnStop());
                }
            } finally {
                checkpointLock.writeLock().unlock();
            }
            synchronized (checkpointThreadLock) {
                if (checkpointThread != null) {
                    try {
                        checkpointThread.join();
                        checkpointThread = null;
                    } catch (InterruptedException e) {
                    }
                }
            }

            if (pageFile != null) {
                pageFile.unload();
                pageFile = null;
            }
            if (this.journal != null) {
                journal.close();
                journal = null;
            }

            metaData = new JobSchedulerKahaDBMetaData(this);
        }
        LOG.info("{} stopped.", this);
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
                        Page<JobSchedulerKahaDBMetaData> page = tx.allocate();
                        assert page.getPageId() == 0;
                        page.set(metaData);
                        metaData.setPage(page);
                        metaData.setState(KahaDBMetaData.CLOSED_STATE);
                        metaData.initialize(tx);
                        tx.store(metaData.getPage(), metaDataMarshaller, true);
                    } else {
                        Page<JobSchedulerKahaDBMetaData> page = null;
                        page = tx.load(0, metaDataMarshaller);
                        metaData = page.get();
                        metaData.setPage(page);
                    }
                    metaData.load(tx);
                    metaData.loadScheduler(tx, schedulers);
                    for (JobSchedulerImpl js : schedulers.values()) {
                        try {
                            js.start();
                        } catch (Exception e) {
                            JobSchedulerStoreImpl.LOG.error("Failed to load " + js.getName(), e);
                        }
                    }
                }
            });

            pageFile.flush();
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    private void upgradeFromLegacy() throws IOException {

        journal.close();
        journal = null;
        try {
            pageFile.unload();
            pageFile = null;
        } catch (Exception ignore) {}

        File storeDir = getDirectory().getAbsoluteFile();
        File storeArchiveDir = getLegacyStoreArchiveDirectory();

        LOG.info("Attempting to move old store files from {} to {}", storeDir, storeArchiveDir);

        // Move only the known store files, locks and other items left in place.
        IOHelper.moveFiles(storeDir, storeArchiveDir, new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                if (name.endsWith(".data") || name.endsWith(".redo") || name.endsWith(".log") || name.endsWith(".free")) {
                    return true;
                }
                return false;
            }
        });

        // We reset everything to clean state, then we can read from the old
        // scheduler store and replay the scheduled jobs into this one as adds.
        getJournal().start();
        metaData = new JobSchedulerKahaDBMetaData(this);
        pageFile = null;
        loadPageFile();

        LegacyStoreReplayer replayer = new LegacyStoreReplayer(getLegacyStoreArchiveDirectory());
        replayer.load();
        replayer.startReplay(this);

        // Cleanup after replay and store what we've done.
        pageFile.tx().execute(new Transaction.Closure<IOException>() {
            @Override
            public void execute(Transaction tx) throws IOException {
                tx.store(metaData.getPage(), metaDataMarshaller, true);
            }
        });

        checkpointUpdate(true);
        getJournal().close();
        getPageFile().unload();
    }

    @Override
    protected void checkpointUpdate(Transaction tx, boolean cleanup) throws IOException {
        LOG.debug("Job Scheduler Store Checkpoint started.");

        // reflect last update exclusive of current checkpoint
        Location lastUpdate = metaData.getLastUpdateLocation();
        metaData.setState(KahaDBMetaData.OPEN_STATE);
        tx.store(metaData.getPage(), metaDataMarshaller, true);
        pageFile.flush();

        if (cleanup) {
            final TreeSet<Integer> completeFileSet = new TreeSet<Integer>(journal.getFileMap().keySet());
            final TreeSet<Integer> gcCandidateSet = new TreeSet<Integer>(completeFileSet);

            LOG.trace("Last update: {}, full gc candidates set: {}", lastUpdate, gcCandidateSet);

            if (lastUpdate != null) {
                gcCandidateSet.remove(lastUpdate.getDataFileId());
            }

            this.metaData.getJournalRC().visit(tx, new BTreeVisitor<Integer, Integer>() {

                @Override
                public void visit(List<Integer> keys, List<Integer> values) {
                    for (Integer key : keys) {
                        if (gcCandidateSet.remove(key)) {
                            LOG.trace("Removed referenced file: {} from GC set", key);
                        }
                    }
                }

                @Override
                public boolean isInterestedInKeysBetween(Integer first, Integer second) {
                    return true;
                }
            });

            LOG.trace("gc candidates after reference check: {}", gcCandidateSet);

            // If there are GC candidates then check the remove command location to see
            // if any of them can go or if they must stay in order to ensure proper recover.
            //
            // A log containing any remove commands must be kept until all the logs with the
            // add commands for all the removed jobs have been dropped.
            if (!gcCandidateSet.isEmpty()) {
                Iterator<Entry<Integer, List<Integer>>> removals = metaData.getRemoveLocationTracker().iterator(tx);
                List<Integer> orphans = new ArrayList<Integer>();
                while (removals.hasNext()) {
                    boolean orphanedRemove = true;
                    Entry<Integer, List<Integer>> entry = removals.next();

                    // If this log is not a GC candidate then there's no need to do a check to rule it out
                    if (gcCandidateSet.contains(entry.getKey())) {
                        for (Integer addLocation : entry.getValue()) {
                            if (completeFileSet.contains(addLocation)) {
                                LOG.trace("A remove in log {} has an add still in existance in {}.", entry.getKey(), addLocation);
                                orphanedRemove = false;
                                break;
                            }
                        }

                        // If it's not orphaned than we can't remove it, otherwise we
                        // stop tracking it it's log will get deleted on the next check.
                        if (!orphanedRemove) {
                            gcCandidateSet.remove(entry.getKey());
                        } else {
                            LOG.trace("All removes in log {} are orphaned, file can be GC'd", entry.getKey());
                            orphans.add(entry.getKey());
                        }
                    }
                }

                // Drop all orphaned removes from the tracker.
                for (Integer orphan : orphans) {
                    metaData.getRemoveLocationTracker().remove(tx, orphan);
                }
            }

            LOG.trace("gc candidates after removals check: {}", gcCandidateSet);
            if (!gcCandidateSet.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cleanup removing the data files: " + gcCandidateSet);
                }
                journal.removeDataFiles(gcCandidateSet);
            }
        }

        LOG.debug("Job Scheduler Store Checkpoint complete.");
    }

    /**
     * Adds a reference for the journal log file pointed to by the given Location value.
     *
     * To prevent log files in the journal that still contain valid data that needs to be
     * kept in order to allow for recovery the logs must have active references.  Each Job
     * scheduler should ensure that the logs are accurately referenced.
     *
     * @param tx
     *      The TX under which the update is to be performed.
     * @param location
     *      The location value to update the reference count of.
     *
     * @throws IOException if an error occurs while updating the journal references table.
     */
    protected void incrementJournalCount(Transaction tx, Location location) throws IOException {
        int logId = location.getDataFileId();
        Integer val = metaData.getJournalRC().get(tx, logId);
        int refCount = val != null ? val.intValue() + 1 : 1;
        metaData.getJournalRC().put(tx, logId, refCount);
    }

    /**
     * Removes one reference for the Journal log file indicated in the given Location value.
     *
     * The references are used to track which log files cannot be GC'd.  When the reference count
     * on a log file reaches zero the file id is removed from the tracker and the log will be
     * removed on the next check point update.
     *
     * @param tx
     *      The TX under which the update is to be performed.
     * @param location
     *      The location value to update the reference count of.
     *
     * @throws IOException if an error occurs while updating the journal references table.
     */
    protected void decrementJournalCount(Transaction tx, Location location) throws IOException {
        int logId = location.getDataFileId();
        Integer refCount = metaData.getJournalRC().get(tx, logId);
        if (refCount != null) {
            int refCountValue = refCount;
            refCountValue--;
            if (refCountValue <= 0) {
                metaData.getJournalRC().remove(tx, logId);
            } else {
                metaData.getJournalRC().put(tx, logId, refCountValue);
            }
        }
    }

    /**
     * Removes multiple references for the Journal log file indicated in the given Location map.
     *
     * The references are used to track which log files cannot be GC'd.  When the reference count
     * on a log file reaches zero the file id is removed from the tracker and the log will be
     * removed on the next check point update.
     *
     * @param tx
     *      The TX under which the update is to be performed.
     * @param decrementsByFileIds
     *      Map indicating how many decrements per fileId.
     *
     * @throws IOException if an error occurs while updating the journal references table.
     */
    protected void decrementJournalCount(Transaction tx, HashMap<Integer, Integer> decrementsByFileIds) throws IOException {
        for(Map.Entry<Integer, Integer> entry : decrementsByFileIds.entrySet()) {
            int logId = entry.getKey();
            Integer refCount = metaData.getJournalRC().get(tx, logId);

            if (refCount != null) {
                int refCountValue = refCount;
                refCountValue -= entry.getValue();
                if (refCountValue <= 0) {
                    metaData.getJournalRC().remove(tx, logId);
                } else {
                    metaData.getJournalRC().put(tx, logId, refCountValue);
                }
            }
        }
    }

    /**
     * Updates the Job removal tracking index with the location of a remove command and the
     * original JobLocation entry.
     *
     * The JobLocation holds the locations in the logs where the add and update commands for
     * a job stored.  The log file containing the remove command can only be discarded after
     * both the add and latest update log files have also been discarded.
     *
     * @param tx
     *      The TX under which the update is to be performed.
     * @param location
     *      The location value to reference a remove command.
     * @param removedJob
     *      The original JobLocation instance that holds the add and update locations
     *
     * @throws IOException if an error occurs while updating the remove location tracker.
     */
    protected void referenceRemovedLocation(Transaction tx, Location location, JobLocation removedJob) throws IOException {
        int logId = location.getDataFileId();
        List<Integer> removed = this.metaData.getRemoveLocationTracker().get(tx, logId);
        if (removed == null) {
            removed = new ArrayList<Integer>();
        }
        removed.add(removedJob.getLocation().getDataFileId());
        this.metaData.getRemoveLocationTracker().put(tx, logId, removed);
    }

    /**
     * Updates the Job removal tracking index with the location of a remove command and the
     * original JobLocation entry.
     *
     * The JobLocation holds the locations in the logs where the add and update commands for
     * a job stored.  The log file containing the remove command can only be discarded after
     * both the add and latest update log files have also been discarded.
     *
     * @param tx
     *      The TX under which the update is to be performed.
     * @param location
     *      The location value to reference a remove command.
     * @param removedJobsFileId
     *      List of the original JobLocation instances that holds the add and update locations
     *
     * @throws IOException if an error occurs while updating the remove location tracker.
     */
    protected void referenceRemovedLocation(Transaction tx, Location location, List<Integer> removedJobsFileId) throws IOException {
        int logId = location.getDataFileId();
        List<Integer> removed = this.metaData.getRemoveLocationTracker().get(tx, logId);
        if (removed == null) {
            removed = new ArrayList<Integer>();
        }
        removed.addAll(removedJobsFileId);
        this.metaData.getRemoveLocationTracker().put(tx, logId, removed);
    }

    /**
     * Retrieve the scheduled Job's byte blob from the journal.
     *
     * @param location
     *      The location of the KahaAddScheduledJobCommand that originated the Job.
     *
     * @return a ByteSequence containing the payload of the scheduled Job.
     *
     * @throws IOException if an error occurs while reading the payload value.
     */
    protected ByteSequence getPayload(Location location) throws IOException {
        KahaAddScheduledJobCommand job = (KahaAddScheduledJobCommand) this.load(location);
        Buffer payload = job.getPayload();
        return new ByteSequence(payload.getData(), payload.getOffset(), payload.getLength());
    }

    public void readLockIndex() {
        this.indexLock.readLock().lock();
    }

    public void readUnlockIndex() {
        this.indexLock.readLock().unlock();
    }

    public void writeLockIndex() {
        this.indexLock.writeLock().lock();
    }

    public void writeUnlockIndex() {
        this.indexLock.writeLock().unlock();
    }

    @Override
    public String toString() {
        return "JobSchedulerStore: " + getDirectory();
    }

    @Override
    protected String getPageFileName() {
        return "scheduleDB";
    }

    @Override
    protected File getDefaultDataDirectory() {
        return new File(IOHelper.getDefaultDataDirectory(), "delayedDB");
    }

    private class MetaDataMarshaller extends VariableMarshaller<JobSchedulerKahaDBMetaData> {

        private final JobSchedulerStoreImpl store;

        MetaDataMarshaller(JobSchedulerStoreImpl store) {
            this.store = store;
        }

        @Override
        public JobSchedulerKahaDBMetaData readPayload(DataInput dataIn) throws IOException {
            JobSchedulerKahaDBMetaData rc = new JobSchedulerKahaDBMetaData(store);
            rc.read(dataIn);
            return rc;
        }

        @Override
        public void writePayload(JobSchedulerKahaDBMetaData object, DataOutput dataOut) throws IOException {
            object.write(dataOut);
        }
    }

    /**
     * Called during index recovery to rebuild the index from the last known good location.  For
     * entries that occur before the last known good position we just ignore then and move on.
     *
     * @param data
     *        the command read from the Journal which should be used to update the index.
     * @param location
     *        the location in the index where the command was read.
     * @param inDoubtlocation
     *        the location in the index known to be the last time the index was valid.
     *
     * @throws IOException if an error occurs while recovering the index.
     */
    protected void doRecover(JournalCommand<?> data, final Location location, final Location inDoubtlocation) throws IOException {
        if (inDoubtlocation != null && location.compareTo(inDoubtlocation) >= 0) {
            process(data, location);
        }
    }

    /**
     * Called during recovery to allow the store to rebuild from scratch.
     *
     * @param data
     *      The command to process, which was read from the Journal.
     * @param location
     *      The location of the command in the Journal.
     *
     * @throws IOException if an error occurs during command processing.
     */
    @Override
    protected void process(JournalCommand<?> data, final Location location) throws IOException {
        data.visit(new Visitor() {
            @Override
            public void visit(final KahaAddScheduledJobCommand command) throws IOException {
                final JobSchedulerImpl scheduler;

                indexLock.writeLock().lock();
                try {
                    try {
                        scheduler = (JobSchedulerImpl) getJobScheduler(command.getScheduler());
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                    getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            scheduler.process(tx, command, location);
                        }
                    });

                    processLocation(location);
                } finally {
                    indexLock.writeLock().unlock();
                }
            }

            @Override
            public void visit(final KahaRemoveScheduledJobCommand command) throws IOException {
                final JobSchedulerImpl scheduler;

                indexLock.writeLock().lock();
                try {
                    try {
                        scheduler = (JobSchedulerImpl) getJobScheduler(command.getScheduler());
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                    getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            scheduler.process(tx, command, location);
                        }
                    });

                    processLocation(location);
                } finally {
                    indexLock.writeLock().unlock();
                }
            }

            @Override
            public void visit(final KahaRemoveScheduledJobsCommand command) throws IOException {
                final JobSchedulerImpl scheduler;

                indexLock.writeLock().lock();
                try {
                    try {
                        scheduler = (JobSchedulerImpl) getJobScheduler(command.getScheduler());
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                    getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            scheduler.process(tx, command, location);
                        }
                    });

                    processLocation(location);
                } finally {
                    indexLock.writeLock().unlock();
                }
            }

            @Override
            public void visit(final KahaRescheduleJobCommand command) throws IOException {
                final JobSchedulerImpl scheduler;

                indexLock.writeLock().lock();
                try {
                    try {
                        scheduler = (JobSchedulerImpl) getJobScheduler(command.getScheduler());
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                    getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            scheduler.process(tx, command, location);
                        }
                    });

                    processLocation(location);
                } finally {
                    indexLock.writeLock().unlock();
                }
            }

            @Override
            public void visit(final KahaDestroySchedulerCommand command) {
                try {
                    removeJobScheduler(command.getScheduler());
                } catch (Exception e) {
                    LOG.warn("Failed to remove scheduler: {}", command.getScheduler());
                }

                processLocation(location);
            }

            @Override
            public void visit(KahaTraceCommand command) {
                processLocation(location);
            }
        });
    }

    protected void processLocation(final Location location) {
        indexLock.writeLock().lock();
        try {
            this.metaData.setLastUpdateLocation(location);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * We recover from the Journal logs as needed to restore the index.
     *
     * @throws IllegalStateException
     * @throws IOException
     */
    private void recover() throws IllegalStateException, IOException {
        this.indexLock.writeLock().lock();
        try {
            long start = System.currentTimeMillis();
            Location lastIndoubtPosition = getRecoveryPosition();
            Location recoveryPosition = lastIndoubtPosition;

            if (recoveryPosition != null) {
                int redoCounter = 0;
                LOG.info("Recovering from the scheduled job journal @" + recoveryPosition);
                while (recoveryPosition != null) {
                    try {
                        JournalCommand<?> message = load(recoveryPosition);
                        metaData.setLastUpdateLocation(recoveryPosition);
                        doRecover(message, recoveryPosition, lastIndoubtPosition);
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
                     if (LOG.isInfoEnabled() && redoCounter % 100000 == 0) {
                         LOG.info("@ {}, {} entries recovered ..", recoveryPosition, redoCounter);
                     }
                }
                long end = System.currentTimeMillis();
                LOG.info("Recovery replayed {} operations from the journal in {} seconds.",
                         redoCounter, ((end - start) / 1000.0f));
            }

            // We may have to undo some index updates.
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    recoverIndex(tx);
                }
            });

        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    private Location getRecoveryPosition() throws IOException {
        // This loads the first position and we completely rebuild the index if we
        // do not override it with some known recovery start location.
        Location result = null;

        if (!isForceRecoverIndex()) {
            if (metaData.getLastUpdateLocation() != null) {
                result = metaData.getLastUpdateLocation();
            }
        }

        return journal.getNextLocation(result);
    }

    private void recoverIndex(Transaction tx) throws IOException {
        long start = System.currentTimeMillis();

        // It is possible index updates got applied before the journal updates..
        // in that case we need to removed references to Jobs that are not in the journal
        final Location lastAppendLocation = journal.getLastAppendLocation();
        long undoCounter = 0;

        // Go through all the jobs in each scheduler and check if any are added after
        // the last appended location and remove those.  For now we ignore the update
        // location since the scheduled job will update itself after the next fire and
        // a new update will replace any existing update.
        for (Iterator<Map.Entry<String, JobSchedulerImpl>> i = metaData.getJobSchedulers().iterator(tx); i.hasNext();) {
            Map.Entry<String, JobSchedulerImpl> entry = i.next();
            JobSchedulerImpl scheduler = entry.getValue();

            for (Iterator<JobLocation> jobLocationIterator = scheduler.getAllScheduledJobs(tx); jobLocationIterator.hasNext();) {
                final JobLocation job = jobLocationIterator.next();
                if (job.getLocation().compareTo(lastAppendLocation) >= 0) {
                    if (scheduler.removeJobAtTime(tx, job.getJobId(), job.getNextTime())) {
                        LOG.trace("Removed Job past last appened in the journal: {}", job.getJobId());
                        undoCounter++;
                    }
                }
            }
        }

        if (undoCounter > 0) {
            // The rolled back operations are basically in flight journal writes.  To avoid getting
            // these the end user should do sync writes to the journal.
            long end = System.currentTimeMillis();
            LOG.info("Rolled back {} messages from the index in {} seconds.", undoCounter, ((end - start) / 1000.0f));
            undoCounter = 0;
        }

        // Now we check for missing and corrupt journal files.

        // 1. Collect the set of all referenced journal files based on the Location of the
        //    the scheduled jobs and the marked last update field.
        HashSet<Integer> missingJournalFiles = new HashSet<Integer>();
        for (Iterator<Map.Entry<String, JobSchedulerImpl>> i = metaData.getJobSchedulers().iterator(tx); i.hasNext();) {
            Map.Entry<String, JobSchedulerImpl> entry = i.next();
            JobSchedulerImpl scheduler = entry.getValue();

            for (Iterator<JobLocation> jobLocationIterator = scheduler.getAllScheduledJobs(tx); jobLocationIterator.hasNext();) {
                final JobLocation job = jobLocationIterator.next();
                missingJournalFiles.add(job.getLocation().getDataFileId());
                if (job.getLastUpdate() != null) {
                    missingJournalFiles.add(job.getLastUpdate().getDataFileId());
                }
            }
        }

        // 2. Remove from that set all known data file Id's in the journal and what's left
        //    is the missing set which will soon also contain the corrupted set.
        missingJournalFiles.removeAll(journal.getFileMap().keySet());
        if (!missingJournalFiles.isEmpty()) {
            LOG.info("Some journal files are missing: {}", missingJournalFiles);
        }

        // 3. Now check all references in the journal logs for corruption and add any
        //    corrupt journal files to the missing set.
        HashSet<Location> corruptedLocations = new HashSet<Location>();

        if (isCheckForCorruptJournalFiles()) {
            Collection<DataFile> dataFiles = journal.getFileMap().values();
            for (DataFile dataFile : dataFiles) {
                int id = dataFile.getDataFileId();
                for (long offset : dataFile.getCorruptedBlocks()) {
                    corruptedLocations.add(new Location(id, (int) offset));
                }
            }

            if (!corruptedLocations.isEmpty()) {
                LOG.debug("Found some corrupted data blocks in the journal: {}", corruptedLocations.size());
            }
        }

        // 4. Now we either fail or we remove all references to missing or corrupt journal
        //    files from the various JobSchedulerImpl instances.  We only remove the Job if
        //    the initial Add operation is missing when the ignore option is set, the updates
        //    could be lost but that's price you pay when ignoring the missing logs.
        if (!missingJournalFiles.isEmpty() || !corruptedLocations.isEmpty()) {
            if (!isIgnoreMissingJournalfiles()) {
                throw new IOException("Detected missing/corrupt journal files.");
            }

            // Remove all Jobs that reference an Location that is either missing or corrupt.
            undoCounter = removeJobsInMissingOrCorruptJounralFiles(tx, missingJournalFiles, corruptedLocations);

            // Clean up the Journal Reference count Map.
            removeJournalRCForMissingFiles(tx, missingJournalFiles);
        }

        if (undoCounter > 0) {
            long end = System.currentTimeMillis();
            LOG.info("Detected missing/corrupt journal files.  Dropped {} jobs from the " +
                     "index in {} seconds.", undoCounter, ((end - start) / 1000.0f));
        }
    }

    private void removeJournalRCForMissingFiles(Transaction tx, Set<Integer> missing) throws IOException {
        List<Integer> matches = new ArrayList<Integer>();

        Iterator<Entry<Integer, Integer>> references = metaData.getJournalRC().iterator(tx);
        while (references.hasNext()) {
            int dataFileId = references.next().getKey();
            if (missing.contains(dataFileId)) {
                matches.add(dataFileId);
            }
        }

        for (Integer match : matches) {
            metaData.getJournalRC().remove(tx, match);
        }
    }

    private int removeJobsInMissingOrCorruptJounralFiles(Transaction tx, Set<Integer> missing, Set<Location> corrupted) throws IOException {
        int removed = 0;

        // Remove Jobs that reference missing or corrupt files.
        // Remove Reference counts to missing or corrupt files.
        // Remove and remove command markers to missing or corrupt files.
        for (Iterator<Map.Entry<String, JobSchedulerImpl>> i = metaData.getJobSchedulers().iterator(tx); i.hasNext();) {
            Map.Entry<String, JobSchedulerImpl> entry = i.next();
            JobSchedulerImpl scheduler = entry.getValue();

            for (Iterator<JobLocation> jobLocationIterator = scheduler.getAllScheduledJobs(tx); jobLocationIterator.hasNext();) {
                final JobLocation job = jobLocationIterator.next();

                // Remove all jobs in missing log files.
                if (missing.contains(job.getLocation().getDataFileId())) {
                    scheduler.removeJobAtTime(tx, job.getJobId(), job.getNextTime());
                    removed++;
                    continue;
                }

                // Remove all jobs in corrupted parts of log files.
                if (corrupted.contains(job.getLocation())) {
                    scheduler.removeJobAtTime(tx, job.getJobId(), job.getNextTime());
                    removed++;
                }
            }
        }

        return removed;
    }
}
