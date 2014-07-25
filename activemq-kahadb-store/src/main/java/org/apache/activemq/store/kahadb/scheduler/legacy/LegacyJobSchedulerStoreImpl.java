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
package org.apache.activemq.store.kahadb.scheduler.legacy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.store.kahadb.disk.index.BTreeIndex;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Page;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.IntegerMarshaller;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.store.kahadb.disk.util.VariableMarshaller;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.LockFile;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only view of a legacy JobSchedulerStore implementation.
 */
final class LegacyJobSchedulerStoreImpl extends ServiceSupport {

    static final Logger LOG = LoggerFactory.getLogger(LegacyJobSchedulerStoreImpl.class);

    private static final int DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

    private File directory;
    private PageFile pageFile;
    private Journal journal;
    private LockFile lockFile;
    private final AtomicLong journalSize = new AtomicLong(0);
    private boolean failIfDatabaseIsLocked;
    private int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    private int journalMaxWriteBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    private boolean enableIndexWriteAsync = false;
    private MetaData metaData = new MetaData(this);
    private final MetaDataMarshaller metaDataMarshaller = new MetaDataMarshaller(this);
    private final Map<String, LegacyJobSchedulerImpl> schedulers = new HashMap<String, LegacyJobSchedulerImpl>();

    protected class MetaData {
        protected MetaData(LegacyJobSchedulerStoreImpl store) {
            this.store = store;
        }

        private final LegacyJobSchedulerStoreImpl store;
        Page<MetaData> page;
        BTreeIndex<Integer, Integer> journalRC;
        BTreeIndex<String, LegacyJobSchedulerImpl> storedSchedulers;

        void createIndexes(Transaction tx) throws IOException {
            this.storedSchedulers = new BTreeIndex<String, LegacyJobSchedulerImpl>(pageFile, tx.allocate().getPageId());
            this.journalRC = new BTreeIndex<Integer, Integer>(pageFile, tx.allocate().getPageId());
        }

        void load(Transaction tx) throws IOException {
            this.storedSchedulers.setKeyMarshaller(StringMarshaller.INSTANCE);
            this.storedSchedulers.setValueMarshaller(new JobSchedulerMarshaller(this.store));
            this.storedSchedulers.load(tx);
            this.journalRC.setKeyMarshaller(IntegerMarshaller.INSTANCE);
            this.journalRC.setValueMarshaller(IntegerMarshaller.INSTANCE);
            this.journalRC.load(tx);
        }

        void loadScheduler(Transaction tx, Map<String, LegacyJobSchedulerImpl> schedulers) throws IOException {
            for (Iterator<Entry<String, LegacyJobSchedulerImpl>> i = this.storedSchedulers.iterator(tx); i.hasNext();) {
                Entry<String, LegacyJobSchedulerImpl> entry = i.next();
                entry.getValue().load(tx);
                schedulers.put(entry.getKey(), entry.getValue());
            }
        }

        public void read(DataInput is) throws IOException {
            this.storedSchedulers = new BTreeIndex<String, LegacyJobSchedulerImpl>(pageFile, is.readLong());
            this.storedSchedulers.setKeyMarshaller(StringMarshaller.INSTANCE);
            this.storedSchedulers.setValueMarshaller(new JobSchedulerMarshaller(this.store));
            this.journalRC = new BTreeIndex<Integer, Integer>(pageFile, is.readLong());
            this.journalRC.setKeyMarshaller(IntegerMarshaller.INSTANCE);
            this.journalRC.setValueMarshaller(IntegerMarshaller.INSTANCE);
        }

        public void write(DataOutput os) throws IOException {
            os.writeLong(this.storedSchedulers.getPageId());
            os.writeLong(this.journalRC.getPageId());
        }
    }

    class MetaDataMarshaller extends VariableMarshaller<MetaData> {
        private final LegacyJobSchedulerStoreImpl store;

        MetaDataMarshaller(LegacyJobSchedulerStoreImpl store) {
            this.store = store;
        }

        @Override
        public MetaData readPayload(DataInput dataIn) throws IOException {
            MetaData rc = new MetaData(this.store);
            rc.read(dataIn);
            return rc;
        }

        @Override
        public void writePayload(MetaData object, DataOutput dataOut) throws IOException {
            object.write(dataOut);
        }
    }

    class ValueMarshaller extends VariableMarshaller<List<LegacyJobLocation>> {
        @Override
        public List<LegacyJobLocation> readPayload(DataInput dataIn) throws IOException {
            List<LegacyJobLocation> result = new ArrayList<LegacyJobLocation>();
            int size = dataIn.readInt();
            for (int i = 0; i < size; i++) {
                LegacyJobLocation jobLocation = new LegacyJobLocation();
                jobLocation.readExternal(dataIn);
                result.add(jobLocation);
            }
            return result;
        }

        @Override
        public void writePayload(List<LegacyJobLocation> value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.size());
            for (LegacyJobLocation jobLocation : value) {
                jobLocation.writeExternal(dataOut);
            }
        }
    }

    class JobSchedulerMarshaller extends VariableMarshaller<LegacyJobSchedulerImpl> {
        private final LegacyJobSchedulerStoreImpl store;

        JobSchedulerMarshaller(LegacyJobSchedulerStoreImpl store) {
            this.store = store;
        }

        @Override
        public LegacyJobSchedulerImpl readPayload(DataInput dataIn) throws IOException {
            LegacyJobSchedulerImpl result = new LegacyJobSchedulerImpl(this.store);
            result.read(dataIn);
            return result;
        }

        @Override
        public void writePayload(LegacyJobSchedulerImpl js, DataOutput dataOut) throws IOException {
            js.write(dataOut);
        }
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
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

    /**
     * Returns the named Job Scheduler if it exists, otherwise throws an exception.
     *
     * @param name
     *     The name of the scheduler that is to be returned.
     *
     * @return the named scheduler if it exists.
     *
     * @throws Exception if the named scheduler does not exist in this store.
     */
    public LegacyJobSchedulerImpl getJobScheduler(final String name) throws Exception {
        LegacyJobSchedulerImpl result = this.schedulers.get(name);
        if (result == null) {
            throw new NoSuchElementException("No such Job Scheduler in this store: " + name);
        }
        return result;
    }

    /**
     * Returns the names of all the schedulers that exist in this scheduler store.
     *
     * @return a set of names of all scheduler instances in this store.
     *
     * @throws Exception if an error occurs while collecting the scheduler names.
     */
    public Set<String> getJobSchedulerNames() throws Exception {
        Set<String> names = Collections.emptySet();

        if (!schedulers.isEmpty()) {
            return this.schedulers.keySet();
        }

        return names;
    }

    @Override
    protected void doStart() throws Exception {
        if (this.directory == null) {
            this.directory = new File(IOHelper.getDefaultDataDirectory() + File.pathSeparator + "delayedDB");
        }
        IOHelper.mkdirs(this.directory);
        lock();
        this.journal = new Journal();
        this.journal.setDirectory(directory);
        this.journal.setMaxFileLength(getJournalMaxFileLength());
        this.journal.setWriteBatchSize(getJournalMaxWriteBatchSize());
        this.journal.setSizeAccumulator(this.journalSize);
        this.journal.start();
        this.pageFile = new PageFile(directory, "scheduleDB");
        this.pageFile.setWriteBatchSize(1);
        this.pageFile.load();

        this.pageFile.tx().execute(new Transaction.Closure<IOException>() {
            @Override
            public void execute(Transaction tx) throws IOException {
                if (pageFile.getPageCount() == 0) {
                    Page<MetaData> page = tx.allocate();
                    assert page.getPageId() == 0;
                    page.set(metaData);
                    metaData.page = page;
                    metaData.createIndexes(tx);
                    tx.store(metaData.page, metaDataMarshaller, true);

                } else {
                    Page<MetaData> page = tx.load(0, metaDataMarshaller);
                    metaData = page.get();
                    metaData.page = page;
                }
                metaData.load(tx);
                metaData.loadScheduler(tx, schedulers);
                for (LegacyJobSchedulerImpl js : schedulers.values()) {
                    try {
                        js.start();
                    } catch (Exception e) {
                        LegacyJobSchedulerStoreImpl.LOG.error("Failed to load " + js.getName(), e);
                    }
                }
            }
        });

        this.pageFile.flush();
        LOG.info(this + " started");
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        for (LegacyJobSchedulerImpl js : this.schedulers.values()) {
            js.stop();
        }
        if (this.pageFile != null) {
            this.pageFile.unload();
        }
        if (this.journal != null) {
            journal.close();
        }
        if (this.lockFile != null) {
            this.lockFile.unlock();
        }
        this.lockFile = null;
        LOG.info(this + " stopped");
    }

    ByteSequence getPayload(Location location) throws IllegalStateException, IOException {
        ByteSequence result = null;
        result = this.journal.read(location);
        return result;
    }

    Location write(ByteSequence payload, boolean sync) throws IllegalStateException, IOException {
        return this.journal.write(payload, sync);
    }

    private void lock() throws IOException {
        if (lockFile == null) {
            File lockFileName = new File(directory, "lock");
            lockFile = new LockFile(lockFileName, true);
            if (failIfDatabaseIsLocked) {
                lockFile.lock();
            } else {
                while (true) {
                    try {
                        lockFile.lock();
                        break;
                    } catch (IOException e) {
                        LOG.info("Database " + lockFileName + " is locked... waiting " + (DATABASE_LOCKED_WAIT_DELAY / 1000)
                            + " seconds for the database to be unlocked. Reason: " + e);
                        try {
                            Thread.sleep(DATABASE_LOCKED_WAIT_DELAY);
                        } catch (InterruptedException e1) {
                        }
                    }
                }
            }
        }
    }

    PageFile getPageFile() {
        this.pageFile.isLoaded();
        return this.pageFile;
    }

    public boolean isFailIfDatabaseIsLocked() {
        return failIfDatabaseIsLocked;
    }

    public void setFailIfDatabaseIsLocked(boolean failIfDatabaseIsLocked) {
        this.failIfDatabaseIsLocked = failIfDatabaseIsLocked;
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

    @Override
    public String toString() {
        return "LegacyJobSchedulerStore:" + this.directory;
    }
}
