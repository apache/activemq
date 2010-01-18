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
package org.apache.activemq.broker.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.journal.Journal;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.IntegerMarshaller;
import org.apache.kahadb.util.LockFile;
import org.apache.kahadb.util.StringMarshaller;
import org.apache.kahadb.util.VariableMarshaller;

public class JobSchedulerStore extends ServiceSupport {
    static final Log LOG = LogFactory.getLog(JobSchedulerStore.class);
    private static final int DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

    public static final int CLOSED_STATE = 1;
    public static final int OPEN_STATE = 2;

    private File directory;
    PageFile pageFile;
    private Journal journal;
    private LockFile lockFile;
    private boolean failIfDatabaseIsLocked;
    private int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    private int journalMaxWriteBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    private boolean enableIndexWriteAsync = false;
    // private int indexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE;
    MetaData metaData = new MetaData(this);
    final MetaDataMarshaller metaDataMarshaller = new MetaDataMarshaller(this);
    Map<String, JobSchedulerImpl> schedulers = new HashMap<String, JobSchedulerImpl>();

    protected class MetaData {
        protected MetaData(JobSchedulerStore store) {
            this.store = store;
        }
        private final JobSchedulerStore store;
        Page<MetaData> page;
        BTreeIndex<Integer, Integer> journalRC;
        BTreeIndex<String, JobSchedulerImpl> storedSchedulers;

        void createIndexes(Transaction tx) throws IOException {
            this.storedSchedulers = new BTreeIndex<String, JobSchedulerImpl>(pageFile, tx.allocate().getPageId());
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

        void loadScheduler(Transaction tx, Map<String, JobSchedulerImpl> schedulers) throws IOException {
            for (Iterator<Entry<String, JobSchedulerImpl>> i = this.storedSchedulers.iterator(tx); i.hasNext();) {
                Entry<String, JobSchedulerImpl> entry = i.next();
                entry.getValue().load(tx);
                schedulers.put(entry.getKey(), entry.getValue());
            }
        }

        public void read(DataInput is) throws IOException {
            this.storedSchedulers = new BTreeIndex<String, JobSchedulerImpl>(pageFile, is.readLong());
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
        private final JobSchedulerStore store;

        MetaDataMarshaller(JobSchedulerStore store) {
            this.store = store;
        }
        public MetaData readPayload(DataInput dataIn) throws IOException {
            MetaData rc = new MetaData(this.store);
            rc.read(dataIn);
            return rc;
        }

        public void writePayload(MetaData object, DataOutput dataOut) throws IOException {
            object.write(dataOut);
        }
    }

    class ValueMarshaller extends VariableMarshaller<List<JobLocation>> {
        public List<JobLocation> readPayload(DataInput dataIn) throws IOException {
            List<JobLocation> result = new ArrayList<JobLocation>();
            int size = dataIn.readInt();
            for (int i = 0; i < size; i++) {
                JobLocation jobLocation = new JobLocation();
                jobLocation.readExternal(dataIn);
                result.add(jobLocation);
            }
            return result;
        }

        public void writePayload(List<JobLocation> value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.size());
            for (JobLocation jobLocation : value) {
                jobLocation.writeExternal(dataOut);
            }
        }
    }

    class JobSchedulerMarshaller extends VariableMarshaller<JobSchedulerImpl> {
        private final JobSchedulerStore store;
        JobSchedulerMarshaller(JobSchedulerStore store) {
            this.store = store;
        }
        public JobSchedulerImpl readPayload(DataInput dataIn) throws IOException {
            JobSchedulerImpl result = new JobSchedulerImpl(this.store);
            result.read(dataIn);
            return result;
        }

        public void writePayload(JobSchedulerImpl js, DataOutput dataOut) throws IOException {
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
        if ( !isStarted() ) {
            return 0;
        }
        try {
            return journal.getDiskSize() + pageFile.getDiskSize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public JobScheduler getJobScheduler(final String name) throws Exception {
        JobSchedulerImpl result = this.schedulers.get(name);
        if (result == null) {
            final JobSchedulerImpl js = new JobSchedulerImpl(this);
            js.setName(name);
            getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    js.createIndexes(tx);
                    js.load(tx);
                    metaData.storedSchedulers.put(tx, name, js);
                }
            });
            result = js;
            this.schedulers.put(name, js);
            if (isStarted()) {
                result.start();
            }
        }
        return result;
    }

    synchronized public boolean removeJobScheduler(final String name) throws Exception {
        boolean result = false;
        final JobSchedulerImpl js = this.schedulers.remove(name);
        result = js != null;
        if (result) {
            js.stop();
            getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    metaData.storedSchedulers.remove(tx, name);
                    js.destroy(tx);
                }
            });
        }
        return result;
    }

    @Override
    protected synchronized void doStart() throws Exception {
        if (this.directory == null) {
            this.directory = new File(IOHelper.getDefaultDataDirectory() + File.pathSeparator + "delayedDB");
        }
        IOHelper.mkdirs(this.directory);
        lock();
        this.journal = new Journal();
        this.journal.setDirectory(directory);
        this.journal.setMaxFileLength(getJournalMaxFileLength());
        this.journal.setWriteBatchSize(getJournalMaxWriteBatchSize());
        this.journal.start();
        this.pageFile = new PageFile(directory, "scheduleDB");
        this.pageFile.load();

        this.pageFile.tx().execute(new Transaction.Closure<IOException>() {
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
                for (JobSchedulerImpl js :schedulers.values()) {
                    try {
                        js.start();
                    } catch (Exception e) {
                        JobSchedulerStore.LOG.error("Failed to load " + js.getName(),e);
                    }
               }
            }
        });

        this.pageFile.flush();
        LOG.info(this + " started");
    }
    
    @Override
    protected synchronized void doStop(ServiceStopper stopper) throws Exception {
        for (JobSchedulerImpl js : this.schedulers.values()) {
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

    synchronized void incrementJournalCount(Transaction tx, Location location) throws IOException {
        int logId = location.getDataFileId();
        Integer val = this.metaData.journalRC.get(tx, logId);
        int refCount = val != null ? val.intValue() + 1 : 1;
        this.metaData.journalRC.put(tx, logId, refCount);

    }

    synchronized void decrementJournalCount(Transaction tx, Location location) throws IOException {
        int logId = location.getDataFileId();
        int refCount = this.metaData.journalRC.get(tx, logId);
        refCount--;
        if (refCount <= 0) {
            this.metaData.journalRC.remove(tx, logId);
            Set<Integer> set = new HashSet<Integer>();
            set.add(logId);
            this.journal.removeDataFiles(set);
        } else {
            this.metaData.journalRC.put(tx, logId, refCount);
        }

    }

    synchronized ByteSequence getJob(Location location) throws IllegalStateException, IOException {
        ByteSequence result = null;
        result = this.journal.read(location);
        return result;
    }

    synchronized Location write(ByteSequence payload, boolean sync) throws IllegalStateException, IOException {
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
                        LOG.info("Database " + lockFileName + " is locked... waiting "
                                + (DATABASE_LOCKED_WAIT_DELAY / 1000)
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
        return "JobSchedulerStore:" + this.directory;
    }

}
