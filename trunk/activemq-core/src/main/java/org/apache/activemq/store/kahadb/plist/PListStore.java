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
package org.apache.activemq.store.kahadb.plist;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * @org.apache.xbean.XBean
 */
public class PListStore extends ServiceSupport {
    static final Logger LOG = LoggerFactory.getLogger(PListStore.class);
    private static final int DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

    static final int CLOSED_STATE = 1;
    static final int OPEN_STATE = 2;

    private File directory;
    PageFile pageFile;
    private Journal journal;
    private LockFile lockFile;
    private boolean failIfDatabaseIsLocked;
    private int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    private int journalMaxWriteBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    private boolean enableIndexWriteAsync = false;
    private boolean initialized = false;
    // private int indexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE;
    MetaData metaData = new MetaData(this);
    final MetaDataMarshaller metaDataMarshaller = new MetaDataMarshaller(this);
    Map<String, PList> persistentLists = new HashMap<String, PList>();
    final Object indexLock = new Object();

    public Object getIndexLock() {
        return indexLock;
    }

    protected class MetaData {
        protected MetaData(PListStore store) {
            this.store = store;
        }

        private final PListStore store;
        Page<MetaData> page;
        BTreeIndex<Integer, Integer> journalRC;
        BTreeIndex<String, PList> storedSchedulers;

        void createIndexes(Transaction tx) throws IOException {
            this.storedSchedulers = new BTreeIndex<String, PList>(pageFile, tx.allocate().getPageId());
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

        void loadLists(Transaction tx, Map<String, PList> schedulers) throws IOException {
            for (Iterator<Entry<String, PList>> i = this.storedSchedulers.iterator(tx); i.hasNext();) {
                Entry<String, PList> entry = i.next();
                entry.getValue().load(tx);
                schedulers.put(entry.getKey(), entry.getValue());
            }
        }

        public void read(DataInput is) throws IOException {
            this.storedSchedulers = new BTreeIndex<String, PList>(pageFile, is.readLong());
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
        private final PListStore store;

        MetaDataMarshaller(PListStore store) {
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

    class ValueMarshaller extends VariableMarshaller<List<EntryLocation>> {
        public List<EntryLocation> readPayload(DataInput dataIn) throws IOException {
            List<EntryLocation> result = new ArrayList<EntryLocation>();
            int size = dataIn.readInt();
            for (int i = 0; i < size; i++) {
                EntryLocation jobLocation = new EntryLocation();
                jobLocation.readExternal(dataIn);
                result.add(jobLocation);
            }
            return result;
        }

        public void writePayload(List<EntryLocation> value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.size());
            for (EntryLocation jobLocation : value) {
                jobLocation.writeExternal(dataOut);
            }
        }
    }

    class JobSchedulerMarshaller extends VariableMarshaller<PList> {
        private final PListStore store;
        JobSchedulerMarshaller(PListStore store) {
            this.store = store;
        }
        public PList readPayload(DataInput dataIn) throws IOException {
            PList result = new PList(this.store);
            result.read(dataIn);
            return result;
        }

        public void writePayload(PList js, DataOutput dataOut) throws IOException {
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
        synchronized (this) {
            if (!initialized) {
                return 0;
            }
        }
        try {
            return journal.getDiskSize() + pageFile.getDiskSize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    synchronized public PList getPList(final String name) throws Exception {
        if (!isStarted()) {
            throw new IllegalStateException("Not started");
        }
        intialize();
        PList result = this.persistentLists.get(name);
        if (result == null) {
            final PList pl = new PList(this);
            pl.setName(name);
            getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    pl.setRootId(tx.allocate().getPageId());
                    pl.load(tx);
                    metaData.storedSchedulers.put(tx, name, pl);
                }
            });
            result = pl;
            this.persistentLists.put(name, pl);
        }
        final PList load = result;
        getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                load.load(tx);
            }
        });

        return result;
    }

    synchronized public boolean removePList(final String name) throws Exception {
        boolean result = false;
        final PList pl = this.persistentLists.remove(name);
        result = pl != null;
        if (result) {
            getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    metaData.storedSchedulers.remove(tx, name);
                    pl.destroy(tx);
                }
            });
        }
        return result;
    }

    protected synchronized void intialize() throws Exception {
        if (isStarted()) {
            if (this.initialized == false) {
                this.initialized = true;
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
                this.pageFile = new PageFile(directory, "tmpDB");
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
                        metaData.loadLists(tx, persistentLists);
                    }
                });

                this.pageFile.flush();
                LOG.info(this + " initialized");
            }
        }
    }

    @Override
    protected synchronized void doStart() throws Exception {
        LOG.info(this + " started");
    }

    @Override
    protected synchronized void doStop(ServiceStopper stopper) throws Exception {
        for (PList pl : this.persistentLists.values()) {
            pl.unload();
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
        this.initialized = false;
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
        if (logId != Location.NOT_SET) {
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
    }

    synchronized ByteSequence getPayload(Location location) throws IllegalStateException, IOException {
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
        return "PListStore:" + this.directory;
    }

}
