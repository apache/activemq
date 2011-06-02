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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.journal.Journal;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.LockFile;
import org.apache.kahadb.util.StringMarshaller;
import org.apache.kahadb.util.VariableMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @org.apache.xbean.XBean
 */
public class PListStore extends ServiceSupport implements BrokerServiceAware, Runnable {
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
    private Scheduler scheduler;
    private long cleanupInterval = 30000;

    private int indexPageSize = PageFile.DEFAULT_PAGE_SIZE;
    private int indexCacheSize = PageFile.DEFAULT_PAGE_CACHE_SIZE;
    private int indexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE;

    public Object getIndexLock() {
        return indexLock;
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.scheduler = brokerService.getScheduler();
    }

    public int getIndexPageSize() {
        return indexPageSize;
    }

    public int getIndexCacheSize() {
        return indexCacheSize;
    }

    public int getIndexWriteBatchSize() {
        return indexWriteBatchSize;
    }

    public void setIndexPageSize(int indexPageSize) {
        this.indexPageSize = indexPageSize;
    }

    public void setIndexCacheSize(int indexCacheSize) {
        this.indexCacheSize = indexCacheSize;
    }

    public void setIndexWriteBatchSize(int indexWriteBatchSize) {
        this.indexWriteBatchSize = indexWriteBatchSize;
    }

    protected class MetaData {
        protected MetaData(PListStore store) {
            this.store = store;
        }

        private final PListStore store;
        Page<MetaData> page;
        BTreeIndex<String, PList> lists;

        void createIndexes(Transaction tx) throws IOException {
            this.lists = new BTreeIndex<String, PList>(pageFile, tx.allocate().getPageId());
        }

        void load(Transaction tx) throws IOException {
            this.lists.setKeyMarshaller(StringMarshaller.INSTANCE);
            this.lists.setValueMarshaller(new PListMarshaller(this.store));
            this.lists.load(tx);
        }

        void loadLists(Transaction tx, Map<String, PList> lists) throws IOException {
            for (Iterator<Entry<String, PList>> i = this.lists.iterator(tx); i.hasNext();) {
                Entry<String, PList> entry = i.next();
                entry.getValue().load(tx);
                lists.put(entry.getKey(), entry.getValue());
            }
        }

        public void read(DataInput is) throws IOException {
            this.lists = new BTreeIndex<String, PList>(pageFile, is.readLong());
            this.lists.setKeyMarshaller(StringMarshaller.INSTANCE);
            this.lists.setValueMarshaller(new PListMarshaller(this.store));
        }

        public void write(DataOutput os) throws IOException {
            os.writeLong(this.lists.getPageId());
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

    class PListMarshaller extends VariableMarshaller<PList> {
        private final PListStore store;
        PListMarshaller(PListStore store) {
            this.store = store;
        }
        public PList readPayload(DataInput dataIn) throws IOException {
            PList result = new PList(this.store);
            result.read(dataIn);
            return result;
        }

        public void writePayload(PList list, DataOutput dataOut) throws IOException {
            list.write(dataOut);
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

    public PList getPList(final String name) throws Exception {
        if (!isStarted()) {
            throw new IllegalStateException("Not started");
        }
        intialize();
        synchronized (indexLock) {
            synchronized (this) {
                PList result = this.persistentLists.get(name);
                if (result == null) {
                    final PList pl = new PList(this);
                    pl.setName(name);
                    getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                        public void execute(Transaction tx) throws IOException {
                            pl.setHeadPageId(tx.allocate().getPageId());
                            pl.load(tx);
                            metaData.lists.put(tx, name, pl);
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
        }
    }

    public boolean removePList(final String name) throws Exception {
        boolean result = false;
        synchronized (indexLock) {
            synchronized (this) {
                final PList pl = this.persistentLists.remove(name);
                result = pl != null;
                if (result) {
                    getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                        public void execute(Transaction tx) throws IOException {
                            metaData.lists.remove(tx, name);
                            pl.destroy();
                        }
                    });
                }
            }
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
                this.pageFile.setPageSize(getIndexPageSize());
                this.pageFile.setWriteBatchSize(getIndexWriteBatchSize());
                this.pageFile.setPageCacheSize(getIndexCacheSize());
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

                if (cleanupInterval > 0) {
                    if (scheduler == null) {
                        scheduler = new Scheduler(PListStore.class.getSimpleName());
                        scheduler.start();
                    }
                    scheduler.executePeriodically(this, cleanupInterval);
                }
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
        if (scheduler != null) {
            if (PListStore.class.getSimpleName().equals(scheduler.getName())) {
                scheduler.stop();
                scheduler = null;
            }
        }
        for (PList pl : this.persistentLists.values()) {
            pl.unload(null);
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

    public void run() {
        try {
            final Set<Integer> candidates = journal.getFileMap().keySet();
            LOG.trace("Full gc candidate set:" + candidates);
            if (candidates.size() > 1) {
                List<PList> plists = null;
                synchronized (this) {
                    plists = new ArrayList(persistentLists.values());
                }
                for (PList list : plists) {
                    list.claimFileLocations(candidates);
                    if (isStopping()) {
                        return;
                    }
                    LOG.trace("Remaining gc candidate set after refs from: " + list.getName() + ":" + candidates);
                }
                LOG.trace("GC Candidate set:" + candidates);
                this.journal.removeDataFiles(candidates);
            }
        } catch (IOException e) {
            LOG.error("Exception on periodic cleanup: " + e, e);
        }
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

    public long getCleanupInterval() {
        return cleanupInterval;
    }

    public void setCleanupInterval(long cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    @Override
    public String toString() {
        String path = getDirectory() != null ? getDirectory().getAbsolutePath() : "DIRECTORY_NOT_SET";
        return "PListStore:[" + path + " ]";
    }

}
