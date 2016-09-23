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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.JournaledStore;
import org.apache.activemq.store.PList;
import org.apache.activemq.store.PListStore;
import org.apache.activemq.store.kahadb.disk.index.BTreeIndex;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Page;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.store.kahadb.disk.util.VariableMarshaller;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.util.*;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * @org.apache.xbean.XBean
 */
public class PListStoreImpl extends ServiceSupport implements BrokerServiceAware, Runnable, PListStore, JournaledStore {
    static final Logger LOG = LoggerFactory.getLogger(PListStoreImpl.class);
    private static final int DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

    static final int CLOSED_STATE = 1;
    static final int OPEN_STATE = 2;

    private File directory;
    private File indexDirectory;
    PageFile pageFile;
    private Journal journal;
    private LockFile lockFile;
    private boolean failIfDatabaseIsLocked;
    private int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    private int journalMaxWriteBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    private boolean enableIndexWriteAsync = false;
    private boolean initialized = false;
    private boolean lazyInit = true;
    // private int indexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE;
    MetaData metaData = new MetaData(this);
    final MetaDataMarshaller metaDataMarshaller = new MetaDataMarshaller(this);
    Map<String, PListImpl> persistentLists = new HashMap<String, PListImpl>();
    final Object indexLock = new Object();
    private Scheduler scheduler;
    private long cleanupInterval = 30000;

    private int indexPageSize = PageFile.DEFAULT_PAGE_SIZE;
    private int indexCacheSize = PageFile.DEFAULT_PAGE_CACHE_SIZE;
    private int indexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE;
    private boolean indexEnablePageCaching = true;

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

    public boolean getIndexEnablePageCaching() {
        return indexEnablePageCaching;
    }

    public void setIndexEnablePageCaching(boolean indexEnablePageCaching) {
        this.indexEnablePageCaching = indexEnablePageCaching;
    }

    protected class MetaData {
        protected MetaData(PListStoreImpl store) {
            this.store = store;
        }

        private final PListStoreImpl store;
        Page<MetaData> page;
        BTreeIndex<String, PListImpl> lists;

        void createIndexes(Transaction tx) throws IOException {
            this.lists = new BTreeIndex<String, PListImpl>(pageFile, tx.allocate().getPageId());
        }

        void load(Transaction tx) throws IOException {
            this.lists.setKeyMarshaller(StringMarshaller.INSTANCE);
            this.lists.setValueMarshaller(new PListMarshaller(this.store));
            this.lists.load(tx);
        }

        void loadLists(Transaction tx, Map<String, PListImpl> lists) throws IOException {
            for (Iterator<Entry<String, PListImpl>> i = this.lists.iterator(tx); i.hasNext();) {
                Entry<String, PListImpl> entry = i.next();
                entry.getValue().load(tx);
                lists.put(entry.getKey(), entry.getValue());
            }
        }

        public void read(DataInput is) throws IOException {
            this.lists = new BTreeIndex<String, PListImpl>(pageFile, is.readLong());
            this.lists.setKeyMarshaller(StringMarshaller.INSTANCE);
            this.lists.setValueMarshaller(new PListMarshaller(this.store));
        }

        public void write(DataOutput os) throws IOException {
            os.writeLong(this.lists.getPageId());
        }
    }

    class MetaDataMarshaller extends VariableMarshaller<MetaData> {
        private final PListStoreImpl store;

        MetaDataMarshaller(PListStoreImpl store) {
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

    class PListMarshaller extends VariableMarshaller<PListImpl> {
        private final PListStoreImpl store;
        PListMarshaller(PListStoreImpl store) {
            this.store = store;
        }
        @Override
        public PListImpl readPayload(DataInput dataIn) throws IOException {
            PListImpl result = new PListImpl(this.store);
            result.read(dataIn);
            return result;
        }

        @Override
        public void writePayload(PListImpl list, DataOutput dataOut) throws IOException {
            list.write(dataOut);
        }
    }

    public Journal getJournal() {
        return this.journal;
    }

    @Override
    public File getDirectory() {
        return directory;
    }

    @Override
    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public File getIndexDirectory() {
        return indexDirectory != null ? indexDirectory : directory;
    }

    public void setIndexDirectory(File indexDirectory) {
        this.indexDirectory = indexDirectory;
    }

    @Override
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

    @Override
    public PListImpl getPList(final String name) throws Exception {
        if (!isStarted()) {
            throw new IllegalStateException("Not started");
        }
        intialize();
        synchronized (indexLock) {
            synchronized (this) {
                PListImpl result = this.persistentLists.get(name);
                if (result == null) {
                    final PListImpl pl = new PListImpl(this);
                    pl.setName(name);
                    getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            pl.setHeadPageId(tx.allocate().getPageId());
                            pl.load(tx);
                            metaData.lists.put(tx, name, pl);
                        }
                    });
                    result = pl;
                    this.persistentLists.put(name, pl);
                }
                final PListImpl toLoad = result;
                getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        toLoad.load(tx);
                    }
                });

                return result;
            }
        }
    }

    @Override
    public boolean removePList(final String name) throws Exception {
        boolean result = false;
        synchronized (indexLock) {
            synchronized (this) {
                final PList pl = this.persistentLists.remove(name);
                result = pl != null;
                if (result) {
                    getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                        @Override
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
                if (this.directory == null) {
                    this.directory = getDefaultDirectory();
                }
                IOHelper.mkdirs(this.directory);
                IOHelper.deleteChildren(this.directory);
                if (this.indexDirectory != null) {
                    IOHelper.mkdirs(this.indexDirectory);
                    IOHelper.deleteChildren(this.indexDirectory);
                }
                lock();
                this.journal = new Journal();
                this.journal.setDirectory(directory);
                this.journal.setMaxFileLength(getJournalMaxFileLength());
                this.journal.setWriteBatchSize(getJournalMaxWriteBatchSize());
                this.journal.start();
                this.pageFile = new PageFile(getIndexDirectory(), "tmpDB");
                this.pageFile.setEnablePageCaching(getIndexEnablePageCaching());
                this.pageFile.setPageSize(getIndexPageSize());
                this.pageFile.setWriteBatchSize(getIndexWriteBatchSize());
                this.pageFile.setPageCacheSize(getIndexCacheSize());
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
                        metaData.loadLists(tx, persistentLists);
                    }
                });
                this.pageFile.flush();

                if (cleanupInterval > 0) {
                    if (scheduler == null) {
                        scheduler = new Scheduler(PListStoreImpl.class.getSimpleName());
                        scheduler.start();
                    }
                    scheduler.executePeriodically(this, cleanupInterval);
                }
                this.initialized = true;
                LOG.info(this + " initialized");
            }
        }
    }

    protected File getDefaultDirectory() {
        return new File(IOHelper.getDefaultDataDirectory() + File.pathSeparator + "delayedDB");
    }

    protected void cleanupDirectory(final File dir) {
        if (dir != null && dir.exists()) {
            IOHelper.delete(dir);
        }
    }

    @Override
    protected synchronized void doStart() throws Exception {
        if (!lazyInit) {
            intialize();
        } else {
            if (this.directory == null) {
                this.directory = getDefaultDirectory();
            }
            //Go ahead and clean up previous data on start up
            cleanupDirectory(this.directory);
            cleanupDirectory(this.indexDirectory);
        }
        LOG.info(this + " started");
    }

    @Override
    protected synchronized void doStop(ServiceStopper stopper) throws Exception {
        if (scheduler != null) {
            if (PListStoreImpl.class.getSimpleName().equals(scheduler.getName())) {
                scheduler.stop();
                scheduler = null;
            }
        }
        for (PListImpl pl : this.persistentLists.values()) {
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

    @Override
    public void run() {
        try {
            if (isStopping()) {
                return;
            }
            final int lastJournalFileId = journal.getLastAppendLocation().getDataFileId();
            final Set<Integer> candidates = journal.getFileMap().keySet();
            LOG.trace("Full gc candidate set:" + candidates);
            if (candidates.size() > 1) {
                // prune current write
                for (Iterator<Integer> iterator = candidates.iterator(); iterator.hasNext();) {
                    if (iterator.next() >= lastJournalFileId) {
                        iterator.remove();
                    }
                }
                List<PListImpl> plists = null;
                synchronized (indexLock) {
                    synchronized (this) {
                        plists = new ArrayList<PListImpl>(persistentLists.values());
                    }
                }
                for (PListImpl list : plists) {
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

    @Override
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

    public boolean isLazyInit() {
        return lazyInit;
    }

    public void setLazyInit(boolean lazyInit) {
        this.lazyInit = lazyInit;
    }

    @Override
    public String toString() {
        String path = getDirectory() != null ? getDirectory().getAbsolutePath() : "DIRECTORY_NOT_SET";
        if (indexDirectory != null) {
            path += "|" + indexDirectory.getAbsolutePath();
        }
        return "PListStore:[" + path + "]";
    }
}
