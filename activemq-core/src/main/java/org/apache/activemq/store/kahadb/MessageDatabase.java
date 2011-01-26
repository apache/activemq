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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.ActiveMQMessageAuditNoSync;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaCommitCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.data.KahaLocalTransactionId;
import org.apache.activemq.store.kahadb.data.KahaPrepareCommand;
import org.apache.activemq.store.kahadb.data.KahaProducerAuditCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaRollbackCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.data.KahaTraceCommand;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.store.kahadb.data.KahaXATransactionId;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.index.BTreeVisitor;
import org.apache.kahadb.journal.DataFile;
import org.apache.kahadb.journal.Journal;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.DataByteArrayInputStream;
import org.apache.kahadb.util.DataByteArrayOutputStream;
import org.apache.kahadb.util.LockFile;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.Marshaller;
import org.apache.kahadb.util.Sequence;
import org.apache.kahadb.util.SequenceSet;
import org.apache.kahadb.util.StringMarshaller;
import org.apache.kahadb.util.VariableMarshaller;

public class MessageDatabase extends ServiceSupport implements BrokerServiceAware {
	
	protected BrokerService brokerService;

    public static final String PROPERTY_LOG_SLOW_ACCESS_TIME = "org.apache.activemq.store.kahadb.LOG_SLOW_ACCESS_TIME";
    public static final int LOG_SLOW_ACCESS_TIME = Integer.parseInt(System.getProperty(PROPERTY_LOG_SLOW_ACCESS_TIME, "0"));

    protected static final Buffer UNMATCHED;
    static {
        UNMATCHED = new Buffer(new byte[]{});
    }
    private static final Log LOG = LogFactory.getLog(MessageDatabase.class);
    private static final int DEFAULT_DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

    static final int CLOSED_STATE = 1;
    static final int OPEN_STATE = 2;
    static final long NOT_ACKED = -1;
    static final long UNMATCHED_SEQ = -2;

    static final int VERSION = 3;


    protected class Metadata {
        protected Page<Metadata> page;
        protected int state;
        protected BTreeIndex<String, StoredDestination> destinations;
        protected Location lastUpdate;
        protected Location firstInProgressTransactionLocation;
        protected Location producerSequenceIdTrackerLocation = null;
        protected transient ActiveMQMessageAuditNoSync producerSequenceIdTracker = new ActiveMQMessageAuditNoSync();
        protected int version = VERSION;
        public void read(DataInput is) throws IOException {
            state = is.readInt();
            destinations = new BTreeIndex<String, StoredDestination>(pageFile, is.readLong());
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
            }catch (EOFException expectedOnUpgrade) {
                version=1;
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
        }
    }

    class MetadataMarshaller extends VariableMarshaller<Metadata> {
        public Metadata readPayload(DataInput dataIn) throws IOException {
            Metadata rc = new Metadata();
            rc.read(dataIn);
            return rc;
        }

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
    protected File directory = new File("KahaDB");
    protected Thread checkpointThread;
    protected boolean enableJournalDiskSyncs=true;
    protected boolean archiveDataLogs;
    protected File directoryArchive;
    protected AtomicLong storeSize = new AtomicLong(0);
    long checkpointInterval = 5*1000;
    long cleanupInterval = 30*1000;
    int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    int journalMaxWriteBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    boolean enableIndexWriteAsync = false;
    int setIndexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE;
    
    
    protected AtomicBoolean opened = new AtomicBoolean();
    private LockFile lockFile;
    private boolean ignoreMissingJournalfiles = false;
    private int indexCacheSize = 10000;
    private boolean checkForCorruptJournalFiles = false;
    private boolean checksumJournalFiles = false;
    private int databaseLockedWaitDelay = DEFAULT_DATABASE_LOCKED_WAIT_DELAY;
    protected boolean forceRecoverIndex = false;
    private final Object checkpointThreadLock = new Object();

    public MessageDatabase() {
    }

    @Override
    public void doStart() throws Exception {
        load();
    }

    @Override
    public void doStop(ServiceStopper stopper) throws Exception {
        unload();
    }

	private void loadPageFile() throws IOException {
	    this.indexLock.writeLock().lock();
	    try {
		    final PageFile pageFile = getPageFile();
            pageFile.load();
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    if (pageFile.getPageCount() == 0) {
                        // First time this is created.. Initialize the metadata
                        Page<Metadata> page = tx.allocate();
                        assert page.getPageId() == 0;
                        page.set(metadata);
                        metadata.page = page;
                        metadata.state = CLOSED_STATE;
                        metadata.destinations = new BTreeIndex<String, StoredDestination>(pageFile, tx.allocate().getPageId());

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
                public void execute(Transaction tx) throws IOException {
                    for (Iterator<Entry<String, StoredDestination>> iterator = metadata.destinations.iterator(tx); iterator.hasNext();) {
                        Entry<String, StoredDestination> entry = iterator.next();
                        StoredDestination sd = loadStoredDestination(tx, entry.getKey(), entry.getValue().subscriptions!=null);
                        storedDestinations.put(entry.getKey(), sd);
                    }
                }
            });
            pageFile.flush();            
        }finally {
            this.indexLock.writeLock().unlock();
        }
	}
	
	private void startCheckpoint() {
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
                            long sleepTime = Math.min(checkpointInterval, 500);
                            while (opened.get()) {
                                Thread.sleep(sleepTime);
                                long now = System.currentTimeMillis();
                                if( now - lastCleanup >= cleanupInterval ) {
                                    checkpointCleanup(true);
                                    lastCleanup = now;
                                    lastCheckpoint = now;
                                } else if( now - lastCheckpoint >= checkpointInterval ) {
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

	public void open() throws IOException {
		if( opened.compareAndSet(false, true) ) {
            getJournal().start();
	        loadPageFile();        
	        startCheckpoint();
            recover();
		}
	}

    private void lock() throws IOException {
        if( lockFile == null ) {
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
                        LOG.info("Database "+lockFileName+" is locked... waiting " + (getDatabaseLockedWaitDelay() / 1000) + " seconds for the database to be unlocked. Reason: " + e);
                        try {
                            Thread.sleep(getDatabaseLockedWaitDelay());
                        } catch (InterruptedException e1) {
                        }
                    }
                }
            }
        }
    }

    public void load() throws IOException {
    	
        this.indexLock.writeLock().lock();
        try {
            lock();
            if (deleteAllMessages) {
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
        }finally {
            this.indexLock.writeLock().unlock();
        }

    }

    
	public void close() throws IOException, InterruptedException {
		if( opened.compareAndSet(true, false)) {
		    this.indexLock.writeLock().lock();
	        try {
	            pageFile.tx().execute(new Transaction.Closure<IOException>() {
	                public void execute(Transaction tx) throws IOException {
	                    checkpointUpdate(tx, true);
	                }
	            });
	            pageFile.unload();
	            metadata = new Metadata();
	        }finally {
	            this.indexLock.writeLock().unlock();
	        }
	        journal.close();
            synchronized (checkpointThreadLock) {
	            checkpointThread.join();
            }
	        lockFile.unlock();
	        lockFile=null;
		}
	}
	
    public void unload() throws IOException, InterruptedException {
        this.indexLock.writeLock().lock();
        try {
            if( pageFile != null && pageFile.isLoaded() ) {
                metadata.state = CLOSED_STATE;
                metadata.firstInProgressTransactionLocation = getFirstInProgressTxLocation();
    
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        tx.store(metadata.page, metadataMarshaller, true);
                    }
                });
            }
        }finally {
            this.indexLock.writeLock().unlock();
        }
        close();
    }

    /**
     * @return
     */
    private Location getFirstInProgressTxLocation() {
        Location l = null;
        synchronized (inflightTransactions) {
            if (!inflightTransactions.isEmpty()) {
                l = inflightTransactions.values().iterator().next().get(0).getLocation();
            }
            if (!preparedTransactions.isEmpty()) {
                Location t = preparedTransactions.values().iterator().next().get(0).getLocation();
                if (l==null || t.compareTo(l) <= 0) {
                    l = t;
                }
            }
        }
        return l;
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
	        Location producerAuditPosition = recoverProducerAudit();
	        Location lastIndoubtPosition = getRecoveryPosition();
	        
	        Location recoveryPosition = minimum(producerAuditPosition, lastIndoubtPosition);
	            
	        if (recoveryPosition != null) {  
	            int redoCounter = 0;
	            LOG.info("Recovering from the journal ...");
	            while (recoveryPosition != null) {
	                JournalCommand<?> message = load(recoveryPosition);
	                metadata.lastUpdate = recoveryPosition;
	                process(message, recoveryPosition, lastIndoubtPosition);
	                redoCounter++;
	                recoveryPosition = journal.getNextLocation(recoveryPosition);
	            }
	            long end = System.currentTimeMillis();
	            LOG.info("Recovery replayed " + redoCounter + " operations from the journal in " + ((end - start) / 1000.0f) + " seconds.");
	        }
	        
	        // We may have to undo some index updates.
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    recoverIndex(tx);
                }
            });
        }finally {
            this.indexLock.writeLock().unlock();
        }
    }
    
	private Location minimum(Location producerAuditPosition,
            Location lastIndoubtPosition) {
	    Location min = null;
	    if (producerAuditPosition != null) {
	        min = producerAuditPosition;
	        if (lastIndoubtPosition != null && lastIndoubtPosition.compareTo(producerAuditPosition) < 0) {
	            min = lastIndoubtPosition;
	        }
	    } else {
	        min = lastIndoubtPosition;
	    }
	    return min;
    }
	
	private Location recoverProducerAudit() throws IOException {
	    if (metadata.producerSequenceIdTrackerLocation != null) {
	        KahaProducerAuditCommand audit = (KahaProducerAuditCommand) load(metadata.producerSequenceIdTrackerLocation);
	        try {
	            ObjectInputStream objectIn = new ObjectInputStream(audit.getAudit().newInput());
	            metadata.producerSequenceIdTracker = (ActiveMQMessageAuditNoSync) objectIn.readObject();
	        } catch (ClassNotFoundException cfe) {
	            IOException ioe = new IOException("Failed to read producerAudit: " + cfe);
	            ioe.initCause(cfe);
	            throw ioe;
	        }
	        return journal.getNextLocation(metadata.producerSequenceIdTrackerLocation);
	    } else {
	        // got no audit stored so got to recreate via replay from start of the journal
	        return journal.getNextLocation(null);
	    }
    }

    protected void recoverIndex(Transaction tx) throws IOException {
        long start = System.currentTimeMillis();
        // It is possible index updates got applied before the journal updates.. 
        // in that case we need to removed references to messages that are not in the journal
        final Location lastAppendLocation = journal.getLastAppendLocation();
        long undoCounter=0;
        
        // Go through all the destinations to see if they have messages past the lastAppendLocation
        for (StoredDestination sd : storedDestinations.values()) {
        	
            final ArrayList<Long> matches = new ArrayList<Long>();
            // Find all the Locations that are >= than the last Append Location.
            sd.locationIndex.visit(tx, new BTreeVisitor.GTEVisitor<Location, Long>(lastAppendLocation) {
				@Override
				protected void matched(Location key, Long value) {
					matches.add(value);
				}
            });
            
            
            for (Long sequenceId : matches) {
                MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
                sd.locationIndex.remove(tx, keys.location);
                sd.messageIdIndex.remove(tx, keys.messageId);
                metadata.producerSequenceIdTracker.rollback(new MessageId(keys.messageId));
                undoCounter++;
                // TODO: do we need to modify the ack positions for the pub sub case?
			}
        }

        long end = System.currentTimeMillis();
        if( undoCounter > 0 ) {
        	// The rolledback operations are basically in flight journal writes.  To avoid getting these the end user
        	// should do sync writes to the journal.
	        LOG.info("Rolled back " + undoCounter + " messages from the index in " + ((end - start) / 1000.0f) + " seconds.");
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

                public boolean isInterestedInKeysBetween(Location first, Location second) {
                    if( first==null ) {
                        return !ss.contains(0, second.getDataFileId());
                    } else if( second==null ) {
                        return true;
                    } else {
                        return !ss.contains(first.getDataFileId(), second.getDataFileId());
                    }
                }

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
        HashSet<Integer> missingJournalFiles = new HashSet<Integer>();
        while( !ss.isEmpty() ) {
            missingJournalFiles.add( (int)ss.removeFirst() );
        }
        missingJournalFiles.removeAll( journal.getFileMap().keySet() );

        if( !missingJournalFiles.isEmpty() ) {
            LOG.info("Some journal files are missing: "+missingJournalFiles);
        }

        ArrayList<BTreeVisitor.Predicate<Location>> missingPredicates = new ArrayList<BTreeVisitor.Predicate<Location>>();
        for (Integer missing : missingJournalFiles) {
            missingPredicates.add(new BTreeVisitor.BetweenVisitor<Location, Long>(new Location(missing,0), new Location(missing+1,0)));
        }

        if ( checkForCorruptJournalFiles ) {
            Collection<DataFile> dataFiles = journal.getFileMap().values();
            for (DataFile dataFile : dataFiles) {
                int id = dataFile.getDataFileId();
                missingPredicates.add(new BTreeVisitor.BetweenVisitor<Location, Long>(new Location(id,dataFile.getLength()), new Location(id+1,0)));
                Sequence seq = dataFile.getCorruptedBlocks().getHead();
                while( seq!=null ) {
                    missingPredicates.add(new BTreeVisitor.BetweenVisitor<Location, Long>(new Location(id, (int) seq.getFirst()), new Location(id, (int) seq.getLast()+1)));
                    seq = seq.getNext();
                }
            }
        }

        if( !missingPredicates.isEmpty() ) {
            for (StoredDestination sd : storedDestinations.values()) {

                final ArrayList<Long> matches = new ArrayList<Long>();
                sd.locationIndex.visit(tx, new BTreeVisitor.OrVisitor<Location, Long>(missingPredicates) {
                    @Override
                    protected void matched(Location key, Long value) {
                        matches.add(value);
                    }
                });

                // If somes message references are affected by the missing data files...
                if( !matches.isEmpty() ) {

                    // We either 'gracefully' recover dropping the missing messages or
                    // we error out.
                    if( ignoreMissingJournalfiles ) {
                        // Update the index to remove the references to the missing data
                        for (Long sequenceId : matches) {
                            MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
                            sd.locationIndex.remove(tx, keys.location);
                            sd.messageIdIndex.remove(tx, keys.messageId);
                            undoCounter++;
                            // TODO: do we need to modify the ack positions for the pub sub case?
                        }

                    } else {
                        throw new IOException("Detected missing/corrupt journal files. "+matches.size()+" messages affected.");
                    }
                }
            }
        }
        
        end = System.currentTimeMillis();
        if( undoCounter > 0 ) {
        	// The rolledback operations are basically in flight journal writes.  To avoid getting these the end user
        	// should do sync writes to the journal.
	        LOG.info("Detected missing/corrupt journal files.  Dropped " + undoCounter + " messages from the index in " + ((end - start) / 1000.0f) + " seconds.");
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
	            process(message, lastRecoveryPosition);            
	            nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition);
	        }
        }finally {
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
                return journal.getNextLocation(metadata.lastUpdate);
            }
        }
        // This loads the first position.
        return journal.getNextLocation(null);
	}

    protected void checkpointCleanup(final boolean cleanup) throws IOException {
    	long start;
    	this.indexLock.writeLock().lock();
        try {
            start = System.currentTimeMillis();
        	if( !opened.get() ) {
        		return;
        	}
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    checkpointUpdate(tx, cleanup);
                }
            });
        }finally {
            this.indexLock.writeLock().unlock();
        }
    	long end = System.currentTimeMillis();
    	if( LOG_SLOW_ACCESS_TIME>0 && end-start > LOG_SLOW_ACCESS_TIME) {
    		LOG.info("Slow KahaDB access: cleanup took "+(end-start));
    	}
    }

    
	public void checkpoint(Callback closure) throws Exception {
	    this.indexLock.writeLock().lock();
        try {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    checkpointUpdate(tx, false);
                }
            });
            closure.execute();
        }finally {
            this.indexLock.writeLock().unlock();
        }
	}

    // /////////////////////////////////////////////////////////////////
    // Methods call by the broker to update and query the store.
    // /////////////////////////////////////////////////////////////////
    public Location store(JournalCommand<?> data) throws IOException {
        return store(data, false, null,null);
    }

    /**
     * All updated are are funneled through this method. The updates are converted
     * to a JournalMessage which is logged to the journal and then the data from
     * the JournalMessage is used to update the index just like it would be done
     * during a recovery process.
     */
    public Location store(JournalCommand<?> data, boolean sync, Runnable before,Runnable after) throws IOException {
    	if (before != null) {
    	    before.run();
    	}
        try {
            int size = data.serializedSizeFramed();
            DataByteArrayOutputStream os = new DataByteArrayOutputStream(size + 1);
            os.writeByte(data.type().getNumber());
            data.writeFramed(os);
    
            long start = System.currentTimeMillis();
            Location location = journal.write(os.toByteSequence(), sync);
            long start2 = System.currentTimeMillis();
            process(data, location);
        	long end = System.currentTimeMillis();
        	if( LOG_SLOW_ACCESS_TIME>0 && end-start > LOG_SLOW_ACCESS_TIME) {
        		LOG.info("Slow KahaDB access: Journal append took: "+(start2-start)+" ms, Index update took "+(end-start2)+" ms");
        	}
    
        	this.indexLock.writeLock().lock();
            try {
            	metadata.lastUpdate = location;
            }finally {
                this.indexLock.writeLock().unlock();
            }
            if (!checkpointThread.isAlive()) {
                startCheckpoint();
            }
            if (after != null) {
                after.run();
            }
            return location;
    	} catch (IOException ioe) {
            LOG.error("KahaDB failed to store to Journal", ioe);
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
        ByteSequence data = journal.read(location);
        DataByteArrayInputStream is = new DataByteArrayInputStream(data);
        byte readByte = is.readByte();
        KahaEntryType type = KahaEntryType.valueOf(readByte);
        if( type == null ) {
            throw new IOException("Could not load journal record. Invalid location: "+location);
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
            process(data, location);
        } else {
            // just recover producer audit
            data.visit(new Visitor() {
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

    void process(JournalCommand<?> data, final Location location) throws IOException {
        data.visit(new Visitor() {
            @Override
            public void visit(KahaAddMessageCommand command) throws IOException {
                process(command, location);
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
                process(command, location);
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
        });
    }

    protected void process(final KahaAddMessageCommand command, final Location location) throws IOException {
        if (command.hasTransactionInfo()) {
            List<Operation> inflightTx = getInflightTx(command.getTransactionInfo(), location);
            inflightTx.add(new AddOpperation(command, location));
        } else {
            this.indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        upadateIndex(tx, command, location);
                    }
                });
            }finally {
                this.indexLock.writeLock().unlock();
            }
        }
    }

    protected void process(final KahaRemoveMessageCommand command, final Location location) throws IOException {
        if (command.hasTransactionInfo()) {
           List<Operation> inflightTx = getInflightTx(command.getTransactionInfo(), location);
           inflightTx.add(new RemoveOpperation(command, location));
        } else {
            this.indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        updateIndex(tx, command, location);
                    }
                });
            }finally {
                this.indexLock.writeLock().unlock();
            }
        }

    }

    protected void process(final KahaRemoveDestinationCommand command, final Location location) throws IOException {
        this.indexLock.writeLock().lock();
        try {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, command, location);
                }
            });
        }finally {
            this.indexLock.writeLock().unlock();
        }
    }

    protected void process(final KahaSubscriptionCommand command, final Location location) throws IOException {
        this.indexLock.writeLock().lock();
        try {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, command, location);
                }
            });
        }finally {
            this.indexLock.writeLock().unlock();
        }
    }

    protected void process(KahaCommitCommand command, Location location) throws IOException {
        TransactionId key = key(command.getTransactionInfo());
        List<Operation> inflightTx;
        synchronized (inflightTransactions) {
            inflightTx = inflightTransactions.remove(key);
            if (inflightTx == null) {
                inflightTx = preparedTransactions.remove(key);
            }
        }
        if (inflightTx == null) {
            return;
        }

        final List<Operation> messagingTx = inflightTx;
        this.indexLock.writeLock().lock();
        try {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    for (Operation op : messagingTx) {
                        op.execute(tx);
                    }
                }
            });
        }finally {
            this.indexLock.writeLock().unlock();
        }
    }

    protected void process(KahaPrepareCommand command, Location location) {
        TransactionId key = key(command.getTransactionInfo());
        synchronized (inflightTransactions) {
            List<Operation> tx = inflightTransactions.remove(key);
            if (tx != null) {
                preparedTransactions.put(key, tx);
            }
        }
    }

    protected void process(KahaRollbackCommand command, Location location) {
        TransactionId key = key(command.getTransactionInfo());
        synchronized (inflightTransactions) {
            List<Operation> tx = inflightTransactions.remove(key);
            if (tx == null) {
                preparedTransactions.remove(key);
            }
        }
    }

    // /////////////////////////////////////////////////////////////////
    // These methods do the actual index updates.
    // /////////////////////////////////////////////////////////////////

    protected final ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();
	private final HashSet<Integer> journalFilesBeingReplicated = new HashSet<Integer>();

    void upadateIndex(Transaction tx, KahaAddMessageCommand command, Location location) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);

        // Skip adding the message to the index if this is a topic and there are
        // no subscriptions.
        if (sd.subscriptions != null && sd.subscriptions.isEmpty(tx)) {
            return;
        }

        // Add the message.
        int priority = command.getPrioritySupported() ? command.getPriority() : javax.jms.Message.DEFAULT_PRIORITY;
        long id = sd.orderIndex.getNextMessageId(priority);
        Long previous = sd.locationIndex.put(tx, location, id);
        if (previous == null) {
            previous = sd.messageIdIndex.put(tx, command.getMessageId(), id);
            if (previous == null) {
                sd.orderIndex.put(tx, priority, id, new MessageKeys(command.getMessageId(), location));
                if (sd.subscriptions != null && !sd.subscriptions.isEmpty(tx)) {
                    addAckLocationForNewMessage(tx, sd, id);
                }
            } else {
                // If the message ID as indexed, then the broker asked us to
                // store a DUP
                // message. Bad BOY! Don't do it, and log a warning.
                LOG.warn("Duplicate message add attempt rejected. Destination: " + command.getDestination().getName() + ", Message id: " + command.getMessageId());
                // TODO: consider just rolling back the tx.
                sd.messageIdIndex.put(tx, command.getMessageId(), previous);
                sd.locationIndex.remove(tx, location);
            }
        } else {
            // restore the previous value.. Looks like this was a redo of a
            // previously
            // added message. We don't want to assign it a new id as the other
            // indexes would
            // be wrong..
            //
            // TODO: consider just rolling back the tx.
            sd.locationIndex.put(tx, location, previous);
        }
        // record this id in any event, initial send or recovery
        metadata.producerSequenceIdTracker.isDuplicate(command.getMessageId());
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
                    recordAckMessageReferenceLocation(ackLocation, keys.location);
                }                
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
                // The following method handles deleting un-referenced messages.
                removeAckLocation(tx, sd, subscriptionKey, sequence);
            }

        }
    }

    Map<Integer, Set<Integer>> ackMessageFileMap = new HashMap<Integer, Set<Integer>>();
    private void recordAckMessageReferenceLocation(Location ackLocation, Location messageLocation) {
        Set<Integer> referenceFileIds = ackMessageFileMap.get(Integer.valueOf(ackLocation.getDataFileId()));
        if (referenceFileIds == null) {
            referenceFileIds = new HashSet<Integer>();
            referenceFileIds.add(messageLocation.getDataFileId());
            ackMessageFileMap.put(ackLocation.getDataFileId(), referenceFileIds);
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
            tx.free(sd.ackPositions.getPageId());
        }

        String key = key(command.getDestination());
        storedDestinations.remove(key);
        metadata.destinations.remove(tx, key);
    }

    void updateIndex(Transaction tx, KahaSubscriptionCommand command, Location location) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);

        // If set then we are creating it.. otherwise we are destroying the sub
        if (command.hasSubscriptionInfo()) {
            String subscriptionKey = command.getSubscriptionKey();
            sd.subscriptions.put(tx, subscriptionKey, command);
            long ackLocation=NOT_ACKED;
            if (!command.getRetroactive()) {
                ackLocation = sd.orderIndex.nextMessageId-1;
            } else {
                addAckLocationForRetroactiveSub(tx, sd, ackLocation, subscriptionKey);
            }
            sd.subscriptionAcks.put(tx, subscriptionKey, new LastAck(ackLocation));
        } else {
            // delete the sub...
            String subscriptionKey = command.getSubscriptionKey();
            sd.subscriptions.remove(tx, subscriptionKey);
            sd.subscriptionAcks.remove(tx, subscriptionKey);
            removeAckLocationsForSub(tx, sd, subscriptionKey);
        }
    }
    
    /**
     * @param tx
     * @throws IOException
     */
    void checkpointUpdate(Transaction tx, boolean cleanup) throws IOException {
        LOG.debug("Checkpoint started.");
        
        metadata.state = OPEN_STATE;
        metadata.producerSequenceIdTrackerLocation = checkpointProducerAudit();
        metadata.firstInProgressTransactionLocation = getFirstInProgressTxLocation();
        tx.store(metadata.page, metadataMarshaller, true);
        pageFile.flush();

        if( cleanup ) {

            final TreeSet<Integer> completeFileSet = new TreeSet<Integer>(journal.getFileMap().keySet());
            final TreeSet<Integer> gcCandidateSet = new TreeSet<Integer>(completeFileSet);
        	
        	// Don't GC files under replication
        	if( journalFilesBeingReplicated!=null ) {
        		gcCandidateSet.removeAll(journalFilesBeingReplicated);
        	}
        	
        	// Don't GC files after the first in progress tx
        	Location firstTxLocation = metadata.lastUpdate;
            if( metadata.firstInProgressTransactionLocation!=null ) {
                firstTxLocation = metadata.firstInProgressTransactionLocation;
            }
            
            if( firstTxLocation!=null ) {
            	while( !gcCandidateSet.isEmpty() ) {
            		Integer last = gcCandidateSet.last();
            		if( last >= firstTxLocation.getDataFileId() ) {
            			gcCandidateSet.remove(last);
            		} else {
            			break;
            		}
            	}
                LOG.trace("gc candidates after first tx:" + firstTxLocation + ", " + gcCandidateSet);
            }

            // Go through all the destinations to see if any of them can remove GC candidates.
            for (Entry<String, StoredDestination> entry : storedDestinations.entrySet()) {
            	if( gcCandidateSet.isEmpty() ) {
                	break;
                }
                
                // Use a visitor to cut down the number of pages that we load
                entry.getValue().locationIndex.visit(tx, new BTreeVisitor<Location, Long>() {
                    int last=-1;
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
                LOG.trace("gc candidates after dest:" + entry.getKey() + ", " + gcCandidateSet);
            }

            // check we are not deleting file with ack for in-use journal files
            LOG.trace("gc candidates: " + gcCandidateSet);
            final TreeSet<Integer> gcCandidates = new TreeSet<Integer>(gcCandidateSet);
            Iterator<Integer> candidates = gcCandidateSet.iterator();
            while (candidates.hasNext()) {
                Integer candidate = candidates.next();
                Set<Integer> referencedFileIds = ackMessageFileMap.get(candidate);
                if (referencedFileIds != null) {
                    for (Integer referencedFileId : referencedFileIds) {
                        if (completeFileSet.contains(referencedFileId) && !gcCandidates.contains(referencedFileId)) {
                            // active file that is not targeted for deletion is referenced so don't delete
                            candidates.remove();
                            break;
                        }
                    }
                    if (gcCandidateSet.contains(candidate)) {
                        ackMessageFileMap.remove(candidate);
                    } else {
                        LOG.trace("not removing data file: " + candidate
                                + " as contained ack(s) refer to referenced file: " + referencedFileIds);
                    }
                }
            }

            if( !gcCandidateSet.isEmpty() ) {
	            LOG.debug("Cleanup removing the data files: "+gcCandidateSet);
	            journal.removeDataFiles(gcCandidateSet);
            }
        }
        
        LOG.debug("Checkpoint done.");
    }
    
    private Location checkpointProducerAudit() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oout = new ObjectOutputStream(baos);
        oout.writeObject(metadata.producerSequenceIdTracker);
        oout.flush();
        oout.close();
        return store(new KahaProducerAuditCommand().setAudit(new Buffer(baos.toByteArray())), true, null, null);
    }

    public HashSet<Integer> getJournalFilesBeingReplicated() {
		return journalFilesBeingReplicated;
	}

    // /////////////////////////////////////////////////////////////////
    // StoredDestination related implementation methods.
    // /////////////////////////////////////////////////////////////////


	private final HashMap<String, StoredDestination> storedDestinations = new HashMap<String, StoredDestination>();

    class StoredSubscription {
        SubscriptionInfo subscriptionInfo;
        String lastAckId;
        Location lastAckLocation;
        Location cursor;
    }
    
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
    
    static protected class MessageKeysMarshaller extends VariableMarshaller<MessageKeys> {
        static final MessageKeysMarshaller INSTANCE = new MessageKeysMarshaller();
        
        public MessageKeys readPayload(DataInput dataIn) throws IOException {
            return new MessageKeys(dataIn.readUTF(), LocationMarshaller.INSTANCE.readPayload(dataIn));
        }

        public void writePayload(MessageKeys object, DataOutput dataOut) throws IOException {
            dataOut.writeUTF(object.messageId);
            LocationMarshaller.INSTANCE.writePayload(object.location, dataOut);
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

        public String toString() {
            return "[" + lastAckedSequence + ":" + priority + "]";
        }
    }

    protected class LastAckMarshaller implements Marshaller<LastAck> {
        
        public void writePayload(LastAck object, DataOutput dataOut) throws IOException {
            dataOut.writeLong(object.lastAckedSequence);
            dataOut.writeByte(object.priority);
        }

        public LastAck readPayload(DataInput dataIn) throws IOException {
            LastAck lastAcked = new LastAck();
            lastAcked.lastAckedSequence = dataIn.readLong();
            if (metadata.version >= 3) {
                lastAcked.priority = dataIn.readByte();
            }
            return lastAcked;
        }

        public int getFixedSize() {
            return 9;
        }

        public LastAck deepCopy(LastAck source) {
            return new LastAck(source);
        }

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
        BTreeIndex<Long, HashSet<String>> ackPositions;
    }

    protected class StoredDestinationMarshaller extends VariableMarshaller<StoredDestination> {

        public StoredDestination readPayload(DataInput dataIn) throws IOException {
            final StoredDestination value = new StoredDestination();
            value.orderIndex.defaultPriorityIndex = new BTreeIndex<Long, MessageKeys>(pageFile, dataIn.readLong());
            value.locationIndex = new BTreeIndex<Location, Long>(pageFile, dataIn.readLong());
            value.messageIdIndex = new BTreeIndex<String, Long>(pageFile, dataIn.readLong());

            if (dataIn.readBoolean()) {
                value.subscriptions = new BTreeIndex<String, KahaSubscriptionCommand>(pageFile, dataIn.readLong());
                value.subscriptionAcks = new BTreeIndex<String, LastAck>(pageFile, dataIn.readLong());
                if (metadata.version >= 3) {
                    value.ackPositions = new BTreeIndex<Long, HashSet<String>>(pageFile, dataIn.readLong());
                } else {
                    // upgrade
                    pageFile.tx().execute(new Transaction.Closure<IOException>() {
                        public void execute(Transaction tx) throws IOException {
                            value.ackPositions = new BTreeIndex<Long, HashSet<String>>(pageFile, tx.allocate());
                            value.ackPositions.setKeyMarshaller(LongMarshaller.INSTANCE);
                            value.ackPositions.setValueMarshaller(HashSetStringMarshaller.INSTANCE);
                            value.ackPositions.load(tx);
                        }
                    });
                }
            }
            if (metadata.version >= 2) {
                value.orderIndex.lowPriorityIndex = new BTreeIndex<Long, MessageKeys>(pageFile, dataIn.readLong());
                value.orderIndex.highPriorityIndex = new BTreeIndex<Long, MessageKeys>(pageFile, dataIn.readLong());
            } else {
                    // upgrade
                    pageFile.tx().execute(new Transaction.Closure<IOException>() {
                        public void execute(Transaction tx) throws IOException {
                            value.orderIndex.lowPriorityIndex = new BTreeIndex<Long, MessageKeys>(pageFile, tx.allocate());
                            value.orderIndex.lowPriorityIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
                            value.orderIndex.lowPriorityIndex.setValueMarshaller(MessageKeysMarshaller.INSTANCE);
                            value.orderIndex.lowPriorityIndex.load(tx);

                            value.orderIndex.highPriorityIndex = new BTreeIndex<Long, MessageKeys>(pageFile, tx.allocate());
                            value.orderIndex.highPriorityIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
                            value.orderIndex.highPriorityIndex.setValueMarshaller(MessageKeysMarshaller.INSTANCE);
                            value.orderIndex.highPriorityIndex.load(tx);
                        }
                    });
            }

            return value;
        }

        public void writePayload(StoredDestination value, DataOutput dataOut) throws IOException {
            dataOut.writeLong(value.orderIndex.defaultPriorityIndex.getPageId());
            dataOut.writeLong(value.locationIndex.getPageId());
            dataOut.writeLong(value.messageIdIndex.getPageId());
            if (value.subscriptions != null) {
                dataOut.writeBoolean(true);
                dataOut.writeLong(value.subscriptions.getPageId());
                dataOut.writeLong(value.subscriptionAcks.getPageId());
                dataOut.writeLong(value.ackPositions.getPageId());
            } else {
                dataOut.writeBoolean(false);
            }
            dataOut.writeLong(value.orderIndex.lowPriorityIndex.getPageId());
            dataOut.writeLong(value.orderIndex.highPriorityIndex.getPageId());
        }
    }

    static class LocationMarshaller implements Marshaller<Location> {
        final static LocationMarshaller INSTANCE = new LocationMarshaller();

        public Location readPayload(DataInput dataIn) throws IOException {
            Location rc = new Location();
            rc.setDataFileId(dataIn.readInt());
            rc.setOffset(dataIn.readInt());
            return rc;
        }

        public void writePayload(Location object, DataOutput dataOut) throws IOException {
            dataOut.writeInt(object.getDataFileId());
            dataOut.writeInt(object.getOffset());
        }

        public int getFixedSize() {
            return 8;
        }

        public Location deepCopy(Location source) {
            return new Location(source);
        }

        public boolean isDeepCopySupported() {
            return true;
        }
    }

    static class KahaSubscriptionCommandMarshaller extends VariableMarshaller<KahaSubscriptionCommand> {
        final static KahaSubscriptionCommandMarshaller INSTANCE = new KahaSubscriptionCommandMarshaller();

        public KahaSubscriptionCommand readPayload(DataInput dataIn) throws IOException {
            KahaSubscriptionCommand rc = new KahaSubscriptionCommand();
            rc.mergeFramed((InputStream)dataIn);
            return rc;
        }

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
            rc.locationIndex = new BTreeIndex<Location, Long>(pageFile, tx.allocate());
            rc.messageIdIndex = new BTreeIndex<String, Long>(pageFile, tx.allocate());

            if (topic) {
                rc.subscriptions = new BTreeIndex<String, KahaSubscriptionCommand>(pageFile, tx.allocate());
                rc.subscriptionAcks = new BTreeIndex<String, LastAck>(pageFile, tx.allocate());
                rc.ackPositions = new BTreeIndex<Long, HashSet<String>>(pageFile, tx.allocate());
            }
            metadata.destinations.put(tx, key, rc);
        }

        // Configure the marshalers and load.
        rc.orderIndex.load(tx);

        // Figure out the next key using the last entry in the destination.
        rc.orderIndex.configureLast(tx);

        rc.locationIndex.setKeyMarshaller(LocationMarshaller.INSTANCE);
        rc.locationIndex.setValueMarshaller(LongMarshaller.INSTANCE);
        rc.locationIndex.load(tx);

        rc.messageIdIndex.setKeyMarshaller(StringMarshaller.INSTANCE);
        rc.messageIdIndex.setValueMarshaller(LongMarshaller.INSTANCE);
        rc.messageIdIndex.load(tx);
        
        // If it was a topic...
        if (topic) {

            rc.subscriptions.setKeyMarshaller(StringMarshaller.INSTANCE);
            rc.subscriptions.setValueMarshaller(KahaSubscriptionCommandMarshaller.INSTANCE);
            rc.subscriptions.load(tx);

            rc.subscriptionAcks.setKeyMarshaller(StringMarshaller.INSTANCE);
            rc.subscriptionAcks.setValueMarshaller(new LastAckMarshaller());
            rc.subscriptionAcks.load(tx);

            rc.ackPositions.setKeyMarshaller(LongMarshaller.INSTANCE);
            rc.ackPositions.setValueMarshaller(HashSetStringMarshaller.INSTANCE);
            rc.ackPositions.load(tx);

            rc.subscriptionCursors = new HashMap<String, MessageOrderCursor>();

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
            
            if (rc.orderIndex.nextMessageId == 0) {
                // check for existing durable sub all acked out - pull next seq from acks as messages are gone
                if (!rc.subscriptionAcks.isEmpty(tx)) {
                    for (Iterator<Entry<String, LastAck>> iterator = rc.subscriptionAcks.iterator(tx); iterator.hasNext();) {
                        Entry<String, LastAck> entry = iterator.next();
                        rc.orderIndex.nextMessageId =
                                Math.max(rc.orderIndex.nextMessageId, entry.getValue().lastAckedSequence +1);
                    }
                }
            }

        }

        if (metadata.version < 3) {
            // store again after upgrade
            metadata.destinations.put(tx, key, rc);
        }        
        return rc;
    }

    private void addAckLocation(Transaction tx, StoredDestination sd, Long messageSequence, String subscriptionKey) throws IOException {
        HashSet<String> hs = sd.ackPositions.get(tx, messageSequence);
        if (hs == null) {
            hs = new HashSet<String>();
        }
        hs.add(subscriptionKey);
        // every ack location addition needs to be a btree modification to get it stored
        sd.ackPositions.put(tx, messageSequence, hs);
    }

    // new sub is interested in potentially all existing messages
    private void addAckLocationForRetroactiveSub(Transaction tx, StoredDestination sd, Long messageSequence, String subscriptionKey) throws IOException {
        for (Iterator<Entry<Long, HashSet<String>>> iterator = sd.ackPositions.iterator(tx, messageSequence); iterator.hasNext(); ) {
            Entry<Long, HashSet<String>> entry = iterator.next();
            entry.getValue().add(subscriptionKey);
            sd.ackPositions.put(tx, entry.getKey(), entry.getValue());
        }
    }

    // on a new message add, all existing subs are interested in this message
    private void addAckLocationForNewMessage(Transaction tx, StoredDestination sd, Long messageSequence) throws IOException {
        HashSet hs = new HashSet<String>();
        for (Iterator<Entry<String, LastAck>> iterator = sd.subscriptionAcks.iterator(tx); iterator.hasNext();) {
            Entry<String, LastAck> entry = iterator.next();
            hs.add(entry.getKey());
        }
        sd.ackPositions.put(tx, messageSequence, hs);
    }

    private void removeAckLocationsForSub(Transaction tx, StoredDestination sd, String subscriptionKey) throws IOException {
        if (!sd.ackPositions.isEmpty(tx)) {        
            Long end = sd.ackPositions.getLast(tx).getKey();
            for (Long sequence = sd.ackPositions.getFirst(tx).getKey(); sequence <= end; sequence++) {
                removeAckLocation(tx, sd, subscriptionKey, sequence);
            }
        }
    }

    /**
     * @param tx
     * @param sd
     * @param subscriptionKey
     * @param sequenceId
     * @throws IOException
     */
    private void removeAckLocation(Transaction tx, StoredDestination sd, String subscriptionKey, Long sequenceId) throws IOException {
        // Remove the sub from the previous location set..
        if (sequenceId != null) {
            HashSet<String> hs = sd.ackPositions.get(tx, sequenceId);
            if (hs != null) {
                hs.remove(subscriptionKey);
                if (hs.isEmpty()) {
                    HashSet<String> firstSet = sd.ackPositions.getFirst(tx).getValue();
                    sd.ackPositions.remove(tx, sequenceId);

                    // Find all the entries that need to get deleted.
                    ArrayList<Entry<Long, MessageKeys>> deletes = new ArrayList<Entry<Long, MessageKeys>>();
                    sd.orderIndex.getDeleteList(tx, deletes, sequenceId);

                    // Do the actual deletes.
                    for (Entry<Long, MessageKeys> entry : deletes) {
                        sd.locationIndex.remove(tx, entry.getValue().location);
                        sd.messageIdIndex.remove(tx, entry.getValue().messageId);
                        sd.orderIndex.remove(tx, entry.getKey());
                    }
                }
            }
        }
    }

    private String key(KahaDestination destination) {
        return destination.getType().getNumber() + ":" + destination.getName();
    }

    // /////////////////////////////////////////////////////////////////
    // Transaction related implementation methods.
    // /////////////////////////////////////////////////////////////////
    protected final LinkedHashMap<TransactionId, List<Operation>> inflightTransactions = new LinkedHashMap<TransactionId, List<Operation>>();
    protected final LinkedHashMap<TransactionId, List<Operation>> preparedTransactions = new LinkedHashMap<TransactionId, List<Operation>>();
 
    private List<Operation> getInflightTx(KahaTransactionInfo info, Location location) {
        TransactionId key = key(info);
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

    private TransactionId key(KahaTransactionInfo transactionInfo) {
        if (transactionInfo.hasLocalTransacitonId()) {
            KahaLocalTransactionId tx = transactionInfo.getLocalTransacitonId();
            LocalTransactionId rc = new LocalTransactionId();
            rc.setConnectionId(new ConnectionId(tx.getConnectionId()));
            rc.setValue(tx.getTransacitonId());
            return rc;
        } else {
            KahaXATransactionId tx = transactionInfo.getXaTransacitonId();
            XATransactionId rc = new XATransactionId();
            rc.setBranchQualifier(tx.getBranchQualifier().toByteArray());
            rc.setGlobalTransactionId(tx.getGlobalTransactionId().toByteArray());
            rc.setFormatId(tx.getFormatId());
            return rc;
        }
    }

    abstract class Operation {
        final Location location;

        public Operation(Location location) {
            this.location = location;
        }

        public Location getLocation() {
            return location;
        }

        abstract public void execute(Transaction tx) throws IOException;
    }

    class AddOpperation extends Operation {
        final KahaAddMessageCommand command;

        public AddOpperation(KahaAddMessageCommand command, Location location) {
            super(location);
            this.command = command;
        }

        @Override
        public void execute(Transaction tx) throws IOException {
            upadateIndex(tx, command, location);
        }

        public KahaAddMessageCommand getCommand() {
            return command;
        }
    }

    class RemoveOpperation extends Operation {
        final KahaRemoveMessageCommand command;

        public RemoveOpperation(KahaRemoveMessageCommand command, Location location) {
            super(location);
            this.command = command;
        }

        @Override
        public void execute(Transaction tx) throws IOException {
            updateIndex(tx, command, location);
        }

        public KahaRemoveMessageCommand getCommand() {
            return command;
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Initialization related implementation methods.
    // /////////////////////////////////////////////////////////////////

    private PageFile createPageFile() {
        PageFile index = new PageFile(directory, "db");
        index.setEnableWriteThread(isEnableIndexWriteAsync());
        index.setWriteBatchSize(getIndexWriteBatchSize());
        index.setPageCacheSize(indexCacheSize);
        return index;
    }

    private Journal createJournal() throws IOException {
        Journal manager = new Journal();
        manager.setDirectory(directory);
        manager.setMaxFileLength(getJournalMaxFileLength());
        manager.setCheckForCorruptionOnStartup(checkForCorruptJournalFiles);
        manager.setChecksum(checksumJournalFiles || checkForCorruptJournalFiles);
        manager.setWriteBatchSize(getJournalMaxWriteBatchSize());
        manager.setArchiveDataLogs(isArchiveDataLogs());
        manager.setSizeAccumulator(storeSize);
        if (getDirectoryArchive() != null) {
            IOHelper.mkdirs(getDirectoryArchive());
            manager.setDirectoryArchive(getDirectoryArchive());
        }
        return manager;
    }

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
    
    public boolean isEnableJournalDiskSyncs() {
        return enableJournalDiskSyncs;
    }

    public void setEnableJournalDiskSyncs(boolean syncWrites) {
        this.enableJournalDiskSyncs = syncWrites;
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

    /**
     * @return the databaseLockedWaitDelay
     */
    public int getDatabaseLockedWaitDelay() {
        return this.databaseLockedWaitDelay;
    }

    /**
     * @param databaseLockedWaitDelay the databaseLockedWaitDelay to set
     */
    public void setDatabaseLockedWaitDelay(int databaseLockedWaitDelay) {
        this.databaseLockedWaitDelay = databaseLockedWaitDelay;
    }
    

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
        MessageOrderCursor cursor = new MessageOrderCursor();
        Long lastDefaultKey;
        Long lastHighKey;
        Long lastLowKey;
        byte lastGetPriority;

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
            defaultPriorityIndex.setValueMarshaller(MessageKeysMarshaller.INSTANCE);
            defaultPriorityIndex.load(tx);
            lowPriorityIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
            lowPriorityIndex.setValueMarshaller(MessageKeysMarshaller.INSTANCE);
            lowPriorityIndex.load(tx);
            highPriorityIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
            highPriorityIndex.setValueMarshaller(MessageKeysMarshaller.INSTANCE);
            highPriorityIndex.load(tx);
        }
        
        void allocate(Transaction tx) throws IOException {
            defaultPriorityIndex = new BTreeIndex<Long, MessageKeys>(pageFile, tx.allocate());
            if (metadata.version >= 2) {
                lowPriorityIndex = new BTreeIndex<Long, MessageKeys>(pageFile, tx.allocate());
                highPriorityIndex = new BTreeIndex<Long, MessageKeys>(pageFile, tx.allocate());
            }
        }
        
        void configureLast(Transaction tx) throws IOException {
            // Figure out the next key using the last entry in the destination.
            if (highPriorityIndex != null) {
                Entry<Long, MessageKeys> lastEntry = highPriorityIndex.getLast(tx);
                if (lastEntry != null) {
                    nextMessageId = lastEntry.getKey() + 1;
                } else {
                    lastEntry = defaultPriorityIndex.getLast(tx);
                    if (lastEntry != null) {
                        nextMessageId = lastEntry.getKey() + 1;
                    } else {
                        lastEntry = lowPriorityIndex.getLast(tx);
                        if (lastEntry != null) {
                            nextMessageId = lastEntry.getKey() + 1;
                        }
                    }
                }
            } else {
                Entry<Long, MessageKeys> lastEntry = defaultPriorityIndex.getLast(tx);
                if (lastEntry != null) {
                    nextMessageId = lastEntry.getKey() + 1;
                }
            }
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
                if (defaultPriorityIndex.containsKey(tx, sequence)) {
                    lastDefaultKey = nextPosition;
                    cursor.defaultCursorPosition = nextPosition.longValue();
                } else if (highPriorityIndex != null) {
                    if (highPriorityIndex.containsKey(tx, sequence)) {
                        lastHighKey = nextPosition;
                        cursor.highPriorityCursorPosition = nextPosition.longValue();
                    } else if (lowPriorityIndex.containsKey(tx, sequence)) {
                        lastLowKey = nextPosition;
                        cursor.lowPriorityCursorPosition = nextPosition.longValue();
                    }
                } else {
                    lastDefaultKey = nextPosition;
                    cursor.defaultCursorPosition = nextPosition.longValue();
                }
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

            Iterator<Entry<Long, MessageKeys>> iterator = index.iterator(tx, sequenceId);
            deletes.add(iterator.next());
        }
        
        long getNextMessageId(int priority) {
            return nextMessageId++;
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
            return new MessageOrderIterator(tx,cursor);
        }
        
        Iterator<Entry<Long, MessageKeys>> iterator(Transaction tx, MessageOrderCursor m) throws IOException{
            return new MessageOrderIterator(tx,m);
        }

        public byte lastGetPriority() {
            return lastGetPriority;
        }

        class MessageOrderIterator implements Iterator<Entry<Long, MessageKeys>>{
            Iterator<Entry<Long, MessageKeys>>currentIterator;
            final Iterator<Entry<Long, MessageKeys>>highIterator;
            final Iterator<Entry<Long, MessageKeys>>defaultIterator;
            final Iterator<Entry<Long, MessageKeys>>lowIterator;
            
            

            MessageOrderIterator(Transaction tx, MessageOrderCursor m) throws IOException {
                this.defaultIterator = defaultPriorityIndex.iterator(tx, m.defaultCursorPosition);
                if (highPriorityIndex != null) {
                    this.highIterator = highPriorityIndex.iterator(tx, m.highPriorityCursorPosition);
                } else {
                    this.highIterator = null;
                }
                if (lowPriorityIndex != null) {
                    this.lowIterator = lowPriorityIndex.iterator(tx, m.lowPriorityCursorPosition);
                } else {
                    this.lowIterator = null;
                }
            }
            
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

            public void remove() {
                throw new UnsupportedOperationException();
            }
           
        }
    }
    
    private static class HashSetStringMarshaller extends VariableMarshaller<HashSet<String>> {
        final static HashSetStringMarshaller INSTANCE = new HashSetStringMarshaller();

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
}
