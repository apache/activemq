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
package org.apache.kahadb.store;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.util.Callback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.index.BTreeVisitor;
import org.apache.kahadb.journal.Journal;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.store.data.KahaAddMessageCommand;
import org.apache.kahadb.store.data.KahaCommitCommand;
import org.apache.kahadb.store.data.KahaDestination;
import org.apache.kahadb.store.data.KahaEntryType;
import org.apache.kahadb.store.data.KahaLocalTransactionId;
import org.apache.kahadb.store.data.KahaPrepareCommand;
import org.apache.kahadb.store.data.KahaRemoveDestinationCommand;
import org.apache.kahadb.store.data.KahaRemoveMessageCommand;
import org.apache.kahadb.store.data.KahaRollbackCommand;
import org.apache.kahadb.store.data.KahaSubscriptionCommand;
import org.apache.kahadb.store.data.KahaTraceCommand;
import org.apache.kahadb.store.data.KahaTransactionInfo;
import org.apache.kahadb.store.data.KahaXATransactionId;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.DataByteArrayInputStream;
import org.apache.kahadb.util.DataByteArrayOutputStream;
import org.apache.kahadb.util.LockFile;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.Marshaller;
import org.apache.kahadb.util.StringMarshaller;

public class MessageDatabase {

    private static final Log LOG = LogFactory.getLog(MessageDatabase.class);
    private static final int DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

    public static final int CLOSED_STATE = 1;
    public static final int OPEN_STATE = 2;

    protected class Metadata {
        protected Page<Metadata> page;
        protected int state;
        protected BTreeIndex<String, StoredDestination> destinations;
        protected Location lastUpdate;
        protected Location firstInProgressTransactionLocation;

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
        }
    }

    class MetadataMarshaller implements Marshaller<Metadata> {
        public Class<Metadata> getType() {
            return Metadata.class;
        }

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
    protected File directory;
    protected Thread checkpointThread;
    protected boolean syncWrites=true;
    int checkpointInterval = 5*1000;
    int cleanupInterval = 30*1000;
    
    protected AtomicBoolean started = new AtomicBoolean();
    protected AtomicBoolean opened = new AtomicBoolean();
    private LockFile lockFile;

    public MessageDatabase() {
    }

    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
        	load();
        }
    }

    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {
            unload();
        }
    }

	private void loadPageFile() throws IOException {
		synchronized (indexMutex) {
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
            pageFile.flush();
            
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
        }
	}
	
	/**
	 * @throws IOException
	 */
	public void open() throws IOException {
		if( opened.compareAndSet(false, true) ) {
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
	                    LOG.info("Database "+lockFileName+" is locked... waiting " + (DATABASE_LOCKED_WAIT_DELAY / 1000) + " seconds for the database to be unlocked.");
	                    try {
	                        Thread.sleep(DATABASE_LOCKED_WAIT_DELAY);
	                    } catch (InterruptedException e1) {
	                    }
	                }
	            }
	        }
	        
            getJournal().start();
            
	        loadPageFile();
	        
	        checkpointThread = new Thread("ActiveMQ Journal Checkpoint Worker") {
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
	                }
	            }
	        };
	        checkpointThread.start();
            recover();
		}
	}
	
    public void load() throws IOException {
    	
        synchronized (indexMutex) {
	    	open();
	    	
	        if (deleteAllMessages) {
	            journal.delete();
	
	            pageFile.unload();
	            pageFile.delete();
	            metadata = new Metadata();
	            
	            LOG.info("Persistence store purged.");
	            deleteAllMessages = false;
	            
	            loadPageFile();
	        }
	        store(new KahaTraceCommand().setMessage("LOADED " + new Date()));

        }

    }

    
	public void close() throws IOException, InterruptedException {
		if( opened.compareAndSet(true, false)) {
	        synchronized (indexMutex) {
	            pageFile.unload();
	            metadata = new Metadata();
	        }
	        journal.close();
	        checkpointThread.join();
	        lockFile.unlock();
	        lockFile=null;
		}
	}
	
    public void unload() throws IOException, InterruptedException {
        synchronized (indexMutex) {
            if( pageFile.isLoaded() ) {
                metadata.state = CLOSED_STATE;
                metadata.firstInProgressTransactionLocation = getFirstInProgressTxLocation();
    
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        tx.store(metadata.page, metadataMarshaller, true);
                    }
                });
                close();
            }
        }
    }

    /**
     * @return
     */
    private Location getFirstInProgressTxLocation() {
        Location l = null;
        if (!inflightTransactions.isEmpty()) {
            l = inflightTransactions.values().iterator().next().get(0).getLocation();
        }
        if (!preparedTransactions.isEmpty()) {
            Location t = preparedTransactions.values().iterator().next().get(0).getLocation();
            if (l==null || t.compareTo(l) <= 0) {
                l = t;
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
     * @throws InvalidLocationException
     * @throws IllegalStateException
     */
    private void recover() throws IllegalStateException, IOException {
        long start = System.currentTimeMillis();
        
        Location recoveryPosition = getRecoveryPosition();
        if( recoveryPosition ==null ) {
        	return;
        }
        
        int redoCounter = 0;
        LOG.info("Journal Recovery Started from: " + journal + " at " + recoveryPosition.getDataFileId() + ":" + recoveryPosition.getOffset());

        while (recoveryPosition != null) {
            JournalCommand message = load(recoveryPosition);
            metadata.lastUpdate = recoveryPosition;
            process(message, recoveryPosition);
            redoCounter++;
            recoveryPosition = journal.getNextLocation(recoveryPosition);
        }
        long end = System.currentTimeMillis();
        LOG.info("Replayed " + redoCounter + " operations from redo log in " + ((end - start) / 1000.0f) + " seconds.");
    }
    
	private Location nextRecoveryPosition;
	private Location lastRecoveryPosition;

	public void incrementalRecover() throws IOException {
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
            JournalCommand message = load(lastRecoveryPosition);
            process(message, lastRecoveryPosition);            
            nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition);
        }
	}
	
    public Location getLastUpdatePosition() throws IOException {
        return metadata.lastUpdate;
    }
    
	private Location getRecoveryPosition() throws IOException {
		
        // If we need to recover the transactions..
        if (metadata.firstInProgressTransactionLocation != null) {
            return metadata.firstInProgressTransactionLocation;
        }
        
        // Perhaps there were no transactions...
        if( metadata.lastUpdate!=null) {
            // Start replay at the record after the last one recorded in the index file.
            return journal.getNextLocation(metadata.lastUpdate);
        }
        
        // This loads the first position.
        return journal.getNextLocation(null);
	}

    protected void checkpointCleanup(final boolean cleanup) {
        try {
            synchronized (indexMutex) {
            	if( !opened.get() ) {
            		return;
            	}
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        checkpointUpdate(tx, cleanup);
                    }
                });
            }
        } catch (IOException e) {
        	e.printStackTrace();
        }
    }

    
	public void checkpoint(Callback closure) throws Exception {
        synchronized (indexMutex) {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    checkpointUpdate(tx, false);
                }
            });
            pageFile.flush();
            closure.execute();
        }
	}

    // /////////////////////////////////////////////////////////////////
    // Methods call by the broker to update and query the store.
    // /////////////////////////////////////////////////////////////////
    public Location store(JournalCommand data) throws IOException {
        return store(data, false);
    }

    /**
     * All updated are are funneled through this method. The updates a converted
     * to a JournalMessage which is logged to the journal and then the data from
     * the JournalMessage is used to update the index just like it would be done
     * durring a recovery process.
     */
    public Location store(JournalCommand data, boolean sync) throws IOException {
        int size = data.serializedSizeFramed();
        DataByteArrayOutputStream os = new DataByteArrayOutputStream(size + 1);
        os.writeByte(data.type().getNumber());
        data.writeFramed(os);
        Location location = journal.write(os.toByteSequence(), sync);
        process(data, location);
        metadata.lastUpdate = location;
        return location;
    }

    /**
     * Loads a previously stored JournalMessage
     * 
     * @param location
     * @return
     * @throws IOException
     */
    public JournalCommand load(Location location) throws IOException {
        ByteSequence data = journal.read(location);
        DataByteArrayInputStream is = new DataByteArrayInputStream(data);
        byte readByte = is.readByte();
        KahaEntryType type = KahaEntryType.valueOf(readByte);
        if( type == null ) {
            throw new IOException("Could not load journal record. Invalid location: "+location);
        }
        JournalCommand message = (JournalCommand)type.createMessage();
        message.mergeFramed(is);
        return message;
    }

    // /////////////////////////////////////////////////////////////////
    // Journaled record processing methods. Once the record is journaled,
    // these methods handle applying the index updates. These may be called
    // from the recovery method too so they need to be idempotent
    // /////////////////////////////////////////////////////////////////

    private void process(JournalCommand data, final Location location) throws IOException {
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

    private void process(final KahaAddMessageCommand command, final Location location) throws IOException {
        if (command.hasTransactionInfo()) {
            synchronized (indexMutex) {
                ArrayList<Operation> inflightTx = getInflightTx(command.getTransactionInfo(), location);
                inflightTx.add(new AddOpperation(command, location));
            }
        } else {
            synchronized (indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        upadateIndex(tx, command, location);
                    }
                });
            }
        }
    }

    protected void process(final KahaRemoveMessageCommand command, final Location location) throws IOException {
        if (command.hasTransactionInfo()) {
            synchronized (indexMutex) {
                ArrayList<Operation> inflightTx = getInflightTx(command.getTransactionInfo(), location);
                inflightTx.add(new RemoveOpperation(command, location));
            }
        } else {
            synchronized (indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        updateIndex(tx, command, location);
                    }
                });
            }
        }

    }

    protected void process(final KahaRemoveDestinationCommand command, final Location location) throws IOException {
        synchronized (indexMutex) {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, command, location);
                }
            });
        }
    }

    protected void process(final KahaSubscriptionCommand command, final Location location) throws IOException {
        synchronized (indexMutex) {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, command, location);
                }
            });
        }
    }

    protected void process(KahaCommitCommand command, Location location) throws IOException {
        TransactionId key = key(command.getTransactionInfo());
        synchronized (indexMutex) {
            ArrayList<Operation> inflightTx = inflightTransactions.remove(key);
            if (inflightTx == null) {
                inflightTx = preparedTransactions.remove(key);
            }
            if (inflightTx == null) {
                return;
            }

            final ArrayList<Operation> messagingTx = inflightTx;
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    for (Operation op : messagingTx) {
                        op.execute(tx);
                    }
                }
            });
        }
    }

    protected void process(KahaPrepareCommand command, Location location) {
        synchronized (indexMutex) {
            TransactionId key = key(command.getTransactionInfo());
            ArrayList<Operation> tx = inflightTransactions.remove(key);
            if (tx != null) {
                preparedTransactions.put(key, tx);
            }
        }
    }

    protected void process(KahaRollbackCommand command, Location location) {
        synchronized (indexMutex) {
            TransactionId key = key(command.getTransactionInfo());
            ArrayList<Operation> tx = inflightTransactions.remove(key);
            if (tx == null) {
                preparedTransactions.remove(key);
            }
        }
    }

    // /////////////////////////////////////////////////////////////////
    // These methods do the actual index updates.
    // /////////////////////////////////////////////////////////////////

    protected final Object indexMutex = new Object();
	private final HashSet<Integer> journalFilesBeingReplicated = new HashSet<Integer>();

    private void upadateIndex(Transaction tx, KahaAddMessageCommand command, Location location) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);

        // Skip adding the message to the index if this is a topic and there are
        // no subscriptions.
        if (sd.subscriptions != null && sd.ackPositions.isEmpty()) {
            return;
        }

        // Add the message.
        long id = sd.nextMessageId++;
        Long previous = sd.locationIndex.put(tx, location, id);
        if( previous == null ) {
            sd.messageIdIndex.put(tx, command.getMessageId(), id);
            sd.orderIndex.put(tx, id, new MessageKeys(command.getMessageId(), location));
        } else {
            // restore the previous value.. Looks like this was a redo of a previously
            // added message.  We don't want to assing it a new id as the other indexes would 
            // be wrong..
            sd.locationIndex.put(tx, location, previous);
        }
        
    }

    private void updateIndex(Transaction tx, KahaRemoveMessageCommand command, Location ackLocation) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);
        if (!command.hasSubscriptionKey()) {
            
            // In the queue case we just remove the message from the index..
            Long sequenceId = sd.messageIdIndex.remove(tx, command.getMessageId());
            if (sequenceId != null) {
                MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
                sd.locationIndex.remove(tx, keys.location);
            }
        } else {
            // In the topic case we need remove the message once it's been acked
            // by all the subs
            Long sequence = sd.messageIdIndex.get(tx, command.getMessageId());

            // Make sure it's a valid message id...
            if (sequence != null) {
                String subscriptionKey = command.getSubscriptionKey();
                Long prev = sd.subscriptionAcks.put(tx, subscriptionKey, sequence);

                // The following method handles deleting un-referenced messages.
                removeAckLocation(tx, sd, subscriptionKey, prev);

                // Add it to the new location set.
                addAckLocation(sd, sequence, subscriptionKey);
            }

        }
    }

    private void updateIndex(Transaction tx, KahaRemoveDestinationCommand command, Location location) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);
        sd.orderIndex.clear(tx);
        sd.orderIndex.unload(tx);
        tx.free(sd.orderIndex.getPageId());
        
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
        }

        String key = key(command.getDestination());
        storedDestinations.remove(key);
        metadata.destinations.remove(tx, key);
    }

    private void updateIndex(Transaction tx, KahaSubscriptionCommand command, Location location) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);

        // If set then we are creating it.. otherwise we are destroying the sub
        if (command.hasSubscriptionInfo()) {
            String subscriptionKey = command.getSubscriptionKey();
            sd.subscriptions.put(tx, subscriptionKey, command);
            long ackLocation=-1;
            if (!command.getRetroactive()) {
                ackLocation = sd.nextMessageId-1;
            }

            sd.subscriptionAcks.put(tx, subscriptionKey, ackLocation);
            addAckLocation(sd, ackLocation, subscriptionKey);
        } else {
            // delete the sub...
            String subscriptionKey = command.getSubscriptionKey();
            sd.subscriptions.remove(tx, subscriptionKey);
            Long prev = sd.subscriptionAcks.remove(tx, subscriptionKey);
            if( prev!=null ) {
                removeAckLocation(tx, sd, subscriptionKey, prev);
            }
        }

    }
    
    /**
     * @param tx
     * @throws IOException
     */
    private void checkpointUpdate(Transaction tx, boolean cleanup) throws IOException {

        LOG.debug("Checkpoint started.");
        
        metadata.state = OPEN_STATE;
        metadata.firstInProgressTransactionLocation = getFirstInProgressTxLocation();
        tx.store(metadata.page, metadataMarshaller, true);
        pageFile.flush();

        if( cleanup ) {
            // Find empty journal files to remove.
            final HashSet<Integer> inUseFiles = new HashSet<Integer>();
            for (StoredDestination sd : storedDestinations.values()) {
                
                // Use a visitor to cut down the number of pages that we load
                sd.locationIndex.visit(tx, new BTreeVisitor<Location, Long>() {
                    int last=-1;
                    public boolean isInterestedInKeysBetween(Location first, Location second) {
                        if( second!=null ) {
                            if( last+1 == second.getDataFileId() ) {
                                last++;
                                inUseFiles.add(last);
                            }
                            if( last == second.getDataFileId() ) {
                                return false;
                            }
                        }
                        return true;
                    }
    
                    public void visit(List<Location> keys, List<Long> values) {
                        for (int i = 0; i < keys.size(); i++) {
                            if( last != keys.get(i).getDataFileId() ) {
                                inUseFiles.add(keys.get(i).getDataFileId());
                                last = keys.get(i).getDataFileId();
                            }
                        }
                        
                    }
    
                });
            }
            inUseFiles.addAll(journalFilesBeingReplicated);
            Location l = metadata.lastUpdate;
            if( metadata.firstInProgressTransactionLocation!=null ) {
                l = metadata.firstInProgressTransactionLocation;
            }
            
            LOG.debug("In use files: "+inUseFiles+", lastUpdate: "+l);
            journal.consolidateDataFilesNotIn(inUseFiles, l==null?null:l.getDataFileId());
        }
        
        LOG.debug("Checkpoint done.");
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
    
    static protected class MessageKeysMarshaller implements Marshaller<MessageKeys> {
        static final MessageKeysMarshaller INSTANCE = new MessageKeysMarshaller();
        
        public Class<MessageKeys> getType() {
            return MessageKeys.class;
        }

        public MessageKeys readPayload(DataInput dataIn) throws IOException {
            return new MessageKeys(dataIn.readUTF(), LocationMarshaller.INSTANCE.readPayload(dataIn));
        }

        public void writePayload(MessageKeys object, DataOutput dataOut) throws IOException {
            dataOut.writeUTF(object.messageId);
            LocationMarshaller.INSTANCE.writePayload(object.location, dataOut);
        }
    }
    
    static class StoredDestination {
        long nextMessageId;
        BTreeIndex<Long, MessageKeys> orderIndex;
        BTreeIndex<Location, Long> locationIndex;
        BTreeIndex<String, Long> messageIdIndex;

        // These bits are only set for Topics
        BTreeIndex<String, KahaSubscriptionCommand> subscriptions;
        BTreeIndex<String, Long> subscriptionAcks;
        HashMap<String, Long> subscriptionCursors;
        TreeMap<Long, HashSet<String>> ackPositions;
    }

    protected class StoredDestinationMarshaller implements Marshaller<StoredDestination> {
        public Class<StoredDestination> getType() {
            return StoredDestination.class;
        }

        public StoredDestination readPayload(DataInput dataIn) throws IOException {
            StoredDestination value = new StoredDestination();
            value.orderIndex = new BTreeIndex<Long, MessageKeys>(pageFile, dataIn.readLong());
            value.locationIndex = new BTreeIndex<Location, Long>(pageFile, dataIn.readLong());
            value.messageIdIndex = new BTreeIndex<String, Long>(pageFile, dataIn.readLong());

            if (dataIn.readBoolean()) {
                value.subscriptions = new BTreeIndex<String, KahaSubscriptionCommand>(pageFile, dataIn.readLong());
                value.subscriptionAcks = new BTreeIndex<String, Long>(pageFile, dataIn.readLong());
            }
            return value;
        }

        public void writePayload(StoredDestination value, DataOutput dataOut) throws IOException {
            dataOut.writeLong(value.orderIndex.getPageId());
            dataOut.writeLong(value.locationIndex.getPageId());
            dataOut.writeLong(value.messageIdIndex.getPageId());
            if (value.subscriptions != null) {
                dataOut.writeBoolean(true);
                dataOut.writeLong(value.subscriptions.getPageId());
                dataOut.writeLong(value.subscriptionAcks.getPageId());
            } else {
                dataOut.writeBoolean(false);
            }
        }
    }

    static class LocationMarshaller implements Marshaller<Location> {
        final static LocationMarshaller INSTANCE = new LocationMarshaller();

        public Class<Location> getType() {
            return Location.class;
        }

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
    }

    static class KahaSubscriptionCommandMarshaller implements Marshaller<KahaSubscriptionCommand> {
        final static KahaSubscriptionCommandMarshaller INSTANCE = new KahaSubscriptionCommandMarshaller();

        public Class<KahaSubscriptionCommand> getType() {
            return KahaSubscriptionCommand.class;
        }

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
            rc.orderIndex = new BTreeIndex<Long, MessageKeys>(pageFile, tx.allocate());
            rc.locationIndex = new BTreeIndex<Location, Long>(pageFile, tx.allocate());
            rc.messageIdIndex = new BTreeIndex<String, Long>(pageFile, tx.allocate());

            if (topic) {
                rc.subscriptions = new BTreeIndex<String, KahaSubscriptionCommand>(pageFile, tx.allocate());
                rc.subscriptionAcks = new BTreeIndex<String, Long>(pageFile, tx.allocate());
            }
            metadata.destinations.put(tx, key, rc);
        }

        // Configure the marshalers and load.
        rc.orderIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
        rc.orderIndex.setValueMarshaller(MessageKeysMarshaller.INSTANCE);
        rc.orderIndex.load(tx);

        // Figure out the next key using the last entry in the destination.
        Entry<Long, MessageKeys> lastEntry = rc.orderIndex.getLast(tx);
        if( lastEntry!=null ) {
            rc.nextMessageId = lastEntry.getKey()+1;
        }

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
            rc.subscriptionAcks.setValueMarshaller(LongMarshaller.INSTANCE);
            rc.subscriptionAcks.load(tx);

            rc.ackPositions = new TreeMap<Long, HashSet<String>>();
            rc.subscriptionCursors = new HashMap<String, Long>();

            for (Iterator<Entry<String, Long>> iterator = rc.subscriptionAcks.iterator(tx); iterator.hasNext();) {
                Entry<String, Long> entry = iterator.next();
                addAckLocation(rc, entry.getValue(), entry.getKey());
            }

        }
        return rc;
    }

    /**
     * @param sd
     * @param messageSequence
     * @param subscriptionKey
     */
    private void addAckLocation(StoredDestination sd, Long messageSequence, String subscriptionKey) {
        HashSet<String> hs = sd.ackPositions.get(messageSequence);
        if (hs == null) {
            hs = new HashSet<String>();
            sd.ackPositions.put(messageSequence, hs);
        }
        hs.add(subscriptionKey);
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
            HashSet<String> hs = sd.ackPositions.get(sequenceId);
            if (hs != null) {
                hs.remove(subscriptionKey);
                if (hs.isEmpty()) {
                    HashSet<String> firstSet = sd.ackPositions.values().iterator().next();
                    sd.ackPositions.remove(sequenceId);

                    // Did we just empty out the first set in the
                    // ordered list of ack locations? Then it's time to
                    // delete some messages.
                    if (hs == firstSet) {

                        // Find all the entries that need to get deleted.
                        ArrayList<Entry<Long, MessageKeys>> deletes = new ArrayList<Entry<Long, MessageKeys>>();
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator.hasNext();) {
                            Entry<Long, MessageKeys> entry = iterator.next();
                            if (entry.getKey().compareTo(sequenceId) <= 0) {
                                // We don't do the actually delete while we are
                                // iterating the BTree since
                                // iterating would fail.
                                deletes.add(entry);
                            }
                        }

                        // Do the actual deletes.
                        for (Entry<Long, MessageKeys> entry : deletes) {
                            sd.locationIndex.remove(tx, entry.getValue().location);
                            sd.messageIdIndex.remove(tx,entry.getValue().messageId);
                            sd.orderIndex.remove(tx,entry.getKey());
                        }
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
    protected final LinkedHashMap<TransactionId, ArrayList<Operation>> inflightTransactions = new LinkedHashMap<TransactionId, ArrayList<Operation>>();
    protected final LinkedHashMap<TransactionId, ArrayList<Operation>> preparedTransactions = new LinkedHashMap<TransactionId, ArrayList<Operation>>();

    private ArrayList<Operation> getInflightTx(KahaTransactionInfo info, Location location) {
        TransactionId key = key(info);
        ArrayList<Operation> tx = inflightTransactions.get(key);
        if (tx == null) {
            tx = new ArrayList<Operation>();
            inflightTransactions.put(key, tx);
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
        PageFile pf = new PageFile(directory, "db");
        return pf;
    }

    private Journal createJournal() {
        Journal manager = new Journal();
        manager.setDirectory(directory);
        manager.setMaxFileLength(1024 * 1024 * 20);
        manager.setUseNio(false);
        return manager;
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

    public boolean isSyncWrites() {
        return syncWrites;
    }

    public void setSyncWrites(boolean syncWrites) {
        this.syncWrites = syncWrites;
    }

    public int getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(int checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public int getCleanupInterval() {
        return cleanupInterval;
    }

    public void setCleanupInterval(int cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    public PageFile getPageFile() {
        if (pageFile == null) {
            pageFile = createPageFile();
        }
		return pageFile;
	}

	public Journal getJournal() {
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
}
