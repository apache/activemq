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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.util.ByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.Marshaller;
import org.apache.kahadb.util.StringMarshaller;
import org.apache.kahadb.util.VariableMarshaller;

public class TempMessageDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(TempMessageDatabase.class);

    public static final int CLOSED_STATE = 1;
    public static final int OPEN_STATE = 2;

    protected BTreeIndex<String, StoredDestination> destinations;
    protected PageFile pageFile;

    protected File directory;
    
    boolean enableIndexWriteAsync = true;
    int setIndexWriteBatchSize = PageFile.DEFAULT_WRITE_BATCH_SIZE; 
    
    protected AtomicBoolean started = new AtomicBoolean();
    protected AtomicBoolean opened = new AtomicBoolean();

    public TempMessageDatabase() {
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
                    destinations = new BTreeIndex<String, StoredDestination>(pageFile, tx.allocate().getPageId());
                    destinations.setKeyMarshaller(StringMarshaller.INSTANCE);
                    destinations.setValueMarshaller(new StoredDestinationMarshaller());
                    destinations.load(tx);
                }
            });
            pageFile.flush();
            storedDestinations.clear();
        }
	}
	
	/**
	 * @throws IOException
	 */
	public void open() throws IOException {
		if( opened.compareAndSet(false, true) ) {
	        loadPageFile();
		}
	}
	
    public void load() throws IOException {
        synchronized (indexMutex) {
	    	open();
            pageFile.unload();
            pageFile.delete();
            loadPageFile();
        }
    }

    
	public void close() throws IOException, InterruptedException {
		if( opened.compareAndSet(true, false)) {
	        synchronized (indexMutex) {
	            pageFile.unload();
	        }
		}
	}
	
    public void unload() throws IOException, InterruptedException {
        synchronized (indexMutex) {
            if( pageFile.isLoaded() ) {
                close();
            }
        }
    }

    public void processAdd(final KahaAddMessageCommand command, TransactionId txid, final ByteSequence data) throws IOException {
        if (txid!=null) {
            synchronized (indexMutex) {
                ArrayList<Operation> inflightTx = getInflightTx(txid);
                inflightTx.add(new AddOpperation(command, data));
            }
        } else {
            synchronized (indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        upadateIndex(tx, command, data);
                    }
                });
            }
        }
    }

    public void processRemove(final KahaRemoveMessageCommand command, TransactionId txid) throws IOException {
        if (txid!=null) {
            synchronized (indexMutex) {
                ArrayList<Operation> inflightTx = getInflightTx(txid);
                inflightTx.add(new RemoveOpperation(command));
            }
        } else {
            synchronized (indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        updateIndex(tx, command);
                    }
                });
            }
        }

    }

    public void process(final KahaRemoveDestinationCommand command) throws IOException {
        synchronized (indexMutex) {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, command);
                }
            });
        }
    }

    public void process(final KahaSubscriptionCommand command) throws IOException {
        synchronized (indexMutex) {
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    updateIndex(tx, command);
                }
            });
        }
    }

    public void processCommit(TransactionId key) throws IOException {
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

    public void processPrepare(TransactionId key) {
        synchronized (indexMutex) {
            ArrayList<Operation> tx = inflightTransactions.remove(key);
            if (tx != null) {
                preparedTransactions.put(key, tx);
            }
        }
    }

    public void processRollback(TransactionId key) {
        synchronized (indexMutex) {
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

    private void upadateIndex(Transaction tx, KahaAddMessageCommand command, ByteSequence data) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);

        // Skip adding the message to the index if this is a topic and there are
        // no subscriptions.
        if (sd.subscriptions != null && sd.ackPositions.isEmpty()) {
            return;
        }

        // Add the message.
        long id = sd.nextMessageId++;
        Long previous = sd.messageIdIndex.put(tx, command.getMessageId(), id);
        if( previous == null ) {
            sd.orderIndex.put(tx, id, new MessageRecord(command.getMessageId(), data));
        } else {
            // restore the previous value.. Looks like this was a redo of a previously
            // added message.  We don't want to assing it a new id as the other indexes would 
            // be wrong..
            sd.messageIdIndex.put(tx, command.getMessageId(), previous);
        }
    }

    private void updateIndex(Transaction tx, KahaRemoveMessageCommand command) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);
        if (!command.hasSubscriptionKey()) {
            
            // In the queue case we just remove the message from the index..
            Long sequenceId = sd.messageIdIndex.remove(tx, command.getMessageId());
            if (sequenceId != null) {
                sd.orderIndex.remove(tx, sequenceId);
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
                removeAckByteSequence(tx, sd, subscriptionKey, prev);

                // Add it to the new location set.
                addAckByteSequence(sd, sequence, subscriptionKey);
            }

        }
    }

    private void updateIndex(Transaction tx, KahaRemoveDestinationCommand command) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);
        sd.orderIndex.clear(tx);
        sd.orderIndex.unload(tx);
        tx.free(sd.orderIndex.getPageId());
        
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
        destinations.remove(tx, key);
    }

    private void updateIndex(Transaction tx, KahaSubscriptionCommand command) throws IOException {
        StoredDestination sd = getStoredDestination(command.getDestination(), tx);

        // If set then we are creating it.. otherwise we are destroying the sub
        if (command.hasSubscriptionInfo()) {
            String subscriptionKey = command.getSubscriptionKey();
            sd.subscriptions.put(tx, subscriptionKey, command);
            long ackByteSequence=-1;
            if (!command.getRetroactive()) {
                ackByteSequence = sd.nextMessageId-1;
            }

            sd.subscriptionAcks.put(tx, subscriptionKey, ackByteSequence);
            addAckByteSequence(sd, ackByteSequence, subscriptionKey);
        } else {
            // delete the sub...
            String subscriptionKey = command.getSubscriptionKey();
            sd.subscriptions.remove(tx, subscriptionKey);
            Long prev = sd.subscriptionAcks.remove(tx, subscriptionKey);
            if( prev!=null ) {
                removeAckByteSequence(tx, sd, subscriptionKey, prev);
            }
        }

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
        ByteSequence lastAckByteSequence;
        ByteSequence cursor;
    }
    
    static class MessageRecord {
        final String messageId;
        final ByteSequence data;
        
        public MessageRecord(String messageId, ByteSequence location) {
            this.messageId=messageId;
            this.data=location;
        }
        
        @Override
        public String toString() {
            return "["+messageId+","+data+"]";
        }
    }
    
    static protected class MessageKeysMarshaller extends VariableMarshaller<MessageRecord> {
        static final MessageKeysMarshaller INSTANCE = new MessageKeysMarshaller();
        
        public MessageRecord readPayload(DataInput dataIn) throws IOException {
            return new MessageRecord(dataIn.readUTF(), ByteSequenceMarshaller.INSTANCE.readPayload(dataIn));
        }

        public void writePayload(MessageRecord object, DataOutput dataOut) throws IOException {
            dataOut.writeUTF(object.messageId);
            ByteSequenceMarshaller.INSTANCE.writePayload(object.data, dataOut);
        }
    }
    
    static class StoredDestination {
        long nextMessageId;
        BTreeIndex<Long, MessageRecord> orderIndex;
        BTreeIndex<String, Long> messageIdIndex;

        // These bits are only set for Topics
        BTreeIndex<String, KahaSubscriptionCommand> subscriptions;
        BTreeIndex<String, Long> subscriptionAcks;
        HashMap<String, Long> subscriptionCursors;
        TreeMap<Long, HashSet<String>> ackPositions;
    }

    protected class StoredDestinationMarshaller extends VariableMarshaller<StoredDestination> {
        public Class<StoredDestination> getType() {
            return StoredDestination.class;
        }

        public StoredDestination readPayload(DataInput dataIn) throws IOException {
            StoredDestination value = new StoredDestination();
            value.orderIndex = new BTreeIndex<Long, MessageRecord>(pageFile, dataIn.readLong());
            value.messageIdIndex = new BTreeIndex<String, Long>(pageFile, dataIn.readLong());

            if (dataIn.readBoolean()) {
                value.subscriptions = new BTreeIndex<String, KahaSubscriptionCommand>(pageFile, dataIn.readLong());
                value.subscriptionAcks = new BTreeIndex<String, Long>(pageFile, dataIn.readLong());
            }
            return value;
        }

        public void writePayload(StoredDestination value, DataOutput dataOut) throws IOException {
            dataOut.writeLong(value.orderIndex.getPageId());
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

    static class ByteSequenceMarshaller extends VariableMarshaller<ByteSequence> {
        final static ByteSequenceMarshaller INSTANCE = new ByteSequenceMarshaller();

        public ByteSequence readPayload(DataInput dataIn) throws IOException {
        	byte data[] = new byte[dataIn.readInt()];
        	dataIn.readFully(data);
            return new ByteSequence(data);
        }

        public void writePayload(ByteSequence object, DataOutput dataOut) throws IOException {
            dataOut.writeInt(object.getLength());
            dataOut.write(object.getData(), object.getOffset(), object.getLength());
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

    /**
     * @param tx
     * @param key
     * @param topic
     * @return
     * @throws IOException
     */
    private StoredDestination loadStoredDestination(Transaction tx, String key, boolean topic) throws IOException {
        // Try to load the existing indexes..
        StoredDestination rc = destinations.get(tx, key);
        if (rc == null) {
            // Brand new destination.. allocate indexes for it.
            rc = new StoredDestination();
            rc.orderIndex = new BTreeIndex<Long, MessageRecord>(pageFile, tx.allocate());
            rc.messageIdIndex = new BTreeIndex<String, Long>(pageFile, tx.allocate());

            if (topic) {
                rc.subscriptions = new BTreeIndex<String, KahaSubscriptionCommand>(pageFile, tx.allocate());
                rc.subscriptionAcks = new BTreeIndex<String, Long>(pageFile, tx.allocate());
            }
            destinations.put(tx, key, rc);
        }

        // Configure the marshalers and load.
        rc.orderIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
        rc.orderIndex.setValueMarshaller(MessageKeysMarshaller.INSTANCE);
        rc.orderIndex.load(tx);

        // Figure out the next key using the last entry in the destination.
        Entry<Long, MessageRecord> lastEntry = rc.orderIndex.getLast(tx);
        if( lastEntry!=null ) {
            rc.nextMessageId = lastEntry.getKey()+1;
        }

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
                addAckByteSequence(rc, entry.getValue(), entry.getKey());
            }

        }
        return rc;
    }

    /**
     * @param sd
     * @param messageSequence
     * @param subscriptionKey
     */
    private void addAckByteSequence(StoredDestination sd, Long messageSequence, String subscriptionKey) {
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
    private void removeAckByteSequence(Transaction tx, StoredDestination sd, String subscriptionKey, Long sequenceId) throws IOException {
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
                        ArrayList<Entry<Long, MessageRecord>> deletes = new ArrayList<Entry<Long, MessageRecord>>();
                        for (Iterator<Entry<Long, MessageRecord>> iterator = sd.orderIndex.iterator(tx); iterator.hasNext();) {
                            Entry<Long, MessageRecord> entry = iterator.next();
                            if (entry.getKey().compareTo(sequenceId) <= 0) {
                                // We don't do the actually delete while we are
                                // iterating the BTree since
                                // iterating would fail.
                                deletes.add(entry);
                            }
                        }

                        // Do the actual deletes.
                        for (Entry<Long, MessageRecord> entry : deletes) {
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
 
    private ArrayList<Operation> getInflightTx(TransactionId key) {
        ArrayList<Operation> tx = inflightTransactions.get(key);
        if (tx == null) {
            tx = new ArrayList<Operation>();
            inflightTransactions.put(key, tx);
        }
        return tx;
    }

    abstract class Operation {
        abstract public void execute(Transaction tx) throws IOException;
    }

    class AddOpperation extends Operation {
        final KahaAddMessageCommand command;
		private final ByteSequence data;

        public AddOpperation(KahaAddMessageCommand command, ByteSequence location) {
            this.command = command;
			this.data = location;
        }

        public void execute(Transaction tx) throws IOException {
            upadateIndex(tx, command, data);
        }

        public KahaAddMessageCommand getCommand() {
            return command;
        }
    }

    class RemoveOpperation extends Operation {
        final KahaRemoveMessageCommand command;

        public RemoveOpperation(KahaRemoveMessageCommand command) {
            this.command = command;
        }

        public void execute(Transaction tx) throws IOException {
            updateIndex(tx, command);
        }

        public KahaRemoveMessageCommand getCommand() {
            return command;
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Initialization related implementation methods.
    // /////////////////////////////////////////////////////////////////

    private PageFile createPageFile() {
        PageFile index = new PageFile(directory, "temp-db");
        index.setEnableWriteThread(isEnableIndexWriteAsync());
        index.setWriteBatchSize(getIndexWriteBatchSize());
        index.setEnableDiskSyncs(false);
        index.setEnableRecoveryFile(false);
        return index;
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
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
        
    public PageFile getPageFile() {
        if (pageFile == null) {
            pageFile = createPageFile();
        }
		return pageFile;
	}

}
