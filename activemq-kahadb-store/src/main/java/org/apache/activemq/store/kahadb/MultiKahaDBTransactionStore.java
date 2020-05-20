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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.IndexListener;
import org.apache.activemq.store.ListenableFuture;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.ProxyTopicMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.data.KahaCommitCommand;
import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.data.KahaPrepareCommand;
import org.apache.activemq.store.kahadb.data.KahaTraceCommand;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.IOHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiKahaDBTransactionStore implements TransactionStore {
    static final Logger LOG = LoggerFactory.getLogger(MultiKahaDBTransactionStore.class);
    final MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter;
    final ConcurrentMap<TransactionId, Tx> inflightTransactions = new ConcurrentHashMap<TransactionId, Tx>();
    final ConcurrentMap<TransactionId, Tx> pendingCommit = new ConcurrentHashMap<TransactionId, Tx>();
    private Journal journal;
    private int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    private int journalWriteBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean recovered = new AtomicBoolean(false);
    private long journalCleanupInterval = Journal.DEFAULT_CLEANUP_INTERVAL;
    private boolean checkForCorruption = true;
    private AtomicBoolean corruptJournalDetected = new AtomicBoolean(false);

    public MultiKahaDBTransactionStore(MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter) {
        this.multiKahaDBPersistenceAdapter = multiKahaDBPersistenceAdapter;
    }

    public MessageStore proxy(final TransactionStore transactionStore, MessageStore messageStore) {
        return new ProxyMessageStore(messageStore) {
            @Override
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                MultiKahaDBTransactionStore.this.addMessage(transactionStore, context, getDelegate(), send);
            }

            @Override
            public void addMessage(ConnectionContext context, final Message send, boolean canOptimizeHint) throws IOException {
                MultiKahaDBTransactionStore.this.addMessage(transactionStore, context, getDelegate(), send);
            }

            @Override
            public ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message) throws IOException {
                return MultiKahaDBTransactionStore.this.asyncAddQueueMessage(transactionStore, context, getDelegate(), message);
            }

            @Override
            public ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message, boolean canOptimizeHint) throws IOException {
                return MultiKahaDBTransactionStore.this.asyncAddQueueMessage(transactionStore, context, getDelegate(), message);
            }

            @Override
            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                MultiKahaDBTransactionStore.this.removeMessage(transactionStore, context, getDelegate(), ack);
            }

            @Override
            public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
                MultiKahaDBTransactionStore.this.removeAsyncMessage(transactionStore, context, getDelegate(), ack);
            }

            @Override
            public void registerIndexListener(IndexListener indexListener) {
                getDelegate().registerIndexListener(indexListener);
                try {
                    if (indexListener instanceof BaseDestination) {
                        // update queue storeUsage
                        Object matchingPersistenceAdapter = multiKahaDBPersistenceAdapter.destinationMap.chooseValue(getDelegate().getDestination());
                        if (matchingPersistenceAdapter instanceof FilteredKahaDBPersistenceAdapter) {
                            FilteredKahaDBPersistenceAdapter filteredAdapter = (FilteredKahaDBPersistenceAdapter) matchingPersistenceAdapter;
                            if (filteredAdapter.getUsage() != null && filteredAdapter.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter) {
                                StoreUsage storeUsage = filteredAdapter.getUsage();
                                storeUsage.setStore(filteredAdapter.getPersistenceAdapter());
                                storeUsage.setParent(multiKahaDBPersistenceAdapter.getBrokerService().getSystemUsage().getStoreUsage());
                                ((BaseDestination) indexListener).getSystemUsage().setStoreUsage(storeUsage);
                            }
                        }
                    }
                } catch (Exception ignored) {
                    LOG.warn("Failed to set mKahaDB destination store usage", ignored);
                }
            }
        };
    }

    public TopicMessageStore proxy(final TransactionStore transactionStore, final TopicMessageStore messageStore) {
        return new ProxyTopicMessageStore(messageStore) {
            @Override
            public void addMessage(ConnectionContext context, final Message send, boolean canOptimizeHint) throws IOException {
                MultiKahaDBTransactionStore.this.addMessage(transactionStore, context, getDelegate(), send);
            }

            @Override
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                MultiKahaDBTransactionStore.this.addMessage(transactionStore, context, getDelegate(), send);
            }

            @Override
            public ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message, boolean canOptimizeHint) throws IOException {
                return MultiKahaDBTransactionStore.this.asyncAddTopicMessage(transactionStore, context, getDelegate(), message);
            }

            @Override
            public ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message) throws IOException {
                return MultiKahaDBTransactionStore.this.asyncAddTopicMessage(transactionStore, context, getDelegate(), message);
            }

            @Override
            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                MultiKahaDBTransactionStore.this.removeMessage(transactionStore, context, getDelegate(), ack);
            }

            @Override
            public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
                MultiKahaDBTransactionStore.this.removeAsyncMessage(transactionStore, context, getDelegate(), ack);
            }

            @Override
            public void acknowledge(ConnectionContext context, String clientId, String subscriptionName,
                                    MessageId messageId, MessageAck ack) throws IOException {
                MultiKahaDBTransactionStore.this.acknowledge(transactionStore, context, (TopicMessageStore) getDelegate(), clientId,
                        subscriptionName, messageId, ack);
            }
        };
    }

    public void deleteAllMessages() {
        IOHelper.deleteChildren(getDirectory());
    }

    public int getJournalMaxFileLength() {
        return journalMaxFileLength;
    }

    public void setJournalMaxFileLength(int journalMaxFileLength) {
        this.journalMaxFileLength = journalMaxFileLength;
    }

    public int getJournalMaxWriteBatchSize() {
        return journalWriteBatchSize;
    }

    public void setJournalMaxWriteBatchSize(int journalWriteBatchSize) {
        this.journalWriteBatchSize = journalWriteBatchSize;
    }

    public void setJournalCleanupInterval(long journalCleanupInterval) {
        this.journalCleanupInterval = journalCleanupInterval;
    }

    public long getJournalCleanupInterval() {
        return journalCleanupInterval;
    }

    public void setCheckForCorruption(boolean checkForCorruption) {
        this.checkForCorruption = checkForCorruption;
    }

    public boolean isCheckForCorruption() {
        return checkForCorruption;
    }

    public class Tx {
        private final HashMap<TransactionStore, TransactionId> stores = new HashMap<TransactionStore, TransactionId>();
        private int prepareLocationId = 0;

        public void trackStore(TransactionStore store, XATransactionId xid) {
            stores.put(store, xid);
        }

        public void trackStore(TransactionStore store) {
            stores.put(store, null);
        }

        public HashMap<TransactionStore, TransactionId> getStoresMap() {
            return stores;
        }

        public Set<TransactionStore> getStores() {
            return stores.keySet();
        }

        public void trackPrepareLocation(Location location) {
            this.prepareLocationId = location.getDataFileId();
        }

        public int getPreparedLocationId() {
            return prepareLocationId;
        }
    }

    public Tx getTx(TransactionId txid) {
        Tx tx = inflightTransactions.get(txid);
        if (tx == null) {
            tx = new Tx();
            inflightTransactions.put(txid, tx);
        }
        return tx;
    }

    public Tx removeTx(TransactionId txid) {
        return inflightTransactions.remove(txid);
    }

    @Override
    public void prepare(TransactionId txid) throws IOException {
        Tx tx = getTx(txid);
        for (TransactionStore store : tx.getStores()) {
            store.prepare(txid);
        }
    }

    @Override
    public void commit(TransactionId txid, boolean wasPrepared, Runnable preCommit, Runnable postCommit)
            throws IOException {

        if (preCommit != null) {
            preCommit.run();
        }

        Tx tx = getTx(txid);
        if (wasPrepared) {
            for (Map.Entry<TransactionStore, TransactionId> storeTx : tx.getStoresMap().entrySet()) {
                TransactionId recovered = storeTx.getValue();
                if (recovered != null) {
                    storeTx.getKey().commit(recovered, true, null, null);
                } else {
                    storeTx.getKey().commit(txid, true, null, null);
                }
            }
        } else {
            // can only do 1pc on a single store
            if (tx.getStores().size() == 1) {
                for (TransactionStore store : tx.getStores()) {
                    store.commit(txid, false, null, null);
                }
            } else {
                // need to do local 2pc
                for (TransactionStore store : tx.getStores()) {
                    store.prepare(txid);
                }
                persistOutcome(tx, txid);
                for (TransactionStore store : tx.getStores()) {
                    store.commit(txid, true, null, null);
                }
                persistCompletion(txid);
            }
        }
        removeTx(txid);
        if (postCommit != null) {
            postCommit.run();
        }
    }

    public void persistOutcome(Tx tx, TransactionId txid) throws IOException {
        tx.trackPrepareLocation(store(new KahaPrepareCommand().setTransactionInfo(TransactionIdConversion.convert(multiKahaDBPersistenceAdapter.transactionIdTransformer.transform(txid)))));
        pendingCommit.put(txid, tx);
    }

    public void persistCompletion(TransactionId txid) throws IOException {
        store(new KahaCommitCommand().setTransactionInfo(TransactionIdConversion.convert(multiKahaDBPersistenceAdapter.transactionIdTransformer.transform(txid))));
        pendingCommit.remove(txid);
    }

    private Location store(JournalCommand<?> data) throws IOException {
        int size = data.serializedSizeFramed();
        DataByteArrayOutputStream os = new DataByteArrayOutputStream(size + 1);
        os.writeByte(data.type().getNumber());
        data.writeFramed(os);
        Location location = journal.write(os.toByteSequence(), true);
        journal.setLastAppendLocation(location);
        return location;
    }

    @Override
    public void rollback(TransactionId txid) throws IOException {
        Tx tx = removeTx(txid);
        if (tx != null) {
            for (Map.Entry<TransactionStore, TransactionId> storeTx : tx.getStoresMap().entrySet()) {
                TransactionId recovered = storeTx.getValue();
                if (recovered != null) {
                    storeTx.getKey().rollback(recovered);
                } else {
                    storeTx.getKey().rollback(txid);
                }
            }
        }
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            journal = new Journal() {
                @Override
                public void cleanup() {
                    super.cleanup();
                    txStoreCleanup();
                }
            };
            journal.setDirectory(getDirectory());
            journal.setMaxFileLength(journalMaxFileLength);
            journal.setWriteBatchSize(journalWriteBatchSize);
            journal.setCleanupInterval(journalCleanupInterval);
            journal.setCheckForCorruptionOnStartup(checkForCorruption);
            journal.setChecksum(checkForCorruption);
            IOHelper.mkdirs(journal.getDirectory());
            journal.start();
            recoverPendingLocalTransactions();
            recovered.set(true);
            loaded();
        }
    }

    private void loaded() throws IOException {
        store(new KahaTraceCommand().setMessage("LOADED " + new Date()));
    }

    private void txStoreCleanup() {
        if (!recovered.get() || corruptJournalDetected.get()) {
            return;
        }
        Set<Integer> knownDataFileIds = new TreeSet<Integer>(journal.getFileMap().keySet());
        for (Tx tx : inflightTransactions.values()) {
            knownDataFileIds.remove(tx.getPreparedLocationId());
        }
        for (Tx tx : pendingCommit.values()) {
            knownDataFileIds.remove(tx.getPreparedLocationId());
        }
        try {
            journal.removeDataFiles(knownDataFileIds);
        } catch (Exception e) {
            LOG.error(this + ", Failed to remove tx journal datafiles " + knownDataFileIds);
        }
    }

    private File getDirectory() {
        return new File(multiKahaDBPersistenceAdapter.getDirectory(), "txStore");
    }

    @Override
    public void stop() throws Exception {
        if (started.compareAndSet(true, false) && journal != null) {
            journal.close();
            journal = null;
        }
    }

    private void recoverPendingLocalTransactions() throws IOException {

        if (checkForCorruption) {
            for (DataFile dataFile: journal.getFileMap().values()) {
                if (!dataFile.getCorruptedBlocks().isEmpty()) {
                    LOG.error("Corrupt Transaction journal records found in db-{}.log at {}", dataFile.getDataFileId(),  dataFile.getCorruptedBlocks());
                    corruptJournalDetected.set(true);
                }
            }
        }
        if (!corruptJournalDetected.get()) {
            Location location = null;
            try {
                location = journal.getNextLocation(null);
                while (location != null) {
                    process(location, load(location));
                    location = journal.getNextLocation(location);
                }
            } catch (Exception oops) {
                LOG.error("Corrupt journal record; unexpected exception on transaction journal replay of location:" + location, oops);
                corruptJournalDetected.set(true);
            }
            pendingCommit.putAll(inflightTransactions);
            LOG.info("pending local transactions: " + pendingCommit.keySet());
        }
    }

    public JournalCommand<?> load(Location location) throws IOException {
        DataByteArrayInputStream is = new DataByteArrayInputStream(journal.read(location));
        byte readByte = is.readByte();
        KahaEntryType type = KahaEntryType.valueOf(readByte);
        if (type == null) {
            throw new IOException("Could not load journal record. Invalid location: " + location);
        }
        JournalCommand<?> message = (JournalCommand<?>) type.createMessage();
        message.mergeFramed(is);
        return message;
    }

    public void process(final Location location, JournalCommand<?> command) throws IOException {
        switch (command.type()) {
            case KAHA_PREPARE_COMMAND:
                KahaPrepareCommand prepareCommand = (KahaPrepareCommand) command;
                getTx(TransactionIdConversion.convert(prepareCommand.getTransactionInfo())).trackPrepareLocation(location);
                break;
            case KAHA_COMMIT_COMMAND:
                KahaCommitCommand commitCommand = (KahaCommitCommand) command;
                removeTx(TransactionIdConversion.convert(commitCommand.getTransactionInfo()));
                break;
            case KAHA_TRACE_COMMAND:
                break;
            default:
                throw new IOException("Unexpected command in transaction journal: " + command);
        }
    }


    @Override
    public synchronized void recover(final TransactionRecoveryListener listener) throws IOException {

        for (final PersistenceAdapter adapter : multiKahaDBPersistenceAdapter.adapters) {
            adapter.createTransactionStore().recover(new TransactionRecoveryListener() {
                @Override
                public void recover(XATransactionId xid, Message[] addedMessages, MessageAck[] acks) {
                    try {
                        getTx(xid).trackStore(adapter.createTransactionStore(), xid);
                    } catch (IOException e) {
                        LOG.error("Failed to access transaction store: " + adapter + " for prepared xa tid: " + xid, e);
                    }
                    listener.recover(xid, addedMessages, acks);
                }
            });
        }

        boolean recoveryWorkPending = false;
        try {
            Broker broker = multiKahaDBPersistenceAdapter.getBrokerService().getBroker();
            // force completion of local xa
            for (TransactionId txid : broker.getPreparedTransactions(null)) {
                if (multiKahaDBPersistenceAdapter.isLocalXid(txid)) {
                    recoveryWorkPending = true;
                    if (corruptJournalDetected.get()) {
                        // not having a record is meaningless once our tx store is corrupt; we need a heuristic decision
                        LOG.warn("Pending multi store local transaction {} requires manual heuristic outcome via JMX", txid);
                        logSomeContext(txid);
                    } else {
                        try {
                            if (pendingCommit.keySet().contains(txid)) {
                                // we recorded the commit outcome, finish the job
                                LOG.info("delivering pending commit outcome for tid: " + txid);
                                broker.commitTransaction(null, txid, false);
                            } else {
                                // we have not record an outcome, and would have reported a commit failure, so we must rollback
                                LOG.info("delivering rollback outcome to store for tid: " + txid);
                                broker.forgetTransaction(null, txid);
                            }
                            persistCompletion(txid);
                        } catch (Exception ex) {
                            LOG.error("failed to deliver pending outcome for tid: " + txid, ex);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("failed to resolve pending local transactions", e);
        }
        // can we ignore corruption and resume
        if (corruptJournalDetected.get() && !recoveryWorkPending) {
            // move to new write file, gc will cleanup
            journal.rotateWriteFile();
            loaded();
            corruptJournalDetected.set(false);
            LOG.info("No heuristics outcome pending after corrupt tx store detection, auto resolving");
        }
    }

    private void logSomeContext(TransactionId txid) throws IOException {
        Tx tx = getTx(txid);
        if (tx != null) {
            for (TransactionStore store: tx.getStores()) {
                for (PersistenceAdapter persistenceAdapter : multiKahaDBPersistenceAdapter.adapters) {
                    if (persistenceAdapter.createTransactionStore() == store) {
                        if (persistenceAdapter instanceof KahaDBPersistenceAdapter) {
                            LOG.warn("Heuristic data in: " + persistenceAdapter + ", "  + ((KahaDBPersistenceAdapter)persistenceAdapter).getStore().getPreparedTransaction(txid));
                        }
                    }
                }
            }
        }
    }

    void addMessage(final TransactionStore transactionStore, ConnectionContext context, final MessageStore destination, final Message message)
            throws IOException {
        if (message.getTransactionId() != null) {
            getTx(message.getTransactionId()).trackStore(transactionStore);
        }
        destination.addMessage(context, message);
    }

    ListenableFuture<Object> asyncAddQueueMessage(final TransactionStore transactionStore, ConnectionContext context, final MessageStore destination, final Message message)
            throws IOException {
        if (message.getTransactionId() != null) {
            getTx(message.getTransactionId()).trackStore(transactionStore);
            destination.addMessage(context, message);
            return AbstractMessageStore.FUTURE;
        } else {
            return destination.asyncAddQueueMessage(context, message);
        }
    }

    ListenableFuture<Object> asyncAddTopicMessage(final TransactionStore transactionStore, ConnectionContext context, final MessageStore destination, final Message message)
            throws IOException {

        if (message.getTransactionId() != null) {
            getTx(message.getTransactionId()).trackStore(transactionStore);
            destination.addMessage(context, message);
            return AbstractMessageStore.FUTURE;
        } else {
            return destination.asyncAddTopicMessage(context, message);
        }
    }

    final void removeMessage(final TransactionStore transactionStore, ConnectionContext context, final MessageStore destination, final MessageAck ack)
            throws IOException {
        if (ack.getTransactionId() != null) {
            getTx(ack.getTransactionId()).trackStore(transactionStore);
        }
        destination.removeMessage(context, ack);
    }

    final void removeAsyncMessage(final TransactionStore transactionStore, ConnectionContext context, final MessageStore destination, final MessageAck ack)
            throws IOException {
        if (ack.getTransactionId() != null) {
            getTx(ack.getTransactionId()).trackStore(transactionStore);
        }
        destination.removeAsyncMessage(context, ack);
    }

    final void acknowledge(final TransactionStore transactionStore, ConnectionContext context, final TopicMessageStore destination,
                           final String clientId, final String subscriptionName,
                           final MessageId messageId, final MessageAck ack) throws IOException {
        if (ack.getTransactionId() != null) {
            getTx(ack.getTransactionId()).trackStore(transactionStore);
        }
        destination.acknowledge(context, clientId, subscriptionName, messageId, ack);
    }

}
