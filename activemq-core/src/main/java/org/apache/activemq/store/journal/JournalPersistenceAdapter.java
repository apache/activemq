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
package org.apache.activemq.store.journal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activeio.journal.InvalidRecordLocationException;
import org.apache.activeio.journal.Journal;
import org.apache.activeio.journal.JournalEventListener;
import org.apache.activeio.journal.RecordLocation;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.JournalQueueAck;
import org.apache.activemq.command.JournalTopicAck;
import org.apache.activemq.command.JournalTrace;
import org.apache.activemq.command.JournalTransaction;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.journal.JournalTransactionStore.Tx;
import org.apache.activemq.store.journal.JournalTransactionStore.TxOperation;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An implementation of {@link PersistenceAdapter} designed for use with a
 * {@link Journal} and then check pointing asynchronously on a timeout with some
 * other long term persistent storage.
 * 
 * @org.apache.xbean.XBean
 * @version $Revision: 1.17 $
 */
public class JournalPersistenceAdapter implements PersistenceAdapter, JournalEventListener, UsageListener, BrokerServiceAware {

    private BrokerService brokerService;
	
    protected static final Scheduler scheduler = Scheduler.getInstance();
    private static final Log LOG = LogFactory.getLog(JournalPersistenceAdapter.class);

    private final Journal journal;
    private final PersistenceAdapter longTermPersistence;

    private final WireFormat wireFormat = new OpenWireFormat();

    private final ConcurrentHashMap<ActiveMQQueue, JournalMessageStore> queues = new ConcurrentHashMap<ActiveMQQueue, JournalMessageStore>();
    private final ConcurrentHashMap<ActiveMQTopic, JournalTopicMessageStore> topics = new ConcurrentHashMap<ActiveMQTopic, JournalTopicMessageStore>();

    private SystemUsage usageManager;
    private long checkpointInterval = 1000 * 60 * 5;
    private long lastCheckpointRequest = System.currentTimeMillis();
    private long lastCleanup = System.currentTimeMillis();
    private int maxCheckpointWorkers = 10;
    private int maxCheckpointMessageAddSize = 1024 * 1024;

    private JournalTransactionStore transactionStore = new JournalTransactionStore(this);
    private ThreadPoolExecutor checkpointExecutor;

    private TaskRunner checkpointTask;
    private CountDownLatch nextCheckpointCountDownLatch = new CountDownLatch(1);
    private boolean fullCheckPoint;

    private AtomicBoolean started = new AtomicBoolean(false);

    private final Runnable periodicCheckpointTask = createPeriodicCheckpointTask();

    public JournalPersistenceAdapter(Journal journal, PersistenceAdapter longTermPersistence, TaskRunnerFactory taskRunnerFactory) throws IOException {

        this.journal = journal;
        journal.setJournalEventListener(this);

        checkpointTask = taskRunnerFactory.createTaskRunner(new Task() {
            public boolean iterate() {
                return doCheckpoint();
            }
        }, "ActiveMQ Journal Checkpoint Worker");

        this.longTermPersistence = longTermPersistence;
    }

    final Runnable createPeriodicCheckpointTask() {
        return new Runnable() {
            public void run() {
                long lastTime = 0;
                synchronized (this) {
                    lastTime = lastCheckpointRequest;
                }
                if (System.currentTimeMillis() > lastTime + checkpointInterval) {
                    checkpoint(false, true);
                }
            }
        };
    }

    /**
     * @param usageManager The UsageManager that is controlling the
     *                destination's memory usage.
     */
    public void setUsageManager(SystemUsage usageManager) {
        this.usageManager = usageManager;
        longTermPersistence.setUsageManager(usageManager);
    }

    public Set<ActiveMQDestination> getDestinations() {
        Set<ActiveMQDestination> destinations = new HashSet<ActiveMQDestination>(longTermPersistence.getDestinations());
        destinations.addAll(queues.keySet());
        destinations.addAll(topics.keySet());
        return destinations;
    }

    private MessageStore createMessageStore(ActiveMQDestination destination) throws IOException {
        if (destination.isQueue()) {
            return createQueueMessageStore((ActiveMQQueue)destination);
        } else {
            return createTopicMessageStore((ActiveMQTopic)destination);
        }
    }

    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        JournalMessageStore store = queues.get(destination);
        if (store == null) {
            MessageStore checkpointStore = longTermPersistence.createQueueMessageStore(destination);
            store = new JournalMessageStore(this, checkpointStore, destination);
            queues.put(destination, store);
        }
        return store;
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destinationName) throws IOException {
        JournalTopicMessageStore store = topics.get(destinationName);
        if (store == null) {
            TopicMessageStore checkpointStore = longTermPersistence.createTopicMessageStore(destinationName);
            store = new JournalTopicMessageStore(this, checkpointStore, destinationName);
            topics.put(destinationName, store);
        }
        return store;
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     *
     * @param destination Destination to forget
     */
    public void removeQueueMessageStore(ActiveMQQueue destination) {
        queues.remove(destination);
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     *
     * @param destination Destination to forget
     */
    public void removeTopicMessageStore(ActiveMQTopic destination) {
        topics.remove(destination);
    }

    public TransactionStore createTransactionStore() throws IOException {
        return transactionStore;
    }

    public long getLastMessageBrokerSequenceId() throws IOException {
        return longTermPersistence.getLastMessageBrokerSequenceId();
    }

    public void beginTransaction(ConnectionContext context) throws IOException {
        longTermPersistence.beginTransaction(context);
    }

    public void commitTransaction(ConnectionContext context) throws IOException {
        longTermPersistence.commitTransaction(context);
    }

    public void rollbackTransaction(ConnectionContext context) throws IOException {
        longTermPersistence.rollbackTransaction(context);
    }

    public synchronized void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        checkpointExecutor = new ThreadPoolExecutor(maxCheckpointWorkers, maxCheckpointWorkers, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            public Thread newThread(Runnable runable) {
                Thread t = new Thread(runable, "Journal checkpoint worker");
                t.setPriority(7);
                return t;
            }
        });
        // checkpointExecutor.allowCoreThreadTimeOut(true);

        this.usageManager.getMemoryUsage().addUsageListener(this);

        if (longTermPersistence instanceof JDBCPersistenceAdapter) {
            // Disabled periodic clean up as it deadlocks with the checkpoint
            // operations.
            ((JDBCPersistenceAdapter)longTermPersistence).setCleanupPeriod(0);
        }

        longTermPersistence.start();
        createTransactionStore();
        recover();

        // Do a checkpoint periodically.
        scheduler.executePeriodically(periodicCheckpointTask, checkpointInterval / 10);

    }

    public void stop() throws Exception {

        this.usageManager.getMemoryUsage().removeUsageListener(this);
        if (!started.compareAndSet(true, false)) {
            return;
        }

        scheduler.cancel(periodicCheckpointTask);

        // Take one final checkpoint and stop checkpoint processing.
        checkpoint(true, true);
        checkpointTask.shutdown();
        checkpointExecutor.shutdown();

        queues.clear();
        topics.clear();

        IOException firstException = null;
        try {
            journal.close();
        } catch (Exception e) {
            firstException = IOExceptionSupport.create("Failed to close journals: " + e, e);
        }
        longTermPersistence.stop();

        if (firstException != null) {
            throw firstException;
        }
    }

    // Properties
    // -------------------------------------------------------------------------
    public PersistenceAdapter getLongTermPersistence() {
        return longTermPersistence;
    }

    /**
     * @return Returns the wireFormat.
     */
    public WireFormat getWireFormat() {
        return wireFormat;
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    /**
     * The Journal give us a call back so that we can move old data out of the
     * journal. Taking a checkpoint does this for us.
     * 
     * @see org.apache.activemq.journal.JournalEventListener#overflowNotification(org.apache.activemq.journal.RecordLocation)
     */
    public void overflowNotification(RecordLocation safeLocation) {
        checkpoint(false, true);
    }

    /**
     * When we checkpoint we move all the journalled data to long term storage.
     * 
     * @param stopping
     * @param b
     */
    public void checkpoint(boolean sync, boolean fullCheckpoint) {
        try {
            if (journal == null) {
                throw new IllegalStateException("Journal is closed.");
            }

            long now = System.currentTimeMillis();
            CountDownLatch latch = null;
            synchronized (this) {
                latch = nextCheckpointCountDownLatch;
                lastCheckpointRequest = now;
                if (fullCheckpoint) {
                    this.fullCheckPoint = true;
                }
            }

            checkpointTask.wakeup();

            if (sync) {
                LOG.debug("Waking for checkpoint to complete.");
                latch.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Request to start checkpoint failed: " + e, e);
        }
    }

    public void checkpoint(boolean sync) {
        checkpoint(sync, sync);
    }

    /**
     * This does the actual checkpoint.
     * 
     * @return
     */
    public boolean doCheckpoint() {
        CountDownLatch latch = null;
        boolean fullCheckpoint;
        synchronized (this) {
            latch = nextCheckpointCountDownLatch;
            nextCheckpointCountDownLatch = new CountDownLatch(1);
            fullCheckpoint = this.fullCheckPoint;
            this.fullCheckPoint = false;
        }
        try {

            LOG.debug("Checkpoint started.");
            RecordLocation newMark = null;

            ArrayList<FutureTask<RecordLocation>> futureTasks = new ArrayList<FutureTask<RecordLocation>>(queues.size() + topics.size());

            //
            // We do many partial checkpoints (fullCheckpoint==false) to move
            // topic messages
            // to long term store as soon as possible.
            // 
            // We want to avoid doing that for queue messages since removes the
            // come in the same
            // checkpoint cycle will nullify the previous message add.
            // Therefore, we only
            // checkpoint queues on the fullCheckpoint cycles.
            //
            if (fullCheckpoint) {
                Iterator<JournalMessageStore> iterator = queues.values().iterator();
                while (iterator.hasNext()) {
                    try {
                        final JournalMessageStore ms = iterator.next();
                        FutureTask<RecordLocation> task = new FutureTask<RecordLocation>(new Callable<RecordLocation>() {
                            public RecordLocation call() throws Exception {
                                return ms.checkpoint();
                            }
                        });
                        futureTasks.add(task);
                        checkpointExecutor.execute(task);
                    } catch (Exception e) {
                        LOG.error("Failed to checkpoint a message store: " + e, e);
                    }
                }
            }

            Iterator<JournalTopicMessageStore> iterator = topics.values().iterator();
            while (iterator.hasNext()) {
                try {
                    final JournalTopicMessageStore ms = iterator.next();
                    FutureTask<RecordLocation> task = new FutureTask<RecordLocation>(new Callable<RecordLocation>() {
                        public RecordLocation call() throws Exception {
                            return ms.checkpoint();
                        }
                    });
                    futureTasks.add(task);
                    checkpointExecutor.execute(task);
                } catch (Exception e) {
                    LOG.error("Failed to checkpoint a message store: " + e, e);
                }
            }

            try {
                for (Iterator<FutureTask<RecordLocation>> iter = futureTasks.iterator(); iter.hasNext();) {
                    FutureTask<RecordLocation> ft = iter.next();
                    RecordLocation mark = ft.get();
                    // We only set a newMark on full checkpoints.
                    if (fullCheckpoint) {
                        if (mark != null && (newMark == null || newMark.compareTo(mark) < 0)) {
                            newMark = mark;
                        }
                    }
                }
            } catch (Throwable e) {
                LOG.error("Failed to checkpoint a message store: " + e, e);
            }

            if (fullCheckpoint) {
                try {
                    if (newMark != null) {
                        LOG.debug("Marking journal at: " + newMark);
                        journal.setMark(newMark, true);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to mark the Journal: " + e, e);
                }

                if (longTermPersistence instanceof JDBCPersistenceAdapter) {
                    // We may be check pointing more often than the
                    // checkpointInterval if under high use
                    // But we don't want to clean up the db that often.
                    long now = System.currentTimeMillis();
                    if (now > lastCleanup + checkpointInterval) {
                        lastCleanup = now;
                        ((JDBCPersistenceAdapter)longTermPersistence).cleanup();
                    }
                }
            }

            LOG.debug("Checkpoint done.");
        } finally {
            latch.countDown();
        }
        synchronized (this) {
            return this.fullCheckPoint;
        }

    }

    /**
     * @param location
     * @return
     * @throws IOException
     */
    public DataStructure readCommand(RecordLocation location) throws IOException {
        try {
            Packet packet = journal.read(location);
            return (DataStructure)wireFormat.unmarshal(toByteSequence(packet));
        } catch (InvalidRecordLocationException e) {
            throw createReadException(location, e);
        } catch (IOException e) {
            throw createReadException(location, e);
        }
    }

    /**
     * Move all the messages that were in the journal into long term storage. We
     * just replay and do a checkpoint.
     * 
     * @throws IOException
     * @throws IOException
     * @throws InvalidRecordLocationException
     * @throws IllegalStateException
     */
    private void recover() throws IllegalStateException, InvalidRecordLocationException, IOException, IOException {

        RecordLocation pos = null;
        int transactionCounter = 0;

        LOG.info("Journal Recovery Started from: " + journal);
        ConnectionContext context = new ConnectionContext(new NonCachedMessageEvaluationContext());

        // While we have records in the journal.
        while ((pos = journal.getNextRecordLocation(pos)) != null) {
            Packet data = journal.read(pos);
            DataStructure c = (DataStructure)wireFormat.unmarshal(toByteSequence(data));

            if (c instanceof Message) {
                Message message = (Message)c;
                JournalMessageStore store = (JournalMessageStore)createMessageStore(message.getDestination());
                if (message.isInTransaction()) {
                    transactionStore.addMessage(store, message, pos);
                } else {
                    store.replayAddMessage(context, message);
                    transactionCounter++;
                }
            } else {
                switch (c.getDataStructureType()) {
                case JournalQueueAck.DATA_STRUCTURE_TYPE: {
                    JournalQueueAck command = (JournalQueueAck)c;
                    JournalMessageStore store = (JournalMessageStore)createMessageStore(command.getDestination());
                    if (command.getMessageAck().isInTransaction()) {
                        transactionStore.removeMessage(store, command.getMessageAck(), pos);
                    } else {
                        store.replayRemoveMessage(context, command.getMessageAck());
                        transactionCounter++;
                    }
                }
                    break;
                case JournalTopicAck.DATA_STRUCTURE_TYPE: {
                    JournalTopicAck command = (JournalTopicAck)c;
                    JournalTopicMessageStore store = (JournalTopicMessageStore)createMessageStore(command.getDestination());
                    if (command.getTransactionId() != null) {
                        transactionStore.acknowledge(store, command, pos);
                    } else {
                        store.replayAcknowledge(context, command.getClientId(), command.getSubscritionName(), command.getMessageId());
                        transactionCounter++;
                    }
                }
                    break;
                case JournalTransaction.DATA_STRUCTURE_TYPE: {
                    JournalTransaction command = (JournalTransaction)c;
                    try {
                        // Try to replay the packet.
                        switch (command.getType()) {
                        case JournalTransaction.XA_PREPARE:
                            transactionStore.replayPrepare(command.getTransactionId());
                            break;
                        case JournalTransaction.XA_COMMIT:
                        case JournalTransaction.LOCAL_COMMIT:
                            Tx tx = transactionStore.replayCommit(command.getTransactionId(), command.getWasPrepared());
                            if (tx == null) {
                                break; // We may be trying to replay a commit
                            }
                            // that
                            // was already committed.

                            // Replay the committed operations.
                            tx.getOperations();
                            for (Iterator iter = tx.getOperations().iterator(); iter.hasNext();) {
                                TxOperation op = (TxOperation)iter.next();
                                if (op.operationType == TxOperation.ADD_OPERATION_TYPE) {
                                    op.store.replayAddMessage(context, (Message)op.data);
                                }
                                if (op.operationType == TxOperation.REMOVE_OPERATION_TYPE) {
                                    op.store.replayRemoveMessage(context, (MessageAck)op.data);
                                }
                                if (op.operationType == TxOperation.ACK_OPERATION_TYPE) {
                                    JournalTopicAck ack = (JournalTopicAck)op.data;
                                    ((JournalTopicMessageStore)op.store).replayAcknowledge(context, ack.getClientId(), ack.getSubscritionName(), ack.getMessageId());
                                }
                            }
                            transactionCounter++;
                            break;
                        case JournalTransaction.LOCAL_ROLLBACK:
                        case JournalTransaction.XA_ROLLBACK:
                            transactionStore.replayRollback(command.getTransactionId());
                            break;
                        default:
                            throw new IOException("Invalid journal command type: " + command.getType());
                        }
                    } catch (IOException e) {
                        LOG.error("Recovery Failure: Could not replay: " + c + ", reason: " + e, e);
                    }
                }
                    break;
                case JournalTrace.DATA_STRUCTURE_TYPE:
                    JournalTrace trace = (JournalTrace)c;
                    LOG.debug("TRACE Entry: " + trace.getMessage());
                    break;
                default:
                    LOG.error("Unknown type of record in transaction log which will be discarded: " + c);
                }
            }
        }

        RecordLocation location = writeTraceMessage("RECOVERED", true);
        journal.setMark(location, true);

        LOG.info("Journal Recovered: " + transactionCounter + " message(s) in transactions recovered.");
    }

    private IOException createReadException(RecordLocation location, Exception e) {
        return IOExceptionSupport.create("Failed to read to journal for: " + location + ". Reason: " + e, e);
    }

    protected IOException createWriteException(DataStructure packet, Exception e) {
        return IOExceptionSupport.create("Failed to write to journal for: " + packet + ". Reason: " + e, e);
    }

    protected IOException createWriteException(String command, Exception e) {
        return IOExceptionSupport.create("Failed to write to journal for command: " + command + ". Reason: " + e, e);
    }

    protected IOException createRecoveryFailedException(Exception e) {
        return IOExceptionSupport.create("Failed to recover from journal. Reason: " + e, e);
    }

    /**
     * @param command
     * @param sync
     * @return
     * @throws IOException
     */
    public RecordLocation writeCommand(DataStructure command, boolean sync) throws IOException {
        if (started.get()) {
            try {
        	    return journal.write(toPacket(wireFormat.marshal(command)), sync);
            } catch (IOException ioe) {
        	    LOG.error("Cannot write to the journal", ioe);
        	    stopBroker();
        	    throw ioe;
            }
        }
        throw new IOException("closed");
    }

    private RecordLocation writeTraceMessage(String message, boolean sync) throws IOException {
        JournalTrace trace = new JournalTrace();
        trace.setMessage(message);
        return writeCommand(trace, sync);
    }

    public void onUsageChanged(Usage usage, int oldPercentUsage, int newPercentUsage) {
        newPercentUsage = (newPercentUsage / 10) * 10;
        oldPercentUsage = (oldPercentUsage / 10) * 10;
        if (newPercentUsage >= 70 && oldPercentUsage < newPercentUsage) {
            boolean sync = newPercentUsage >= 90;
            checkpoint(sync, true);
        }
    }

    public JournalTransactionStore getTransactionStore() {
        return transactionStore;
    }

    public void deleteAllMessages() throws IOException {
        try {
            JournalTrace trace = new JournalTrace();
            trace.setMessage("DELETED");
            RecordLocation location = journal.write(toPacket(wireFormat.marshal(trace)), false);
            journal.setMark(location, true);
            LOG.info("Journal deleted: ");
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
        longTermPersistence.deleteAllMessages();
    }

    public SystemUsage getUsageManager() {
        return usageManager;
    }

    public int getMaxCheckpointMessageAddSize() {
        return maxCheckpointMessageAddSize;
    }

    public void setMaxCheckpointMessageAddSize(int maxCheckpointMessageAddSize) {
        this.maxCheckpointMessageAddSize = maxCheckpointMessageAddSize;
    }

    public int getMaxCheckpointWorkers() {
        return maxCheckpointWorkers;
    }

    public void setMaxCheckpointWorkers(int maxCheckpointWorkers) {
        this.maxCheckpointWorkers = maxCheckpointWorkers;
    }

    public boolean isUseExternalMessageReferences() {
        return false;
    }

    public void setUseExternalMessageReferences(boolean enable) {
        if (enable) {
            throw new IllegalArgumentException("The journal does not support message references.");
        }
    }

    public Packet toPacket(ByteSequence sequence) {
        return new ByteArrayPacket(new org.apache.activeio.packet.ByteSequence(sequence.data, sequence.offset, sequence.length));
    }

    public ByteSequence toByteSequence(Packet packet) {
        org.apache.activeio.packet.ByteSequence sequence = packet.asByteSequence();
        return new ByteSequence(sequence.getData(), sequence.getOffset(), sequence.getLength());
    }

    public void setBrokerName(String brokerName) {
        longTermPersistence.setBrokerName(brokerName);
    }

    public String toString() {
        return "JournalPersistenceAdapator(" + longTermPersistence + ")";
    }

    public void setDirectory(File dir) {
    }
    
    public long size(){
        return 0;
    }

    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
        PersistenceAdapter pa = getLongTermPersistence();
        if( pa instanceof BrokerServiceAware ) {
            ((BrokerServiceAware)pa).setBrokerService(brokerService);
        }
    }
    
    protected void stopBroker() {
        new Thread() {
           public void run() {
        	   try {
    	            brokerService.stop();
    	        } catch (Exception e) {
    	            LOG.warn("Failure occured while stopping broker", e);
    	        }    			
    		}
    	}.start();
    }

}
