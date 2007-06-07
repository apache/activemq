/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.store.amq;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activeio.journal.Journal;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.BrokerService;
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
import org.apache.activemq.kaha.impl.async.AsyncDataManager;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.memory.UsageListener;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ReferenceStore;
import org.apache.activemq.store.ReferenceStoreAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TopicReferenceStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.amq.AMQTransactionStore.Tx;
import org.apache.activemq.store.amq.AMQTransactionStore.TxOperation;
import org.apache.activemq.store.kahadaptor.KahaReferenceStoreAdapter;
import org.apache.activemq.thread.DefaultThreadPools;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An implementation of {@link PersistenceAdapter} designed for use with a {@link Journal} and then check pointing
 * asynchronously on a timeout with some other long term persistent storage.
 * 
 * @org.apache.xbean.XBean element="amqPersistenceAdapter"
 * 
 * @version $Revision: 1.17 $
 */
public class AMQPersistenceAdapter implements PersistenceAdapter, UsageListener, BrokerServiceAware {

    private static final Log log=LogFactory.getLog(AMQPersistenceAdapter.class);
    private final ConcurrentHashMap<ActiveMQQueue,AMQMessageStore> queues=new ConcurrentHashMap<ActiveMQQueue,AMQMessageStore>();
    private final ConcurrentHashMap<ActiveMQTopic,AMQMessageStore> topics=new ConcurrentHashMap<ActiveMQTopic,AMQMessageStore>();
    private AsyncDataManager asyncDataManager;
    private ReferenceStoreAdapter referenceStoreAdapter;
    private TaskRunnerFactory taskRunnerFactory;
    private WireFormat wireFormat=new OpenWireFormat();
    private UsageManager usageManager;
    private long cleanupInterval=1000*60;
    private long checkpointInterval=1000*10;
    private int maxCheckpointWorkers=1;
    private int maxCheckpointMessageAddSize=1024*4;
    private AMQTransactionStore transactionStore=new AMQTransactionStore(this);
    private TaskRunner checkpointTask;
    private CountDownLatch nextCheckpointCountDownLatch=new CountDownLatch(1);
    private final AtomicBoolean started=new AtomicBoolean(false);
    private Runnable periodicCheckpointTask;
    private Runnable periodicCleanupTask;
    private boolean deleteAllMessages;
    private boolean syncOnWrite;
    private String brokerName="";
    private File directory;
    private BrokerService brokerService;

    public String getBrokerName(){
        return this.brokerName;
    }

    public void setBrokerName(String brokerName){
        this.brokerName=brokerName;
        if(this.referenceStoreAdapter!=null){
            this.referenceStoreAdapter.setBrokerName(brokerName);
        }
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    public synchronized void start() throws Exception{
        if(!started.compareAndSet(false,true))
            return;
        if(this.directory==null) {
            if (brokerService != null) {
                this.directory = brokerService.getBrokerDataDirectory();
            }
            else {
                this.directory=new File(IOHelper.getDefaultDataDirectory(),brokerName);
                this.directory=new File(directory,"amqstore");
            }
        }
        log.info("AMQStore starting using directory: " + directory);
        this.directory.mkdirs();

        if(this.usageManager!=null){
            this.usageManager.addUsageListener(this);
        }
        if(asyncDataManager==null){
            asyncDataManager=createAsyncDataManager();
        }
        if(referenceStoreAdapter==null){
            referenceStoreAdapter=createReferenceStoreAdapter();
        }
        referenceStoreAdapter.setDirectory(new File(directory,"kr-store"));
        referenceStoreAdapter.setBrokerName(getBrokerName());
        referenceStoreAdapter.setUsageManager(usageManager);
        if(taskRunnerFactory==null){
            taskRunnerFactory=createTaskRunnerFactory();
        }
        asyncDataManager.start();
        if(deleteAllMessages){
            asyncDataManager.delete();
            try{
                JournalTrace trace=new JournalTrace();
                trace.setMessage("DELETED "+new Date());
                Location location=asyncDataManager.write(wireFormat.marshal(trace),false);
                asyncDataManager.setMark(location,true);
                log.info("Journal deleted: ");
                deleteAllMessages=false;
            }catch(IOException e){
                throw e;
            }catch(Throwable e){
                throw IOExceptionSupport.create(e);
            }
            referenceStoreAdapter.deleteAllMessages();
        }
        referenceStoreAdapter.start();
        Set<Integer> files=referenceStoreAdapter.getReferenceFileIdsInUse();
        log.info("Active data files: "+files);
        checkpointTask=taskRunnerFactory.createTaskRunner(new Task(){

            public boolean iterate(){
                doCheckpoint();
                return false;
            }
        },"ActiveMQ Journal Checkpoint Worker");
        createTransactionStore();
        
//
// The following was attempting to reduce startup times by avoiding the log 
// file scanning that recovery performs.  The problem with it is that XA transactions
// only live in transaction log and are not stored in the reference store, but they still
// need to be recovered when the broker starts up.  Perhaps on a graceful shutdown we 
// should record all the in flight XA transactions to a file to avoid having to scan 
// the entire transaction log.  For now going to comment this bit out.        
//        
//        if(referenceStoreAdapter.isStoreValid()==false){
//            log.warn("The ReferenceStore is not valid - recovering ...");
//            recover();
//            log.info("Finished recovering the ReferenceStore");
//        }else {
//            Location location=writeTraceMessage("RECOVERED "+new Date(),true);
//            asyncDataManager.setMark(location,true);
//        }
        
        recover();
        
        // Do a checkpoint periodically.
        periodicCheckpointTask=new Runnable(){

            public void run(){
                checkpoint(false);
            }
        };
        Scheduler.executePeriodically(periodicCheckpointTask,checkpointInterval);
        periodicCleanupTask=new Runnable(){

            public void run(){
                cleanup();
            }
        };
        Scheduler.executePeriodically(periodicCleanupTask,cleanupInterval);
    }

    public void stop() throws Exception{
      
        if(!started.compareAndSet(true,false))
            return;
        this.usageManager.removeUsageListener(this);
        Scheduler.cancel(periodicCheckpointTask);
        Scheduler.cancel(periodicCleanupTask);
        Iterator<AMQMessageStore> iterator=queues.values().iterator();
        while(iterator.hasNext()){
            AMQMessageStore ms=iterator.next();
            ms.stop();
        }
        iterator=topics.values().iterator();
        while(iterator.hasNext()){
            final AMQTopicMessageStore ms=(AMQTopicMessageStore)iterator.next();
            ms.stop();
        }
        // Take one final checkpoint and stop checkpoint processing.
        checkpoint(true);
        checkpointTask.shutdown();
        queues.clear();
        topics.clear();
        IOException firstException=null;
        referenceStoreAdapter.stop();
        try{
            log.debug("Journal close");
            asyncDataManager.close();
        }catch(Exception e){
            firstException=IOExceptionSupport.create("Failed to close journals: "+e,e);
        }
        if(firstException!=null){
            throw firstException;
        }
    }

    /**
     * When we checkpoint we move all the journalled data to long term storage.
     * @param sync    
     */
    public void checkpoint(boolean sync){
        try{
            if(asyncDataManager==null)
                throw new IllegalStateException("Journal is closed.");
            CountDownLatch latch=null;
            synchronized(this){
                latch=nextCheckpointCountDownLatch;
            }
            checkpointTask.wakeup();
            if(sync){
                if(log.isDebugEnabled()){
                    log.debug("Waitng for checkpoint to complete.");
                }
                latch.await();
            }
            referenceStoreAdapter.checkpoint(sync);
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
            log.warn("Request to start checkpoint failed: "+e,e);
        }catch(IOException e){
            log.error("checkpoint failed: "+e,e);
        }
    }

    /**
     * This does the actual checkpoint.
     * 
     * @return true if successful
     */
    public boolean doCheckpoint(){
        CountDownLatch latch=null;
        synchronized(this){
            latch=nextCheckpointCountDownLatch;
            nextCheckpointCountDownLatch=new CountDownLatch(1);
        }
        try{
            if(log.isDebugEnabled()){
                log.debug("Checkpoint started.");
            }
           
            Location newMark=null;
            Iterator<AMQMessageStore> iterator=queues.values().iterator();
            while(iterator.hasNext()){
                final AMQMessageStore ms=iterator.next();
                Location mark=(Location)ms.getMark();
                if(mark!=null&&(newMark==null||newMark.compareTo(mark)<0)){
                    newMark=mark;
                }
            }
            iterator=topics.values().iterator();
            while(iterator.hasNext()){
                final AMQTopicMessageStore ms=(AMQTopicMessageStore)iterator.next();
                Location mark=(Location)ms.getMark();
                if(mark!=null&&(newMark==null||newMark.compareTo(mark)<0)){
                    newMark=mark;
                }
            }
            try{
                if(newMark!=null){
                    if(log.isDebugEnabled()){
                        log.debug("Marking journal at: "+newMark);
                    }
                    asyncDataManager.setMark(newMark,false);
                    writeTraceMessage("CHECKPOINT "+new Date(),true);
                }
            }catch(Exception e){
                log.error("Failed to mark the Journal: "+e,e);
            }
            if(log.isDebugEnabled()){
                log.debug("Checkpoint done.");
            }
        }finally{
            latch.countDown();
        }
        return true;
    }

    /**
     * Cleans up the data files
     * 
     * @return
     * @throws IOException
     */
    public void cleanup(){
        try{
            Set<Integer> inUse=referenceStoreAdapter.getReferenceFileIdsInUse();
            asyncDataManager.consolidateDataFilesNotIn(inUse);
        }catch(IOException e){
            log.error("Could not cleanup data files: "+e,e);
        }
    }

    public Set<ActiveMQDestination> getDestinations(){
        Set<ActiveMQDestination> destinations=new HashSet<ActiveMQDestination>(referenceStoreAdapter.getDestinations());
        destinations.addAll(queues.keySet());
        destinations.addAll(topics.keySet());
        return destinations;
    }

    private MessageStore createMessageStore(ActiveMQDestination destination) throws IOException{
        if(destination.isQueue()){
            return createQueueMessageStore((ActiveMQQueue)destination);
        }else{
            return createTopicMessageStore((ActiveMQTopic)destination);
        }
    }

    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException{
        AMQMessageStore store=queues.get(destination);
        if(store==null){
            ReferenceStore checkpointStore=referenceStoreAdapter.createQueueReferenceStore(destination);
            store=new AMQMessageStore(this,checkpointStore,destination);
            try{
                store.start();
            }catch(Exception e){
                throw IOExceptionSupport.create(e);
            }
            queues.put(destination,store);
        }
        return store;
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destinationName) throws IOException{
        AMQTopicMessageStore store=(AMQTopicMessageStore)topics.get(destinationName);
        if(store==null){
            TopicReferenceStore checkpointStore=referenceStoreAdapter.createTopicReferenceStore(destinationName);
            store=new AMQTopicMessageStore(this,checkpointStore,destinationName);
            try{
                store.start();
            }catch(Exception e){
                throw IOExceptionSupport.create(e);
            }
            topics.put(destinationName,store);
        }
        return store;
    }

    public TransactionStore createTransactionStore() throws IOException{
        return transactionStore;
    }

    public long getLastMessageBrokerSequenceId() throws IOException{
        return referenceStoreAdapter.getLastMessageBrokerSequenceId();
    }

    public void beginTransaction(ConnectionContext context) throws IOException{
        referenceStoreAdapter.beginTransaction(context);
    }

    public void commitTransaction(ConnectionContext context) throws IOException{
        referenceStoreAdapter.commitTransaction(context);
    }

    public void rollbackTransaction(ConnectionContext context) throws IOException{
        referenceStoreAdapter.rollbackTransaction(context);
    }

    /**
     * @param location
     * @return
     * @throws IOException
     */
    public DataStructure readCommand(Location location) throws IOException{
        try{
            ByteSequence packet=asyncDataManager.read(location);
            return (DataStructure)wireFormat.unmarshal(packet);
        }catch(IOException e){
            throw createReadException(location,e);
        }
    }

    /**
     * Move all the messages that were in the journal into long term storage. We just replay and do a checkpoint.
     * 
     * @throws IOException
     * @throws IOException
     * @throws InvalidLocationException
     * @throws IllegalStateException
     */
    private void recover() throws IllegalStateException,IOException{
        Location pos=null;
        int redoCounter=0;
        log.info("Journal Recovery Started from: "+asyncDataManager);
        long start=System.currentTimeMillis();
        ConnectionContext context=new ConnectionContext();
        // While we have records in the journal.
        while((pos=asyncDataManager.getNextLocation(pos))!=null){
            ByteSequence data=asyncDataManager.read(pos);
            DataStructure c=(DataStructure)wireFormat.unmarshal(data);
            if(c instanceof Message){
                Message message=(Message)c;
                AMQMessageStore store=(AMQMessageStore)createMessageStore(message.getDestination());
                if(message.isInTransaction()){
                    transactionStore.addMessage(store,message,pos);
                }else{
                    if(store.replayAddMessage(context,message,pos)){
                        redoCounter++;
                    }
                }
            }else{
                switch(c.getDataStructureType()){
                case JournalQueueAck.DATA_STRUCTURE_TYPE: {
                    JournalQueueAck command=(JournalQueueAck)c;
                    AMQMessageStore store=(AMQMessageStore)createMessageStore(command.getDestination());
                    if(command.getMessageAck().isInTransaction()){
                        transactionStore.removeMessage(store,command.getMessageAck(),pos);
                    }else{
                        if(store.replayRemoveMessage(context,command.getMessageAck())){
                            redoCounter++;
                        }
                    }
                }
                    break;
                case JournalTopicAck.DATA_STRUCTURE_TYPE: {
                    JournalTopicAck command=(JournalTopicAck)c;
                    AMQTopicMessageStore store=(AMQTopicMessageStore)createMessageStore(command.getDestination());
                    if(command.getTransactionId()!=null){
                        transactionStore.acknowledge(store,command,pos);
                    }else{
                        if(store.replayAcknowledge(context,command.getClientId(),command.getSubscritionName(),command
                                .getMessageId())){
                            redoCounter++;
                        }
                    }
                }
                    break;
                case JournalTransaction.DATA_STRUCTURE_TYPE: {
                    JournalTransaction command=(JournalTransaction)c;
                    try{
                        // Try to replay the packet.
                        switch(command.getType()){
                        case JournalTransaction.XA_PREPARE:
                            transactionStore.replayPrepare(command.getTransactionId());
                            break;
                        case JournalTransaction.XA_COMMIT:
                        case JournalTransaction.LOCAL_COMMIT:
                            Tx tx=transactionStore.replayCommit(command.getTransactionId(),command.getWasPrepared());
                            if(tx==null)
                                break; // We may be trying to replay a commit that
                            // was already committed.
                            // Replay the committed operations.
                            tx.getOperations();
                            for(Iterator iter=tx.getOperations().iterator();iter.hasNext();){
                                TxOperation op=(TxOperation)iter.next();
                                if(op.operationType==TxOperation.ADD_OPERATION_TYPE){
                                    if(op.store.replayAddMessage(context,(Message)op.data,op.location))
                                        redoCounter++;
                                }
                                if(op.operationType==TxOperation.REMOVE_OPERATION_TYPE){
                                    if(op.store.replayRemoveMessage(context,(MessageAck)op.data))
                                        redoCounter++;
                                }
                                if(op.operationType==TxOperation.ACK_OPERATION_TYPE){
                                    JournalTopicAck ack=(JournalTopicAck)op.data;
                                    if(((AMQTopicMessageStore)op.store).replayAcknowledge(context,ack.getClientId(),ack
                                            .getSubscritionName(),ack.getMessageId())){
                                        redoCounter++;
                                    }
                                }
                            }
                            break;
                        case JournalTransaction.LOCAL_ROLLBACK:
                        case JournalTransaction.XA_ROLLBACK:
                            transactionStore.replayRollback(command.getTransactionId());
                            break;
                        }
                    }catch(IOException e){
                        log.error("Recovery Failure: Could not replay: "+c+", reason: "+e,e);
                    }
                }
                    break;
                case JournalTrace.DATA_STRUCTURE_TYPE:
                    JournalTrace trace=(JournalTrace)c;
                    log.debug("TRACE Entry: "+trace.getMessage());
                    break;
                default:
                    log.error("Unknown type of record in transaction log which will be discarded: "+c);
                }
            }
        }
        Location location=writeTraceMessage("RECOVERED "+new Date(),true);
        asyncDataManager.setMark(location,true);
        long end=System.currentTimeMillis();
        log.info("Recovered "+redoCounter+" operations from redo log in "+((end-start)/1000.0f)+" seconds.");
    }

    private IOException createReadException(Location location,Exception e){
        return IOExceptionSupport.create("Failed to read to journal for: "+location+". Reason: "+e,e);
    }

    protected IOException createWriteException(DataStructure packet,Exception e){
        return IOExceptionSupport.create("Failed to write to journal for: "+packet+". Reason: "+e,e);
    }

    protected IOException createWriteException(String command,Exception e){
        return IOExceptionSupport.create("Failed to write to journal for command: "+command+". Reason: "+e,e);
    }

    protected IOException createRecoveryFailedException(Exception e){
        return IOExceptionSupport.create("Failed to recover from journal. Reason: "+e,e);
    }

    /**
     * 
     * @param command
     * @param syncHint
     * @return
     * @throws IOException
     */
    public Location writeCommand(DataStructure command,boolean syncHint) throws IOException{
        return asyncDataManager.write(wireFormat.marshal(command),(syncHint&&syncOnWrite));
    }

    private Location writeTraceMessage(String message,boolean sync) throws IOException{
        JournalTrace trace=new JournalTrace();
        trace.setMessage(message);
        return writeCommand(trace,sync);
    }

    public void onMemoryUseChanged(UsageManager memoryManager,int oldPercentUsage,int newPercentUsage){
        newPercentUsage=((newPercentUsage)/10)*10;
        oldPercentUsage=((oldPercentUsage)/10)*10;
        if(newPercentUsage>=70&&oldPercentUsage<newPercentUsage){
            checkpoint(false);
        }
    }

    public AMQTransactionStore getTransactionStore(){
        return transactionStore;
    }

    public void deleteAllMessages() throws IOException{
        deleteAllMessages=true;
    }

    public String toString(){
        return "AMQPersistenceAdapter("+directory+")";
    }

    // /////////////////////////////////////////////////////////////////
    // Subclass overridables
    // /////////////////////////////////////////////////////////////////
    protected AsyncDataManager createAsyncDataManager(){
        AsyncDataManager manager=new AsyncDataManager();
        manager.setDirectory(new File(directory,"journal"));
        return manager;
    }

    protected KahaReferenceStoreAdapter createReferenceStoreAdapter() throws IOException{
        KahaReferenceStoreAdapter adaptor=new KahaReferenceStoreAdapter();
        return adaptor;
    }

    protected TaskRunnerFactory createTaskRunnerFactory(){
        return DefaultThreadPools.getDefaultTaskRunnerFactory();
    }

    // /////////////////////////////////////////////////////////////////
    // Property Accessors
    // /////////////////////////////////////////////////////////////////
    public AsyncDataManager getAsyncDataManager(){
        return asyncDataManager;
    }

    public void setAsyncDataManager(AsyncDataManager asyncDataManager){
        this.asyncDataManager=asyncDataManager;
    }

    public ReferenceStoreAdapter getReferenceStoreAdapter(){
        return referenceStoreAdapter;
    }

    public TaskRunnerFactory getTaskRunnerFactory(){
        return taskRunnerFactory;
    }

    public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory){
        this.taskRunnerFactory=taskRunnerFactory;
    }

    /**
     * @return Returns the wireFormat.
     */
    public WireFormat getWireFormat(){
        return wireFormat;
    }

    public void setWireFormat(WireFormat wireFormat){
        this.wireFormat=wireFormat;
    }

    public UsageManager getUsageManager(){
        return usageManager;
    }

    public void setUsageManager(UsageManager usageManager){
        this.usageManager=usageManager;
    }

    public int getMaxCheckpointMessageAddSize(){
        return maxCheckpointMessageAddSize;
    }

    public void setMaxCheckpointMessageAddSize(int maxCheckpointMessageAddSize){
        this.maxCheckpointMessageAddSize=maxCheckpointMessageAddSize;
    }

    public int getMaxCheckpointWorkers(){
        return maxCheckpointWorkers;
    }

    public void setMaxCheckpointWorkers(int maxCheckpointWorkers){
        this.maxCheckpointWorkers=maxCheckpointWorkers;
    }

    public File getDirectory(){
        return directory;
    }

    public void setDirectory(File directory){
        this.directory=directory;
    }

    public boolean isSyncOnWrite(){
        return this.syncOnWrite;
    }

    public void setSyncOnWrite(boolean syncOnWrite){
        this.syncOnWrite=syncOnWrite;
    }

    
    /**
     * @param referenceStoreAdapter the referenceStoreAdapter to set
     */
    public void setReferenceStoreAdapter(ReferenceStoreAdapter referenceStoreAdapter){
        this.referenceStoreAdapter=referenceStoreAdapter;
    }
}
