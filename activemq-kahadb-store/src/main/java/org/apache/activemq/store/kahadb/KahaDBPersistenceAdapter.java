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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.LockableServiceSupport;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.PersistenceAdapterView;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.JournaledStore;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.NoLocalSubscriptionAware;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterStatistics;
import org.apache.activemq.store.SharedFileLocker;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionIdTransformer;
import org.apache.activemq.store.TransactionIdTransformerAware;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.data.KahaLocalTransactionId;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.store.kahadb.data.KahaXATransactionId;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.ServiceStopper;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.apache.activemq.broker.jmx.BrokerMBeanSupport.createPersistenceAdapterName;

/**
 * An implementation of {@link PersistenceAdapter} designed for use with
 * KahaDB - Embedded Lightweight Non-Relational Database
 *
 * @org.apache.xbean.XBean element="kahaDB"
 *
 */
public class KahaDBPersistenceAdapter extends LockableServiceSupport implements PersistenceAdapter,
    JournaledStore, TransactionIdTransformerAware, NoLocalSubscriptionAware {

    private final KahaDBStore letter = new KahaDBStore();

    /**
     * @param context
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#beginTransaction(org.apache.activemq.broker.ConnectionContext)
     */
    @Override
    public void beginTransaction(ConnectionContext context) throws IOException {
        this.letter.beginTransaction(context);
    }

    /**
     * @param cleanup
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#checkpoint(boolean)
     */
    @Override
    public void checkpoint(boolean cleanup) throws IOException {
        this.letter.checkpoint(cleanup);
    }

    /**
     * @param context
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#commitTransaction(org.apache.activemq.broker.ConnectionContext)
     */
    @Override
    public void commitTransaction(ConnectionContext context) throws IOException {
        this.letter.commitTransaction(context);
    }

    /**
     * @param destination
     * @return MessageStore
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#createQueueMessageStore(org.apache.activemq.command.ActiveMQQueue)
     */
    @Override
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        return this.letter.createQueueMessageStore(destination);
    }

    /**
     * @param destination
     * @return TopicMessageStore
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#createTopicMessageStore(org.apache.activemq.command.ActiveMQTopic)
     */
    @Override
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        return this.letter.createTopicMessageStore(destination);
    }

    /**
     * @return TransactionStore
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#createTransactionStore()
     */
    @Override
    public TransactionStore createTransactionStore() throws IOException {
        return this.letter.createTransactionStore();
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#deleteAllMessages()
     */
    @Override
    public void deleteAllMessages() throws IOException {
        this.letter.deleteAllMessages();
    }

    /**
     * @return destinations
     * @see org.apache.activemq.store.PersistenceAdapter#getDestinations()
     */
    @Override
    public Set<ActiveMQDestination> getDestinations() {
        return this.letter.getDestinations();
    }

    /**
     * @return lastMessageBrokerSequenceId
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#getLastMessageBrokerSequenceId()
     */
    @Override
    public long getLastMessageBrokerSequenceId() throws IOException {
        return this.letter.getLastMessageBrokerSequenceId();
    }

    @Override
    public long getLastProducerSequenceId(ProducerId id) throws IOException {
        return this.letter.getLastProducerSequenceId(id);
    }

    @Override
    public void allowIOResumption() {
        this.letter.allowIOResumption();
    }

    /**
     * @param destination
     * @see org.apache.activemq.store.PersistenceAdapter#removeQueueMessageStore(org.apache.activemq.command.ActiveMQQueue)
     */
    @Override
    public void removeQueueMessageStore(ActiveMQQueue destination) {
        this.letter.removeQueueMessageStore(destination);
    }

    /**
     * @param destination
     * @see org.apache.activemq.store.PersistenceAdapter#removeTopicMessageStore(org.apache.activemq.command.ActiveMQTopic)
     */
    @Override
    public void removeTopicMessageStore(ActiveMQTopic destination) {
        this.letter.removeTopicMessageStore(destination);
    }

    /**
     * @param context
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#rollbackTransaction(org.apache.activemq.broker.ConnectionContext)
     */
    @Override
    public void rollbackTransaction(ConnectionContext context) throws IOException {
        this.letter.rollbackTransaction(context);
    }

    /**
     * @param brokerName
     * @see org.apache.activemq.store.PersistenceAdapter#setBrokerName(java.lang.String)
     */
    @Override
    public void setBrokerName(String brokerName) {
        this.letter.setBrokerName(brokerName);
    }

    /**
     * @param usageManager
     * @see org.apache.activemq.store.PersistenceAdapter#setUsageManager(org.apache.activemq.usage.SystemUsage)
     */
    @Override
    public void setUsageManager(SystemUsage usageManager) {
        this.letter.setUsageManager(usageManager);
    }

    /**
     * @return the size of the store
     * @see org.apache.activemq.store.PersistenceAdapter#size()
     */
    @Override
    public long size() {
        return this.letter.isStarted() ? this.letter.size() : 0l;
    }

    /**
     * @throws Exception
     * @see org.apache.activemq.Service#start()
     */
    @Override
    public void doStart() throws Exception {
        this.letter.start();

        if (brokerService != null && brokerService.isUseJmx()) {
            PersistenceAdapterView view = new PersistenceAdapterView(this);
            view.setInflightTransactionViewCallable(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return letter.getTransactions();
                }
            });
            view.setDataViewCallable(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return letter.getJournal().getFileMap().keySet().toString();
                }
            });

            view.setPersistenceAdapterStatistics(letter.persistenceAdapterStatistics);

            AnnotatedMBean.registerMBean(brokerService.getManagementContext(), view,
                    createPersistenceAdapterName(brokerService.getBrokerObjectName().toString(), toString()));
        }
    }

    /**
     * @throws Exception
     * @see org.apache.activemq.Service#stop()
     */
    @Override
    public void doStop(ServiceStopper stopper) throws Exception {
        this.letter.stop();

        if (brokerService != null && brokerService.isUseJmx()) {
            ObjectName brokerObjectName = brokerService.getBrokerObjectName();
            brokerService.getManagementContext().unregisterMBean(createPersistenceAdapterName(brokerObjectName.toString(), toString()));
        }
    }

    /**
     * Get the journalMaxFileLength
     *
     * @return the journalMaxFileLength
     */
    @Override
    public int getJournalMaxFileLength() {
        return this.letter.getJournalMaxFileLength();
    }

    /**
     * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can
     * be used
     *
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryIntPropertyEditor"
     */
    public void setJournalMaxFileLength(int journalMaxFileLength) {
        this.letter.setJournalMaxFileLength(journalMaxFileLength);
    }

    /**
     * Set the max number of producers (LRU cache) to track for duplicate sends
     */
    public void setMaxFailoverProducersToTrack(int maxFailoverProducersToTrack) {
        this.letter.setMaxFailoverProducersToTrack(maxFailoverProducersToTrack);
    }

    public int getMaxFailoverProducersToTrack() {
        return this.letter.getMaxFailoverProducersToTrack();
    }

    /**
     * set the audit window depth for duplicate suppression (should exceed the max transaction
     * batch)
     */
    public void setFailoverProducersAuditDepth(int failoverProducersAuditDepth) {
        this.letter.setFailoverProducersAuditDepth(failoverProducersAuditDepth);
    }

    public int getFailoverProducersAuditDepth() {
        return this.letter.getFailoverProducersAuditDepth();
    }

    /**
     * Get the checkpointInterval
     *
     * @return the checkpointInterval
     */
    public long getCheckpointInterval() {
        return this.letter.getCheckpointInterval();
    }

    /**
     * Set the checkpointInterval
     *
     * @param checkpointInterval
     *            the checkpointInterval to set
     */
    public void setCheckpointInterval(long checkpointInterval) {
        this.letter.setCheckpointInterval(checkpointInterval);
    }

    /**
     * Get the cleanupInterval
     *
     * @return the cleanupInterval
     */
    public long getCleanupInterval() {
        return this.letter.getCleanupInterval();
    }

    /**
     * Set the cleanupInterval
     *
     * @param cleanupInterval
     *            the cleanupInterval to set
     */
    public void setCleanupInterval(long cleanupInterval) {
        this.letter.setCleanupInterval(cleanupInterval);
    }

    /**
     * Get the indexWriteBatchSize
     *
     * @return the indexWriteBatchSize
     */
    public int getIndexWriteBatchSize() {
        return this.letter.getIndexWriteBatchSize();
    }

    /**
     * Set the indexWriteBatchSize
     * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can be used
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     * @param indexWriteBatchSize
     *            the indexWriteBatchSize to set
     */
    public void setIndexWriteBatchSize(int indexWriteBatchSize) {
        this.letter.setIndexWriteBatchSize(indexWriteBatchSize);
    }

    /**
     * Get the journalMaxWriteBatchSize
     *
     * @return the journalMaxWriteBatchSize
     */
    public int getJournalMaxWriteBatchSize() {
        return this.letter.getJournalMaxWriteBatchSize();
    }

    /**
     * Set the journalMaxWriteBatchSize
     *  * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can be used
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     * @param journalMaxWriteBatchSize
     *            the journalMaxWriteBatchSize to set
     */
    public void setJournalMaxWriteBatchSize(int journalMaxWriteBatchSize) {
        this.letter.setJournalMaxWriteBatchSize(journalMaxWriteBatchSize);
    }

    /**
     * Get the enableIndexWriteAsync
     *
     * @return the enableIndexWriteAsync
     */
    public boolean isEnableIndexWriteAsync() {
        return this.letter.isEnableIndexWriteAsync();
    }

    /**
     * Set the enableIndexWriteAsync
     *
     * @param enableIndexWriteAsync
     *            the enableIndexWriteAsync to set
     */
    public void setEnableIndexWriteAsync(boolean enableIndexWriteAsync) {
        this.letter.setEnableIndexWriteAsync(enableIndexWriteAsync);
    }

    /**
     * Get the PersistenceAdapterStatistics
     *
     * @return the persistenceAdapterStatistics
     */
    public PersistenceAdapterStatistics getPersistenceAdapterStatistics() {
        return this.letter.getPersistenceAdapterStatistics();
    }

    /**
     * Get the directory
     *
     * @return the directory
     */
    @Override
    public File getDirectory() {
        return this.letter.getDirectory();
    }

    /**
     * @param dir
     * @see org.apache.activemq.store.PersistenceAdapter#setDirectory(java.io.File)
     */
    @Override
    public void setDirectory(File dir) {
        this.letter.setDirectory(dir);
    }

    /**
     * @return the currently configured location of the KahaDB index files.
     */
    public File getIndexDirectory() {
        return this.letter.getIndexDirectory();
    }

    /**
     * Sets the directory where KahaDB index files should be written.
     *
     * @param indexDirectory
     *        the directory where the KahaDB store index files should be written.
     */
    public void setIndexDirectory(File indexDirectory) {
        this.letter.setIndexDirectory(indexDirectory);
    }

    /**
     * Get the enableJournalDiskSyncs
     * @deprecated use {@link #getJournalDiskSyncStrategy} instead
     * @return the enableJournalDiskSyncs
     */
    public boolean isEnableJournalDiskSyncs() {
        return this.letter.isEnableJournalDiskSyncs();
    }

    /**
     * Set the enableJournalDiskSyncs
     *
     * @deprecated use {@link #setJournalDiskSyncStrategy} instead
     * @param enableJournalDiskSyncs
     *            the enableJournalDiskSyncs to set
     */
    public void setEnableJournalDiskSyncs(boolean enableJournalDiskSyncs) {
        this.letter.setEnableJournalDiskSyncs(enableJournalDiskSyncs);
    }

    /**
     * @return
     */
    public String getJournalDiskSyncStrategy() {
        return letter.getJournalDiskSyncStrategy();
    }

    public JournalDiskSyncStrategy getJournalDiskSyncStrategyEnum() {
        return letter.getJournalDiskSyncStrategyEnum();
    }

    /**
     * @param journalDiskSyncStrategy
     */
    public void setJournalDiskSyncStrategy(String journalDiskSyncStrategy) {
        letter.setJournalDiskSyncStrategy(journalDiskSyncStrategy);
    }

    /**
     * @return
     */
    public long getJournalDiskSyncInterval() {
        return letter.getJournalDiskSyncInterval();
    }

    /**
     * @param journalDiskSyncInterval
     */
    public void setJournalDiskSyncInterval(long journalDiskSyncInterval) {
        letter.setJournalDiskSyncInterval(journalDiskSyncInterval);
    }

    /**
     * Get the indexCacheSize
     *
     * @return the indexCacheSize
     */
    public int getIndexCacheSize() {
        return this.letter.getIndexCacheSize();
    }

    /**
     * Set the indexCacheSize
     * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can be used
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     * @param indexCacheSize
     *            the indexCacheSize to set
     */
    public void setIndexCacheSize(int indexCacheSize) {
        this.letter.setIndexCacheSize(indexCacheSize);
    }

    /**
     * Get the ignoreMissingJournalfiles
     *
     * @return the ignoreMissingJournalfiles
     */
    public boolean isIgnoreMissingJournalfiles() {
        return this.letter.isIgnoreMissingJournalfiles();
    }

    /**
     * Set the ignoreMissingJournalfiles
     *
     * @param ignoreMissingJournalfiles
     *            the ignoreMissingJournalfiles to set
     */
    public void setIgnoreMissingJournalfiles(boolean ignoreMissingJournalfiles) {
        this.letter.setIgnoreMissingJournalfiles(ignoreMissingJournalfiles);
    }

    public boolean isChecksumJournalFiles() {
        return letter.isChecksumJournalFiles();
    }

    public boolean isCheckForCorruptJournalFiles() {
        return letter.isCheckForCorruptJournalFiles();
    }

    public void setChecksumJournalFiles(boolean checksumJournalFiles) {
        letter.setChecksumJournalFiles(checksumJournalFiles);
    }

    public void setCheckForCorruptJournalFiles(boolean checkForCorruptJournalFiles) {
        letter.setCheckForCorruptJournalFiles(checkForCorruptJournalFiles);
    }

    public String getPurgeRecoveredXATransactionStrategy() {
        return letter.getPurgeRecoveredXATransactionStrategy();
    }

    public void setPurgeRecoveredXATransactionStrategy(String purgeRecoveredXATransactionStrategy) {
        letter.setPurgeRecoveredXATransactionStrategy(purgeRecoveredXATransactionStrategy);
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        super.setBrokerService(brokerService);
        letter.setBrokerService(brokerService);
    }

    public String getPreallocationScope() {
        return letter.getPreallocationScope();
    }

    public void setPreallocationScope(String preallocationScope) {
        this.letter.setPreallocationScope(preallocationScope);
    }

    public String getPreallocationStrategy() {
        return letter.getPreallocationStrategy();
    }

    public void setPreallocationStrategy(String preallocationStrategy) {
        this.letter.setPreallocationStrategy(preallocationStrategy);
    }

    public boolean isArchiveDataLogs() {
        return letter.isArchiveDataLogs();
    }

    public void setArchiveDataLogs(boolean archiveDataLogs) {
        letter.setArchiveDataLogs(archiveDataLogs);
    }

    public File getDirectoryArchive() {
        return letter.getDirectoryArchive();
    }

    public void setDirectoryArchive(File directoryArchive) {
        letter.setDirectoryArchive(directoryArchive);
    }

    public boolean isConcurrentStoreAndDispatchQueues() {
        return letter.isConcurrentStoreAndDispatchQueues();
    }

    public void setConcurrentStoreAndDispatchQueues(boolean concurrentStoreAndDispatch) {
        letter.setConcurrentStoreAndDispatchQueues(concurrentStoreAndDispatch);
    }

    public boolean isConcurrentStoreAndDispatchTopics() {
        return letter.isConcurrentStoreAndDispatchTopics();
    }

    public void setConcurrentStoreAndDispatchTopics(boolean concurrentStoreAndDispatch) {
        letter.setConcurrentStoreAndDispatchTopics(concurrentStoreAndDispatch);
    }

    public int getMaxAsyncJobs() {
        return letter.getMaxAsyncJobs();
    }
    /**
     * @param maxAsyncJobs
     *            the maxAsyncJobs to set
     */
    public void setMaxAsyncJobs(int maxAsyncJobs) {
        letter.setMaxAsyncJobs(maxAsyncJobs);
    }

    /**
     * @deprecated use {@link Locker#setLockAcquireSleepInterval(long)} instead
     *
     * @param databaseLockedWaitDelay the databaseLockedWaitDelay to set
     */
    @Deprecated
    public void setDatabaseLockedWaitDelay(int databaseLockedWaitDelay) throws IOException {
       getLocker().setLockAcquireSleepInterval(databaseLockedWaitDelay);
    }

    public boolean getForceRecoverIndex() {
        return letter.getForceRecoverIndex();
    }

    public void setForceRecoverIndex(boolean forceRecoverIndex) {
        letter.setForceRecoverIndex(forceRecoverIndex);
    }

    public boolean isArchiveCorruptedIndex() {
        return letter.isArchiveCorruptedIndex();
    }

    public void setArchiveCorruptedIndex(boolean archiveCorruptedIndex) {
        letter.setArchiveCorruptedIndex(archiveCorruptedIndex);
    }

    public float getIndexLFUEvictionFactor() {
        return letter.getIndexLFUEvictionFactor();
    }

    public void setIndexLFUEvictionFactor(float indexLFUEvictionFactor) {
        letter.setIndexLFUEvictionFactor(indexLFUEvictionFactor);
    }

    public boolean isUseIndexLFRUEviction() {
        return letter.isUseIndexLFRUEviction();
    }

    public void setUseIndexLFRUEviction(boolean useIndexLFRUEviction) {
        letter.setUseIndexLFRUEviction(useIndexLFRUEviction);
    }

    public void setEnableIndexDiskSyncs(boolean diskSyncs) {
        letter.setEnableIndexDiskSyncs(diskSyncs);
    }

    public boolean isEnableIndexDiskSyncs() {
        return letter.isEnableIndexDiskSyncs();
    }

    public void setEnableIndexRecoveryFile(boolean enable) {
        letter.setEnableIndexRecoveryFile(enable);
    }

    public boolean  isEnableIndexRecoveryFile() {
        return letter.isEnableIndexRecoveryFile();
    }

    public void setEnableIndexPageCaching(boolean enable) {
        letter.setEnableIndexPageCaching(enable);
    }

    public boolean isEnableIndexPageCaching() {
        return letter.isEnableIndexPageCaching();
    }

    public int getCompactAcksAfterNoGC() {
        return letter.getCompactAcksAfterNoGC();
    }

    /**
     * Sets the number of GC cycles where no journal logs were removed before an attempt to
     * move forward all the acks in the last log that contains them and is otherwise unreferenced.
     * <p>
     * A value of -1 will disable this feature.
     *
     * @param compactAcksAfterNoGC
     *      Number of empty GC cycles before we rewrite old ACKS.
     */
    public void setCompactAcksAfterNoGC(int compactAcksAfterNoGC) {
        this.letter.setCompactAcksAfterNoGC(compactAcksAfterNoGC);
    }

    public boolean isCompactAcksIgnoresStoreGrowth() {
        return this.letter.isCompactAcksIgnoresStoreGrowth();
    }

    /**
     * Configure if Ack compaction will occur regardless of continued growth of the
     * journal logs meaning that the store has not run out of space yet.  Because the
     * compaction operation can be costly this value is defaulted to off and the Ack
     * compaction is only done when it seems that the store cannot grow and larger.
     *
     * @param compactAcksIgnoresStoreGrowth the compactAcksIgnoresStoreGrowth to set
     */
    public void setCompactAcksIgnoresStoreGrowth(boolean compactAcksIgnoresStoreGrowth) {
        this.letter.setCompactAcksIgnoresStoreGrowth(compactAcksIgnoresStoreGrowth);
    }

    /**
     * Returns whether Ack compaction is enabled
     *
     * @return enableAckCompaction
     */
    public boolean isEnableAckCompaction() {
        return letter.isEnableAckCompaction();
    }

    /**
     * Configure if the Ack compaction task should be enabled to run
     *
     * @param enableAckCompaction
     */
    public void setEnableAckCompaction(boolean enableAckCompaction) {
        letter.setEnableAckCompaction(enableAckCompaction);
    }

    /**
     * Whether non-blocking subscription statistics have been enabled
     *
     * @return
     */
    public boolean isEnableSubscriptionStatistics() {
        return letter.isEnableSubscriptionStatistics();
    }

    /**
     * Enable caching statistics for each subscription to allow non-blocking
     * retrieval of metrics.  This could incur some overhead to compute if there are a lot
     * of subscriptions.
     *
     * @param enableSubscriptionStatistics
     */
    public void setEnableSubscriptionStatistics(boolean enableSubscriptionStatistics) {
        letter.setEnableSubscriptionStatistics(enableSubscriptionStatistics);
    }

    public KahaDBStore getStore() {
        return letter;
    }

    public KahaTransactionInfo createTransactionInfo(TransactionId txid) {
        if (txid == null) {
            return null;
        }
        KahaTransactionInfo rc = new KahaTransactionInfo();

        if (txid.isLocalTransaction()) {
            LocalTransactionId t = (LocalTransactionId) txid;
            KahaLocalTransactionId kahaTxId = new KahaLocalTransactionId();
            kahaTxId.setConnectionId(t.getConnectionId().getValue());
            kahaTxId.setTransactionId(t.getValue());
            rc.setLocalTransactionId(kahaTxId);
        } else {
            XATransactionId t = (XATransactionId) txid;
            KahaXATransactionId kahaTxId = new KahaXATransactionId();
            kahaTxId.setBranchQualifier(new Buffer(t.getBranchQualifier()));
            kahaTxId.setGlobalTransactionId(new Buffer(t.getGlobalTransactionId()));
            kahaTxId.setFormatId(t.getFormatId());
            rc.setXaTransactionId(kahaTxId);
        }
        return rc;
    }

    @Override
    public Locker createDefaultLocker() throws IOException {
        SharedFileLocker locker = new SharedFileLocker();
        locker.configure(this);
        return locker;
    }

    @Override
    public void init() throws Exception {}

    @Override
    public String toString() {
        String path = getDirectory() != null ? getDirectory().getAbsolutePath() : "DIRECTORY_NOT_SET";
        return "KahaDBPersistenceAdapter[" + path + (getIndexDirectory() != null ? ",Index:" + getIndexDirectory().getAbsolutePath() : "") +  "]";
    }

    @Override
    public void setTransactionIdTransformer(TransactionIdTransformer transactionIdTransformer) {
        getStore().setTransactionIdTransformer(transactionIdTransformer);
    }

    @Override
    public JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException {
        return this.letter.createJobSchedulerStore();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.store.NoLocalSubscriptionAware#isPersistNoLocal()
     */
    @Override
    public boolean isPersistNoLocal() {
        return this.letter.isPersistNoLocal();
    }

    /*
     * When set, ensure that the cleanup/gc operation is executed during the stop procedure
     */
    public void setCleanupOnStop(boolean cleanupOnStop) {
        this.letter.setCleanupOnStop(cleanupOnStop);
    }

    public boolean getCleanupOnStop() {
        return this.letter.getCleanupOnStop();
    }
}
