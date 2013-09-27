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

import static org.apache.activemq.broker.jmx.BrokerMBeanSupport.createPersistenceAdapterName;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.LockableServiceSupport;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.PersistenceAdapterView;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.*;
import org.apache.activemq.store.kahadb.data.KahaLocalTransactionId;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.store.kahadb.data.KahaXATransactionId;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.ServiceStopper;

/**
 * An implementation of {@link PersistenceAdapter} designed for use with
 * KahaDB - Embedded Lightweight Non-Relational Database
 *
 * @org.apache.xbean.XBean element="kahaDB"
 *
 */
public class KahaDBPersistenceAdapter extends LockableServiceSupport implements PersistenceAdapter, JournaledStore, TransactionIdTransformerAware {
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
     * @param sync
     * @throws IOException
     * @see org.apache.activemq.store.PersistenceAdapter#checkpoint(boolean)
     */
    @Override
    public void checkpoint(boolean sync) throws IOException {
        this.letter.checkpoint(sync);
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
        return this.letter.size();
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
     * Get the enableJournalDiskSyncs
     *
     * @return the enableJournalDiskSyncs
     */
    public boolean isEnableJournalDiskSyncs() {
        return this.letter.isEnableJournalDiskSyncs();
    }

    /**
     * Set the enableJournalDiskSyncs
     *
     * @param enableJournalDiskSyncs
     *            the enableJournalDiskSyncs to set
     */
    public void setEnableJournalDiskSyncs(boolean enableJournalDiskSyncs) {
        this.letter.setEnableJournalDiskSyncs(enableJournalDiskSyncs);
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

    @Override
    public void setBrokerService(BrokerService brokerService) {
        super.setBrokerService(brokerService);
        letter.setBrokerService(brokerService);
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

    /**
     * When true, persist the redelivery status such that the message redelivery flag can survive a broker failure
     * used with org.apache.activemq.ActiveMQConnectionFactory#setTransactedIndividualAck(boolean)  true
     */
    public void setRewriteOnRedelivery(boolean rewriteOnRedelivery) {
        letter.setRewriteOnRedelivery(rewriteOnRedelivery);
    }

    public boolean isRewriteOnRedelivery() {
        return letter.isRewriteOnRedelivery();
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
        return "KahaDBPersistenceAdapter[" + path + "]";
    }

    @Override
    public void setTransactionIdTransformer(TransactionIdTransformer transactionIdTransformer) {
        getStore().setTransactionIdTransformer(transactionIdTransformer);
    }
}
