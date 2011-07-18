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
import java.util.Set;

import org.apache.activeio.journal.Journal;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.usage.SystemUsage;

/**
 * An implementation of {@link PersistenceAdapter} designed for use with a
 * {@link Journal} and then check pointing asynchronously on a timeout with some
 * other long term persistent storage.
 * 
 * @org.apache.xbean.XBean element="kahaDB"
 * 
 */
public class KahaDBPersistenceAdapter implements PersistenceAdapter, BrokerServiceAware {
    private final KahaDBStore letter = new KahaDBStore();

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#beginTransaction(org.apache.activemq.broker.ConnectionContext)
     */
    public void beginTransaction(ConnectionContext context) throws IOException {
        this.letter.beginTransaction(context);
    }

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#checkpoint(boolean)
     */
    public void checkpoint(boolean sync) throws IOException {
        this.letter.checkpoint(sync);
    }

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#commitTransaction(org.apache.activemq.broker.ConnectionContext)
     */
    public void commitTransaction(ConnectionContext context) throws IOException {
        this.letter.commitTransaction(context);
    }

    /**
     * @return MessageStore
     * @see org.apache.activemq.store.PersistenceAdapter#createQueueMessageStore(org.apache.activemq.command.ActiveMQQueue)
     */
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        return this.letter.createQueueMessageStore(destination);
    }

    /**
     * @return TopicMessageStore
     * @see org.apache.activemq.store.PersistenceAdapter#createTopicMessageStore(org.apache.activemq.command.ActiveMQTopic)
     */
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        return this.letter.createTopicMessageStore(destination);
    }

    /**
     * @return TrandactionStore
     * @see org.apache.activemq.store.PersistenceAdapter#createTransactionStore()
     */
    public TransactionStore createTransactionStore() throws IOException {
        return this.letter.createTransactionStore();
    }

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#deleteAllMessages()
     */
    public void deleteAllMessages() throws IOException {
        this.letter.deleteAllMessages();
    }

    /**
     * @return destinations
     * @see org.apache.activemq.store.PersistenceAdapter#getDestinations()
     */
    public Set<ActiveMQDestination> getDestinations() {
        return this.letter.getDestinations();
    }

    /**
     * @return lastMessageBrokerSequenceId
     * @see org.apache.activemq.store.PersistenceAdapter#getLastMessageBrokerSequenceId()
     */
    public long getLastMessageBrokerSequenceId() throws IOException {
        return this.letter.getLastMessageBrokerSequenceId();
    }

    public long getLastProducerSequenceId(ProducerId id) throws IOException {
        return this.letter.getLastProducerSequenceId(id);
    }

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#removeQueueMessageStore(org.apache.activemq.command.ActiveMQQueue)
     */
    public void removeQueueMessageStore(ActiveMQQueue destination) {
        this.letter.removeQueueMessageStore(destination);
    }

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#removeTopicMessageStore(org.apache.activemq.command.ActiveMQTopic)
     */
    public void removeTopicMessageStore(ActiveMQTopic destination) {
        this.letter.removeTopicMessageStore(destination);
    }

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#rollbackTransaction(org.apache.activemq.broker.ConnectionContext)
     */
    public void rollbackTransaction(ConnectionContext context) throws IOException {
        this.letter.rollbackTransaction(context);
    }

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#setBrokerName(java.lang.String)
     */
    public void setBrokerName(String brokerName) {
        this.letter.setBrokerName(brokerName);
    }

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#setUsageManager(org.apache.activemq.usage.SystemUsage)
     */
    public void setUsageManager(SystemUsage usageManager) {
        this.letter.setUsageManager(usageManager);
    }

    /**
     * @return the size of the store
     * @see org.apache.activemq.store.PersistenceAdapter#size()
     */
    public long size() {
        return this.letter.size();
    }

    /**
     * @see org.apache.activemq.Service#start()
     */
    public void start() throws Exception {
        this.letter.start();
    }

    /**
     * @see org.apache.activemq.Service#stop()
     */
    public void stop() throws Exception {
        this.letter.stop();
    }

    /**
     * Get the journalMaxFileLength
     *
     * @return the journalMaxFileLength
     */
    public int getJournalMaxFileLength() {
        return this.letter.getJournalMaxFileLength();
    }

    /**
     * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can
     * be used
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
        return this.getFailoverProducersAuditDepth();
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
     * @param checkpointInterval the checkpointInterval to set
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
     * @param cleanupInterval the cleanupInterval to set
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
     *
     * @param indexWriteBatchSize the indexWriteBatchSize to set
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
     * * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can be used
     *
     * @param journalMaxWriteBatchSize the journalMaxWriteBatchSize to set
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
     * @param enableIndexWriteAsync the enableIndexWriteAsync to set
     */
    public void setEnableIndexWriteAsync(boolean enableIndexWriteAsync) {
        this.letter.setEnableIndexWriteAsync(enableIndexWriteAsync);
    }

    /**
     * Get the directory
     *
     * @return the directory
     */
    public File getDirectory() {
        return this.letter.getDirectory();
    }

    /**
     * @see org.apache.activemq.store.PersistenceAdapter#setDirectory(java.io.File)
     */
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
     * @param enableJournalDiskSyncs the enableJournalDiskSyncs to set
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
     *
     * @param indexCacheSize the indexCacheSize to set
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
     * @param ignoreMissingJournalfiles the ignoreMissingJournalfiles to set
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

    public void setBrokerService(BrokerService brokerService) {
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
     * @param maxAsyncJobs the maxAsyncJobs to set
     */
    public void setMaxAsyncJobs(int maxAsyncJobs) {
        letter.setMaxAsyncJobs(maxAsyncJobs);
    }

    /**
     * @return the databaseLockedWaitDelay
     */
    public int getDatabaseLockedWaitDelay() {
        return letter.getDatabaseLockedWaitDelay();
    }

    /**
     * @param databaseLockedWaitDelay the databaseLockedWaitDelay to set
     */
    public void setDatabaseLockedWaitDelay(int databaseLockedWaitDelay) {
        letter.setDatabaseLockedWaitDelay(databaseLockedWaitDelay);
    }

    public boolean getForceRecoverIndex() {
        return letter.getForceRecoverIndex();
    }

    public void setForceRecoverIndex(boolean forceRecoverIndex) {
        letter.setForceRecoverIndex(forceRecoverIndex);
    }

    public boolean isJournalPerDestination() {
        return letter.isJournalPerDestination();
    }

    public void setJournalPerDestination(boolean journalPerDestination) {
        letter.setJournalPerDestination(journalPerDestination);
    }

    //  for testing
    public KahaDBStore getStore() {
        return letter;
    }

    @Override
    public String toString() {
        String path = getDirectory() != null ? getDirectory().getAbsolutePath() : "DIRECTORY_NOT_SET";
        return "KahaDBPersistenceAdapter[" + path + "]";
    }

}
