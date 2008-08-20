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
package org.apache.activemq.store.amq;

import java.io.File;

import org.apache.activemq.kaha.impl.async.AsyncDataManager;
import org.apache.activemq.kaha.impl.index.hash.HashIndex;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterFactory;
import org.apache.activemq.store.ReferenceStoreAdapter;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.IOHelper;

/**
 * An implementation of {@link PersistenceAdapterFactory}
 * 
 * @org.apache.xbean.XBean element="amqPersistenceAdapterFactory"
 * 
 * @version $Revision: 1.17 $
 */
public class AMQPersistenceAdapterFactory implements PersistenceAdapterFactory {
    static final int DEFAULT_MAX_REFERNCE_FILE_LENGTH=2*1024*1024;
    private TaskRunnerFactory taskRunnerFactory;
    private File dataDirectory;
    private int journalThreadPriority = Thread.MAX_PRIORITY;
    private String brokerName = "localhost";
    private ReferenceStoreAdapter referenceStoreAdapter;
    private boolean syncOnWrite;
    private boolean syncOnTransaction=true;
    private boolean persistentIndex=true;
    private boolean useNio = true;
    private int maxFileLength = AsyncDataManager.DEFAULT_MAX_FILE_LENGTH;
    private long cleanupInterval = AsyncDataManager.DEFAULT_CLEANUP_INTERVAL;
    private int indexBinSize = HashIndex.DEFAULT_BIN_SIZE;
    private int indexKeySize = HashIndex.DEFAULT_KEY_SIZE;
    private int indexPageSize = HashIndex.DEFAULT_PAGE_SIZE;
    private int indexMaxBinSize = HashIndex.MAXIMUM_CAPACITY;
    private int indexLoadFactor = HashIndex.DEFAULT_LOAD_FACTOR;
    private int maxReferenceFileLength=DEFAULT_MAX_REFERNCE_FILE_LENGTH;
    private boolean recoverReferenceStore=true;
    private boolean forceRecoverReferenceStore=false;
    private long checkpointInterval = 1000 * 20;


    /**
     * @return a AMQPersistenceAdapter
     * @see org.apache.activemq.store.PersistenceAdapterFactory#createPersistenceAdapter()
     */
    public PersistenceAdapter createPersistenceAdapter() {
        AMQPersistenceAdapter result = new AMQPersistenceAdapter();
        result.setDirectory(getDataDirectory());
        result.setTaskRunnerFactory(getTaskRunnerFactory());
        result.setBrokerName(getBrokerName());
        result.setSyncOnWrite(isSyncOnWrite());
        result.setPersistentIndex(isPersistentIndex());
        result.setReferenceStoreAdapter(getReferenceStoreAdapter());
        result.setUseNio(isUseNio());
        result.setMaxFileLength(getMaxFileLength());
        result.setCleanupInterval(getCleanupInterval());
        result.setIndexBinSize(getIndexBinSize());
        result.setIndexKeySize(getIndexKeySize());
        result.setIndexPageSize(getIndexPageSize());
        result.setIndexMaxBinSize(getIndexMaxBinSize());
        result.setIndexLoadFactor(getIndexLoadFactor());
        result.setMaxReferenceFileLength(getMaxReferenceFileLength());
        result.setForceRecoverReferenceStore(isForceRecoverReferenceStore());
        result.setRecoverReferenceStore(isRecoverReferenceStore());
        return result;
    }

    public long getCleanupInterval() {
        return cleanupInterval;
    }
    
    public void setCleanupInterval(long val) {
        cleanupInterval = val;
    }

    /**
     * @return the dataDirectory
     */
    public File getDataDirectory() {
        if (this.dataDirectory == null) {
            this.dataDirectory = new File(IOHelper.getDefaultDataDirectory(), IOHelper.toFileSystemSafeName(brokerName));
        }
        return this.dataDirectory;
    }

    /**
     * @param dataDirectory the dataDirectory to set
     */
    public void setDataDirectory(File dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    /**
     * @return the taskRunnerFactory
     */
    public TaskRunnerFactory getTaskRunnerFactory() {
        if (taskRunnerFactory == null) {
            taskRunnerFactory = new TaskRunnerFactory("AMQPersistenceAdaptor Task", journalThreadPriority,
                                                      true, 1000);
        }
        return taskRunnerFactory;
    }

    /**
     * @param taskRunnerFactory the taskRunnerFactory to set
     */
    public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory) {
        this.taskRunnerFactory = taskRunnerFactory;
    }

    /**
     * @return the journalThreadPriority
     */
    public int getJournalThreadPriority() {
        return this.journalThreadPriority;
    }

    /**
     * @param journalThreadPriority the journalThreadPriority to set
     */
    public void setJournalThreadPriority(int journalThreadPriority) {
        this.journalThreadPriority = journalThreadPriority;
    }

    /**
     * @return the brokerName
     */
    public String getBrokerName() {
        return this.brokerName;
    }

    /**
     * @param brokerName the brokerName to set
     */
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    /**
     * @return the referenceStoreAdapter
     */
    public ReferenceStoreAdapter getReferenceStoreAdapter() {
        return this.referenceStoreAdapter;
    }

    /**
     * @param referenceStoreAdapter the referenceStoreAdapter to set
     */
    public void setReferenceStoreAdapter(ReferenceStoreAdapter referenceStoreAdapter) {
        this.referenceStoreAdapter = referenceStoreAdapter;
    }
    
    public boolean isPersistentIndex() {
		return persistentIndex;
	}

	public void setPersistentIndex(boolean persistentIndex) {
		this.persistentIndex = persistentIndex;
	}

	public boolean isSyncOnWrite() {
		return syncOnWrite;
	}

	public void setSyncOnWrite(boolean syncOnWrite) {
		this.syncOnWrite = syncOnWrite;
	}
	
	public boolean isSyncOnTransaction() {
        return syncOnTransaction;
    }

    public void setSyncOnTransaction(boolean syncOnTransaction) {
        this.syncOnTransaction = syncOnTransaction;
    }

	public boolean isUseNio() {
		return useNio;
	}

	public void setUseNio(boolean useNio) {
		this.useNio = useNio;
	}

	public int getMaxFileLength() {
		return maxFileLength;
	}

	public void setMaxFileLength(int maxFileLength) {
		this.maxFileLength = maxFileLength;
	}

    /**
     * @return the indexBinSize
     */
    public int getIndexBinSize() {
        return indexBinSize;
    }

    /**
     * @param indexBinSize the indexBinSize to set
     */
    public void setIndexBinSize(int indexBinSize) {
        this.indexBinSize = indexBinSize;
    }

    /**
     * @return the indexKeySize
     */
    public int getIndexKeySize() {
        return indexKeySize;
    }

    /**
     * @param indexKeySize the indexKeySize to set
     */
    public void setIndexKeySize(int indexKeySize) {
        this.indexKeySize = indexKeySize;
    }

    /**
     * @return the indexPageSize
     */
    public int getIndexPageSize() {
        return indexPageSize;
    }

    /**
     * @param indexPageSize the indexPageSize to set
     */
    public void setIndexPageSize(int indexPageSize) {
        this.indexPageSize = indexPageSize;
    }

    /**
     * @return the indexMaxBinSize
     */
    public int getIndexMaxBinSize() {
        return indexMaxBinSize;
    }

    /**
     * @param indexMaxBinSize the indexMaxBinSize to set
     */
    public void setIndexMaxBinSize(int indexMaxBinSize) {
        this.indexMaxBinSize = indexMaxBinSize;
    }

    /**
     * @return the indexLoadFactor
     */
    public int getIndexLoadFactor() {
        return indexLoadFactor;
    }

    /**
     * @param indexLoadFactor the indexLoadFactor to set
     */
    public void setIndexLoadFactor(int indexLoadFactor) {
        this.indexLoadFactor = indexLoadFactor;
    }

    /**
     * @return the maxReferenceFileLength
     */
    public int getMaxReferenceFileLength() {
        return maxReferenceFileLength;
    }

    /**
     * @param maxReferenceFileLength the maxReferenceFileLength to set
     */
    public void setMaxReferenceFileLength(int maxReferenceFileLength) {
        this.maxReferenceFileLength = maxReferenceFileLength;
    }

    /**
     * @return the recoverReferenceStore
     */
    public boolean isRecoverReferenceStore() {
        return recoverReferenceStore;
    }

    /**
     * @param recoverReferenceStore the recoverReferenceStore to set
     */
    public void setRecoverReferenceStore(boolean recoverReferenceStore) {
        this.recoverReferenceStore = recoverReferenceStore;
    }

    /**
     * @return the forceRecoverReferenceStore
     */
    public boolean isForceRecoverReferenceStore() {
        return forceRecoverReferenceStore;
    }

    /**
     * @param forceRecoverReferenceStore the forceRecoverReferenceStore to set
     */
    public void setForceRecoverReferenceStore(boolean forceRecoverReferenceStore) {
        this.forceRecoverReferenceStore = forceRecoverReferenceStore;
    }

    /**
     * @return the checkpointInterval
     */
    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    /**
     * @param checkpointInterval the checkpointInterval to set
     */
    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }
}
