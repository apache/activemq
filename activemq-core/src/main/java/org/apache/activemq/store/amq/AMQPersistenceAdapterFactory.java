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

    private TaskRunnerFactory taskRunnerFactory;
    private File dataDirectory;
    private int journalThreadPriority = Thread.MAX_PRIORITY;
    private String brokerName = "localhost";
    private ReferenceStoreAdapter referenceStoreAdapter;
    private boolean syncOnWrite;
    private boolean persistentIndex=true;

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
        return result;
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
}
