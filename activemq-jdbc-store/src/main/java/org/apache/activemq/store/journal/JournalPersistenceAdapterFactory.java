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

import org.apache.activeio.journal.Journal;
import org.apache.activeio.journal.active.JournalImpl;
import org.apache.activeio.journal.active.JournalLockedException;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterFactory;
import org.apache.activemq.store.jdbc.DataSourceServiceSupport;
import org.apache.activemq.store.jdbc.JDBCAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.jdbc.Statements;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class that can create PersistenceAdapter objects.
 * 
 * @org.apache.xbean.XBean
 * 
 */
public class JournalPersistenceAdapterFactory extends DataSourceServiceSupport implements PersistenceAdapterFactory {

    private static final int JOURNAL_LOCKED_WAIT_DELAY = 10 * 1000;

    private static final Logger LOG = LoggerFactory.getLogger(JournalPersistenceAdapterFactory.class);

    private long checkpointInterval = 1000 * 60 * 5;
    private int journalLogFileSize = 1024 * 1024 * 20;
    private int journalLogFiles = 2;
    private TaskRunnerFactory taskRunnerFactory;
    private Journal journal;
    private boolean useJournal = true;
    private boolean useQuickJournal;
    private File journalArchiveDirectory;
    private boolean failIfJournalIsLocked;
    private int journalThreadPriority = Thread.MAX_PRIORITY;
    private JDBCPersistenceAdapter jdbcPersistenceAdapter = new JDBCPersistenceAdapter();
    private boolean useDedicatedTaskRunner;

    public PersistenceAdapter createPersistenceAdapter() throws IOException {
        jdbcPersistenceAdapter.setDataSource(getDataSource());

        if (!useJournal) {
            return jdbcPersistenceAdapter;
        }
        JournalPersistenceAdapter result =  new JournalPersistenceAdapter(getJournal(), jdbcPersistenceAdapter, getTaskRunnerFactory());
        result.setDirectory(getDataDirectoryFile());
        result.setCheckpointInterval(getCheckpointInterval());
        return result;

    }

    public int getJournalLogFiles() {
        return journalLogFiles;
    }

    /**
     * Sets the number of journal log files to use
     */
    public void setJournalLogFiles(int journalLogFiles) {
        this.journalLogFiles = journalLogFiles;
    }

    public int getJournalLogFileSize() {
        return journalLogFileSize;
    }

    /**
     * Sets the size of the journal log files
     * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can be used
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryIntPropertyEditor"
     */
    public void setJournalLogFileSize(int journalLogFileSize) {
        this.journalLogFileSize = journalLogFileSize;
    }

    public JDBCPersistenceAdapter getJdbcAdapter() {
        return jdbcPersistenceAdapter;
    }

    public void setJdbcAdapter(JDBCPersistenceAdapter jdbcAdapter) {
        this.jdbcPersistenceAdapter = jdbcAdapter;
    }

    public boolean isUseJournal() {
        return useJournal;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    /**
     * Enables or disables the use of the journal. The default is to use the
     * journal
     * 
     * @param useJournal
     */
    public void setUseJournal(boolean useJournal) {
        this.useJournal = useJournal;
    }

    public boolean isUseDedicatedTaskRunner() {
        return useDedicatedTaskRunner;
    }
    
    public void setUseDedicatedTaskRunner(boolean useDedicatedTaskRunner) {
        this.useDedicatedTaskRunner = useDedicatedTaskRunner;
    }
    
    public TaskRunnerFactory getTaskRunnerFactory() {
        if (taskRunnerFactory == null) {
            taskRunnerFactory = new TaskRunnerFactory("Persistence Adaptor Task", journalThreadPriority,
                                                      true, 1000, isUseDedicatedTaskRunner());
        }
        return taskRunnerFactory;
    }

    public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory) {
        this.taskRunnerFactory = taskRunnerFactory;
    }

    public Journal getJournal() throws IOException {
        if (journal == null) {
            createJournal();
        }
        return journal;
    }

    public void setJournal(Journal journal) {
        this.journal = journal;
    }

    public File getJournalArchiveDirectory() {
        if (journalArchiveDirectory == null && useQuickJournal) {
            journalArchiveDirectory = new File(getDataDirectoryFile(), "journal");
        }
        return journalArchiveDirectory;
    }

    public void setJournalArchiveDirectory(File journalArchiveDirectory) {
        this.journalArchiveDirectory = journalArchiveDirectory;
    }

    public boolean isUseQuickJournal() {
        return useQuickJournal;
    }

    /**
     * Enables or disables the use of quick journal, which keeps messages in the
     * journal and just stores a reference to the messages in JDBC. Defaults to
     * false so that messages actually reside long term in the JDBC database.
     */
    public void setUseQuickJournal(boolean useQuickJournal) {
        this.useQuickJournal = useQuickJournal;
    }

    public JDBCAdapter getAdapter() throws IOException {
        return jdbcPersistenceAdapter.getAdapter();
    }

    public void setAdapter(JDBCAdapter adapter) {
        jdbcPersistenceAdapter.setAdapter(adapter);
    }

    public Statements getStatements() {
        return jdbcPersistenceAdapter.getStatements();
    }

    public void setStatements(Statements statements) {
        jdbcPersistenceAdapter.setStatements(statements);
    }

    /**
     * Sets whether or not an exclusive database lock should be used to enable
     * JDBC Master/Slave. Enabled by default.
     */
    public void setUseDatabaseLock(boolean useDatabaseLock) {
        jdbcPersistenceAdapter.setUseLock(useDatabaseLock);
    }

    public boolean isCreateTablesOnStartup() {
        return jdbcPersistenceAdapter.isCreateTablesOnStartup();
    }

    /**
     * Sets whether or not tables are created on startup
     */
    public void setCreateTablesOnStartup(boolean createTablesOnStartup) {
        jdbcPersistenceAdapter.setCreateTablesOnStartup(createTablesOnStartup);
    }

    public int getJournalThreadPriority() {
        return journalThreadPriority;
    }

    /**
     * Sets the thread priority of the journal thread
     */
    public void setJournalThreadPriority(int journalThreadPriority) {
        this.journalThreadPriority = journalThreadPriority;
    }

    /**
     * @throws IOException
     */
    protected void createJournal() throws IOException {
        File journalDir = new File(getDataDirectoryFile(), "journal").getCanonicalFile();
        if (failIfJournalIsLocked) {
            journal = new JournalImpl(journalDir, journalLogFiles, journalLogFileSize,
                                      getJournalArchiveDirectory());
        } else {
            while (true) {
                try {
                    journal = new JournalImpl(journalDir, journalLogFiles, journalLogFileSize,
                                              getJournalArchiveDirectory());
                    break;
                } catch (JournalLockedException e) {
                    LOG.info("Journal is locked... waiting " + (JOURNAL_LOCKED_WAIT_DELAY / 1000)
                             + " seconds for the journal to be unlocked.");
                    try {
                        Thread.sleep(JOURNAL_LOCKED_WAIT_DELAY);
                    } catch (InterruptedException e1) {
                    }
                }
            }
        }
    }

    @Override
    public Locker createDefaultLocker() throws IOException {
        return null;
    }

    @Override
    public void init() throws Exception {
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {}

    @Override
    protected void doStart() throws Exception {}
}
