/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store;

import java.io.File;
import java.io.IOException;

import javax.sql.DataSource;

import org.apache.activeio.journal.Journal;
import org.apache.activeio.journal.active.JournalImpl;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.jdbc.JDBCAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.jdbc.Statements;
import org.apache.activemq.store.journal.JournalPersistenceAdapter;
import org.apache.activemq.store.journal.QuickJournalPersistenceAdapter;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.derby.jdbc.EmbeddedDataSource;

/**
 * Factory class that can create PersistenceAdapter objects.
 *
 * @version $Revision: 1.4 $
 */
public class DefaultPersistenceAdapterFactory implements PersistenceAdapterFactory {
    
    private int journalLogFileSize = 1024*1024*20;
    private int journalLogFiles = 2;
    private File dataDirectory;
    private UsageManager memManager;
    private DataSource dataSource;
    private TaskRunnerFactory taskRunnerFactory;
    private Journal journal;
    private boolean useJournal=true;
    private boolean useQuickJournal=false;
    private File journalArchiveDirectory;
    private JDBCPersistenceAdapter jdbcPersistenceAdapter = new JDBCPersistenceAdapter();
    
    public PersistenceAdapter createPersistenceAdapter() throws IOException {
        File dataDirectory = getDataDirectory();                
        jdbcPersistenceAdapter.setDataSource(getDataSource());
        
        if( !useJournal )
            return jdbcPersistenceAdapter;
        
        // Setup the Journal
        if( useQuickJournal ) {
            return new QuickJournalPersistenceAdapter(getJournal(), jdbcPersistenceAdapter, getMemManager(), getTaskRunnerFactory());
        }  else {
            return new JournalPersistenceAdapter(getJournal(), jdbcPersistenceAdapter, getMemManager(), getTaskRunnerFactory());
        }
    }

    public File getDataDirectory() {
        if( dataDirectory==null ) {
            dataDirectory = new File("activemq-data");
        }
        return dataDirectory;
    }

    public void setDataDirectory(File dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    public int getJournalLogFiles() {
        return journalLogFiles;
    }

    public void setJournalLogFiles(int journalLogFiles) {
        this.journalLogFiles = journalLogFiles;
    }

    public int getJournalLogFileSize() {
        return journalLogFileSize;
    }

    public void setJournalLogFileSize(int journalLogFileSize) {
        this.journalLogFileSize = journalLogFileSize;
    }

    public UsageManager getMemManager() {
        if( memManager==null ) {
            memManager = new UsageManager();
        }
        return memManager;
    }

    public void setMemManager(UsageManager memManager) {
        this.memManager = memManager;
    }
    
    public DataSource getDataSource() throws IOException {
        if (dataSource == null) {
            dataSource = createDataSource();
        }
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
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

    public void setUseJournal(boolean useJournal) {
        this.useJournal = useJournal;
    }

    public TaskRunnerFactory getTaskRunnerFactory() {
        if( taskRunnerFactory == null ) {
            taskRunnerFactory = new TaskRunnerFactory();
        }
        return taskRunnerFactory;
    }

    public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory) {
        this.taskRunnerFactory = taskRunnerFactory;
    }

    public Journal getJournal() throws IOException {
        if( journal == null ) {
            createJournal();
        }
        return journal;
    }

    public void setJournal(Journal journal) {
        this.journal = journal;
    }

    public File getJournalArchiveDirectory() {
        if( journalArchiveDirectory == null && useQuickJournal ) {
            journalArchiveDirectory = new File(getDataDirectory(), "journal");
        }
        return journalArchiveDirectory;
    }

    public void setJournalArchiveDirectory(File journalArchiveDirectory) {
        this.journalArchiveDirectory = journalArchiveDirectory;
    }


    public boolean isUseQuickJournal() {
        return useQuickJournal;
    }

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

    // Implementation methods
    // -------------------------------------------------------------------------
    protected DataSource createDataSource() throws IOException {
        
        // Setup the Derby datasource.
        System.setProperty("derby.system.home", getDataDirectory().getCanonicalPath());
        System.setProperty("derby.storage.fileSyncTransactionLog", "true");
        System.setProperty("derby.storage.pageCacheSize", "100");
        
        final EmbeddedDataSource ds = new EmbeddedDataSource();
        ds.setDatabaseName("derbydb");
        ds.setCreateDatabase("create");
        return ds;
    }

    /**
     * @throws IOException
     */
    protected void createJournal() throws IOException {
        File journalDir = new File(getDataDirectory(), "journal").getCanonicalFile();
        journal = new JournalImpl(journalDir, journalLogFiles, journalLogFileSize, getJournalArchiveDirectory());
    }

}
