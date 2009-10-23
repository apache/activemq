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
package org.apache.activemq.store.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter;
import org.apache.activemq.store.memory.MemoryTransactionStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link PersistenceAdapter} implementation using JDBC for persistence
 * storage.
 * 
 * This persistence adapter will correctly remember prepared XA transactions,
 * but it will not keep track of local transaction commits so that operations
 * performed against the Message store are done as a single uow.
 * 
 * @org.apache.xbean.XBean element="jdbcPersistenceAdapter"
 * 
 * @version $Revision: 1.9 $
 */
public class JDBCPersistenceAdapter extends DataSourceSupport implements PersistenceAdapter,
    BrokerServiceAware {

    private static final Log LOG = LogFactory.getLog(JDBCPersistenceAdapter.class);
    private static FactoryFinder adapterFactoryFinder = new FactoryFinder(
                                                                   "META-INF/services/org/apache/activemq/store/jdbc/");
    private static FactoryFinder lockFactoryFinder = new FactoryFinder(
                                                                    "META-INF/services/org/apache/activemq/store/jdbc/lock/");

    private WireFormat wireFormat = new OpenWireFormat();
    private BrokerService brokerService;
    private Statements statements;
    private JDBCAdapter adapter;
    private MemoryTransactionStore transactionStore;
    private ScheduledThreadPoolExecutor clockDaemon;
    private ScheduledFuture<?> cleanupTicket, keepAliveTicket;
    private int cleanupPeriod = 1000 * 60 * 5;
    private boolean useExternalMessageReferences;
    private boolean useDatabaseLock = true;
    private long lockKeepAlivePeriod = 1000*30;
    private long lockAcquireSleepInterval = DefaultDatabaseLocker.DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL;
    private DatabaseLocker databaseLocker;
    private boolean createTablesOnStartup = true;
    private DataSource lockDataSource;
    private int transactionIsolation;

    public JDBCPersistenceAdapter() {
    }

    public JDBCPersistenceAdapter(DataSource ds, WireFormat wireFormat) {
        super(ds);
        this.wireFormat = wireFormat;
    }

    public Set<ActiveMQDestination> getDestinations() {
        // Get a connection and insert the message into the DB.
        TransactionContext c = null;
        try {
            c = getTransactionContext();
            return getAdapter().doGetDestinations(c);
        } catch (IOException e) {
            return emptyDestinationSet();
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            return emptyDestinationSet();
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (Throwable e) {
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Set<ActiveMQDestination> emptyDestinationSet() {
        return Collections.EMPTY_SET;
    }

    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        MessageStore rc = new JDBCMessageStore(this, getAdapter(), wireFormat, destination);
        if (transactionStore != null) {
            rc = transactionStore.proxy(rc);
        }
        return rc;
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        TopicMessageStore rc = new JDBCTopicMessageStore(this, getAdapter(), wireFormat, destination);
        if (transactionStore != null) {
            rc = transactionStore.proxy(rc);
        }
        return rc;
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     * No state retained.... nothing to do
     *
     * @param destination Destination to forget
     */
    public void removeQueueMessageStore(ActiveMQQueue destination) {
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     * No state retained.... nothing to do
     *
     * @param destination Destination to forget
     */
    public void removeTopicMessageStore(ActiveMQTopic destination) {
    }

    public TransactionStore createTransactionStore() throws IOException {
        if (transactionStore == null) {
            transactionStore = new MemoryTransactionStore(this);
        }
        return this.transactionStore;
    }

    public long getLastMessageBrokerSequenceId() throws IOException {
        // Get a connection and insert the message into the DB.
        TransactionContext c = getTransactionContext();
        try {
            return getAdapter().doGetLastMessageBrokerSequenceId(c);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to get last broker message id: " + e, e);
        } finally {
            c.close();
        }
    }

    public void start() throws Exception {
        getAdapter().setUseExternalMessageReferences(isUseExternalMessageReferences());

        if (isCreateTablesOnStartup()) {
            TransactionContext transactionContext = getTransactionContext();
            transactionContext.begin();
            try {
                try {
                    getAdapter().doCreateTables(transactionContext);
                } catch (SQLException e) {
                    LOG.warn("Cannot create tables due to: " + e);
                    JDBCPersistenceAdapter.log("Failure Details: ", e);
                }
            } finally {
                transactionContext.commit();
            }
        }

        if (isUseDatabaseLock()) {
            DatabaseLocker service = getDatabaseLocker();
            if (service == null) {
                LOG.warn("No databaseLocker configured for the JDBC Persistence Adapter");
            } else {
                service.start();
                if (lockKeepAlivePeriod > 0) {
                    keepAliveTicket = getScheduledThreadPoolExecutor().scheduleAtFixedRate(new Runnable() {
                        public void run() {
                            databaseLockKeepAlive();
                        }
                    }, lockKeepAlivePeriod, lockKeepAlivePeriod, TimeUnit.MILLISECONDS);
                }
                if (brokerService != null) {
                    brokerService.getBroker().nowMasterBroker();
                }
            }
        }

        cleanup();

        // Cleanup the db periodically.
        if (cleanupPeriod > 0) {
            cleanupTicket = getScheduledThreadPoolExecutor().scheduleAtFixedRate(new Runnable() {
                public void run() {
                    cleanup();
                }
            }, cleanupPeriod, cleanupPeriod, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized void stop() throws Exception {
        if (cleanupTicket != null) {
            cleanupTicket.cancel(true);
            cleanupTicket = null;
        }
        if (keepAliveTicket != null) {
            keepAliveTicket.cancel(false);
            keepAliveTicket = null;
        }
        
        // do not shutdown clockDaemon as it may kill the thread initiating shutdown
        DatabaseLocker service = getDatabaseLocker();
        if (service != null) {
            service.stop();
        }
    }

    public void cleanup() {
        TransactionContext c = null;
        try {
            LOG.debug("Cleaning up old messages.");
            c = getTransactionContext();
            getAdapter().doDeleteOldMessages(c);
        } catch (IOException e) {
            LOG.warn("Old message cleanup failed due to: " + e, e);
        } catch (SQLException e) {
            LOG.warn("Old message cleanup failed due to: " + e);
            JDBCPersistenceAdapter.log("Failure Details: ", e);
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (Throwable e) {
                }
            }
            LOG.debug("Cleanup done.");
        }
    }

    public void setScheduledThreadPoolExecutor(ScheduledThreadPoolExecutor clockDaemon) {
        this.clockDaemon = clockDaemon;
    }

    public ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
        if (clockDaemon == null) {
            clockDaemon = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "ActiveMQ Cleanup Timer");
                    thread.setDaemon(true);
                    return thread;
                }
            });
        }
        return clockDaemon;
    }

    public JDBCAdapter getAdapter() throws IOException {
        if (adapter == null) {
            setAdapter(createAdapter());
        }
        return adapter;
    }

    public DatabaseLocker getDatabaseLocker() throws IOException {
        if (databaseLocker == null && isUseDatabaseLock()) {
            setDatabaseLocker(loadDataBaseLocker());
        }
        return databaseLocker;
    }

    /**
     * Sets the database locker strategy to use to lock the database on startup
     * @throws IOException 
     */
    public void setDatabaseLocker(DatabaseLocker locker) throws IOException {
        databaseLocker = locker;
        databaseLocker.setPersistenceAdapter(this);
        databaseLocker.setLockAcquireSleepInterval(getLockAcquireSleepInterval());
    }

    public DataSource getLockDataSource() throws IOException {
        if (lockDataSource == null) {
            lockDataSource = getDataSource();
            if (lockDataSource == null) {
                throw new IllegalArgumentException(
                        "No dataSource property has been configured");
            }
        } else {
            LOG.info("Using a separate dataSource for locking: "
                    + lockDataSource);
        }
        return lockDataSource;
    }
    
    public void setLockDataSource(DataSource dataSource) {
        this.lockDataSource = dataSource;
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    /**
     * @throws IOException
     */
    protected JDBCAdapter createAdapter() throws IOException {
       
        adapter = (JDBCAdapter) loadAdapter(adapterFactoryFinder, "adapter");
       
        // Use the default JDBC adapter if the
        // Database type is not recognized.
        if (adapter == null) {
            adapter = new DefaultJDBCAdapter();
            LOG.debug("Using default JDBC Adapter: " + adapter);
        }
        return adapter;
    }

    private Object loadAdapter(FactoryFinder finder, String kind) throws IOException {
        Object adapter = null;
        TransactionContext c = getTransactionContext();
        try {
            try {
                // Make the filename file system safe.
                String dirverName = c.getConnection().getMetaData().getDriverName();
                dirverName = dirverName.replaceAll("[^a-zA-Z0-9\\-]", "_").toLowerCase();

                try {
                    adapter = finder.newInstance(dirverName);
                    LOG.info("Database " + kind + " driver override recognized for : [" + dirverName + "] - adapter: " + adapter.getClass());
                } catch (Throwable e) {
                    LOG.warn("Database " + kind + " driver override not found for : [" + dirverName
                             + "].  Will use default implementation.");
                }
            } catch (SQLException e) {
                LOG.warn("JDBC error occurred while trying to detect database type for overrides. Will use default implementations: "
                          + e.getMessage());
                JDBCPersistenceAdapter.log("Failure Details: ", e);
            }
        } finally {
            c.close();
        }
        return adapter;
    }

    public void setAdapter(JDBCAdapter adapter) {
        this.adapter = adapter;
        this.adapter.setStatements(getStatements());
    }

    public WireFormat getWireFormat() {
        return wireFormat;
    }

    public void setWireFormat(WireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    public TransactionContext getTransactionContext(ConnectionContext context) throws IOException {
        if (context == null) {
            return getTransactionContext();
        } else {
            TransactionContext answer = (TransactionContext)context.getLongTermStoreContext();
            if (answer == null) {
                answer = getTransactionContext();
                context.setLongTermStoreContext(answer);
            }
            return answer;
        }
    }

    public TransactionContext getTransactionContext() throws IOException {
        TransactionContext answer = new TransactionContext(getDataSource());
        if (transactionIsolation > 0) {
            answer.setTransactionIsolation(transactionIsolation);
        }
        return answer;
    }

    public void beginTransaction(ConnectionContext context) throws IOException {
        TransactionContext transactionContext = getTransactionContext(context);
        transactionContext.begin();
    }

    public void commitTransaction(ConnectionContext context) throws IOException {
        TransactionContext transactionContext = getTransactionContext(context);
        transactionContext.commit();
    }

    public void rollbackTransaction(ConnectionContext context) throws IOException {
        TransactionContext transactionContext = getTransactionContext(context);
        transactionContext.rollback();
    }

    public int getCleanupPeriod() {
        return cleanupPeriod;
    }

    /**
     * Sets the number of milliseconds until the database is attempted to be
     * cleaned up for durable topics
     */
    public void setCleanupPeriod(int cleanupPeriod) {
        this.cleanupPeriod = cleanupPeriod;
    }

    public void deleteAllMessages() throws IOException {
        TransactionContext c = getTransactionContext();
        try {
            getAdapter().doDropTables(c);
            getAdapter().setUseExternalMessageReferences(isUseExternalMessageReferences());
            getAdapter().doCreateTables(c);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create(e);
        } finally {
            c.close();
        }
    }

    public boolean isUseExternalMessageReferences() {
        return useExternalMessageReferences;
    }

    public void setUseExternalMessageReferences(boolean useExternalMessageReferences) {
        this.useExternalMessageReferences = useExternalMessageReferences;
    }

    public boolean isCreateTablesOnStartup() {
        return createTablesOnStartup;
    }

    /**
     * Sets whether or not tables are created on startup
     */
    public void setCreateTablesOnStartup(boolean createTablesOnStartup) {
        this.createTablesOnStartup = createTablesOnStartup;
    }

    public boolean isUseDatabaseLock() {
        return useDatabaseLock;
    }

    /**
     * Sets whether or not an exclusive database lock should be used to enable
     * JDBC Master/Slave. Enabled by default.
     */
    public void setUseDatabaseLock(boolean useDatabaseLock) {
        this.useDatabaseLock = useDatabaseLock;
    }

    public static void log(String msg, SQLException e) {
        String s = msg + e.getMessage();
        while (e.getNextException() != null) {
            e = e.getNextException();
            s += ", due to: " + e.getMessage();
        }
        LOG.debug(s, e);
    }

    public Statements getStatements() {
        if (statements == null) {
            statements = new Statements();
        }
        return statements;
    }

    public void setStatements(Statements statements) {
        this.statements = statements;
    }

    /**
     * @param usageManager The UsageManager that is controlling the
     *                destination's memory usage.
     */
    public void setUsageManager(SystemUsage usageManager) {
    }

    protected void databaseLockKeepAlive() {
        boolean stop = false;
        try {
            DatabaseLocker locker = getDatabaseLocker();
            if (locker != null) {
                if (!locker.keepAlive()) {
                    stop = true;
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to get database when trying keepalive: " + e, e);
        }
        if (stop) {
            stopBroker();
        }
    }

    protected void stopBroker() {
        // we can no longer keep the lock so lets fail
        LOG.info("No longer able to keep the exclusive lock so giving up being a master");
        try {
            brokerService.stop();
        } catch (Exception e) {
            LOG.warn("Failure occured while stopping broker");
        }
    }

    protected DatabaseLocker loadDataBaseLocker() throws IOException {
        DatabaseLocker locker = (DefaultDatabaseLocker) loadAdapter(lockFactoryFinder, "lock");       
        if (locker == null) {
            locker = new DefaultDatabaseLocker();
            LOG.debug("Using default JDBC Locker: " + locker);
        }
        return locker;
    }

    public void setBrokerName(String brokerName) {
    }

    public String toString() {
        return "JDBCPersistenceAdapter(" + super.toString() + ")";
    }

    public void setDirectory(File dir) {
    }

    public void checkpoint(boolean sync) throws IOException {
    }

    public long size(){
        return 0;
    }

    public long getLockKeepAlivePeriod() {
        return lockKeepAlivePeriod;
    }

    public void setLockKeepAlivePeriod(long lockKeepAlivePeriod) {
        this.lockKeepAlivePeriod = lockKeepAlivePeriod;
    }

    public long getLockAcquireSleepInterval() {
        return lockAcquireSleepInterval;
    }

    /**
     * millisecond interval between lock acquire attempts, applied to newly created DefaultDatabaseLocker
     * not applied if DataBaseLocker is injected.
     */
    public void setLockAcquireSleepInterval(long lockAcquireSleepInterval) {
        this.lockAcquireSleepInterval = lockAcquireSleepInterval;
    }
    
    /**
     * set the Transaction isolation level to something other that TRANSACTION_READ_UNCOMMITTED
     * This allowable dirty isolation level may not be achievable in clustered DB environments
     * so a more restrictive and expensive option may be needed like TRANSACTION_REPEATABE_READ
     * see isolation level constants in {@link java.sql.Connection}
     * @param transactionIsolation the isolation level to use
     */
    public void setTransactionIsolation(int transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }
}
