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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter;
import org.apache.activemq.store.memory.MemoryTransactionStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ThreadPoolUtils;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */
public class JDBCPersistenceAdapter extends DataSourceServiceSupport implements PersistenceAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCPersistenceAdapter.class);
    private static FactoryFinder adapterFactoryFinder = new FactoryFinder(
        "META-INF/services/org/apache/activemq/store/jdbc/");
    private static FactoryFinder lockFactoryFinder = new FactoryFinder(
        "META-INF/services/org/apache/activemq/store/jdbc/lock/");

    public static final long DEFAULT_LOCK_KEEP_ALIVE_PERIOD = 30 * 1000;

    private WireFormat wireFormat = new OpenWireFormat();
    private Statements statements;
    private JDBCAdapter adapter;
    private final JdbcMemoryTransactionStore transactionStore = new JdbcMemoryTransactionStore(this);
    private ScheduledFuture<?> cleanupTicket;
    private int cleanupPeriod = 1000 * 60 * 5;
    private boolean useExternalMessageReferences;
    private boolean createTablesOnStartup = true;
    private DataSource lockDataSource;
    private int transactionIsolation;
    private File directory;
    private boolean changeAutoCommitAllowed = true;
    private int queryTimeout = -1;
    private int networkTimeout = -1;

    protected int maxProducersToAudit=1024;
    protected int maxAuditDepth=1000;
    protected boolean enableAudit=false;
    protected int auditRecoveryDepth = 1024;
    protected ActiveMQMessageAudit audit;

    protected LongSequenceGenerator sequenceGenerator = new LongSequenceGenerator();
    protected int maxRows = DefaultJDBCAdapter.MAX_ROWS;
    protected final HashMap<ActiveMQDestination, MessageStore> storeCache = new HashMap<>();

    {
        setLockKeepAlivePeriod(DEFAULT_LOCK_KEEP_ALIVE_PERIOD);
    }

    public JDBCPersistenceAdapter() {
    }

    public JDBCPersistenceAdapter(DataSource ds, WireFormat wireFormat) {
        super(ds);
        this.wireFormat = wireFormat;
    }

    @Override
    public Set<ActiveMQDestination> getDestinations() {
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

    protected void createMessageAudit() {
        if (enableAudit && audit == null) {
            audit = new ActiveMQMessageAudit(maxAuditDepth,maxProducersToAudit);
            TransactionContext c = null;

            try {
                c = getTransactionContext();
                getAdapter().doMessageIdScan(c, auditRecoveryDepth, new JDBCMessageIdScanListener() {
                    @Override
                    public void messageId(MessageId id) {
                        audit.isDuplicate(id);
                    }
                });
            } catch (Exception e) {
                LOG.error("Failed to reload store message audit for JDBC persistence adapter", e);
            } finally {
                if (c != null) {
                    try {
                        c.close();
                    } catch (Throwable e) {
                    }
                }
            }
        }
    }

    public void initSequenceIdGenerator() {
        TransactionContext c = null;
        try {
            c = getTransactionContext();
            getAdapter().doMessageIdScan(c, auditRecoveryDepth, new JDBCMessageIdScanListener() {
                @Override
                public void messageId(MessageId id) {
                    audit.isDuplicate(id);
                }
            });
        } catch (Exception e) {
            LOG.error("Failed to reload store message audit for JDBC persistence adapter", e);
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (Throwable e) {
                }
            }
        }
    }

    @Override
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        MessageStore rc = storeCache.get(destination);
        if (rc == null) {
            MessageStore store = transactionStore.proxy(new JDBCMessageStore(this, getAdapter(), wireFormat, destination, audit));
            rc = storeCache.putIfAbsent(destination, store);
            if (rc == null) {
                rc = store;
            }
        }
        return rc;
    }

    @Override
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        TopicMessageStore rc = (TopicMessageStore) storeCache.get(destination);
        if (rc == null) {
            TopicMessageStore store = transactionStore.proxy(new JDBCTopicMessageStore(this, getAdapter(), wireFormat, destination, audit));
            rc = (TopicMessageStore) storeCache.putIfAbsent(destination, store);
            if (rc == null) {
                rc = store;
            }
        }
        return rc;
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     * @param destination Destination to forget
     */
    @Override
    public void removeQueueMessageStore(ActiveMQQueue destination) {
        if (destination.isQueue() && getBrokerService().shouldRecordVirtualDestination(destination)) {
            try {
                removeConsumerDestination(destination);
            } catch (IOException ioe) {
                LOG.error("Failed to remove consumer destination: " + destination, ioe);
            }
        }
        storeCache.remove(destination);
    }

    private void removeConsumerDestination(ActiveMQQueue destination) throws IOException {
        TransactionContext c = getTransactionContext();
        try {
            String id = destination.getQualifiedName();
            getAdapter().doDeleteSubscription(c, destination, id, id);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to remove consumer destination: " + destination, e);
        } finally {
            c.close();
        }
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     * No state retained.... nothing to do
     *
     * @param destination Destination to forget
     */
    @Override
    public void removeTopicMessageStore(ActiveMQTopic destination) {
        storeCache.remove(destination);
    }

    @Override
    public TransactionStore createTransactionStore() throws IOException {
        return this.transactionStore;
    }

    @Override
    public long getLastMessageBrokerSequenceId() throws IOException {
        TransactionContext c = getTransactionContext();
        try {
            long seq =  getAdapter().doGetLastMessageStoreSequenceId(c);
            sequenceGenerator.setLastSequenceId(seq);
            long brokerSeq = 0;
            if (seq != 0) {
                byte[] msg = getAdapter().doGetMessageById(c, seq);
                if (msg != null) {
                    Message last = (Message)wireFormat.unmarshal(new ByteSequence(msg));
                    brokerSeq = last.getMessageId().getBrokerSequenceId();
                } else {
                   LOG.warn("Broker sequence id wasn't recovered properly, possible duplicates!");
                }
            }
            return brokerSeq;
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to get last broker message id: " + e, e);
        } finally {
            c.close();
        }
    }

    @Override
    public long getLastProducerSequenceId(ProducerId id) throws IOException {
        TransactionContext c = getTransactionContext();
        try {
            return getAdapter().doGetLastProducerSequenceId(c, id);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to get last broker message id: " + e, e);
        } finally {
            c.close();
        }
    }

    @Override
    public void allowIOResumption() {}

    @Override
    public void init() throws Exception {
        getAdapter().setUseExternalMessageReferences(isUseExternalMessageReferences());

        if (isCreateTablesOnStartup()) {
            TransactionContext transactionContext = getTransactionContext();
            transactionContext.getExclusiveConnection();
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
    }

    @Override
    public void doStart() throws Exception {

        if( brokerService!=null ) {
          wireFormat.setVersion(brokerService.getStoreOpenWireVersion());
        }

        // Cleanup the db periodically.
        if (cleanupPeriod > 0) {
            cleanupTicket = getScheduledThreadPoolExecutor().scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    cleanup();
                }
            }, 0, cleanupPeriod, TimeUnit.MILLISECONDS);
        }
        createMessageAudit();
    }

    @Override
    public synchronized void doStop(ServiceStopper stopper) throws Exception {
        if (cleanupTicket != null) {
            cleanupTicket.cancel(true);
            cleanupTicket = null;
        }
        closeDataSource(getDataSource());
    }

    public void cleanup() {
        TransactionContext c = null;
        try {
            LOG.debug("Cleaning up old messages.");
            c = getTransactionContext();
            c.getExclusiveConnection();
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

    @Override
    public ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
        if (clockDaemon == null) {
            clockDaemon = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "ActiveMQ JDBC PA Scheduled Task");
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

    /**
     * @deprecated as of 5.7.0, replaced by {@link #getLocker()}
     */
    @Deprecated
    public Locker getDatabaseLocker() throws IOException {
        return getLocker();
    }

    /**
     * Sets the database locker strategy to use to lock the database on startup
     * @throws IOException
     *
     * @deprecated as of 5.7.0, replaced by {@link #setLocker(org.apache.activemq.broker.Locker)}
     */
    @Deprecated
    public void setDatabaseLocker(Locker locker) throws IOException {
        setLocker(locker);
    }

    public DataSource getLockDataSource() throws IOException {
        if (lockDataSource == null) {
            lockDataSource = getDataSource();
            if (lockDataSource == null) {
                throw new IllegalArgumentException(
                        "No dataSource property has been configured");
            }
        }
        return lockDataSource;
    }

    public void setLockDataSource(DataSource dataSource) {
        this.lockDataSource = dataSource;
        LOG.info("Using a separate dataSource for locking: "
                            + lockDataSource);
    }

    @Override
    public BrokerService getBrokerService() {
        return brokerService;
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
                dirverName = dirverName.replaceAll("[^a-zA-Z0-9\\-]", "_").toLowerCase(Locale.ENGLISH);

                try {
                    adapter = finder.newInstance(dirverName);
                    LOG.info("Database " + kind + " driver override recognized for : [" + dirverName + "] - adapter: " + adapter.getClass());
                } catch (Throwable e) {
                    LOG.info("Database " + kind + " driver override not found for : [" + dirverName
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
        this.adapter.setMaxRows(getMaxRows());
    }

    public WireFormat getWireFormat() {
        return wireFormat;
    }

    public void setWireFormat(WireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    public TransactionContext getTransactionContext(ConnectionContext context) throws IOException {
        if (context == null || isBrokerContext(context)) {
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

    private boolean isBrokerContext(ConnectionContext context) {
        return context.getSecurityContext() != null && context.getSecurityContext().isBrokerContext();
    }

    public TransactionContext getTransactionContext() throws IOException {
        TransactionContext answer = new TransactionContext(this, networkTimeout, queryTimeout);
        if (transactionIsolation > 0) {
            answer.setTransactionIsolation(transactionIsolation);
        }
        return answer;
    }

    @Override
    public void beginTransaction(ConnectionContext context) throws IOException {
        TransactionContext transactionContext = getTransactionContext(context);
        transactionContext.begin();
    }

    @Override
    public void commitTransaction(ConnectionContext context) throws IOException {
        TransactionContext transactionContext = getTransactionContext(context);
        transactionContext.commit();
    }

    @Override
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

    public boolean isChangeAutoCommitAllowed() {
        return changeAutoCommitAllowed;
    }

    /**
     * Whether the JDBC driver allows to set the auto commit.
     * Some drivers does not allow changing the auto commit. The default value is true.
     *
     * @param changeAutoCommitAllowed true to change, false to not change.
     */
    public void setChangeAutoCommitAllowed(boolean changeAutoCommitAllowed) {
        this.changeAutoCommitAllowed = changeAutoCommitAllowed;
    }

    public int getNetworkTimeout() {
        return networkTimeout;
    }

    /**
     * Define the JDBC connection network timeout.
     *
     * @param networkTimeout the connection network timeout (in milliseconds).
     */
    public void setNetworkTimeout(int networkTimeout) {
        this.networkTimeout = networkTimeout;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Define the JDBC statement query timeout.
     *
     * @param queryTimeout the statement query timeout (in seconds).
     */
    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    @Override
    public void deleteAllMessages() throws IOException {
        TransactionContext c = getTransactionContext();
        c.getExclusiveConnection();
        try {
            getAdapter().doDropTables(c);
            getAdapter().setUseExternalMessageReferences(isUseExternalMessageReferences());
            getAdapter().doCreateTables(c);
            LOG.info("Persistence store purged.");
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

    /**
     * @deprecated use {@link #setUseLock(boolean)} instead
     *
     * Sets whether or not an exclusive database lock should be used to enable
     * JDBC Master/Slave. Enabled by default.
     */
    @Deprecated
    public void setUseDatabaseLock(boolean useDatabaseLock) {
        setUseLock(useDatabaseLock);
    }

    public static void log(String msg, SQLException e) {
        String s = msg + e.getMessage();
        while (e.getNextException() != null) {
            e = e.getNextException();
            s += ", due to: " + e.getMessage();
        }
        LOG.warn(s, e);
    }

    public Statements getStatements() {
        if (statements == null) {
            statements = new Statements();
        }
        return statements;
    }

    public void setStatements(Statements statements) {
        this.statements = statements;
        if (adapter != null) {
            this.adapter.setStatements(getStatements());
        }
    }

    /**
     * @param usageManager The UsageManager that is controlling the
     *                destination's memory usage.
     */
    @Override
    public void setUsageManager(SystemUsage usageManager) {
    }

    @Override
    public Locker createDefaultLocker() throws IOException {
        Locker locker = (Locker) loadAdapter(lockFactoryFinder, "lock");
        if (locker == null) {
            locker = new DefaultDatabaseLocker();
            LOG.debug("Using default JDBC Locker: " + locker);
        }
        locker.configure(this);
        return locker;
    }

    @Override
    public void setBrokerName(String brokerName) {
    }

    @Override
    public String toString() {
        return "JDBCPersistenceAdapter(" + super.toString() + ")";
    }

    @Override
    public void setDirectory(File dir) {
        this.directory=dir;
    }

    @Override
    public File getDirectory(){
        if (this.directory==null && brokerService != null){
            this.directory=brokerService.getBrokerDataDirectory();
        }
        return this.directory;
    }

    // interesting bit here is proof that DB is ok
    @Override
    public void checkpoint(boolean sync) throws IOException {
        // by pass TransactionContext to avoid IO Exception handler
        Connection connection = null;
        try {
            connection = getDataSource().getConnection();
            if (!connection.isValid(10)) {
                throw new IOException("isValid(10) failed for: " + connection);
            }
        } catch (SQLException e) {
            LOG.debug("Could not get JDBC connection for checkpoint: " + e, e);
            throw IOExceptionSupport.create(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    @Override
    public long size(){
        return 0;
    }

    /**
     * @deprecated use {@link Locker#setLockAcquireSleepInterval(long)} instead
     *
     * millisecond interval between lock acquire attempts, applied to newly created DefaultDatabaseLocker
     * not applied if DataBaseLocker is injected.
     *
     */
    @Deprecated
    public void setLockAcquireSleepInterval(long lockAcquireSleepInterval) throws IOException {
        getLocker().setLockAcquireSleepInterval(lockAcquireSleepInterval);
    }

    /**
     * set the Transaction isolation level to something other that TRANSACTION_READ_UNCOMMITTED
     * This allowable dirty isolation level may not be achievable in clustered DB environments
     * so a more restrictive and expensive option may be needed like TRANSACTION_REPEATABLE_READ
     * see isolation level constants in {@link java.sql.Connection}
     * @param transactionIsolation the isolation level to use
     */
    public void setTransactionIsolation(int transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }

    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    public void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
    }

    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }

    public void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
    }

    public boolean isEnableAudit() {
        return enableAudit;
    }

    public void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
    }

    public int getAuditRecoveryDepth() {
        return auditRecoveryDepth;
    }

    public void setAuditRecoveryDepth(int auditRecoveryDepth) {
        this.auditRecoveryDepth = auditRecoveryDepth;
    }

    public long getNextSequenceId() {
        return sequenceGenerator.getNextSequenceId();
    }

    public int getMaxRows() {
        return maxRows;
    }

    /*
     * the max rows return from queries, with sparse selectors this may need to be increased
     */
    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public void recover(JdbcMemoryTransactionStore jdbcMemoryTransactionStore) throws IOException {
        TransactionContext c = getTransactionContext();
        try {
            getAdapter().doRecoverPreparedOps(c, jdbcMemoryTransactionStore);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to recover from: " + jdbcMemoryTransactionStore + ". Reason: " + e,e);
        } finally {
            c.close();
        }
    }

    public void commitAdd(ConnectionContext context, final MessageId messageId, final long preparedSequenceId, final long newSequence) throws IOException {
        TransactionContext c = getTransactionContext(context);
        try {
            getAdapter().doCommitAddOp(c, preparedSequenceId, newSequence);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to commit add: " + messageId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    public void commitRemove(ConnectionContext context, MessageAck ack) throws IOException {
        TransactionContext c = getTransactionContext(context);
        try {
            if (c != null && ack != null && ack.getLastMessageId() != null && ack.getLastMessageId().getEntryLocator() != null) {
                getAdapter().doRemoveMessage(c, (Long) ack.getLastMessageId().getEntryLocator(), null);
            }
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to commit last ack: " + ack + ". Reason: " + e,e);
        } finally {
            c.close();
        }
    }

    public void commitLastAck(ConnectionContext context, long xidLastAck, long priority, ActiveMQDestination destination, String subName, String clientId) throws IOException {
        TransactionContext c = getTransactionContext(context);
        try {
            getAdapter().doSetLastAck(c, destination, null, clientId, subName, xidLastAck, priority);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to commit last ack with priority: " + priority + " on " + destination + " for " + subName + ":" + clientId + ". Reason: " + e,e);
        } finally {
            c.close();
        }
    }

    public void rollbackLastAck(ConnectionContext context, JDBCTopicMessageStore store, MessageAck ack, String subName, String clientId) throws IOException {
        TransactionContext c = getTransactionContext(context);
        try {
            byte priority = (byte) store.getCachedStoreSequenceId(c, store.getDestination(), ack.getLastMessageId())[1];
            getAdapter().doClearLastAck(c, store.getDestination(), priority, clientId, subName);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to rollback last ack: " + ack + " on " +  store.getDestination() + " for " + subName + ":" + clientId + ". Reason: " + e,e);
        } finally {
            c.close();
        }
    }

    // after recovery there is no record of the original messageId for the ack
    public void rollbackLastAck(ConnectionContext context, byte priority, ActiveMQDestination destination, String subName, String clientId) throws IOException {
        TransactionContext c = getTransactionContext(context);
        try {
            getAdapter().doClearLastAck(c, destination, priority, clientId, subName);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to rollback last ack with priority: " + priority + " on " + destination + " for " + subName + ":" + clientId + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    long[] getStoreSequenceIdForMessageId(ConnectionContext context, MessageId messageId, ActiveMQDestination destination) throws IOException {
        long[] result = new long[]{-1, Byte.MAX_VALUE -1};
        TransactionContext c = getTransactionContext(context);
        try {
            result = adapter.getStoreSequenceId(c, destination, messageId);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to get store sequenceId for messageId: " + messageId +", on: " + destination + ". Reason: " + e, e);
        } finally {
            c.close();
        }
        return result;
    }

    @Override
    public JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
}
