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

import java.io.IOException;
import java.sql.*;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.sql.DataSource;

import org.apache.activemq.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helps keep track of the current transaction/JDBC connection.
 */
public class TransactionContext {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionContext.class);

    private final DataSource dataSource;
    private final JDBCPersistenceAdapter persistenceAdapter;
    private Connection connection;
    private boolean inTx;
    private PreparedStatement addMessageStatement;
    private PreparedStatement removedMessageStatement;
    private PreparedStatement updateLastAckStatement;
    // a cheap dirty level that we can live with    
    private int transactionIsolation = Connection.TRANSACTION_READ_UNCOMMITTED;
    private LinkedList<Runnable> completions = new LinkedList<Runnable>();
    private ReentrantReadWriteLock exclusiveConnectionLock = new ReentrantReadWriteLock();

    public TransactionContext(JDBCPersistenceAdapter persistenceAdapter) throws IOException {
        this.persistenceAdapter = persistenceAdapter;
        this.dataSource = persistenceAdapter.getDataSource();
    }

    public Connection getExclusiveConnection() throws IOException {
        return lockAndWrapped(exclusiveConnectionLock.writeLock());
    }

    public Connection getConnection() throws IOException {
        return lockAndWrapped(exclusiveConnectionLock.readLock());
    }

    private Connection lockAndWrapped(Lock toLock) throws IOException {
        if (connection == null) {
            toLock.lock();
            try {
                connection = dataSource.getConnection();
                if (persistenceAdapter.isChangeAutoCommitAllowed()) {
                    boolean autoCommit = !inTx;
                    if (connection.getAutoCommit() != autoCommit) {
                        LOG.trace("Setting auto commit to {} on connection {}", autoCommit, connection);
                        connection.setAutoCommit(autoCommit);
                    }
                }
                connection = new UnlockOnCloseConnection(connection, toLock);
            } catch (SQLException e) {
                JDBCPersistenceAdapter.log("Could not get JDBC connection: ", e);
                inTx = false;
                try {
                    toLock.unlock();
                } catch (IllegalMonitorStateException oops) {
                    LOG.error("Thread does not hold the context lock on close of:"  + connection, oops);
                }
                close();
                IOException ioe = IOExceptionSupport.create(e);
                if (persistenceAdapter.getBrokerService() != null) {
                    persistenceAdapter.getBrokerService().handleIOException(ioe);
                }
                throw ioe;
            }

            try {
                connection.setTransactionIsolation(transactionIsolation);
            } catch (Throwable e) {
                // ignore
                LOG.trace("Cannot set transaction isolation to " + transactionIsolation + " due " + e.getMessage()
                        + ". This exception is ignored.", e);
            }
        }
        return connection;
    }

    public void executeBatch() throws SQLException {
        try {
            executeBatch(addMessageStatement, "Failed add a message");
        } finally {
            addMessageStatement = null;
            try {
                executeBatch(removedMessageStatement, "Failed to remove a message");
            } finally {
                removedMessageStatement = null;
                try {
                    executeBatch(updateLastAckStatement, "Failed to ack a message");
                } finally {
                    updateLastAckStatement = null;
                }
            }
        }
    }

    private void executeBatch(PreparedStatement p, String message) throws SQLException {
        if (p == null) {
            return;
        }

        try {
            int[] rc = p.executeBatch();
            for (int i = 0; i < rc.length; i++) {
                int code = rc[i];
                if (code < 0 && code != Statement.SUCCESS_NO_INFO) {
                    throw new SQLException(message + ". Response code: " + code);
                }
            }
        } finally {
            try {
                p.close();
            } catch (Throwable e) {
            }
        }
    }

    public void close() throws IOException {
        if (!inTx) {
            try {

                /**
                 * we are not in a transaction so should not be committing ??
                 * This was previously commented out - but had adverse affects
                 * on testing - so it's back!
                 * 
                 */
                try {
                    executeBatch();
                } finally {
                    if (connection != null && !connection.getAutoCommit()) {
                        connection.commit();
                    }
                }

            } catch (SQLException e) {
                JDBCPersistenceAdapter.log("Error while closing connection: ", e);
                IOException ioe = IOExceptionSupport.create(e);
                persistenceAdapter.getBrokerService().handleIOException(ioe);
                throw ioe;
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (Throwable e) {
                    // ignore
                    LOG.trace("Closing connection failed due: " + e.getMessage() + ". This exception is ignored.", e);
                } finally {
                    connection = null;
                }
                for (Runnable completion: completions) {
                    completion.run();
                }
                completions.clear();
            }
        }
    }

    public void begin() throws IOException {
        if (inTx) {
            throw new IOException("Already started.");
        }
        inTx = true;
        connection = getConnection();
    }

    public void commit() throws IOException {
        if (!inTx) {
            throw new IOException("Not started.");
        }
        try {
            executeBatch();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("Commit failed: ", e);
            try {
                doRollback();
            } catch (Exception ignored) {}
            IOException ioe = IOExceptionSupport.create(e);
            persistenceAdapter.getBrokerService().handleIOException(ioe);
            throw ioe;
        } finally {
            inTx = false;
            close();
        }
    }

    public void rollback() throws IOException {
        if (!inTx) {
            throw new IOException("Not started.");
        }
        try {
            doRollback();
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("Rollback failed: ", e);
            throw IOExceptionSupport.create(e);
        } finally {
            inTx = false;
            close();
        }
    }

    private void doRollback() throws SQLException {
        if (addMessageStatement != null) {
            addMessageStatement.close();
            addMessageStatement = null;
        }
        if (removedMessageStatement != null) {
            removedMessageStatement.close();
            removedMessageStatement = null;
        }
        if (updateLastAckStatement != null) {
            updateLastAckStatement.close();
            updateLastAckStatement = null;
        }
        connection.rollback();
    }

    public PreparedStatement getAddMessageStatement() {
        return addMessageStatement;
    }

    public void setAddMessageStatement(PreparedStatement addMessageStatement) {
        this.addMessageStatement = addMessageStatement;
    }

    public PreparedStatement getUpdateLastAckStatement() {
        return updateLastAckStatement;
    }

    public void setUpdateLastAckStatement(PreparedStatement ackMessageStatement) {
        this.updateLastAckStatement = ackMessageStatement;
    }

    public PreparedStatement getRemovedMessageStatement() {
        return removedMessageStatement;
    }

    public void setRemovedMessageStatement(PreparedStatement removedMessageStatement) {
        this.removedMessageStatement = removedMessageStatement;
    }
    
    public void setTransactionIsolation(int transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }

    public void onCompletion(Runnable runnable) {
        completions.add(runnable);
    }

    final private class UnlockOnCloseConnection implements Connection {

        private final Connection delegate;
        private final Lock lock;

        UnlockOnCloseConnection(Connection delegate, Lock toUnlockOnClose) {
            this.delegate = delegate;
            this.lock = toUnlockOnClose;
        }

        @Override
        public void close() throws SQLException {
            try {
                delegate.close();
            } finally {
                lock.unlock();
            }
        }

        // simple delegate for the  rest of the impl..
        @Override
        public Statement createStatement() throws SQLException {
            return delegate.createStatement();
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return delegate.prepareStatement(sql);
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return delegate.prepareCall(sql);
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return delegate.nativeSQL(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            delegate.setAutoCommit(autoCommit);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return delegate.getAutoCommit();
        }

        @Override
        public void commit() throws SQLException {
            delegate.commit();
        }

        @Override
        public void rollback() throws SQLException {
            delegate.rollback();
        }

        @Override
        public boolean isClosed() throws SQLException {
            return delegate.isClosed();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return delegate.getMetaData();
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            delegate.setReadOnly(readOnly);
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return delegate.isReadOnly();
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            delegate.setCatalog(catalog);
        }

        @Override
        public String getCatalog() throws SQLException {
            return delegate.getCatalog();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            delegate.setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return delegate.getTransactionIsolation();
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return delegate.getWarnings();
        }

        @Override
        public void clearWarnings() throws SQLException {
            delegate.clearWarnings();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return delegate.getTypeMap();
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            delegate.setTypeMap(map);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            delegate.setHoldability(holdability);
        }

        @Override
        public int getHoldability() throws SQLException {
            return delegate.getHoldability();
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return delegate.setSavepoint();
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return delegate.setSavepoint(name);
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            delegate.rollback(savepoint);
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            delegate.releaseSavepoint(savepoint);
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return delegate.prepareStatement(sql, autoGeneratedKeys);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return delegate.prepareStatement(sql, columnIndexes);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return delegate.prepareStatement(sql, columnNames);
        }

        @Override
        public Clob createClob() throws SQLException {
            return delegate.createClob();
        }

        @Override
        public Blob createBlob() throws SQLException {
            return delegate.createBlob();
        }

        @Override
        public NClob createNClob() throws SQLException {
            return delegate.createNClob();
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return delegate.createSQLXML();
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return delegate.isValid(timeout);
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            delegate.setClientInfo(name, value);
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            delegate.setClientInfo(properties);
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return delegate.getClientInfo(name);
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return delegate.getClientInfo();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return delegate.createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return delegate.createStruct(typeName, attributes);
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            delegate.setSchema(schema);
        }

        @Override
        public String getSchema() throws SQLException {
            return delegate.getSchema();
        }

        @Override
        public void abort(Executor executor) throws SQLException {
            delegate.abort(executor);
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            delegate.setNetworkTimeout(executor, milliseconds);
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return delegate.getNetworkTimeout();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }
}
