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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.activemq.util.IOExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Helps keep track of the current transaction/JDBC connection.
 * 
 * @version $Revision: 1.2 $
 */
public class TransactionContext {

    private static final Log LOG = LogFactory.getLog(TransactionContext.class);

    private final DataSource dataSource;
    private Connection connection;
    private boolean inTx;
    private PreparedStatement addMessageStatement;
    private PreparedStatement removedMessageStatement;
    private PreparedStatement updateLastAckStatement;

    public TransactionContext(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Connection getConnection() throws IOException {
        if (connection == null) {
            try {
                connection = dataSource.getConnection();
                boolean autoCommit = !inTx;
                if (connection.getAutoCommit() != autoCommit) {
                    connection.setAutoCommit(autoCommit);
                }
            } catch (SQLException e) {
                JDBCPersistenceAdapter.log("Could not get JDBC connection: ", e);
                throw IOExceptionSupport.create(e);
            }

            try {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            } catch (Throwable e) {
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
                throw IOExceptionSupport.create(e);
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (Throwable e) {
                    LOG.warn("Close failed: " + e.getMessage(), e);
                } finally {
                    connection = null;
                }
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
            
            this.rollback(); 
            
            throw IOExceptionSupport.create(e);
        } finally {
            close();
            inTx = false;
        }
    }

    public void rollback() throws IOException {
        if (!inTx) {
            throw new IOException("Not started.");
        }
        try {
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

        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("Rollback failed: ", e);
            throw IOExceptionSupport.create(e);
        } finally {
            close();
            inTx = false;
        }
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

}
