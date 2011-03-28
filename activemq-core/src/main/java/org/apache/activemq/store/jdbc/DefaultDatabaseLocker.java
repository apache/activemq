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

import javax.sql.DataSource;

import org.apache.activemq.util.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an exclusive lock on a database to avoid multiple brokers running
 * against the same logical database.
 * 
 * @org.apache.xbean.XBean element="database-locker"
 * 
 */
public class DefaultDatabaseLocker implements DatabaseLocker {
    public static final long DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL = 1000;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseLocker.class);
    protected DataSource dataSource;
    protected Statements statements;
    protected long lockAcquireSleepInterval = DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL;

    protected Connection connection;
    protected boolean stopping;
    protected Handler<Exception> exceptionHandler;
    
    public DefaultDatabaseLocker() {
    }
    
    public DefaultDatabaseLocker(JDBCPersistenceAdapter persistenceAdapter) throws IOException {
        setPersistenceAdapter(persistenceAdapter);
    }

    public void setPersistenceAdapter(JDBCPersistenceAdapter adapter) throws IOException {
        this.dataSource = adapter.getLockDataSource();
        this.statements = adapter.getStatements();
    }
    
    public void start() throws Exception {
        stopping = false;

        LOG.info("Attempting to acquire the exclusive lock to become the Master broker");
        String sql = statements.getLockCreateStatement();
        LOG.debug("Locking Query is "+sql);
        
        PreparedStatement statement = null;
        while (true) {
            try {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                statement = connection.prepareStatement(sql);
                statement.execute();
                break;
            } catch (Exception e) {
                try {
                    if (stopping) {
                        throw new Exception(
                                "Cannot start broker as being asked to shut down. " 
                                        + "Interrupted attempt to acquire lock: "
                                        + e, e);
                    }
                    if (exceptionHandler != null) {
                        try {
                            exceptionHandler.handle(e);
                        } catch (Throwable handlerException) {
                            LOG.error( "The exception handler "
                                    + exceptionHandler.getClass().getCanonicalName()
                                    + " threw this exception: "
                                    + handlerException
                                    + " while trying to handle this exception: "
                                    + e, handlerException);
                        }

                    } else {
                        LOG.debug("Lock failure: "+ e, e);
                    }
                } finally {
                    // Let's make sure the database connection is properly
                    // closed when an error occurs so that we're not leaking
                    // connections 
                    if (null != connection) {
                        try {
                            connection.close();
                        } catch (SQLException e1) {
                            LOG.error("Caught exception while closing connection: " + e1, e1);
                        }
                        
                        connection = null;
                    }
                }
            } finally {
                if (null != statement) {
                    try {
                        statement.close();
                    } catch (SQLException e1) {
                        LOG.debug("Caught while closing statement: " + e1, e1);
                    }
                    statement = null;
                }
            }

            LOG.info("Failed to acquire lock.  Sleeping for " + lockAcquireSleepInterval + " milli(s) before trying again...");
            try {
                Thread.sleep(lockAcquireSleepInterval);
            } catch (InterruptedException ie) {
                LOG.warn("Master lock retry sleep interrupted", ie);
            }
        }

        LOG.info("Becoming the master on dataSource: " + dataSource);
    }

    public void stop() throws Exception {
        stopping = true;
        try {
            if (connection != null && !connection.isClosed()) {
                try {
                    connection.rollback();
                } catch (SQLException sqle) {
                    LOG.warn("Exception while rollbacking the connection on shutdown", sqle);
                } finally {
                    try {
                        connection.close();
                    } catch (SQLException ignored) {
                        LOG.debug("Exception while closing connection on shutdown", ignored);
                    }
                }
            }
        } catch (SQLException sqle) {
            LOG.warn("Exception while checking close status of connection on shutdown", sqle);
        }
    }

    public boolean keepAlive() {
        PreparedStatement statement = null;
        boolean result = false;
        try {
            statement = connection.prepareStatement(statements.getLockUpdateStatement());
            statement.setLong(1, System.currentTimeMillis());
            int rows = statement.executeUpdate();
            if (rows == 1) {
                result=true;
            }
        } catch (Exception e) {
            LOG.error("Failed to update database lock: " + e, e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.error("Failed to close statement",e);
                }
            }
        }
        return result;
    }
 
    public long getLockAcquireSleepInterval() {
        return lockAcquireSleepInterval;
    }

    public void setLockAcquireSleepInterval(long lockAcquireSleepInterval) {
        this.lockAcquireSleepInterval = lockAcquireSleepInterval;
    }
    
    public Handler getExceptionHandler() {
        return exceptionHandler;
    }

    public void setExceptionHandler(Handler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

}
