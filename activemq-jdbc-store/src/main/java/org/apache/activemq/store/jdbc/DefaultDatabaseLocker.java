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
import java.sql.SQLFeatureNotSupportedException;
import org.apache.activemq.util.Handler;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an exclusive lock on a database to avoid multiple brokers running
 * against the same logical database.
 * 
 * @org.apache.xbean.XBean element="database-locker"
 * 
 */
public class DefaultDatabaseLocker extends AbstractJDBCLocker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseLocker.class);

    protected volatile PreparedStatement lockCreateStatement;
    protected volatile PreparedStatement lockUpdateStatement;
    protected volatile Connection connection;
    protected Handler<Exception> exceptionHandler;

    public void doStart() throws Exception {

        LOG.info("Attempting to acquire the exclusive lock to become the Master broker");
        String sql = getStatements().getLockCreateStatement();
        LOG.debug("Locking Query is "+sql);
        
        while (true) {
            try {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                lockCreateStatement = connection.prepareStatement(sql);
                lockCreateStatement.execute();
                break;
            } catch (Exception e) {
                try {
                    if (isStopping()) {
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
                            connection.rollback();
                        } catch (SQLException e1) {
                            LOG.debug("Caught exception during rollback on connection: " + e1, e1);
                        }
                        try {
                            connection.close();
                        } catch (SQLException e1) {
                            LOG.debug("Caught exception while closing connection: " + e1, e1);
                        }
                        
                        connection = null;
                    }
                }
            } finally {
                if (null != lockCreateStatement) {
                    try {
                        lockCreateStatement.close();
                    } catch (SQLException e1) {
                        LOG.debug("Caught while closing statement: " + e1, e1);
                    }
                    lockCreateStatement = null;
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

    public void doStop(ServiceStopper stopper) throws Exception {
        try {
            if (lockCreateStatement != null) {
                lockCreateStatement.cancel();    			
            }
        } catch (SQLFeatureNotSupportedException e) {
            LOG.warn("Failed to cancel locking query on dataSource" + dataSource, e);    		
        }
        try {
    	    if (lockUpdateStatement != null) {
        	    lockUpdateStatement.cancel();    			
    	    }
        } catch (SQLFeatureNotSupportedException e) {
            LOG.warn("Failed to cancel locking query on dataSource" + dataSource, e);    		
        }

        // when the connection is closed from an outside source (lost TCP
        // connection, db server, etc) and this connection is managed by a pool
        // it is important to close the connection so that we don't leak
        // connections

        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                LOG.debug("Exception while rollbacking the connection on shutdown. This exception is ignored.", sqle);
            } finally {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                    LOG.debug("Exception while closing connection on shutdown. This exception is ignored.", ignored);
                }
                lockCreateStatement = null;
            }
        }
    }

    public boolean keepAlive() throws IOException {
        boolean result = false;
        try {
            lockUpdateStatement = connection.prepareStatement(getStatements().getLockUpdateStatement());
            lockUpdateStatement.setLong(1, System.currentTimeMillis());
            setQueryTimeout(lockUpdateStatement);
            int rows = lockUpdateStatement.executeUpdate();
            if (rows == 1) {
                result=true;
            }
        } catch (Exception e) {
            LOG.error("Failed to update database lock: " + e, e);
        } finally {
            if (lockUpdateStatement != null) {
                try {
                    lockUpdateStatement.close();
                } catch (SQLException e) {
                    LOG.error("Failed to close statement",e);
                }
                lockUpdateStatement = null;
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
