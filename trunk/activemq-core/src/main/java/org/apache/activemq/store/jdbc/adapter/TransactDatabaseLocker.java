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
package org.apache.activemq.store.jdbc.adapter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.activemq.store.jdbc.DefaultDatabaseLocker;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an exclusive lock on a database to avoid multiple brokers running
 * against the same logical database.
 * 
 * @org.apache.xbean.XBean element="transact-database-locker"
 * 
 */
public class TransactDatabaseLocker extends DefaultDatabaseLocker {
    private static final Logger LOG = LoggerFactory.getLogger(TransactDatabaseLocker.class);
    
    public TransactDatabaseLocker() {
    }
    
    public TransactDatabaseLocker(JDBCPersistenceAdapter persistenceAdapter) throws IOException {
        setPersistenceAdapter(persistenceAdapter);
    }
    
    @Override
    public void start() throws Exception {
        stopping = false;

        LOG.info("Attempting to acquire the exclusive lock to become the Master broker");
        PreparedStatement statement = null;
        while (true) {
            try {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                String sql = statements.getLockCreateStatement();
                statement = connection.prepareStatement(sql);
                if (statement.getMetaData() != null) {
                    ResultSet rs = statement.executeQuery();
                    // if not already locked the statement below blocks until lock acquired
                    rs.next();
                } else {
                    statement.execute();
                }
                break;
            } catch (Exception e) {
                if (stopping) {
                    throw new Exception("Cannot start broker as being asked to shut down. Interrupted attempt to acquire lock: " + e, e);
                }

                if (exceptionHandler != null) {
                    try {
                        exceptionHandler.handle(e);
                    } catch (Throwable handlerException) {
                        LOG.error("The exception handler " + exceptionHandler.getClass().getCanonicalName() + " threw this exception: " + handlerException
                                + " while trying to handle this excpetion: " + e, handlerException);
                    }

                } else {
                    LOG.error("Failed to acquire lock: " + e, e);
                }
            } finally {

                if (null != statement) {
                    try {
                        statement.close();
                    } catch (SQLException e1) {
                        LOG.warn("Caught while closing statement: " + e1, e1);
                    }
                    statement = null;
                }
            }

            LOG.debug("Sleeping for " + lockAcquireSleepInterval + " milli(s) before trying again to get the lock...");
            try {
            	Thread.sleep(lockAcquireSleepInterval);
            } catch (InterruptedException ie) {
            	LOG.warn("Master lock retry sleep interrupted", ie);
            }
        }

        LOG.info("Becoming the master on dataSource: " + dataSource);
    }

}
