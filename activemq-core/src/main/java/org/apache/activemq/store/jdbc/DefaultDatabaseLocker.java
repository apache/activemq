/*
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Represents an exclusive lock on a database to avoid multiple brokers running
 * against the same logical database.
 * 
 * @version $Revision: $
 */
public class DefaultDatabaseLocker implements DatabaseLocker {
    private static final Log log = LogFactory.getLog(DefaultDatabaseLocker.class);
    private final DataSource dataSource;
    private final Statements statements;
    private long sleepTime = 1000;
    private Connection connection;
    private PreparedStatement statement;
    private boolean stopping;

    public DefaultDatabaseLocker(DataSource dataSource, Statements statements) {
        this.dataSource = dataSource;
        this.statements = statements;
    }

    public void start() throws Exception {
        stopping = false;

        log.info("Attempting to acquire the exclusive lock to become the Master broker");

        while (true) {
            try {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                String sql = statements.getLockCreateStatement();
                statement = connection.prepareStatement(sql);
                statement.execute();
                break;
            } catch (Exception e) {
                if (stopping) {
                    throw new Exception("Cannot start broker as being asked to shut down. Interrupted attempt to acquire lock: " + e, e);
                }
                log.error("Failed to acquire lock: " + e, e);
                if (null != statement) {
                    try {
                        statement.close();
                    } catch (SQLException e1) {
                        log.warn("Caught while closing statement: " + e1, e1);
                    }
                    statement = null;
                }
                if (null != connection) {
                    try {
                        connection.close();
                    } catch (SQLException e1) {
                        log.warn("Caught while closing connection: " + e1, e1);
                    }
                    connection = null;
                }
            }

            log.debug("Sleeping for " + sleepTime + " milli(s) before trying again to get the lock...");
            Thread.sleep(sleepTime);
        }

        log.info("Becoming the master on dataSource: " + dataSource);
    }

    public void stop() throws Exception {
        stopping = true;
        if (connection != null) {
            connection.rollback();
            connection.close();
        }
    }

    public boolean keepAlive() {
        try {
            PreparedStatement statement = connection.prepareStatement(statements.getLockUpdateStatement());
            statement.setLong(1, System.currentTimeMillis());
            int rows = statement.executeUpdate();
            if (rows == 1) {
                return true;
            }
        } catch (Exception e) {
            log.error("Failed to update database lock: " + e, e);
        }
        return false;
    }
}
