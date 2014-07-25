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
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.activemq.broker.AbstractLocker;
import org.apache.activemq.store.PersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJDBCLocker extends AbstractLocker {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJDBCLocker.class);
    protected DataSource dataSource;
    private Statements statements;
    protected JDBCPersistenceAdapter jdbcAdapter;

    protected boolean createTablesOnStartup;
    protected int queryTimeout = -1;

    public void configure(PersistenceAdapter adapter) throws IOException {
        if (adapter instanceof JDBCPersistenceAdapter) {
            this.jdbcAdapter = (JDBCPersistenceAdapter) adapter;
            this.dataSource = ((JDBCPersistenceAdapter) adapter).getLockDataSource();
            // we cannot get the statements (yet) as they may be configured later
        }
    }

    protected Statements getStatements() {
        if (statements == null && jdbcAdapter != null) {
            statements = jdbcAdapter.getStatements();
        }
        return statements;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setStatements(Statements statements) {
        this.statements = statements;
    }

    protected void setQueryTimeout(Statement statement) throws SQLException {
        if (queryTimeout > 0) {
            statement.setQueryTimeout(queryTimeout);
        }
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public void setCreateTablesOnStartup(boolean createTablesOnStartup) {
        this.createTablesOnStartup = createTablesOnStartup;
    }

    protected Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    protected void close(Connection connection) {
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e1) {
                LOG.debug("exception while closing connection: " + e1, e1);
            }
        }
    }

    protected void close(Statement statement) {
        if (null != statement) {
            try {
                statement.close();
            } catch (SQLException e1) {
                LOG.debug("exception while closing statement: " + e1, e1);
            }
        }
    }

    @Override
    public void preStart() {
        if (createTablesOnStartup) {

            String[] createStatements = getStatements().getCreateLockSchemaStatements();

            Connection connection = null;
            Statement statement = null;
            try {
                connection = getConnection();
                statement = connection.createStatement();
                setQueryTimeout(statement);

                for (int i = 0; i < createStatements.length; i++) {
                    LOG.debug("Executing SQL: " + createStatements[i]);
                    try {
                        statement.execute(createStatements[i]);
                    } catch (SQLException e) {
                        LOG.info("Could not create lock tables; they could already exist." + " Failure was: "
                                + createStatements[i] + " Message: " + e.getMessage() + " SQLState: " + e.getSQLState()
                                + " Vendor code: " + e.getErrorCode());
                    }
                }
            } catch (SQLException e) {
                LOG.warn("Could not create lock tables; Failure Message: " + e.getMessage() + " SQLState: " + e.getSQLState()
                        + " Vendor code: " + e.getErrorCode(), e);
            } finally {
                close(statement);
                close(connection);
            }
        }
    }

}
