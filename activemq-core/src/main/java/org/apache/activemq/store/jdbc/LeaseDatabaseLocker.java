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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import org.apache.activemq.broker.AbstractLocker;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an exclusive lease on a database to avoid multiple brokers running
 * against the same logical database.
 * 
 * @org.apache.xbean.XBean element="lease-database-locker"
 * 
 */
public class LeaseDatabaseLocker extends AbstractLocker {
    private static final Logger LOG = LoggerFactory.getLogger(LeaseDatabaseLocker.class);
    protected DataSource dataSource;
    protected Statements statements;

    protected boolean stopping;
    protected int maxAllowableDiffFromDBTime = 0;
    protected long diffFromCurrentTime = Long.MAX_VALUE;
    protected String leaseHolderId;
    protected int queryTimeout = -1;
    JDBCPersistenceAdapter persistenceAdapter;


    public void configure(PersistenceAdapter adapter) throws IOException {
        if (adapter instanceof JDBCPersistenceAdapter) {
            this.persistenceAdapter = (JDBCPersistenceAdapter)adapter;
            this.dataSource = ((JDBCPersistenceAdapter) adapter).getLockDataSource();
            this.statements = ((JDBCPersistenceAdapter) adapter).getStatements();
        }
    }
    
    public void doStart() throws Exception {
        stopping = false;

        LOG.info(getLeaseHolderId() + " attempting to acquire exclusive lease to become the Master broker");
        String sql = statements.getLeaseObtainStatement();
        LOG.debug(getLeaseHolderId() + " locking Query is "+sql);

        while (!stopping) {
            Connection connection = null;
            PreparedStatement statement = null;
            try {
                connection = getConnection();
                initTimeDiff(connection);

                statement = connection.prepareStatement(sql);
                setQueryTimeout(statement);

                final long now = System.currentTimeMillis() + diffFromCurrentTime;
                statement.setString(1, getLeaseHolderId());
                statement.setLong(2, now + lockAcquireSleepInterval);
                statement.setLong(3, now);

                int result = statement.executeUpdate();
                if (result == 1) {
                    // we got the lease, verify we still have it
                    if (keepAlive()) {
                        break;
                    }
                }

                reportLeasOwnerShipAndDuration(connection);

            } catch (Exception e) {
                LOG.debug(getLeaseHolderId() + " lease acquire failure: "+ e, e);
            } finally {
                close(statement);
                close(connection);
            }

            LOG.info(getLeaseHolderId() + " failed to acquire lease.  Sleeping for " + lockAcquireSleepInterval + " milli(s) before trying again...");
            TimeUnit.MILLISECONDS.sleep(lockAcquireSleepInterval);
        }
        if (stopping) {
            throw new RuntimeException(getLeaseHolderId() + " failing lease acquire due to stop");
        }

        LOG.info(getLeaseHolderId() + ", becoming the master on dataSource: " + dataSource);
    }

    private void setQueryTimeout(PreparedStatement statement) throws SQLException {
        if (queryTimeout > 0) {
            statement.setQueryTimeout(queryTimeout);
        }
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private void close(Connection connection) {
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e1) {
                LOG.debug(getLeaseHolderId() + " caught exception while closing connection: " + e1, e1);
            }
        }
    }

    private void close(PreparedStatement statement) {
        if (null != statement) {
            try {
                statement.close();
            } catch (SQLException e1) {
                LOG.debug(getLeaseHolderId() + ", caught while closing statement: " + e1, e1);
            }
        }
    }

    private void reportLeasOwnerShipAndDuration(Connection connection) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(statements.getLeaseOwnerStatement());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                LOG.info(getLeaseHolderId() + " Lease held by " + resultSet.getString(1) + " till " + new Date(resultSet.getLong(2)));
            }
        } finally {
            close(statement);
        }
    }

    protected long initTimeDiff(Connection connection) throws SQLException {
        if (Long.MAX_VALUE == diffFromCurrentTime) {
            if (maxAllowableDiffFromDBTime > 0) {
                diffFromCurrentTime = determineTimeDifference(connection);
            } else {
                diffFromCurrentTime = 0l;
            }
        }
        return diffFromCurrentTime;
    }

    private long determineTimeDifference(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(statements.getCurrentDateTime());
        ResultSet resultSet = statement.executeQuery();
        long result = 0l;
        if (resultSet.next()) {
            Timestamp timestamp = resultSet.getTimestamp(1);
            long diff = System.currentTimeMillis() - timestamp.getTime();
            LOG.info(getLeaseHolderId() + " diff from db: " + diff + ", db time: " + timestamp);
            if (diff > maxAllowableDiffFromDBTime || diff < -maxAllowableDiffFromDBTime) {
                // off by more than maxAllowableDiffFromDBTime so lets adjust
                result = diff;
            }
        }
        return result;
    }

    public void doStop(ServiceStopper stopper) throws Exception {
        releaseLease();
        stopping = true;
    }

    private void releaseLease() {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = getConnection();
            statement = connection.prepareStatement(statements.getLeaseUpdateStatement());
            statement.setString(1, null);
            statement.setLong(2, 0l);
            statement.setString(3, getLeaseHolderId());
            if (statement.executeUpdate() == 1) {
                LOG.info(getLeaseHolderId() + ", released lease");
            }
        } catch (Exception e) {
            LOG.error(getLeaseHolderId() + " failed to release lease: " + e, e);
        } finally {
            close(statement);
            close(connection);
        }
    }

    @Override
    public boolean keepAlive() throws IOException {
        boolean result = false;
        final String sql = statements.getLeaseUpdateStatement();
        LOG.debug(getLeaseHolderId() + ", lease keepAlive Query is " + sql);

        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = getConnection();

            initTimeDiff(connection);
            statement = connection.prepareStatement(sql);
            setQueryTimeout(statement);

            final long now = System.currentTimeMillis() + diffFromCurrentTime;
            statement.setString(1, getLeaseHolderId());
            statement.setLong(2, now + lockAcquireSleepInterval);
            statement.setString(3, getLeaseHolderId());

            result = (statement.executeUpdate() == 1);
        } catch (Exception e) {
            LOG.warn(getLeaseHolderId() + ", failed to update lease: " + e, e);
            IOException ioe = IOExceptionSupport.create(e);
            persistenceAdapter.getBrokerService().handleIOException(ioe);
            throw ioe;
        } finally {
            close(statement);
            close(connection);
        }
        return result;
    }

    public long getLockAcquireSleepInterval() {
        return lockAcquireSleepInterval;
    }

    public void setLockAcquireSleepInterval(long lockAcquireSleepInterval) {
        this.lockAcquireSleepInterval = lockAcquireSleepInterval;
    }
    
    public int getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public String getLeaseHolderId() {
        if (leaseHolderId == null) {
            if (persistenceAdapter.getBrokerService() != null) {
                leaseHolderId = persistenceAdapter.getBrokerService().getBrokerName();
            }
        }
        return leaseHolderId;
    }

    public void setLeaseHolderId(String leaseHolderId) {
        this.leaseHolderId = leaseHolderId;
    }

    public int getMaxAllowableDiffFromDBTime() {
        return maxAllowableDiffFromDBTime;
    }

    public void setMaxAllowableDiffFromDBTime(int maxAllowableDiffFromDBTime) {
        this.maxAllowableDiffFromDBTime = maxAllowableDiffFromDBTime;
    }
}
