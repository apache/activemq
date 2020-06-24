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
public class LeaseDatabaseLocker extends AbstractJDBCLocker {
    private static final Logger LOG = LoggerFactory.getLogger(LeaseDatabaseLocker.class);

    protected int maxAllowableDiffFromDBTime = 0;
    protected long diffFromCurrentTime = Long.MAX_VALUE;
    protected String leaseHolderId;
    protected boolean handleStartException;

    public void doStart() throws Exception {

//IC see: https://issues.apache.org/jira/browse/AMQ-4365
        if (lockAcquireSleepInterval < lockable.getLockKeepAlivePeriod()) {
            LOG.warn("LockableService keep alive period: " + lockable.getLockKeepAlivePeriod()
                    + ", which renews the lease, is greater than lockAcquireSleepInterval: " + lockAcquireSleepInterval
                    + ", the lease duration. These values will allow the lease to expire.");
        }

        LOG.info(getLeaseHolderId() + " attempting to acquire exclusive lease to become the master");
//IC see: https://issues.apache.org/jira/browse/AMQ-4841
        String sql = getStatements().getLeaseObtainStatement();
        LOG.debug(getLeaseHolderId() + " locking Query is "+sql);

//IC see: https://issues.apache.org/jira/browse/AMQ-4643
        long now = 0l;
        while (!isStopping()) {
            Connection connection = null;
            PreparedStatement statement = null;
            try {
                connection = getConnection();
                initTimeDiff(connection);

                statement = connection.prepareStatement(sql);
                setQueryTimeout(statement);

//IC see: https://issues.apache.org/jira/browse/AMQ-4643
                now = System.currentTimeMillis() + diffFromCurrentTime;
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
                LOG.warn(getLeaseHolderId() + " lease acquire failure: "+ e, e);
//IC see: https://issues.apache.org/jira/browse/AMQ-5162
                if (isStopping()) {
                    throw new Exception(
                            "Cannot start broker as being asked to shut down. "
                                    + "Interrupted attempt to acquire lock: "
                                    + e, e);
                }
                if (handleStartException) {
//IC see: https://issues.apache.org/jira/browse/AMQ-6799
                    throw e;
                }
            } finally {
                close(statement);
                close(connection);
            }

            LOG.debug(getLeaseHolderId() + " failed to acquire lease.  Sleeping for " + lockAcquireSleepInterval + " milli(s) before trying again...");
            TimeUnit.MILLISECONDS.sleep(lockAcquireSleepInterval);
        }
//IC see: https://issues.apache.org/jira/browse/AMQ-4365
        if (isStopping()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-4122
            throw new RuntimeException(getLeaseHolderId() + " failing lease acquire due to stop");
        }

        LOG.info(getLeaseHolderId() + ", becoming master with lease expiry " + new Date(now + lockAcquireSleepInterval) + " on dataSource: " + dataSource);
    }

    private void reportLeasOwnerShipAndDuration(Connection connection) throws SQLException {
        PreparedStatement statement = null;
        try {
//IC see: https://issues.apache.org/jira/browse/AMQ-4841
            statement = connection.prepareStatement(getStatements().getLeaseOwnerStatement());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                LOG.debug(getLeaseHolderId() + " Lease held by " + resultSet.getString(1) + " till " + new Date(resultSet.getLong(2)));
            }
        } finally {
            close(statement);
        }
    }

    protected long initTimeDiff(Connection connection) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/AMQ-3654
        if (Long.MAX_VALUE == diffFromCurrentTime) {
            if (maxAllowableDiffFromDBTime > 0) {
                diffFromCurrentTime = determineTimeDifference(connection);
            } else {
                diffFromCurrentTime = 0l;
            }
        }
        return diffFromCurrentTime;
    }

    protected long determineTimeDifference(Connection connection) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/AMQ-7403
        try (PreparedStatement statement = connection.prepareStatement(getStatements().getCurrentDateTime());
             ResultSet resultSet = statement.executeQuery()) {
            long result = 0l;
            if (resultSet.next()) {
                Timestamp timestamp = resultSet.getTimestamp(1);
                long diff = System.currentTimeMillis() - timestamp.getTime();
//IC see: https://issues.apache.org/jira/browse/AMQ-4645
                if (Math.abs(diff) > maxAllowableDiffFromDBTime) {
                    // off by more than maxAllowableDiffFromDBTime so lets adjust
                    result = (-diff);
                }
                LOG.info(getLeaseHolderId() + " diff adjust from db: " + result + ", db time: " + timestamp);
            }
            return result;
        }
    }

    public void doStop(ServiceStopper stopper) throws Exception {
//IC see: https://issues.apache.org/jira/browse/AMQ-4365
        if (lockable.getBrokerService() != null && lockable.getBrokerService().isRestartRequested()) {
            // keep our lease for restart
//IC see: https://issues.apache.org/jira/browse/AMQ-4643
            return;
        }
        releaseLease();
    }

    private void releaseLease() {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = getConnection();
//IC see: https://issues.apache.org/jira/browse/AMQ-4841
            statement = connection.prepareStatement(getStatements().getLeaseUpdateStatement());
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4841
        final String sql = getStatements().getLeaseUpdateStatement();
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

//IC see: https://issues.apache.org/jira/browse/AMQ-4643
            if (!result) {
                reportLeasOwnerShipAndDuration(connection);
            }
        } catch (Exception e) {
            LOG.warn(getLeaseHolderId() + ", failed to update lease: " + e, e);
            IOException ioe = IOExceptionSupport.create(e);
//IC see: https://issues.apache.org/jira/browse/AMQ-4365
            lockable.getBrokerService().handleIOException(ioe);
            throw ioe;
        } finally {
            close(statement);
            close(connection);
        }
        return result;
    }

    public String getLeaseHolderId() {
        if (leaseHolderId == null) {
//IC see: https://issues.apache.org/jira/browse/AMQ-4365
            if (lockable.getBrokerService() != null) {
                leaseHolderId = lockable.getBrokerService().getBrokerName();
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

    public boolean isHandleStartException() {
//IC see: https://issues.apache.org/jira/browse/AMQ-5162
        return handleStartException;
    }

    public void setHandleStartException(boolean handleStartException) {
        this.handleStartException = handleStartException;
    }

    @Override
    public String toString() {
//IC see: https://issues.apache.org/jira/browse/AMQ-4643
        return "LeaseDatabaseLocker owner:" + leaseHolderId + ",duration:" + lockAcquireSleepInterval + ",renew:" + lockAcquireSleepInterval;
    }
}
