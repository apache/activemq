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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.apache.activemq.broker.AbstractLocker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter;
import org.apache.activemq.util.Wait;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LeaseDatabaseLockerTest {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseDatabaseLockerTest.class);

    JDBCPersistenceAdapter jdbc;
    BrokerService brokerService;
    DataSource dataSource;

    @Before
    public void setUpStore() throws Exception {
        jdbc = new JDBCPersistenceAdapter();
        dataSource = jdbc.getDataSource();
        brokerService = new BrokerService();
        jdbc.setBrokerService(brokerService);
        jdbc.getAdapter().doCreateTables(jdbc.getTransactionContext());
    }

    @After
    public void stopDerby() {
        DataSourceServiceSupport.shutdownDefaultDataSource(dataSource);
    }

    @Test
    public void testLockInterleave() throws Exception {

        LeaseDatabaseLocker lockerA = new LeaseDatabaseLocker();
        lockerA.setLeaseHolderId("First");
        jdbc.setLocker(lockerA);

        final LeaseDatabaseLocker lockerB = new LeaseDatabaseLocker();
        lockerB.setLeaseHolderId("Second");
        jdbc.setLocker(lockerB);
        final AtomicBoolean blocked = new AtomicBoolean(true);

        final Connection connection = dataSource.getConnection();
        printLockTable(connection);
        lockerA.start();
        printLockTable(connection);

        assertTrue("First has lock", lockerA.keepAlive());

        final CountDownLatch lockerBStarting = new CountDownLatch(1);
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    lockerBStarting.countDown();
                    lockerB.start();
                    blocked.set(false);
                    printLockTable(connection);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return lockerBStarting.await(1, TimeUnit.SECONDS);
            }
        });

        TimeUnit.MILLISECONDS.sleep(lockerB.getLockAcquireSleepInterval() / 2);
        assertTrue("B is blocked", blocked.get());

        assertTrue("A is good", lockerA.keepAlive());
        printLockTable(connection);

        lockerA.stop();
        printLockTable(connection);

        TimeUnit.MILLISECONDS.sleep(2 * lockerB.getLockAcquireSleepInterval());
        assertFalse("lockerB has the lock", blocked.get());
        lockerB.stop();
        printLockTable(connection);
    }

    @Test
    public void testLockAcquireRace() throws Exception {

        // build a fake lock
        final String fakeId = "Anon";
        final Connection connection = dataSource.getConnection();
        printLockTable(connection);
        PreparedStatement statement = connection.prepareStatement(jdbc.getStatements().getLeaseObtainStatement());

        final long now = System.currentTimeMillis();
        statement.setString(1,fakeId);
        statement.setLong(2, now + 30000);
        statement.setLong(3, now);

        assertEquals("we got the lease", 1, statement.executeUpdate());
        printLockTable(connection);

        final LeaseDatabaseLocker lockerA = new LeaseDatabaseLocker();
        lockerA.setLeaseHolderId("A");
        jdbc.setLocker(lockerA);

        final LeaseDatabaseLocker lockerB = new LeaseDatabaseLocker();
        lockerB.setLeaseHolderId("B");
        jdbc.setLocker(lockerB);

        final Set<LeaseDatabaseLocker> lockedSet = new HashSet<LeaseDatabaseLocker>();
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    lockerA.start();
                    lockedSet.add(lockerA);
                    printLockTable(connection);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    lockerB.start();
                    lockedSet.add(lockerB);
                    printLockTable(connection);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // sleep for a bit till both are alive
        TimeUnit.SECONDS.sleep(2);
        assertTrue("no start", lockedSet.isEmpty());
        assertFalse("A is blocked", lockerA.keepAlive());
        assertFalse("B is blocked", lockerB.keepAlive());

        LOG.info("releasing phony lock " + fakeId);

        statement = connection.prepareStatement(jdbc.getStatements().getLeaseUpdateStatement());
        statement.setString(1, null);
        statement.setLong(2, 0l);
        statement.setString(3, fakeId);
        assertEquals("we released " + fakeId, 1, statement.executeUpdate());
        LOG.info("released " + fakeId);
        printLockTable(connection);

        TimeUnit.MILLISECONDS.sleep(AbstractLocker.DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL);
        assertEquals("one locker started", 1, lockedSet.size());

        assertTrue("one isAlive", lockerA.keepAlive() || lockerB.keepAlive());

        LeaseDatabaseLocker winner = lockedSet.iterator().next();
        winner.stop();
        lockedSet.remove(winner);

        TimeUnit.MILLISECONDS.sleep(AbstractLocker.DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL);
        assertEquals("one locker started", 1, lockedSet.size());

        lockedSet.iterator().next().stop();
        printLockTable(connection);
    }

    @Test
    public void testDiffOffsetAhead() throws Exception {
        LeaseDatabaseLocker underTest = new LeaseDatabaseLocker();
        assertTrue("when ahead of db adjustment is negative", callDiffOffset(underTest, System.currentTimeMillis() - 60000) < 0);
    }

    @Test
    public void testDiffOffsetBehind() throws Exception {
        LeaseDatabaseLocker underTest = new LeaseDatabaseLocker();
        assertTrue("when behind db adjustment is positive", callDiffOffset(underTest, System.currentTimeMillis() + 60000) > 0);
    }

    @Test
    public void testDiffIngoredIfLessthanMaxAllowableDiffFromDBTime() throws Exception {
        LeaseDatabaseLocker underTest = new LeaseDatabaseLocker();
        underTest.setMaxAllowableDiffFromDBTime(60000);
        assertEquals("no adjust when under limit", 0, callDiffOffset(underTest,System.currentTimeMillis() - 40000 ));
    }

    public long callDiffOffset(LeaseDatabaseLocker underTest, final long dbTime) throws Exception {

        Mockery context = new Mockery() {{
            setImposteriser(ClassImposteriser.INSTANCE);
        }};
        final Statements statements = context.mock(Statements.class);
        final JDBCPersistenceAdapter jdbcPersistenceAdapter = context.mock(JDBCPersistenceAdapter.class);
        final Connection connection = context.mock(Connection.class);
        final PreparedStatement preparedStatement = context.mock(PreparedStatement.class);
        final ResultSet resultSet = context.mock(ResultSet.class);
        final Timestamp timestamp = context.mock(Timestamp.class);

        context.checking(new Expectations() {{
            allowing(jdbcPersistenceAdapter).getStatements();
            will(returnValue(statements));
            allowing(jdbcPersistenceAdapter);
            allowing(statements);
            allowing(connection).prepareStatement(with(any(String.class)));
            will(returnValue(preparedStatement));
            allowing(connection);
            allowing(preparedStatement).executeQuery();
            will(returnValue(resultSet));
            allowing(resultSet).next();
            will(returnValue(true));
            allowing(resultSet).getTimestamp(1);
            will(returnValue(timestamp));
            allowing(timestamp).getTime();
            will(returnValue(dbTime));
            allowing(resultSet).close();
            allowing(preparedStatement).close();
        }});

        underTest.configure(jdbcPersistenceAdapter);
        underTest.setLockable(jdbcPersistenceAdapter);
        return underTest.determineTimeDifference(connection);
    }

    private void printLockTable(Connection connection) throws Exception {
        DefaultJDBCAdapter.printQuery(connection, "SELECT * from ACTIVEMQ_LOCK", System.err);
    }
}
