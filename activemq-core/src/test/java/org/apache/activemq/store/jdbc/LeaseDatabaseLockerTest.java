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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activemq.broker.BrokerService;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.Before;
import org.junit.Test;


import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;


public class LeaseDatabaseLockerTest {

    JDBCPersistenceAdapter jdbc;
    BrokerService brokerService;
    EmbeddedDataSource dataSource;

    @Before
    public void setUpStore() throws Exception {
        dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        jdbc = new JDBCPersistenceAdapter();
        jdbc.setDataSource(dataSource);
        brokerService = new BrokerService();
        jdbc.setBrokerService(brokerService);
        jdbc.getAdapter().doCreateTables(jdbc.getTransactionContext());
    }

    @Test
    public void testLockInterleave() throws Exception {

        LeaseDatabaseLocker lockerA = new LeaseDatabaseLocker();
        brokerService.setBrokerName("First");
        lockerA.setPersistenceAdapter(jdbc);

        final LeaseDatabaseLocker lockerB = new LeaseDatabaseLocker();
        brokerService.setBrokerName("Second");
        lockerB.setPersistenceAdapter(jdbc);
        final AtomicBoolean blocked = new AtomicBoolean(true);

        final Connection connection = dataSource.getConnection();
        printLockTable(connection);
        lockerA.start();
        printLockTable(connection);

        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    lockerB.start();
                    blocked.set(false);
                    printLockTable(connection);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

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

    private void printLockTable(Connection connection) throws IOException {
        //((DefaultJDBCAdapter)jdbc.getAdapter()).printQuery(connection, "SELECT * from ACTIVEMQ_LOCK", System.err);
    }
}
