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
package org.apache.activemq.broker.ft;

import java.io.IOException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.jdbc.LeaseDatabaseLocker;
import org.apache.activemq.util.LeaseLockerIOExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbRestartJDBCQueueMasterSlaveLeaseTest extends DbRestartJDBCQueueMasterSlaveTest {
    private static final transient Logger LOG = LoggerFactory.getLogger(DbRestartJDBCQueueMasterSlaveLeaseTest.class);

    @Override
    protected void configureJdbcPersistenceAdapter(JDBCPersistenceAdapter persistenceAdapter) throws IOException {
        super.configureJdbcPersistenceAdapter(persistenceAdapter);
        persistenceAdapter.setLocker(new LeaseDatabaseLocker());
        persistenceAdapter.getLocker().setLockAcquireSleepInterval(getLockAcquireSleepInterval());
        persistenceAdapter.setLockKeepAlivePeriod(getLockKeepAlivePeriod());
    }

    @Override
    protected void configureBroker(BrokerService brokerService) {
        //let the brokers die on exception and master should have lease on restart
        // which will delay slave start till it expires
        LeaseLockerIOExceptionHandler ioExceptionHandler = new LeaseLockerIOExceptionHandler();
        ioExceptionHandler.setIgnoreSQLExceptions(false);
        ioExceptionHandler.setStopStartConnectors(false);
        ioExceptionHandler.setResumeCheckSleepPeriod(500l);
        brokerService.setIoExceptionHandler(ioExceptionHandler);
    }

    private long getLockKeepAlivePeriod() {
        return 1000;
    }

    private long getLockAcquireSleepInterval() {
        return 8000;
    }
}
