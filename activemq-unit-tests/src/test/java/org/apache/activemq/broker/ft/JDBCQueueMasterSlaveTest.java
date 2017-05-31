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
import java.io.PrintWriter;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.jdbc.DataSourceServiceSupport;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.util.DefaultIOExceptionHandler;
import org.apache.activemq.util.IOHelper;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class JDBCQueueMasterSlaveTest extends QueueMasterSlaveTestSupport {
    protected SyncCreateDataSource sharedDs;
    protected String MASTER_URL = "tcp://localhost:62001";
    protected String SLAVE_URL  = "tcp://localhost:62002";
    protected String findStatement;

    protected void setUp() throws Exception {
        // startup db
        sharedDs = new SyncCreateDataSource((EmbeddedDataSource) DataSourceServiceSupport.createDataSource(IOHelper.getDefaultDataDirectory()));
        super.setUp();
        findStatement = ((JDBCPersistenceAdapter) master.getPersistenceAdapter()).getStatements().getFindMessageStatement();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        DataSourceServiceSupport.shutdownDefaultDataSource(sharedDs.delegate);
    }

    protected void createMaster() throws Exception {
        master = new BrokerService();
        master.setBrokerName("master");
        master.addConnector(MASTER_URL);
        master.setUseJmx(false);
        master.setPersistent(true);
        master.setDeleteAllMessagesOnStartup(true);
        JDBCPersistenceAdapter persistenceAdapter = new JDBCPersistenceAdapter();
        persistenceAdapter.setDataSource(getExistingDataSource());
        configureJdbcPersistenceAdapter(persistenceAdapter);
        master.setPersistenceAdapter(persistenceAdapter);
        configureBroker(master);
        master.start();
    }

    protected void configureBroker(BrokerService brokerService) {
        DefaultIOExceptionHandler stopBrokerOnStoreException = new DefaultIOExceptionHandler();
        // we want any store io exception to stop the broker
        stopBrokerOnStoreException.setIgnoreSQLExceptions(false);
        brokerService.setIoExceptionHandler(stopBrokerOnStoreException);
    }

    protected void createSlave() throws Exception {
        // use a separate thread as the slave will block waiting for
        // the exclusive db lock
        Thread t = new Thread() {
            public void run() {
                try {
                    BrokerService broker = new BrokerService();
                    broker.setBrokerName("slave");
                    TransportConnector connector = new TransportConnector();
                    connector.setUri(new URI(SLAVE_URL));
                    broker.addConnector(connector);
                    // no need for broker.setMasterConnectorURI(masterConnectorURI)
                    // as the db lock provides the slave/master initialisation
                    broker.setUseJmx(false);
                    broker.setPersistent(true);
                    JDBCPersistenceAdapter persistenceAdapter = new JDBCPersistenceAdapter();
                    persistenceAdapter.setDataSource(getExistingDataSource());
                    persistenceAdapter.setCreateTablesOnStartup(false);
                    broker.setPersistenceAdapter(persistenceAdapter);
                    configureJdbcPersistenceAdapter(persistenceAdapter);
                    configureBroker(broker);
                    broker.start();
                    slave.set(broker);
                    slaveStarted.countDown();
                } catch (IllegalStateException expectedOnShutdown) {
                } catch (Exception e) {
                    fail("failed to start slave broker, reason:" + e);
                }
            }
        };
        t.start();
    }

    protected void configureJdbcPersistenceAdapter(JDBCPersistenceAdapter persistenceAdapter) throws IOException {
        persistenceAdapter.setLockKeepAlivePeriod(500);
        persistenceAdapter.getLocker().setLockAcquireSleepInterval(500);
    }

    protected DataSource getExistingDataSource() throws Exception {
        return sharedDs;
    }

}
