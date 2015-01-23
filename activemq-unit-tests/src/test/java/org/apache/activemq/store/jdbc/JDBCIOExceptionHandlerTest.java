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

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.LeaseLockerIOExceptionHandler;
import org.apache.activemq.util.Wait;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to see if the JDBCExceptionIOHandler will restart the transport connectors correctly after
 * the underlying DB has been stopped and restarted
 *
 * see AMQ-4575
 */
public class JDBCIOExceptionHandlerTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCIOExceptionHandlerTest.class);
    private static final String TRANSPORT_URL = "tcp://0.0.0.0:0";

    private static final String DATABASE_NAME = "DERBY_OVERRIDE";
    private ActiveMQConnectionFactory factory;
    private ReconnectingEmbeddedDataSource dataSource;
    private BrokerService broker;

    protected BrokerService createBroker(boolean withJMX) throws Exception {
        return createBroker("localhost", withJMX, true, true);
    }

    protected BrokerService createBroker(String name, boolean withJMX, boolean leaseLocker, boolean startStopConnectors) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(name);

        broker.setUseJmx(withJMX);

        EmbeddedDataSource embeddedDataSource = new EmbeddedDataSource();
        embeddedDataSource.setDatabaseName(DATABASE_NAME);
        embeddedDataSource.setCreateDatabase("create");

        // create a wrapper to EmbeddedDataSource to allow the connection be
        // reestablished to derby db
        dataSource = new ReconnectingEmbeddedDataSource(embeddedDataSource);

        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        jdbc.setDataSource(dataSource);

        jdbc.setLockKeepAlivePeriod(1000l);
        if (leaseLocker) {
            LeaseDatabaseLocker leaseDatabaseLocker = new LeaseDatabaseLocker();
            leaseDatabaseLocker.setHandleStartException(true);
            leaseDatabaseLocker.setLockAcquireSleepInterval(2000l);
            jdbc.setLocker(leaseDatabaseLocker);
        }

        broker.setPersistenceAdapter(jdbc);
        LeaseLockerIOExceptionHandler ioExceptionHandler = new LeaseLockerIOExceptionHandler();
        ioExceptionHandler.setResumeCheckSleepPeriod(1000l);
        ioExceptionHandler.setStopStartConnectors(startStopConnectors);
        broker.setIoExceptionHandler(ioExceptionHandler);
        String connectionUri = broker.addConnector(TRANSPORT_URL).getPublishableConnectString();

        factory = new ActiveMQConnectionFactory(connectionUri);

        return broker;
    }

    /*
     * run test without JMX enabled
     */
    public void testRecoverWithOutJMX() throws Exception {
        recoverFromDisconnectDB(false);
    }

    /*
     * run test with JMX enabled
     */
    public void testRecoverWithJMX() throws Exception {
        recoverFromDisconnectDB(true);
    }

    public void testSlaveStoppedLease() throws Exception {
        testSlaveStopped(true);
    }

    public void testSlaveStoppedDefault() throws Exception {
        testSlaveStopped(false);
    }

    public void testSlaveStopped(final boolean lease) throws Exception {
        final BrokerService master = createBroker("master", true, lease, false);
        master.start();
        master.waitUntilStarted();

        final AtomicReference<BrokerService> slave = new AtomicReference<BrokerService>();

        Thread slaveThread = new Thread() {
            public void run() {
                try {
                    BrokerService broker = new BrokerService();
                    broker.setBrokerName("slave");

                    JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
                    jdbc.setDataSource(dataSource);

                    jdbc.setLockKeepAlivePeriod(1000l);

                    if (lease) {
                        LeaseDatabaseLocker leaseDatabaseLocker = new LeaseDatabaseLocker();
                        leaseDatabaseLocker.setHandleStartException(true);
                        leaseDatabaseLocker.setLockAcquireSleepInterval(2000l);
                        jdbc.setLocker(leaseDatabaseLocker);
                    }

                    broker.setPersistenceAdapter(jdbc);
                    LeaseLockerIOExceptionHandler ioExceptionHandler = new LeaseLockerIOExceptionHandler();
                    ioExceptionHandler.setResumeCheckSleepPeriod(1000l);
                    ioExceptionHandler.setStopStartConnectors(false);
                    broker.setIoExceptionHandler(ioExceptionHandler);
                    slave.set(broker);
                    broker.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        slaveThread.start();

        Thread.sleep(5000);

        dataSource.stopDB();

        assertTrue("Master hasn't been stopped", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return master.isStopped();
            }
        }));

        assertTrue("Slave hasn't been stopped", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return slave.get().isStopped();
            }
        }));

    }

    public void recoverFromDisconnectDB(boolean withJMX) throws Exception {
        try {
            broker = createBroker(withJMX);
            broker.start();
            broker.waitUntilStarted();

            // broker started - stop db underneath it
            dataSource.stopDB();

            // wait - allow the leaselocker to kick the JDBCIOExceptionHandler
            TimeUnit.SECONDS.sleep(3);

            // check connector has shutdown
            checkTransportConnectorStopped();

            // restart db underneath
            dataSource.restartDB();

            Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    LOG.debug("*** checking connector to start...");
                    try {
                        checkTransportConnectorStarted();
                        return true;
                    } catch (Throwable t) {
                        LOG.debug(t.toString());
                    }
                    return false;
                }
            });


        } finally {
            LOG.debug("*** broker is stopping...");
            broker.stop();
        }
    }

    private void checkTransportConnectorStopped() {
        // connection is expected to fail
        try {
            factory.createConnection();
            fail("Transport connector should be stopped");
        } catch (Exception ex) {
            // expected an exception
            LOG.debug(" checkTransportConnectorStopped() threw", ex);
        }
    }

    private void checkTransportConnectorStarted() {
        // connection is expected to succeed
        try {
            Connection conn = factory.createConnection();
            conn.close();
        } catch (Exception ex) {
            LOG.debug("checkTransportConnectorStarted() threw", ex);
            fail("Transport connector should have been started");
        }
    }

    /*
     * Wrapped the derby datasource object to get DB reconnect functionality as I not
     * manage to get that working directly on the EmbeddedDataSource
     *
     * NOTE: Not a thread Safe but for this unit test it should be fine
     */
    public class ReconnectingEmbeddedDataSource implements javax.sql.DataSource {

        private EmbeddedDataSource realDatasource;

        public ReconnectingEmbeddedDataSource(EmbeddedDataSource datasource) {
            this.realDatasource = datasource;
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return this.realDatasource.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            this.realDatasource.setLogWriter(out);

        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            this.realDatasource.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return this.realDatasource.getLoginTimeout();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return this.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return this.isWrapperFor(iface);
        }

        @Override
        public java.sql.Connection getConnection() throws SQLException {
            return this.realDatasource.getConnection();
        }

        @Override
        public java.sql.Connection getConnection(String username, String password) throws SQLException {
            return this.getConnection(username, password);
        }

        /**
         *
         * To simulate a db reconnect I just create a new EmbeddedDataSource .
         *
         * @throws SQLException
         */
        public void restartDB() throws SQLException {
            EmbeddedDataSource newDatasource = new EmbeddedDataSource();
            newDatasource.setDatabaseName(DATABASE_NAME);
            newDatasource.getConnection();
            LOG.info("*** DB restarted now...");
            this.realDatasource = newDatasource;
        }

        public void stopDB() {
            try {
                realDatasource.setShutdownDatabase("shutdown");
                LOG.info("***DB is being shutdown...");
                dataSource.getConnection();
                fail("should have thrown a db closed exception");
            } catch (Exception ex) {
                ex.printStackTrace(System.out);
            }
        }

        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return null;
        }
    }
}
