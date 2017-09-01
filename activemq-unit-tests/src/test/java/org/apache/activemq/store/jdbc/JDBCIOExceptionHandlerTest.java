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

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.management.*;
import javax.management.loading.ClassLoaderRepository;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import com.sun.jndi.rmi.registry.RegistryContext;
import com.sun.jndi.rmi.registry.RegistryContextFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ft.SyncCreateDataSource;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.bugs.embedded.ThreadExplorer;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.LeaseLockerIOExceptionHandler;
import org.apache.activemq.util.Wait;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test to see if the JDBCExceptionIOHandler will restart the transport connectors correctly after
 * the underlying DB has been stopped and restarted
 *
 * see AMQ-4575
 */
public class JDBCIOExceptionHandlerTest {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCIOExceptionHandlerTest.class);
    private static final String TRANSPORT_URL = "tcp://0.0.0.0:0";

    private ActiveMQConnectionFactory factory;
    private ReconnectingEmbeddedDataSource dataSource;
    private BrokerService broker;

    @After
    public void stopDB() {
        if (dataSource != null) {
            dataSource.stopDB();
        }
    }

    protected BrokerService createBroker(boolean withJMX) throws Exception {
        return createBroker("localhost", withJMX, true, true);
    }

    protected BrokerService createBroker(String name, boolean withJMX, boolean leaseLocker, boolean startStopConnectors) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(name);

        broker.setUseJmx(withJMX);

        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource embeddedDataSource = (EmbeddedDataSource) jdbc.getDataSource();
        // create a wrapper to EmbeddedDataSource to allow the connection be
        // reestablished to derby db
        dataSource = new ReconnectingEmbeddedDataSource(new SyncCreateDataSource(embeddedDataSource));
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

    @Test
    public void testStartWithDatabaseDown() throws Exception {
        final AtomicBoolean connectorStarted = new AtomicBoolean(false);
        final AtomicBoolean connectorStopped = new AtomicBoolean(false);

        DefaultTestAppender appender = new DefaultTestAppender() {

            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getMessage().toString().startsWith("JMX consoles can connect to")) {
                    connectorStarted.set(true);
                }

                if (event.getMessage().toString().equals("Stopping jmx connector")) {
                    connectorStopped.set(true);
                }
            }
        };

        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        Level previousLevel = rootLogger.getLevel();
        rootLogger.setLevel(Level.DEBUG);
        rootLogger.addAppender(appender);


        BrokerService broker = new BrokerService();
        broker.getManagementContext().setCreateConnector(true);
        broker.getManagementContext().setCreateMBeanServer(true);

        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource embeddedDataSource = (EmbeddedDataSource) jdbc.getDataSource();
        // create a wrapper to EmbeddedDataSource to allow the connection be
        // reestablished to derby db
        dataSource = new ReconnectingEmbeddedDataSource(new SyncCreateDataSource(embeddedDataSource));
        dataSource.stopDB();
        jdbc.setDataSource(dataSource);

        jdbc.setLockKeepAlivePeriod(1000l);
        LeaseDatabaseLocker leaseDatabaseLocker = new LeaseDatabaseLocker();
        leaseDatabaseLocker.setHandleStartException(true);
        leaseDatabaseLocker.setLockAcquireSleepInterval(2000l);
        jdbc.setLocker(leaseDatabaseLocker);

        broker.setPersistenceAdapter(jdbc);
        LeaseLockerIOExceptionHandler ioExceptionHandler = new LeaseLockerIOExceptionHandler();
        ioExceptionHandler.setResumeCheckSleepPeriod(1000l);
        ioExceptionHandler.setStopStartConnectors(true);
        broker.setIoExceptionHandler(ioExceptionHandler);
        try {
            broker.start();
            fail("Broker should have been stopped!");
        } catch (Exception e) {
            Thread.sleep(5000);
            assertTrue("Broker should have been stopped!", broker.isStopped());
            Thread[] threads = ThreadExplorer.listThreads();
            for (int i = 0; i < threads.length; i++) {
                if (threads[i].getName().startsWith("IOExceptionHandler")) {
                    fail("IOExceptionHanlder still active");
                }
            }

            if (connectorStarted.get() && !connectorStopped.get()) {
                fail("JMX Server Connector should have been stopped!");
            }

        } finally {
            dataSource = null;
            broker = null;
            rootLogger.removeAppender(appender);
            rootLogger.setLevel(previousLevel);
        }
    }

    /*
     * run test without JMX enabled
     */
    @Test
    public void testRecoverWithOutJMX() throws Exception {
        recoverFromDisconnectDB(false);
    }

    /*
     * run test with JMX enabled
     */
    @Test
    public void testRecoverWithJMX() throws Exception {
        recoverFromDisconnectDB(true);
    }

    @Test
    public void testSlaveStoppedLease() throws Exception {
        testSlaveStopped(true);
    }

    @Test
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
            broker.waitUntilStopped();
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
     */
    public class ReconnectingEmbeddedDataSource implements javax.sql.DataSource {

        private SyncCreateDataSource realDatasource;

        public ReconnectingEmbeddedDataSource(SyncCreateDataSource datasource) {
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
        public void restartDB() throws Exception {
            EmbeddedDataSource newDatasource =
                    (EmbeddedDataSource) DataSourceServiceSupport.createDataSource(broker.getDataDirectoryFile().getCanonicalPath());
            newDatasource.getConnection();
            LOG.info("*** DB restarted now...");
            Object existingDataSource = realDatasource;
            synchronized (existingDataSource) {
                this.realDatasource = new SyncCreateDataSource(newDatasource);
            }
        }

        public void stopDB() {
            LOG.info("***DB is being shutdown...");
            synchronized (realDatasource) {
                DataSourceServiceSupport.shutdownDefaultDataSource(realDatasource.getDelegate());
            }
        }

        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return null;
        }
    }
}
