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

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.management.ObjectName;
import javax.sql.DataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLRecoverableException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.DiscardingDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.DefaultIOExceptionHandler;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.util.TestUtils.createXid;

// https://issues.apache.org/activemq/browse/AMQ-2880
public class JDBCXACommitExceptionTest extends JDBCCommitExceptionTest {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCXACommitExceptionTest.class);

    protected ActiveMQXAConnectionFactory factory;

    boolean onePhase = true;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        onePhase = true;
        factory = new ActiveMQXAConnectionFactory(
            connectionUri + "?jms.prefetchPolicy.all=0&jms.redeliveryPolicy.maximumRedeliveries="+messagesExpected);
    }

    public void testTwoPhaseSqlException() throws Exception {
        onePhase = false;
        doTestSqlException();
    }

    @Override
    protected int receiveMessages(int messagesExpected) throws Exception {
        XAConnection connection = factory.createXAConnection();
        connection.start();
        XASession session = connection.createXASession();

        jdbc.setShouldBreak(true);

        // first try and receive these messages, they'll continually fail
        receiveMessages(messagesExpected, session, onePhase);

        jdbc.setShouldBreak(false);

        // now that the store is sane, try and get all the messages sent
        return receiveMessages(messagesExpected, session, onePhase);
    }

    protected int receiveMessages(int messagesExpected, XASession session, boolean onePhase) throws Exception {
        int messagesReceived = 0;

        for (int i=0; i<messagesExpected; i++) {
            Destination destination = session.createQueue("TEST");
            MessageConsumer consumer = session.createConsumer(destination);

            XAResource resource = session.getXAResource();
            resource.recover(XAResource.TMSTARTRSCAN);
            resource.recover(XAResource.TMNOFLAGS);

            Xid tid = createXid();

            Message message = null;
            try {
                LOG.debug("Receiving message " + (messagesReceived+1) + " of " + messagesExpected);
                resource.start(tid, XAResource.TMNOFLAGS);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                resource.end(tid, XAResource.TMSUCCESS);
                if (message != null) {
                    if (onePhase) {
                        resource.commit(tid, true);
                    } else {
                        resource.prepare(tid);
                        resource.commit(tid, false);
                    }
                    messagesReceived++;
                }
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);

                try {
                    LOG.debug("Rolling back transaction (just in case, no need to do this as it is implicit in a 1pc commit failure) " + tid);
                    resource.rollback(tid);
                }
                catch (XAException ex) {
                    try {
                        LOG.debug("Caught exception during rollback: " + ex + " forgetting transaction " + tid);
                        resource.forget(tid);
                    }
                    catch (XAException ex1) {
                        LOG.debug("rollback/forget failed: " + ex1.errorCode);
                    }
                }
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        return messagesReceived;
    }


    public void testCommitSendErrorRecovery() throws Exception {

        XAConnection connection = factory.createXAConnection();
        connection.start();
        XASession session = connection.createXASession();

        Destination destination = session.createQueue("TEST");
        MessageProducer producer = session.createProducer(destination);

        XAResource resource = session.getXAResource();

        Xid tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);
        ActiveMQMessage message = (ActiveMQMessage) session.createMessage();
        message.setTransactionId(new XATransactionId(tid));
        producer.send(message);

        resource.end(tid, XAResource.TMSUCCESS);
        resource.prepare(tid);

        jdbc.setShouldBreak(true);
        try {
            resource.commit(tid, true);
        } catch (Exception expected) {
            expected.printStackTrace();
        }

        // recover
        Xid[] recovered = resource.recover(XAResource.TMSTARTRSCAN);
        resource.recover(XAResource.TMNOFLAGS);

        jdbc.setShouldBreak(false);
        resource.commit(recovered[0], false);

        assertEquals("one enque", 1, ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());
    }


    final AtomicInteger getAutoCommitCount = new AtomicInteger();
    private ArrayList<Integer> getAutoCommitErrors = new ArrayList<Integer>();
    private ArrayList<Integer> executeUpdateErrorOps = new ArrayList<Integer>();
    final AtomicInteger executeUpdateErrorOpsCount = new AtomicInteger();
    private ArrayList<Integer> executeBatchErrorOps = new ArrayList<Integer>();
    final AtomicInteger executeBatchErrorOpsCount = new AtomicInteger();

    public void testXAEnqueueErrors() throws Exception {
        getAutoCommitCount.set(0);
        getAutoCommitErrors.clear();
        executeUpdateErrorOpsCount.set(0);
        executeUpdateErrorOps.clear();

        broker.stop();
        broker = new BrokerService();
        broker.setAdvisorySupport(false);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(0);
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);

        //broker.setDeleteAllMessagesOnStartup(true);

        JDBCPersistenceAdapter jdbcPersistenceAdapter = new JDBCPersistenceAdapter();
        DataSource realDataSource = jdbc.getDataSource();
        jdbcPersistenceAdapter.setDataSource(new TestDataSource(realDataSource));
        jdbcPersistenceAdapter.setUseLock(false);
        broker.setPersistenceAdapter(jdbcPersistenceAdapter);
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();
        broker.start();


        // inject error
        executeUpdateErrorOps.add(5);
        executeUpdateErrorOps.add(9);
        executeUpdateErrorOps.add(12);

        getAutoCommitErrors.add(61);
        getAutoCommitErrors.add(62);


        factory = new ActiveMQXAConnectionFactory(connectionUri);

        XAConnection c = factory.createXAConnection();
        c.start();
        XASession s = c.createXASession();
        final XAResource recoveryResource = s.getXAResource();

        for (int i = 0; i < 10; i++) {
            XAConnection connection = factory.createXAConnection();
            connection.start();
            XASession session = connection.createXASession();

            Destination destination = session.createQueue("TEST");
            MessageProducer producer = session.createProducer(destination);

            XAResource resource = session.getXAResource();

            Xid tid = createXid();
            resource.start(tid, XAResource.TMNOFLAGS);
            ActiveMQMessage message = (ActiveMQMessage) session.createMessage();
            message.setTransactionId(new XATransactionId(tid));
            producer.send(message);

            resource.end(tid, XAResource.TMSUCCESS);
            resource.prepare(tid);

            try {
                resource.commit(tid, false);
            } catch (Exception expected) {
                expected.printStackTrace();

                dumpMessages();

                boolean done = false;
                while (!done) {
                    // recover
                    Xid[] recovered = recoveryResource.recover(XAResource.TMSTARTRSCAN);
                    recoveryResource.recover(XAResource.TMNOFLAGS);

                    try {
                        recoveryResource.commit(recovered[0], false);
                        done = true;
                    } catch (XAException ok) {
                        ok.printStackTrace();
                    }
                }
            }
        }

        dumpMessages();

        assertEquals("en-queue", 10, ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());
        assertEquals("en-queue", 10, ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().getMessages().getCount());


        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
           .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        assertEquals("qs", 10, proxy.getQueueSize());
        assertEquals("enq", 10, proxy.getEnqueueCount());
        assertEquals("curs", 10, proxy.cursorSize());
    }

    public void testNonTxEnqueueErrors() throws Exception {
        getAutoCommitCount.set(0);
        getAutoCommitErrors.clear();
        executeUpdateErrorOpsCount.set(0);
        executeUpdateErrorOps.clear();
        executeBatchErrorOps.clear();
        executeBatchErrorOpsCount.set(0);

        broker.stop();
        broker = new BrokerService();
        broker.setAdvisorySupport(false);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(0);
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);


        JDBCPersistenceAdapter jdbcPersistenceAdapter = new JDBCPersistenceAdapter();
        DataSource realDataSource = jdbc.getDataSource();
        jdbcPersistenceAdapter.setDataSource(new TestDataSource(realDataSource));
        jdbcPersistenceAdapter.setUseLock(false);
        jdbcPersistenceAdapter.setCleanupPeriod(0);
        broker.setPersistenceAdapter(jdbcPersistenceAdapter);
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();
        broker.start();


        executeBatchErrorOps.add(2);
        executeBatchErrorOps.add(3);
        getAutoCommitCount.set(0);
        getAutoCommitErrors.add(10);


        ActiveMQConnectionFactory nonTxFactory = new ActiveMQConnectionFactory(connectionUri);

        for (int i = 0; i < 10; i++) {
            javax.jms.Connection connection = nonTxFactory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination destination = session.createQueue("TEST");
            MessageProducer producer = session.createProducer(destination);
            ActiveMQMessage message = (ActiveMQMessage) session.createMessage();

            try {
                producer.send(message);
            } catch (Exception expected) {
                expected.printStackTrace();

                dumpMessages();

                boolean done = false;
                while (!done) {
                    try {
                        producer.send(message);
                        done = true;
                    } catch (Exception ok) {
                        ok.printStackTrace();
                    }
                }
            }
        }

        assertEquals("messages in db", 10, dumpMessages());


        assertEquals("en-queue", 10, ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());
        assertEquals("en-queue", 10, ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().getMessages().getCount());


        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
           .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        assertEquals("qs", 10, proxy.getQueueSize());
        assertEquals("enq", 10, proxy.getEnqueueCount());
        assertEquals("curs", 10, proxy.cursorSize());
    }

    public void testNonTxEnqueueOverNetworkErrorsRestart() throws Exception {
        getAutoCommitCount.set(0);
        getAutoCommitErrors.clear();
        executeUpdateErrorOpsCount.set(0);
        executeUpdateErrorOps.clear();
        executeBatchErrorOps.clear();
        executeBatchErrorOpsCount.set(0);

        broker.stop();

        final AtomicBoolean done = new AtomicBoolean(false);
        Thread thread = new Thread() {
            @Override
            public void run() {

                while (!done.get()) {
                    try {

                        broker = new BrokerService();
                        broker.setAdvisorySupport(false);
                        PolicyMap policyMap = new PolicyMap();
                        PolicyEntry policyEntry = new PolicyEntry();
                        policyEntry.setUseCache(false);
                        policyEntry.setExpireMessagesPeriod(0);
                        policyEntry.setDeadLetterStrategy(new DiscardingDeadLetterStrategy());
                        policyMap.setDefaultEntry(policyEntry);
                        broker.setDestinationPolicy(policyMap);

                        JDBCPersistenceAdapter jdbcPersistenceAdapter = new JDBCPersistenceAdapter();
                        DataSource realDataSource = jdbc.getDataSource();
                        jdbcPersistenceAdapter.setDataSource(new TestDataSource(realDataSource));
                        jdbcPersistenceAdapter.setUseLock(false);
                        jdbcPersistenceAdapter.setCleanupPeriod(0);
                        broker.setPersistenceAdapter(jdbcPersistenceAdapter);
                        TransportConnector transportConnector = broker.addConnector("tcp://localhost:61616");
                        //transportConnector.setAuditNetworkProducers(true);
                        connectionUri = transportConnector.getPublishableConnectString();
                        DefaultIOExceptionHandler stopOnIOEx = new DefaultIOExceptionHandler();
                        stopOnIOEx.setIgnoreSQLExceptions(false);
                        stopOnIOEx.setStopStartConnectors(false);
                        broker.setIoExceptionHandler(stopOnIOEx);
                        broker.start();

                        broker.waitUntilStopped();

                    } catch (Exception oops) {
                        oops.printStackTrace();
                        done.set(true);
                    }
                }
            }
        };
        thread.start();

        //executeBatchErrorOps.add(5);
        //executeBatchErrorOps.add(3);
        getAutoCommitCount.set(0);
        getAutoCommitErrors.add(39);


        // network broker to push messages
        final BrokerService other = new BrokerService();
        other.setBrokerName("other");
        other.setAdvisorySupport(false);
        other.setUseJmx(false);
        other.setPersistent(false);
        NetworkConnector netwokConnector = other.addNetworkConnector("static://tcp://localhost:61616");
        netwokConnector.setStaticBridge(true);
        netwokConnector.setStaticallyIncludedDestinations(Arrays.asList(new ActiveMQDestination[]{new ActiveMQQueue("TEST")}));
        other.start();

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://other");
        ActiveMQConnection activeMQConnection = (ActiveMQConnection) connectionFactory.createConnection();
        activeMQConnection.setWatchTopicAdvisories(false);
        Session session = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        activeMQConnection.start();
        Destination destination = session.createQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        ActiveMQMessage message = (ActiveMQMessage) session.createMessage();

        for (int i = 0; i < 10; i++) {
            producer.send(message);
        }

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("MESSAGES DRAINED :" + ((RegionBroker)other.getRegionBroker()).getDestinationStatistics().getMessages().getCount());
                return 0 == ((RegionBroker)other.getRegionBroker()).getDestinationStatistics().getMessages().getCount();
            }
        });
        activeMQConnection.close();


        assertEquals("db", 10, dumpMessages());
        assertEquals("messages count", 10, ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().getMessages().getCount());


        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
           .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        assertEquals("qs", 10, proxy.getQueueSize());
        assertEquals("curs", 10, proxy.cursorSize());

        done.set(true);
        other.stop();
    }


    private class TestDataSource implements javax.sql.DataSource {

        private final javax.sql.DataSource realDataSource;

        public TestDataSource(javax.sql.DataSource dataSource) {
            realDataSource = dataSource;
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection autoCommitCheckConnection = new AutoCommitCheckConnection(realDataSource.getConnection());
            return autoCommitCheckConnection;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            Connection autoCommitCheckConnection = new AutoCommitCheckConnection(realDataSource.getConnection(username, password));

            return autoCommitCheckConnection;
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return realDataSource.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            realDataSource.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            realDataSource.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return realDataSource.getLoginTimeout();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return realDataSource.getParentLogger();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return realDataSource.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return realDataSource.isWrapperFor(iface);
        }
    }

    private class AutoCommitCheckConnection implements Connection {

        private final Connection realConnection;

        public AutoCommitCheckConnection(Connection connection) {
            this.realConnection = connection;
        }

        @Override
        public void commit() throws SQLException {
            realConnection.commit();
        }

        // Just plumbing for wrapper. Might have been better to do a Dynamic Proxy here.

        @Override
        public Statement createStatement() throws SQLException {
            return realConnection.createStatement();
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            //final AtomicInteger executeCount = new AtomicInteger();

            final PreparedStatement delegate = realConnection.prepareStatement(sql);
            return new PreparedStatement() {
                public ResultSet executeQuery() throws SQLException {
                    return delegate.executeQuery();
                }

                final
                public int executeUpdate() throws SQLException {
                    int ret = delegate.executeUpdate();
                    if (executeUpdateErrorOps.contains(executeUpdateErrorOpsCount.incrementAndGet())) {
                        throw new SQLRecoverableException("SOME executeUpdate ERROR[" + executeUpdateErrorOpsCount.get() +"]");
                    }
                    return ret;
                }

                public void setNull(int parameterIndex, int sqlType) throws SQLException {
                    delegate.setNull(parameterIndex, sqlType);
                }

                public void setBoolean(int parameterIndex, boolean x) throws SQLException {
                    delegate.setBoolean(parameterIndex, x);
                }

                public void setByte(int parameterIndex, byte x) throws SQLException {
                    delegate.setByte(parameterIndex, x);
                }

                public void setShort(int parameterIndex, short x) throws SQLException {
                    delegate.setShort(parameterIndex, x);
                }

                public void setInt(int parameterIndex, int x) throws SQLException {
                    delegate.setInt(parameterIndex, x);
                }

                public void setLong(int parameterIndex, long x) throws SQLException {
                    delegate.setLong(parameterIndex, x);
                }

                public void setFloat(int parameterIndex, float x) throws SQLException {
                    delegate.setFloat(parameterIndex, x);
                }

                public void setDouble(int parameterIndex, double x) throws SQLException {
                    delegate.setDouble(parameterIndex, x);
                }

                public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
                    delegate.setBigDecimal(parameterIndex, x);
                }

                public void setString(int parameterIndex, String x) throws SQLException {
                    delegate.setString(parameterIndex, x);
                }

                public void setBytes(int parameterIndex, byte[] x) throws SQLException {
                    delegate.setBytes(parameterIndex, x);
                }

                public void setDate(int parameterIndex, Date x) throws SQLException {
                    delegate.setDate(parameterIndex, x);
                }

                public void setTime(int parameterIndex, Time x) throws SQLException {
                    delegate.setTime(parameterIndex, x);
                }

                public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
                    delegate.setTimestamp(parameterIndex, x);
                }

                public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
                    delegate.setAsciiStream(parameterIndex, x, length);
                }

                @Deprecated
                public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
                    delegate.setUnicodeStream(parameterIndex, x, length);
                }

                public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
                    delegate.setBinaryStream(parameterIndex, x, length);
                }

                public void clearParameters() throws SQLException {
                    delegate.clearParameters();
                }

                public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
                    delegate.setObject(parameterIndex, x, targetSqlType);
                }

                public void setObject(int parameterIndex, Object x) throws SQLException {
                    delegate.setObject(parameterIndex, x);
                }

                public boolean execute() throws SQLException {
                    return delegate.execute();
                }

                public void addBatch() throws SQLException {
                    delegate.addBatch();
                }

                public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
                    delegate.setCharacterStream(parameterIndex, reader, length);
                }

                public void setRef(int parameterIndex, Ref x) throws SQLException {
                    delegate.setRef(parameterIndex, x);
                }

                public void setBlob(int parameterIndex, Blob x) throws SQLException {
                    delegate.setBlob(parameterIndex, x);
                }

                public void setClob(int parameterIndex, Clob x) throws SQLException {
                    delegate.setClob(parameterIndex, x);
                }

                public void setArray(int parameterIndex, Array x) throws SQLException {
                    delegate.setArray(parameterIndex, x);
                }

                public ResultSetMetaData getMetaData() throws SQLException {
                    return delegate.getMetaData();
                }

                public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
                    delegate.setDate(parameterIndex, x, cal);
                }

                public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
                    delegate.setTime(parameterIndex, x, cal);
                }

                public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
                    delegate.setTimestamp(parameterIndex, x, cal);
                }

                public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
                    delegate.setNull(parameterIndex, sqlType, typeName);
                }

                public void setURL(int parameterIndex, URL x) throws SQLException {
                    delegate.setURL(parameterIndex, x);
                }

                public ParameterMetaData getParameterMetaData() throws SQLException {
                    return delegate.getParameterMetaData();
                }

                public void setRowId(int parameterIndex, RowId x) throws SQLException {
                    delegate.setRowId(parameterIndex, x);
                }

                public void setNString(int parameterIndex, String value) throws SQLException {
                    delegate.setNString(parameterIndex, value);
                }

                public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
                    delegate.setNCharacterStream(parameterIndex, value, length);
                }

                public void setNClob(int parameterIndex, NClob value) throws SQLException {
                    delegate.setNClob(parameterIndex, value);
                }

                public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
                    delegate.setClob(parameterIndex, reader, length);
                }

                public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
                    delegate.setBlob(parameterIndex, inputStream, length);
                }

                public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
                    delegate.setNClob(parameterIndex, reader, length);
                }

                public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
                    delegate.setSQLXML(parameterIndex, xmlObject);
                }

                public void setObject(int parameterIndex,
                                      Object x,
                                      int targetSqlType,
                                      int scaleOrLength) throws SQLException {
                    delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
                }

                public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
                    delegate.setAsciiStream(parameterIndex, x, length);
                }

                public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
                    delegate.setBinaryStream(parameterIndex, x, length);
                }

                public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
                    delegate.setCharacterStream(parameterIndex, reader, length);
                }

                public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
                    delegate.setAsciiStream(parameterIndex, x);
                }

                public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
                    delegate.setBinaryStream(parameterIndex, x);
                }

                public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
                    delegate.setCharacterStream(parameterIndex, reader);
                }

                public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
                    delegate.setNCharacterStream(parameterIndex, value);
                }

                public void setClob(int parameterIndex, Reader reader) throws SQLException {
                    delegate.setClob(parameterIndex, reader);
                }

                public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
                    delegate.setBlob(parameterIndex, inputStream);
                }

                public void setNClob(int parameterIndex, Reader reader) throws SQLException {
                    delegate.setNClob(parameterIndex, reader);
                }
/*
                public void setObject(int parameterIndex,
                                      Object x,
                                      SQLType targetSqlType,
                                      int scaleOrLength) throws SQLException {
                    delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
                }

                public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
                    delegate.setObject(parameterIndex, x, targetSqlType);
                }

                public long executeLargeUpdate() throws SQLException {
                    return delegate.executeLargeUpdate();
                }
*/
                public ResultSet executeQuery(String sql) throws SQLException {
                    return delegate.executeQuery(sql);
                }

                public int executeUpdate(String sql) throws SQLException {
                    return delegate.executeUpdate(sql);
                }

                public void close() throws SQLException {
                    delegate.close();
                }

                public int getMaxFieldSize() throws SQLException {
                    return delegate.getMaxFieldSize();
                }

                public void setMaxFieldSize(int max) throws SQLException {
                    delegate.setMaxFieldSize(max);
                }

                public int getMaxRows() throws SQLException {
                    return delegate.getMaxRows();
                }

                public void setMaxRows(int max) throws SQLException {
                    delegate.setMaxRows(max);
                }

                public void setEscapeProcessing(boolean enable) throws SQLException {
                    delegate.setEscapeProcessing(enable);
                }

                public int getQueryTimeout() throws SQLException {
                    return delegate.getQueryTimeout();
                }

                public void setQueryTimeout(int seconds) throws SQLException {
                    delegate.setQueryTimeout(seconds);
                }

                public void cancel() throws SQLException {
                    delegate.cancel();
                }

                public SQLWarning getWarnings() throws SQLException {
                    return delegate.getWarnings();
                }

                public void clearWarnings() throws SQLException {
                    delegate.clearWarnings();
                }

                public void setCursorName(String name) throws SQLException {
                    delegate.setCursorName(name);
                }

                public boolean execute(String sql) throws SQLException {
                    return delegate.execute(sql);
                }

                public ResultSet getResultSet() throws SQLException {
                    return delegate.getResultSet();
                }

                public int getUpdateCount() throws SQLException {
                    return delegate.getUpdateCount();
                }

                public boolean getMoreResults() throws SQLException {
                    return delegate.getMoreResults();
                }

                public void setFetchDirection(int direction) throws SQLException {
                    delegate.setFetchDirection(direction);
                }

                public int getFetchDirection() throws SQLException {
                    return delegate.getFetchDirection();
                }

                public void setFetchSize(int rows) throws SQLException {
                    delegate.setFetchSize(rows);
                }

                public int getFetchSize() throws SQLException {
                    return delegate.getFetchSize();
                }

                public int getResultSetConcurrency() throws SQLException {
                    return delegate.getResultSetConcurrency();
                }

                public int getResultSetType() throws SQLException {
                    return delegate.getResultSetType();
                }

                public void addBatch(String sql) throws SQLException {
                    delegate.addBatch(sql);
                }

                public void clearBatch() throws SQLException {
                    delegate.clearBatch();
                }

                public int[] executeBatch() throws SQLException {
                    if (executeBatchErrorOps.contains(executeBatchErrorOpsCount.incrementAndGet())) {
                        throw new SQLRecoverableException("SOME executeBatch ERROR[" + executeBatchErrorOpsCount.get() +"]");
                    }
                    return delegate.executeBatch();
                }

                public Connection getConnection() throws SQLException {
                    return delegate.getConnection();
                }

                public boolean getMoreResults(int current) throws SQLException {
                    return delegate.getMoreResults(current);
                }

                public ResultSet getGeneratedKeys() throws SQLException {
                    return delegate.getGeneratedKeys();
                }

                public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
                    return delegate.executeUpdate(sql, autoGeneratedKeys);
                }

                public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
                    return delegate.executeUpdate(sql, columnIndexes);
                }

                public int executeUpdate(String sql, String[] columnNames) throws SQLException {
                    return delegate.executeUpdate(sql, columnNames);
                }

                public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
                    return delegate.execute(sql, autoGeneratedKeys);
                }

                public boolean execute(String sql, int[] columnIndexes) throws SQLException {
                    return delegate.execute(sql, columnIndexes);
                }

                public boolean execute(String sql, String[] columnNames) throws SQLException {
                    return delegate.execute(sql, columnNames);
                }

                public int getResultSetHoldability() throws SQLException {
                    return delegate.getResultSetHoldability();
                }

                public boolean isClosed() throws SQLException {
                    return delegate.isClosed();
                }

                public void setPoolable(boolean poolable) throws SQLException {
                    delegate.setPoolable(poolable);
                }

                public boolean isPoolable() throws SQLException {
                    return delegate.isPoolable();
                }

                public void closeOnCompletion() throws SQLException {
                    delegate.closeOnCompletion();
                }

                public boolean isCloseOnCompletion() throws SQLException {
                    return delegate.isCloseOnCompletion();
                }
/*
                public long getLargeUpdateCount() throws SQLException {
                    return delegate.getLargeUpdateCount();
                }

                public void setLargeMaxRows(long max) throws SQLException {
                    delegate.setLargeMaxRows(max);
                }

                public long getLargeMaxRows() throws SQLException {
                    return delegate.getLargeMaxRows();
                }

                public long[] executeLargeBatch() throws SQLException {
                    return delegate.executeLargeBatch();
                }

                public long executeLargeUpdate(String sql) throws SQLException {
                    return delegate.executeLargeUpdate(sql);
                }

                public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
                    return delegate.executeLargeUpdate(sql, autoGeneratedKeys);
                }

                public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
                    return delegate.executeLargeUpdate(sql, columnIndexes);
                }

                public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
                    return delegate.executeLargeUpdate(sql, columnNames);
                }
*/
                public <T> T unwrap(Class<T> iface) throws SQLException {
                    return delegate.unwrap(iface);
                }

                public boolean isWrapperFor(Class<?> iface) throws SQLException {
                    return delegate.isWrapperFor(iface);
                }
            };
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return realConnection.prepareCall(sql);
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return realConnection.nativeSQL(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            realConnection.setAutoCommit(autoCommit);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            if (getAutoCommitErrors.contains(getAutoCommitCount.incrementAndGet())) {
                throw new SQLRecoverableException("AutoCommit[" + getAutoCommitCount.get() +"]");
            }
            return realConnection.getAutoCommit();
        }

        @Override
        public void rollback() throws SQLException {
            realConnection.rollback();
        }

        @Override
        public void close() throws SQLException {
            realConnection.close();
        }

        @Override
        public boolean isClosed() throws SQLException {
            return realConnection.isClosed();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return realConnection.getMetaData();
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            realConnection.setReadOnly(readOnly);
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return realConnection.isReadOnly();
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            realConnection.setCatalog(catalog);
        }

        @Override
        public String getCatalog() throws SQLException {
            return realConnection.getCatalog();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            realConnection.setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return realConnection.getTransactionIsolation();
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return realConnection.getWarnings();
        }

        @Override
        public void clearWarnings() throws SQLException {
            realConnection.clearWarnings();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return realConnection.createStatement(resultSetType, resultSetConcurrency);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return realConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return realConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return realConnection.getTypeMap();
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            realConnection.setTypeMap(map);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            realConnection.setHoldability(holdability);
        }

        @Override
        public int getHoldability() throws SQLException {
            return realConnection.getHoldability();
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return realConnection.setSavepoint();
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return realConnection.setSavepoint(name);
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            realConnection.rollback();
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            realConnection.releaseSavepoint(savepoint);
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return realConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return realConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return realConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return realConnection.prepareStatement(sql, autoGeneratedKeys);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return realConnection.prepareStatement(sql, columnIndexes);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return realConnection.prepareStatement(sql, columnNames);
        }

        @Override
        public Clob createClob() throws SQLException {
            return realConnection.createClob();
        }

        @Override
        public Blob createBlob() throws SQLException {
            return realConnection.createBlob();
        }

        @Override
        public NClob createNClob() throws SQLException {
            return realConnection.createNClob();
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return realConnection.createSQLXML();
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return realConnection.isValid(timeout);
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            realConnection.setClientInfo(name, value);
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            realConnection.setClientInfo(properties);
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return realConnection.getClientInfo(name);
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return realConnection.getClientInfo();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return realConnection.createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return realConnection.createStruct(typeName, attributes);
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            realConnection.setSchema(schema);
        }

        @Override
        public String getSchema() throws SQLException {
            return realConnection.getSchema();
        }

        @Override
        public void abort(Executor executor) throws SQLException {
            realConnection.abort(executor);
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            realConnection.setNetworkTimeout(executor, milliseconds);
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return realConnection.getNetworkTimeout();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return realConnection.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return realConnection.isWrapperFor(iface);
        }
    }
}
