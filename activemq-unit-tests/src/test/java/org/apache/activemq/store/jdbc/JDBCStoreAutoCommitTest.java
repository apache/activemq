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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import javax.sql.DataSource;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.Test;

/**
 * to be compliant with JDBC spec; officially commit is not supposed to be
 * called on a connection that uses autocommit.The oracle v12 driver does a
 * check for autocommitSpecCompliance and it causes issues
 * <p/>
 * To test; wrap the datasource used by the broker and check for autocommit
 * before delegating to real datasource. If commit is called on connection with
 * autocommit, wrapper throws a SQLException.
 */

public class JDBCStoreAutoCommitTest {

    private static final String BROKER_NAME = "AutoCommitTest";
    private static final String TEST_DEST = "commitCheck";
    private static final String MSG_TEXT = "JDBCStoreAutoCommitTest TEST";

    /**
     * verify dropping and recreating tables
     *
     * @throws Exception
     */
    @Test
    public void testDeleteAllMessages() throws Exception {
        BrokerService broker = createBrokerService();
        broker.getPersistenceAdapter().deleteAllMessages();
        broker.setUseJmx(false);
        broker.start();
        broker.waitUntilStarted();
        broker.stop();
        broker.waitUntilStopped();
    }

    /**
     * Send message and consume message, JMS session is not transacted
     *
     * @throws Exception
     */
    @Test
    public void testSendConsume() throws Exception {
        this.doSendConsume(false);
    }

    /**
     * send message and consume message, JMS session is transacted
     *
     * @throws Exception
     */
    @Test
    public void testSendConsumeTransacted() throws Exception {
        this.doSendConsume(true);
    }

    private void doSendConsume(boolean transacted) throws Exception {

        BrokerService broker = createBrokerService();
        broker.setUseJmx(false);
        broker.start();
        broker.waitUntilStarted();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(new URI("vm:" + BROKER_NAME));
        ActiveMQConnection c1 = (ActiveMQConnection) cf.createConnection();
        c1.start();

        try {
            // message send
            Session session1 = c1.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session1.createProducer(session1.createQueue(TEST_DEST));
            TextMessage textMessage = session1.createTextMessage(MSG_TEXT);
            messageProducer.send(textMessage);

            if (transacted) {
                session1.commit();
            }

            // consume
            Session session2 = c1.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session2.createConsumer(session2.createQueue(TEST_DEST));
            TextMessage messageReceived = (TextMessage) messageConsumer.receive(1000);

            assertEquals("check message received", MSG_TEXT, messageReceived.getText());
        } finally {
            c1.close();
            broker.stop();
            broker.waitUntilStopped();
            if (realDataSource != null) {
                DataSourceServiceSupport.shutdownDefaultDataSource(realDataSource);
            }
        }
    }

    DataSource realDataSource;
    private BrokerService createBrokerService() throws IOException {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(BROKER_NAME);
        broker.setUseJmx(false);

        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        realDataSource = jdbc.getDataSource();
        jdbc.setDataSource(new TestDataSource(realDataSource));

        broker.setPersistenceAdapter(jdbc);
        return broker;
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
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
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

        // verify commit is not called on an auto-commit connection
        @Override
        public void commit() throws SQLException {
            if (getAutoCommit() == true) {
                throw new SQLException("AutoCommitCheckConnection: Called commit on autoCommit Connection");
            }
            realConnection.commit();
        }

        // Just plumbing for wrapper. Might have been better to do a Dynamic Proxy here.

        @Override
        public Statement createStatement() throws SQLException {
            return realConnection.createStatement();
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return realConnection.prepareStatement(sql);
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