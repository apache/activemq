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
package org.apache.activemq.bugs;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.jdbc.DataSourceServiceSupport;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.jdbc.LeaseDatabaseLocker;
import org.apache.activemq.store.jdbc.TransactionContext;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.LeaseLockerIOExceptionHandler;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.fail;

/**
 * Testing how the broker reacts when a SQL Exception is thrown from
 * org.apache.activemq.store.jdbc.TransactionContext.executeBatch().
 * <p/>
 * see https://issues.apache.org/jira/browse/AMQ-4636
 */
public class AMQ4636Test {

    private static final String MY_TEST_TOPIC = "MY_TEST_TOPIC";
    private static final Logger LOG = LoggerFactory
            .getLogger(AMQ4636Test.class);
    private String transportUrl = "tcp://0.0.0.0:0";
    private BrokerService broker;
    EmbeddedDataSource embeddedDataSource;
    CountDownLatch throwSQLException = new CountDownLatch(0);

    @Before
    public void startBroker() throws Exception {
        broker = createBroker();
        broker.deleteAllMessages();
        broker.start();
        broker.waitUntilStarted();
        LOG.info("Broker started...");
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            LOG.info("Stopping broker...");
            broker.stop();
            broker.waitUntilStopped();
        }
        if (embeddedDataSource != null) {
            DataSourceServiceSupport.shutdownDefaultDataSource(embeddedDataSource);
        }
    }

    protected BrokerService createBroker() throws Exception {

        embeddedDataSource = (EmbeddedDataSource) DataSourceServiceSupport.createDataSource(IOHelper.getDefaultDataDirectory());
        embeddedDataSource.setCreateDatabase("create");
        embeddedDataSource.getConnection().close();

        //wire in a TestTransactionContext (wrapper to TransactionContext) that has an executeBatch()
        // method that can be configured to throw a SQL exception on demand
        JDBCPersistenceAdapter jdbc = new TestJDBCPersistenceAdapter();
        jdbc.setDataSource(embeddedDataSource);

        jdbc.setLockKeepAlivePeriod(1000l);
        LeaseDatabaseLocker leaseDatabaseLocker = new LeaseDatabaseLocker();
        leaseDatabaseLocker.setLockAcquireSleepInterval(2000l);
        jdbc.setLocker(leaseDatabaseLocker);

        broker = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(0);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
        broker.setPersistenceAdapter(jdbc);

        broker.setIoExceptionHandler(new LeaseLockerIOExceptionHandler());

        transportUrl = broker.addConnector(transportUrl).getPublishableConnectString();
        return broker;
    }

    /**
     * adding a TestTransactionContext (wrapper to TransactionContext) so an SQLException is triggered
     * during TransactionContext.executeBatch() when called in the broker.
     * <p/>
     * Expectation: SQLException triggers a connection shutdown and failover should kick and try to redeliver the
     * message. SQLException should NOT be returned to client
     */
    @Test
    public void testProducerWithDBShutdown() throws Exception {

        // failover but timeout in 1 seconds so the test does not hang
        String failoverTransportURL = "failover:(" + transportUrl
                + ")?timeout=1000";

        this.createDurableConsumer(MY_TEST_TOPIC, failoverTransportURL);

        this.sendMessage(MY_TEST_TOPIC, failoverTransportURL, false, false);

    }

    @Test
    public void testTransactedProducerCommitWithDBShutdown() throws Exception {

        // failover but timeout in 1 seconds so the test does not hang
        String failoverTransportURL = "failover:(" + transportUrl
                + ")?timeout=1000";

        this.createDurableConsumer(MY_TEST_TOPIC, failoverTransportURL);

        try {
            this.sendMessage(MY_TEST_TOPIC, failoverTransportURL, true, true);
            fail("Expect rollback after failover - inddoubt commit");
        } catch (javax.jms.TransactionRolledBackException expectedInDoubt) {
            LOG.info("Got rollback after failover failed commit", expectedInDoubt);
        }
    }

    @Test
    public void testTransactedProducerRollbackWithDBShutdown() throws Exception {

        // failover but timeout in 1 seconds so the test does not hang
        String failoverTransportURL = "failover:(" + transportUrl
                + ")?timeout=1000";

        this.createDurableConsumer(MY_TEST_TOPIC, failoverTransportURL);

        this.sendMessage(MY_TEST_TOPIC, failoverTransportURL, true, false);
    }

    public void createDurableConsumer(String topic,
                                      String transportURL) throws JMSException {
        Connection connection = null;
        LOG.info("*** createDurableConsumer() called ...");

        try {

            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                    transportURL);

            connection = factory.createConnection();
            connection.setClientID("myconn1");
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic(topic);

            TopicSubscriber topicSubscriber = session.createDurableSubscriber(
                    (Topic) destination, "MySub1");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public void sendMessage(String topic, String transportURL, boolean transacted, boolean commit)
            throws JMSException {
        Connection connection = null;

        try {

            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                    transportURL);

            connection = factory.createConnection();
            Session session = connection.createSession(transacted,
                    transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic(topic);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            Message m = session.createTextMessage("testMessage");
            LOG.info("*** send message to broker...");

            // trigger SQL exception in transactionContext
            throwSQLException = new CountDownLatch(1);
            producer.send(m);

            if (transacted) {
                if (commit) {
                    session.commit();
                } else {
                    session.rollback();
                }
            }

            LOG.info("*** Finished send message to broker");

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

	/*
     * Mock classes used for testing
	 */

    public class TestJDBCPersistenceAdapter extends JDBCPersistenceAdapter {

        public TransactionContext getTransactionContext() throws IOException {
            return new TestTransactionContext(this);
        }
    }

    public class TestTransactionContext extends TransactionContext {

        public TestTransactionContext(
                JDBCPersistenceAdapter jdbcPersistenceAdapter)
                throws IOException {
            super(jdbcPersistenceAdapter, -1, -1);
        }

        @Override
        public void executeBatch() throws SQLException {
            if (throwSQLException.getCount() > 0) {
                // only throw exception once
                throwSQLException.countDown();
                throw new SQLException("TEST SQL EXCEPTION");
            }
            super.executeBatch();
        }
    }

}