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

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.*;
import org.apache.activemq.store.jdbc.*;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOHelper;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.jms.Message;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Test to demostrate a message trapped in the JDBC store and not
 * delivered to consumer
 *
 * The test throws issues the commit to the DB but throws
 * an exception back to the broker. This scenario could happen when a network
 * cable is disconnected - message is committed to DB but broker does not know.
 *
 *
 */

public class TrapMessageInJDBCStoreTest extends TestCase {

    private static final String MY_TEST_Q = "MY_TEST_Q";
    private static final Logger LOG = LoggerFactory
            .getLogger(TrapMessageInJDBCStoreTest.class);
    private String transportUrl = "tcp://127.0.0.1:0";
    private BrokerService broker;
    private TestTransactionContext testTransactionContext;
    private TestJDBCPersistenceAdapter jdbc;

    protected BrokerService createBroker(boolean withJMX) throws Exception {
        BrokerService broker = new BrokerService();

        broker.setUseJmx(withJMX);

        EmbeddedDataSource embeddedDataSource = (EmbeddedDataSource) DataSourceServiceSupport.createDataSource(IOHelper.getDefaultDataDirectory());
        embeddedDataSource.setCreateDatabase("create");

        //wire in a TestTransactionContext (wrapper to TransactionContext) that has an executeBatch()
        // method that can be configured to throw a SQL exception on demand
        jdbc = new TestJDBCPersistenceAdapter();
        jdbc.setDataSource(embeddedDataSource);
        testTransactionContext = new TestTransactionContext(jdbc);

        jdbc.setLockKeepAlivePeriod(1000l);
        LeaseDatabaseLocker leaseDatabaseLocker = new LeaseDatabaseLocker();
        leaseDatabaseLocker.setLockAcquireSleepInterval(2000l);
        jdbc.setLocker(leaseDatabaseLocker);

        broker.setPersistenceAdapter(jdbc);

        broker.setIoExceptionHandler(new JDBCIOExceptionHandler());

        transportUrl = broker.addConnector(transportUrl).getPublishableConnectString();
        return broker;
    }

    /**
     *
     * sends 3 messages to the queue. When the second message is being committed to the JDBCStore, $
     * it throws a dummy SQL exception - the message has been committed to the embedded DB before the exception
     * is thrown
     *
     * Excepted correct outcome: receive 3 messages and the DB should contain no messages
     *
     * @throws Exception
     */

    public void testDBCommitException() throws Exception {

        broker = this.createBroker(false);
        broker.deleteAllMessages();
        broker.start();
        broker.waitUntilStarted();

        LOG.info("***Broker started...");

        // failover but timeout in 5 seconds so the test does not hang
        String failoverTransportURL = "failover:(" + transportUrl
                + ")?timeout=5000";


        sendMessage(MY_TEST_Q, failoverTransportURL);

        List<TextMessage> consumedMessages = consumeMessages(MY_TEST_Q,failoverTransportURL);

        //check db contents
        ArrayList<Long> dbSeq = dbMessageCount();

        LOG.debug("*** db contains message seq " +dbSeq );

        assertEquals("number of messages in DB after test",0,dbSeq.size());
        assertEquals("number of consumed messages",3,consumedMessages.size());

        broker.stop();
        broker.waitUntilStopped();
    }



    public List<TextMessage> consumeMessages(String queue,
                                      String transportURL) throws JMSException {
        Connection connection = null;
        LOG.debug("*** consumeMessages() called ...");

        try {

            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                    transportURL);

            connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queue);

            ArrayList<TextMessage> consumedMessages = new ArrayList<TextMessage>();

            MessageConsumer messageConsumer = session.createConsumer(destination);

            while(true){
                TextMessage textMessage= (TextMessage) messageConsumer.receive(100);
                LOG.debug("*** consumed Messages :"+textMessage);

                if(textMessage==null){
                    return consumedMessages;
                }
                consumedMessages.add(textMessage);
            }


        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public void sendMessage(String queue, String transportURL)
            throws JMSException {
        Connection connection = null;

        try {

            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                    transportURL);

            connection = factory.createConnection();
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queue);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            TextMessage m = session.createTextMessage("1");

            testTransactionContext.throwSQLException = false;
            jdbc.throwSQLException = false;


            LOG.debug("*** send message 1 to broker...");
            producer.send(m);

            testTransactionContext.throwSQLException = true;
            jdbc.throwSQLException = true;

            // trigger SQL exception in transactionContext
            LOG.debug("***  send message 2 to broker");
            m.setText("2");

            // need to reset the flag in a seperate thread during the send
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    testTransactionContext.throwSQLException = false;
                    jdbc.throwSQLException = false;
                }
            }, 2 , TimeUnit.SECONDS);

             producer.send(m);


            LOG.debug("***  send  message 3 to broker");
            m.setText("3");
            producer.send(m);
            LOG.debug("*** Finished sending messages to broker");

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     *  query the DB to see what messages are left in the store
     * @return
     * @throws SQLException
     * @throws IOException
     */
    private ArrayList<Long> dbMessageCount() throws SQLException, IOException {
        java.sql.Connection conn = ((JDBCPersistenceAdapter) broker.getPersistenceAdapter()).getDataSource().getConnection();
        PreparedStatement statement = conn.prepareStatement("SELECT MSGID_SEQ FROM ACTIVEMQ_MSGS");

        try{

            ResultSet result = statement.executeQuery();
            ArrayList<Long> dbSeq = new ArrayList<Long>();

            while (result.next()){
                dbSeq.add(result.getLong(1));
            }

            return dbSeq;

        }finally{
            statement.close();
            conn.close();

        }

    }

	/*
     * Mock classes used for testing
	 */

    public class TestJDBCPersistenceAdapter extends JDBCPersistenceAdapter {

        public boolean throwSQLException;

        public TransactionContext getTransactionContext() throws IOException {
            return testTransactionContext;
        }

        @Override
        public void checkpoint(boolean sync) throws IOException {
            if (throwSQLException) {
                throw new IOException("checkpoint failed");
            }
            super.checkpoint(sync);
        }
    }

    public class TestTransactionContext extends TransactionContext {

        public boolean throwSQLException;

        public TestTransactionContext(
                JDBCPersistenceAdapter jdbcPersistenceAdapter)
                throws IOException {
            super(jdbcPersistenceAdapter);
        }

        public void executeBatch() throws SQLException {
            //call
            super.executeBatch();

            if (throwSQLException){
                throw new SQLException("TEST SQL EXCEPTION from executeBatch after super. execution");
            }


        }




    }

}