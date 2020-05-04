/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.store.jdbc;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import javax.sql.DataSource;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

public class JmsTransactionCommitFailureTest {
    private static final Log LOGGER = LogFactory.getLog(JmsTransactionCommitFailureTest.class);
    private static final String OUTPUT_DIR = "target/" + JmsTransactionCommitFailureTest.class.getSimpleName();

    private Properties originalSystemProps;
    private DataSource dataSource;
    private CommitFailurePersistenceAdapter persistenceAdapter;
    private BrokerService broker;
    private ConnectionFactory connectionFactory;
    private int messageCounter = 1;

    @Before
    public void setUp() throws Exception {
        originalSystemProps = System.getProperties();
        Properties systemProps = (Properties) originalSystemProps.clone();
        systemProps.setProperty("derby.stream.error.file", OUTPUT_DIR + "/derby.log");
        System.setProperties(systemProps);

        dataSource = createDataSource();
        persistenceAdapter = new CommitFailurePersistenceAdapter(dataSource);
        broker = createBroker(persistenceAdapter);
        broker.start();
        connectionFactory = createConnectionFactory(broker.getBrokerName());
    }

    private DataSource createDataSource() {
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName(OUTPUT_DIR + "/derby-db");
        dataSource.setCreateDatabase("create");
        return dataSource;
    }

    private BrokerService createBroker(PersistenceAdapter persistenceAdapter)
            throws IOException {
        String brokerName = JmsTransactionCommitFailureTest.class.getSimpleName();
        BrokerService broker = new BrokerService();
        broker.setDataDirectory(OUTPUT_DIR + "/activemq");
        broker.setBrokerName(brokerName);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setAdvisorySupport(false);
        broker.setUseJmx(false);
        if (persistenceAdapter != null) {
            broker.setPersistent(true);
            broker.setPersistenceAdapter(persistenceAdapter);
        }
        return broker;
    }

    private ConnectionFactory createConnectionFactory(String brokerName) {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://" + brokerName);
        factory.setWatchTopicAdvisories(false);
        return factory;
    }

    private void stopDataSource() {
        if (dataSource instanceof EmbeddedDataSource) {
            EmbeddedDataSource derbyDataSource = (EmbeddedDataSource) dataSource;
            derbyDataSource.setShutdownDatabase("shutdown");
            try {
                derbyDataSource.getConnection();
            } catch (SQLException ignored) {
            }
        }
    }

    private void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            stopBroker();
            stopDataSource();
        } finally {
            System.setProperties(originalSystemProps);
        }
    }

    @Test
    public void testJmsTransactionCommitFailure() throws Exception {
        String queueName = "testJmsTransactionCommitFailure";
        // Send 1.message
        sendMessage(queueName, 1);
        // Check message count directly in database
        Assert.assertEquals(1L, getMessageCount());

        // Set failure flag on persistence adapter
        persistenceAdapter.setCommitFailureEnabled(true);
        // Send 2.message and 3.message in one JMS transaction
        try {
            LOGGER.warn("Attempt to send Message-2/Message-3 (first time)...");
            sendMessage(queueName, 2);
            LOGGER.warn("Message-2/Message-3 successfuly sent (first time)");
            Assert.fail();
        } catch (JMSException jmse) {
            // Expected - decrease message counter (I want to repeat message send)
            LOGGER.warn("Attempt to send Message-2/Message-3 failed", jmse);
            messageCounter -= 2;
            Assert.assertEquals(1L, getMessageCount());
        }

        // Reset failure flag on persistence adapter
        persistenceAdapter.setCommitFailureEnabled(false);
        // Send 2.message again
        LOGGER.warn("Attempt to send Message-2/Message-3 (second time)...");
        sendMessage(queueName, 2);
        LOGGER.warn("Message-2/Message-3 successfuly sent (second time)");

        int expectedMessageCount = 3;
        // Check message count directly in database
        Assert.assertEquals(3L, getMessageCount());
        // Attempt to receive 3 (expected) messages
        for (int i = 1; i <= expectedMessageCount; i++) {
            Message message = receiveMessage(queueName, 10000);
            LOGGER.warn(i + ". Message received (" + message + ")");
            Assert.assertNotNull(message);
            Assert.assertTrue(message instanceof TextMessage);
            Assert.assertEquals(i, message.getIntProperty("MessageId"));
            Assert.assertEquals("Message-" + i, ((TextMessage) message).getText());
            // Check message count directly in database
            //Assert.assertEquals(expectedMessageCount - i, getMessageCount());
        }

        // Check message count directly in database
        Assert.assertEquals(0, getMessageCount());
        // No next message is expected
        Assert.assertNull(receiveMessage(queueName, 4000));
    }

    @Test
    public void testQueueMemoryLeak() throws Exception {
        String queueName = "testMemoryLeak";

        sendMessage(queueName, 1);

        // Set failure flag on persistence adapter
        persistenceAdapter.setCommitFailureEnabled(true);
        try {
            for (int i = 0; i < 10; i++) {
                try {
                    sendMessage(queueName, 2);
                } catch (JMSException jmse) {
                    // Expected
                }
            }
        } finally {
            persistenceAdapter.setCommitFailureEnabled(false);
        }
        Destination destination = broker.getDestination(new ActiveMQQueue(queueName));
        if (destination instanceof org.apache.activemq.broker.region.Queue) {
            org.apache.activemq.broker.region.Queue queue = (org.apache.activemq.broker.region.Queue) destination;
            Field listField = org.apache.activemq.broker.region.Queue.class.getDeclaredField("indexOrderedCursorUpdates");
            listField.setAccessible(true);
            List<?> list = (List<?>) listField.get(queue);
            Assert.assertEquals(0, list.size());
        }
    }


    @Test
    public void testQueueMemoryLeakNoTx() throws Exception {
        String queueName = "testMemoryLeak";

        sendMessage(queueName, 1);

        // Set failure flag on persistence adapter
        persistenceAdapter.setCommitFailureEnabled(true);
        try {
            for (int i = 0; i < 10; i++) {
                try {
                    sendMessage(queueName, 2, false);
                } catch (JMSException jmse) {
                    // Expected
                }
            }
        } finally {
            persistenceAdapter.setCommitFailureEnabled(false);
        }
        Destination destination = broker.getDestination(new ActiveMQQueue(queueName));
        if (destination instanceof org.apache.activemq.broker.region.Queue) {
            org.apache.activemq.broker.region.Queue queue = (org.apache.activemq.broker.region.Queue) destination;
            Field listField = org.apache.activemq.broker.region.Queue.class.getDeclaredField("indexOrderedCursorUpdates");
            listField.setAccessible(true);
            List<?> list = (List<?>) listField.get(queue);
            Assert.assertEquals(0, list.size());
        }
    }

    private void sendMessage(String queueName, int count) throws JMSException {
        sendMessage(queueName, count, true);
    }

    private void sendMessage(String queueName, int count, boolean transacted) throws JMSException {
        Connection con = connectionFactory.createConnection();
        try {
            Session session = con.createSession(transacted, transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
            try {
                Queue destination = session.createQueue(queueName);
                MessageProducer producer = session.createProducer(destination);
                try {
                    for (int i = 0; i < count; i++) {
                        TextMessage message = session.createTextMessage();
                        message.setIntProperty("MessageId", messageCounter);
                        message.setText("Message-" + messageCounter++);
                        producer.send(message);
                    }
                    if (transacted) {
                        session.commit();
                    }
                } finally {
                    producer.close();
                }
            } finally {
                session.close();
            }
        } finally {
            con.close();
        }
    }

    private Message receiveMessage(String queueName, long receiveTimeout)
            throws JMSException {
        Message message = null;
        Connection con = connectionFactory.createConnection();
        try {
            con.start();
            try {
                Session session = con.createSession(true, Session.SESSION_TRANSACTED);
                try {
                    Queue destination = session.createQueue(queueName);
                    MessageConsumer consumer = session.createConsumer(destination);
                    try {
                        message = consumer.receive(receiveTimeout);
                        session.commit();
                    } finally {
                        consumer.close();
                    }
                } finally {
                    session.close();
                }
            } finally {
                con.stop();
            }
        } finally {
            con.close();
        }
        return message;
    }

    private long getMessageCount() throws SQLException {
        long messageCount = -1;
        java.sql.Connection con = dataSource.getConnection();
        try {
            Statement stmt = con.createStatement();
            try {
                ResultSet rs = stmt.executeQuery("select count(*) from activemq_msgs");
                try {
                    while (rs.next())
                        messageCount = rs.getLong(1);
                } finally {
                    rs.close();
                }
            } finally {
                stmt.close();
            }
        } finally {
            con.close();
        }
        return messageCount;
    }

    private static class CommitFailurePersistenceAdapter extends JDBCPersistenceAdapter {
        private boolean isCommitFailureEnabled;
        private int transactionIsolation;

        public CommitFailurePersistenceAdapter(DataSource dataSource) {
            setDataSource(dataSource);
        }

        public void setCommitFailureEnabled(boolean isCommitFailureEnabled) {
            this.isCommitFailureEnabled = isCommitFailureEnabled;
        }

        @Override
        public void setTransactionIsolation(int transactionIsolation) {
            super.setTransactionIsolation(transactionIsolation);
            this.transactionIsolation = transactionIsolation;
        }

        @Override
        public TransactionContext getTransactionContext() throws IOException {
            TransactionContext answer = new TransactionContext(this, -1, -1) {
                @Override
                public void executeBatch() throws SQLException {
                    if (isCommitFailureEnabled) {
                        throw new SQLException("Test commit failure exception");
                    }
                    super.executeBatch();
                }
            };
            if (transactionIsolation > 0) {
                answer.setTransactionIsolation(transactionIsolation);
            }
            return answer;
        }
    }

}