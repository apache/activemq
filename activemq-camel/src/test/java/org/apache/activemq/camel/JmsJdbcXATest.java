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
package org.apache.activemq.camel;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.Wait;
import org.apache.camel.spring.SpringTestSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.enhydra.jdbc.pool.StandardXAPoolDataSource;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 *  shows broker heuristic rollback (no prepare memory), hence duplicate message delivery
 */
public class JmsJdbcXATest extends SpringTestSupport {
    private static final Log LOG = LogFactory.getLog(JmsJdbcXATest.class);
    BrokerService broker = null;

    public java.sql.Connection initDb() throws Exception {
        String createStatement =
                "CREATE TABLE SCP_INPUT_MESSAGES (" +
                        "id int NOT NULL GENERATED ALWAYS AS IDENTITY, " +
                        "messageId varchar(96) NOT NULL, " +
                        "messageCorrelationId varchar(96) NOT NULL, " +
                        "messageContent varchar(2048) NOT NULL, " +
                        "PRIMARY KEY (id) )";

        java.sql.Connection conn = null;
        StandardXAPoolDataSource pool = getMandatoryBean(StandardXAPoolDataSource.class, "jdbcEnhydraXaDataSource");
        conn = pool.getConnection();
        try {
            conn.createStatement().execute(createStatement);
        } catch (SQLException alreadyExists) {
            log.info("ex on create tables", alreadyExists);
        }

        try {
            conn.createStatement().execute("DELETE FROM SCP_INPUT_MESSAGES");
        } catch (SQLException ex) {
            log.info("ex on create delete all", ex);
        }

        return conn;
    }

    private int dumpDb(java.sql.Connection jdbcConn) throws Exception {
        int count = 0;
        ResultSet resultSet = jdbcConn.createStatement().executeQuery("SELECT * FROM SCP_INPUT_MESSAGES");
        while (resultSet.next()) {
            count++;
            log.info("message - seq:" + resultSet.getInt(1)
                    + ", id: " + resultSet.getString(2)
                    + ", corr: " + resultSet.getString(3)
                    + ", content: " + resultSet.getString(4));
        }
        return count;
    }

    public void testRecovery() throws Exception {

        broker = createBroker(true);
        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public void commitTransaction(ConnectionContext context,
                                                  TransactionId xid, boolean onePhase) throws Exception {
                        if (onePhase) {
                            super.commitTransaction(context, xid, onePhase);
                        } else {
                            // die before doing the commit
                            // so commit will hang as if reply is lost
                            context.setDontSendReponse(true);
                            Executors.newSingleThreadExecutor().execute(new Runnable() {
                                public void run() {
                                    LOG.info("Stopping broker post commit...");
                                    try {
                                        broker.stop();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }
                    }
                }
        });
        broker.start();

        final java.sql.Connection jdbcConn = initDb();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://testXA");
        factory.setWatchTopicAdvisories(false);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue("scp_transacted"));
        TextMessage message = session.createTextMessage("Some Text");
        message.setJMSCorrelationID("pleaseCorrelate");
        producer.send(message);

        LOG.info("waiting for route to kick in, it will kill the broker on first 2pc commit");
        // will be stopped by the plugin on first 2pc commit
        broker.waitUntilStopped();
        assertEquals("message in db, commit to db worked", 1, dumpDb(jdbcConn));

        LOG.info("Broker stopped, restarting...");
        broker = createBroker(false);
        broker.start();
        broker.waitUntilStarted();

        LOG.info("waiting for completion or route with replayed message");
        assertTrue("got a second message in the db", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 2 == dumpDb(jdbcConn);
            }
        }));
        assertEquals("message in db", 2, dumpDb(jdbcConn));
    }

    private BrokerService createBroker(boolean deleteAllMessages) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(deleteAllMessages);
        brokerService.setBrokerName("testXA");
        brokerService.setAdvisorySupport(false);
        brokerService.setUseJmx(false);
        brokerService.setDataDirectory("target/data");
        brokerService.addConnector("tcp://0.0.0.0:61616");
        return brokerService;
    }

    @Override
    protected AbstractXmlApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("org/apache/activemq/camel/jmsXajdbc.xml");
    }
}
