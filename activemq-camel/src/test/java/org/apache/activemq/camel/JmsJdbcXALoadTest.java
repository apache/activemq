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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.apache.camel.test.junit4.CamelSpringTestSupport;
import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.sql.ResultSet;
import java.sql.SQLException;

@Ignore("Test hangs")
public class JmsJdbcXALoadTest extends CamelSpringTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(JmsJdbcXATest.class);
    BrokerService broker = null;
    int messageCount;

    public java.sql.Connection initDb() throws Exception {
        String createStatement =
                "CREATE TABLE SCP_INPUT_MESSAGES (" +
                        "id int NOT NULL GENERATED ALWAYS AS IDENTITY, " +
                        "messageId varchar(96) NOT NULL, " +
                        "messageCorrelationId varchar(96) NOT NULL, " +
                        "messageContent varchar(2048) NOT NULL, " +
                        "PRIMARY KEY (id) )";

        java.sql.Connection conn = getJDBCConnection();
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

    private java.sql.Connection getJDBCConnection() throws Exception {
        BasicDataSource dataSource = getMandatoryBean(BasicDataSource.class, "managedDataSourceWithRecovery");
        return dataSource.getConnection();
    }

    private int dumpDb(java.sql.Connection jdbcConn) throws Exception {
        int count = 0;
        ResultSet resultSet = jdbcConn.createStatement().executeQuery("SELECT * FROM SCP_INPUT_MESSAGES");
        while (resultSet.next()) {
            count++;
        }
        log.info(count + " messages");
        return count;
    }

    @Test
    public void testRecoveryCommit() throws Exception {
        java.sql.Connection jdbcConn = initDb();
        final int count = 1000;

        sendJMSMessageToKickOffRoute(count);


        final java.sql.Connection freshConnection = getJDBCConnection();
        assertTrue("did not get replay", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return count == dumpDb(freshConnection);
            }
        }, 20*60*1000));
        assertEquals("still one message in db", count, dumpDb(freshConnection));
    }

    private void sendJMSMessageToKickOffRoute(int count) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://testXA");
        factory.setWatchTopicAdvisories(false);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue("scp_transacted"));
        for (int i = 0; i < count; i++) {
            TextMessage message = session.createTextMessage("Some Text, messageCount:" + messageCount++);
            message.setJMSCorrelationID("pleaseCorrelate");
            producer.send(message);
        }
        connection.close();
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

        deleteDirectory("target/data/howl");

        // make broker available to recovery processing on app context start
        try {
            broker = createBroker(true);
            broker.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start broker", e);
        }

        return new ClassPathXmlApplicationContext("org/apache/activemq/camel/jmsXajdbc.xml");
    }



    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
