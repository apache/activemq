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

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.jdbc.EmbeddedDataSource;

import junit.framework.TestCase;

// https://issues.apache.org/activemq/browse/AMQ-2880
public class JDBCCommitExceptionTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(JDBCCommitExceptionTest.class);

    private static final int messagesExpected = 10;
    private ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
            "tcp://localhost:61616?jms.prefetchPolicy.all=0&jms.redeliveryPolicy.maximumRedeliveries="+messagesExpected); 
    private BrokerService broker;
    private EmbeddedDataSource dataSource;
    private java.sql.Connection dbConnection;
    private MyPersistenceAdapter jdbc;

    public void testSqlException() throws Exception {
        broker = createBroker();
        broker.start();


        sendMessages(messagesExpected);
        int messagesReceived = receiveMessages(messagesExpected);

        dumpMessages();

        assertEquals("Messages expected doesn't equal messages received", messagesExpected, messagesReceived);

        broker.stop();
    }

     protected void dumpMessages() throws Exception {
        WireFormat wireFormat = new OpenWireFormat();
        java.sql.Connection conn = ((JDBCPersistenceAdapter) broker.getPersistenceAdapter()).getDataSource().getConnection();
        PreparedStatement statement = conn.prepareStatement("SELECT ID, MSG FROM ACTIVEMQ_MSGS");    
        ResultSet result = statement.executeQuery();
        LOG.info("Messages left in broker after test");
        while(result.next()) {
            long id = result.getLong(1);
            org.apache.activemq.command.Message message = (org.apache.activemq.command.Message)wireFormat.unmarshal(new ByteSequence(result.getBytes(2)));
            LOG.info("id: " + id + ", message SeqId: " + message.getMessageId().getBrokerSequenceId() + ", MSG: " + message);
        }
        statement.close();
        conn.close();
    }

    protected int receiveMessages(int messagesExpected) throws Exception {
        javax.jms.Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        jdbc.setShouldBreak(true);

        // first try and receive these messages, they'll continually fail
        receiveMessages(messagesExpected, session);

        jdbc.setShouldBreak(false);

        // now that the store is sane, try and get all the messages sent
        return receiveMessages(messagesExpected, session);
    }

    protected int receiveMessages(int messagesExpected, Session session) throws Exception {
        int messagesReceived = 0;

        for (int i=0; i<messagesExpected; i++) {
            Destination destination = session.createQueue("TEST");
            MessageConsumer consumer = session.createConsumer(destination);
            Message message = null;
            try {
                LOG.debug("Receiving message " + (messagesReceived+1) + " of " + messagesExpected);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                if (message != null) {
                    session.commit();
                    messagesReceived++;
                }
            } catch (Exception e) {
                LOG.debug("Caught exception " + e);
                session.rollback();
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        return messagesReceived;
    }

    protected void sendMessages(int messagesExpected) throws Exception {
        javax.jms.Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("TEST");        
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        for (int i=0; i<messagesExpected; i++) {
            LOG.debug("Sending message " + (i+1) + " of " + messagesExpected);
            producer.send(session.createTextMessage("test message " + (i+1)));
        }
    }

    protected BrokerService createBroker() throws Exception {
        Properties p = System.getProperties();

        BrokerService broker = new BrokerService();
        jdbc = new MyPersistenceAdapter();

        dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("target/derbyDb");
        dataSource.setCreateDatabase("create");

        jdbc.setDataSource(dataSource);
        jdbc.setUseDatabaseLock(false);
        jdbc.deleteAllMessages();

        broker.setPersistenceAdapter(jdbc);
        broker.setPersistent(true);
        broker.addConnector("tcp://localhost:61616");

        return broker;
    }

    class MyPersistenceAdapter extends JDBCPersistenceAdapter {

        private  final Log LOG = LogFactory.getLog(MyPersistenceAdapter.class);

        private boolean shouldBreak = false;

        @Override
        public void commitTransaction(ConnectionContext context) throws IOException {
            if ( shouldBreak ) {
                LOG.warn("Throwing exception on purpose");
                throw new IOException("Breaking on purpose");
            }
            LOG.debug("in commitTransaction");
            super.commitTransaction(context);
        }

        public void setShouldBreak(boolean shouldBreak) {
            this.shouldBreak = shouldBreak;
        }
    }

}



