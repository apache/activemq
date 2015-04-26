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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.Message;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCTablePrefixAssignedTest {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCTablePrefixAssignedTest.class);

    private BrokerService service;

    @Before
    public void setUp() throws Exception {
        service = createBroker();
        service.start();
        service.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        service.stop();
        service.waitUntilStopped();
    }

    @Test
    public void testTablesHave() throws Exception {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?create=false");
        ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("TEST.FOO");
        MessageProducer producer = session.createProducer(destination);

        for (int i = 0; i < 10; ++i) {
            producer.send(session.createTextMessage("test"));
        }
        producer.close();
        connection.close();

        List<Message> queuedMessages = null;
        try {
            queuedMessages = dumpMessages();
        } catch (Exception ex) {
            LOG.info("Caught ex: ", ex);
            fail("Should not have thrown an exception");
        }

        assertNotNull(queuedMessages);
        assertEquals("Should have found 10 messages", 10, queuedMessages.size());
    }

    protected List<Message> dumpMessages() throws Exception {
        WireFormat wireFormat = new OpenWireFormat();
        java.sql.Connection conn = ((JDBCPersistenceAdapter) service.getPersistenceAdapter()).getDataSource().getConnection();
        PreparedStatement statement = conn.prepareStatement("SELECT ID, MSG FROM MYPREFIX_ACTIVEMQ_MSGS");
        ResultSet result = statement.executeQuery();
        ArrayList<Message> results = new ArrayList<Message>();
        while(result.next()) {
            long id = result.getLong(1);
            Message message = (Message)wireFormat.unmarshal(new ByteSequence(result.getBytes(2)));
            LOG.info("id: " + id + ", message SeqId: " + message.getMessageId().getBrokerSequenceId() + ", MSG: " + message);
            results.add(message);
        }
        statement.close();
        conn.close();

        return results;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        DefaultJDBCAdapter adapter = new DefaultJDBCAdapter();
        jdbc.setAdapter(adapter);

        Statements statements = new Statements();
        statements.setTablePrefix("MYPREFIX_");
        jdbc.setStatements(statements);

        jdbc.setUseLock(false);
        jdbc.deleteAllMessages();
        broker.setPersistenceAdapter(jdbc);
        return broker;
    }
}
