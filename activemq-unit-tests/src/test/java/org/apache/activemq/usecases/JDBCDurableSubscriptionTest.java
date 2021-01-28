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
package org.apache.activemq.usecases;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;

import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * 
 */
public class JDBCDurableSubscriptionTest extends DurableSubscriptionTestSupport {

    protected PersistenceAdapter createPersistenceAdapter() throws IOException {
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        jdbc.setCleanupPeriod(1000); // set up small cleanup period
        return jdbc;
    }

    public void testUnmatchedCleanedUp() throws Exception {

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("TestSelectorNoMatchCleanup");
        TopicSubscriber consumer = session.createDurableSubscriber(topic, "sub1", "color='red'", false);
        TopicSubscriber consumerNoMatch = session.createDurableSubscriber(topic, "sub2", "color='green'", false);
        MessageProducer producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();

        TextMessage msg = session.createTextMessage();
        msg.setText("Msg:1");
        msg.setStringProperty("color", "blue");
        producer.send(msg);
        msg.setText("Msg:2");
        msg.setStringProperty("color", "red");
        producer.send(msg);

        assertTextMessageEquals("Msg:2", consumer.receive(5000));

        assertNull(consumerNoMatch.receiveNoWait());

        // verify cleanup
        java.sql.Connection conn = ((JDBCPersistenceAdapter) broker.getPersistenceAdapter()).getDataSource().getConnection();
        PreparedStatement statement = conn.prepareStatement("SELECT ID FROM ACTIVEMQ_MSGS");
        ResultSet result = statement.executeQuery();
        printResults("MSGS", result);
        statement.close();

        statement = conn.prepareStatement("SELECT * FROM ACTIVEMQ_ACKS");
        result = statement.executeQuery();
        printResults("ACKS", result);
        statement.close();

        // run for each priority
        for (int i=0; i<10; i++) {
            ((JDBCPersistenceAdapter) broker.getPersistenceAdapter()).cleanup();
        }

        // after cleanup
        statement = conn.prepareStatement("SELECT ID FROM ACTIVEMQ_MSGS");
        result = statement.executeQuery();
        printResults("MSGS-AFTER", result);
        statement.close();

        statement = conn.prepareStatement("SELECT * FROM ACTIVEMQ_ACKS");
        result = statement.executeQuery();
        printResults("ACKS-AFTER", result);
        statement.close();


        // verify empty
        statement = conn.prepareStatement("SELECT * FROM ACTIVEMQ_MSGS");
        result = statement.executeQuery();
        assertFalse(result.next());

        conn.close();
    }

    private void printResults(String detail, ResultSet result) throws SQLException {
        System.out.println("**" + detail  + "**");
        ResultSetMetaData resultSetMetaData = result.getMetaData();
        int columnsNumber = resultSetMetaData.getColumnCount();
        while (result.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = result.getString(i);
                System.out.print(columnValue + " " + resultSetMetaData.getColumnName(i));
            }
            System.out.println();
        }
    }
}
