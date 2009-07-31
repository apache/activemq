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
package org.apache.activemq.store.kahadb;

import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;
import java.io.File;
import java.io.IOException;

/**
 * @author chirino
 */
public class KahaDBTest extends TestCase {

    protected BrokerService createBroker(KahaDBStore kaha) throws Exception {

        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistenceAdapter(kaha);
        broker.start();
        return broker;

    }

    private KahaDBStore createStore(boolean delete) throws IOException {
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        if( delete ) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    public void testIgnoreMissingJournalfilesOptionSetFalse() throws Exception {
        KahaDBStore kaha = createStore(true);
        kaha.setJournalMaxFileLength(1024*100);
        assertFalse(kaha.isIgnoreMissingJournalfiles());
        BrokerService broker = createBroker(kaha);
        sendMessages(1000);
        broker.stop();

        // Delete some journal files..
        assertExistsAndDelete(new File(kaha.getDirectory(), "db-4.log"));
        assertExistsAndDelete(new File(kaha.getDirectory(), "db-8.log"));

        kaha = createStore(false);
        kaha.setJournalMaxFileLength(1024*100);
        assertFalse(kaha.isIgnoreMissingJournalfiles());
        try {
            broker = createBroker(kaha);
            fail("expected IOException");
        } catch (IOException e) {
            assertTrue( e.getMessage().startsWith("Detected missing journal files") );
        }

    }


    public void testIgnoreMissingJournalfilesOptionSetTrue() throws Exception {
        KahaDBStore kaha = createStore(true);
        kaha.setJournalMaxFileLength(1024*100);
        assertFalse(kaha.isIgnoreMissingJournalfiles());
        BrokerService broker = createBroker(kaha);
        sendMessages(1000);
        broker.stop();

        // Delete some journal files..
        assertExistsAndDelete(new File(kaha.getDirectory(), "db-4.log"));
        assertExistsAndDelete(new File(kaha.getDirectory(), "db-8.log"));

        kaha = createStore(false);
        kaha.setIgnoreMissingJournalfiles(true);
        kaha.setJournalMaxFileLength(1024*100);
        broker = createBroker(kaha);

        // We know we won't get all the messages but we should get most of them.
        int count = receiveMessages();
        assertTrue( count > 800 ); 
        assertTrue( count < 1000 );

        broker.stop();
    }

    private void assertExistsAndDelete(File file) {
        assertTrue(file.exists());
        file.delete();
        assertFalse(file.exists());
    }

    private void sendMessages(int count) throws JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(new ActiveMQQueue("TEST"));
            for (int i = 0; i < count; i++) {
                producer.send(session.createTextMessage(createContent(i)));
            }
        } finally {
            connection.close();
        }
    }

    private int receiveMessages() throws JMSException {
        int rc=0;
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(new ActiveMQQueue("TEST"));
            while ( messageConsumer.receive(1000) !=null ) {
                rc++;
            }
            return rc;
        } finally {
            connection.close();
        }
    }

    private String createContent(int i) {
        StringBuilder sb = new StringBuilder(i+":");
        while( sb.length() < 1024 ) {
            sb.append("*");
        }
        return sb.toString();
    }

}