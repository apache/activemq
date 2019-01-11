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

import java.io.File;
import java.io.IOException;
import java.security.ProtectionDomain;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IOHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chirino
 */
public class KahaDBVersionTest extends TestCase {
    static String basedir;
    static {
        try {
            ProtectionDomain protectionDomain = KahaDBVersionTest.class.getProtectionDomain();
            basedir = new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalPath();
        } catch (IOException e) {
            basedir = ".";
        }
    }

    static final Logger LOG = LoggerFactory.getLogger(KahaDBVersionTest.class);
    final static File VERSION_1_DB = new File(basedir + "/src/test/resources/org/apache/activemq/store/kahadb/KahaDBVersion1");
    final static File VERSION_2_DB = new File(basedir + "/src/test/resources/org/apache/activemq/store/kahadb/KahaDBVersion2");
    final static File VERSION_3_DB = new File(basedir + "/src/test/resources/org/apache/activemq/store/kahadb/KahaDBVersion3");
    final static File VERSION_4_DB = new File(basedir + "/src/test/resources/org/apache/activemq/store/kahadb/KahaDBVersion4");
    final static File VERSION_5_DB = new File(basedir + "/src/test/resources/org/apache/activemq/store/kahadb/KahaDBVersion5");
    final static File VERSION_6_DB = new File(basedir + "/src/test/resources/org/apache/activemq/store/kahadb/KahaDBVersion6");


    BrokerService broker = null;

    protected BrokerService createBroker(KahaDBPersistenceAdapter kaha) throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistenceAdapter(kaha);
        broker.start();
        return broker;
    }

    @Override
    protected void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    public void XtestCreateStore() throws Exception {
        KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
        File dir = new File("src/test/resources/org/apache/activemq/store/kahadb/KahaDBVersion5");
        IOHelper.deleteFile(dir);
        kaha.setDirectory(dir);
        kaha.setJournalMaxFileLength(1024 * 1024);
        BrokerService broker = createBroker(kaha);
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        connection.setClientID("test");
        connection.start();
        producerSomeMessages(connection, 1000);
        connection.close();
        broker.stop();
    }

    private void producerSomeMessages(Connection connection, int numToSend) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.topic");
        Queue queue = session.createQueue("test.queue");
        MessageConsumer consumer = session.createDurableSubscriber(topic, "test");
        consumer.close();
        MessageProducer producer = session.createProducer(topic);
        producer.setPriority(9);
        for (int i = 0; i < numToSend; i++) {
            Message msg = session.createTextMessage("test message:" + i);
            producer.send(msg);
        }
        LOG.info("sent " + numToSend + " to topic");
        producer = session.createProducer(queue);
        for (int i = 0; i < numToSend; i++) {
            Message msg = session.createTextMessage("test message:" + i);
            producer.send(msg);
        }
        LOG.info("sent " + numToSend + " to queue");
    }

    public void testVersion1Conversion() throws Exception {
        doConvertRestartCycle(VERSION_1_DB);
    }

    public void testVersion2Conversion() throws Exception {
        doConvertRestartCycle(VERSION_2_DB);
    }

    public void testVersion3Conversion() throws Exception {
        doConvertRestartCycle(VERSION_3_DB);
    }

    public void testVersion4Conversion() throws Exception {
        doConvertRestartCycle(VERSION_4_DB);
    }

    public void testVersion5Conversion() throws Exception {
        doConvertRestartCycle(VERSION_5_DB);
    }

    public void testVersion6Conversion() throws Exception {
        doConvertRestartCycle(VERSION_6_DB);
    }

    public void doConvertRestartCycle(File existingStore) throws Exception {

        File testDir = new File("target/activemq-data/kahadb/versionDB");
        IOHelper.deleteFile(testDir);
        IOHelper.copyFile(existingStore, testDir);
        final int numToSend = 1000;

        // on repeat store will be upgraded
        for (int repeats = 0; repeats < 3; repeats++) {
            KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
            kaha.setDirectory(testDir);
            kaha.setJournalMaxFileLength(1024 * 1024);
            BrokerService broker = createBroker(kaha);
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
            Connection connection = cf.createConnection();
            connection.setClientID("test");
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("test.topic");
            Queue queue = session.createQueue("test.queue");

            if (repeats > 0) {
                // upgraded store will be empty so generated some more messages
                producerSomeMessages(connection, numToSend);
            }

            MessageConsumer queueConsumer = session.createConsumer(queue);
            int count = 0;
            for (int i = 0; i < (repeats == 0 ? 1000 : numToSend); i++) {
                TextMessage msg = (TextMessage) queueConsumer.receive(10000);
                count++;
                // System.err.println(msg.getText());
                assertNotNull(msg);
            }
            LOG.info("Consumed " + count + " from queue");
            count = 0;
            MessageConsumer topicConsumer = session.createDurableSubscriber(topic, "test");
            for (int i = 0; i < (repeats == 0 ? 1000 : numToSend); i++) {
                TextMessage msg = (TextMessage) topicConsumer.receive(10000);
                count++;
                // System.err.println(msg.getText());
                assertNotNull("" + count, msg);
            }
            LOG.info("Consumed " + count + " from topic");
            connection.close();

            broker.stop();
        }
    }
}
