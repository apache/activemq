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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KahaDBIndexLocationTest {

    private static final Logger LOG = LoggerFactory.getLogger(KahaDBIndexLocationTest.class);

    @Rule public TestName name = new TestName();

    private BrokerService broker;

    private final File testDataDir = new File("target/activemq-data/QueuePurgeTest");
    private final File kahaDataDir = new File(testDataDir, "kahadb");
    private final File kahaIndexDir = new File(testDataDir, "kahadb/index");

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        startBroker();
    }

    @After
    public void tearDown() throws Exception {
        stopBroker();
    }

    private void startBroker() throws Exception {
        createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    private void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private void restartBroker() throws Exception {
        stopBroker();
        createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    private void createBroker() throws Exception {
        broker = new BrokerService();

        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(kahaDataDir);
        persistenceAdapter.setIndexDirectory(kahaIndexDir);
        persistenceAdapter.setPreallocationScope(Journal.PreallocationScope.ENTIRE_JOURNAL.name());

        broker.setDataDirectoryFile(testDataDir);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistenceAdapter(persistenceAdapter);
    }

    @Test
    public void testIndexDirExists() throws Exception {
        LOG.info("Index dir is configured as: {}", kahaIndexDir);
        assertTrue(kahaDataDir.exists());
        assertTrue(kahaIndexDir.exists());

        String[] index = kahaIndexDir.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                LOG.info("Testing filename: {}", name);
                return name.endsWith("data") || name.endsWith("redo");
            }
        });

        String[] journal = kahaDataDir.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                LOG.info("Testing filename: {}", name);
                return name.endsWith("log") || name.equals("lock");
            }
        });

        produceMessages();

        // Should be db.data and db.redo and nothing else.
        assertNotNull(index);
        assertEquals(2, index.length);

        // Should contain the initial log for the journal and the lock.
        assertNotNull(journal);
        assertEquals(2, journal.length);
    }

    @Test
    public void testRestartWithDeleteWorksWhenIndexIsSeparate() throws Exception {
        produceMessages();
        restartBroker();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?create=false");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);
        assertNull(consumer.receive(2000));
    }

    private void produceMessages() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?create=false");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < 5; ++i) {
            producer.send(session.createTextMessage("test:" + i));
        }
        connection.close();
    }
}
