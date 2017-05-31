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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

import static org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter.nameFromDestinationFilter;
import static org.junit.Assert.*;

public class MKahaDBIndexLocationTest {

    private static final Logger LOG = LoggerFactory.getLogger(MKahaDBIndexLocationTest.class);

    private BrokerService broker;

    private final File testDataDir = new File("target/activemq-data/ConfigIndexDir");
    private final File kahaDataDir = new File(testDataDir, "log");
    private final File kahaIndexDir = new File(testDataDir, "index");
    private final ActiveMQQueue queue = new ActiveMQQueue("Qq");

    @Before
    public void startBroker() throws Exception {
        createBroker();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private void createBroker() throws Exception {
        broker = new BrokerService();

        // setup multi-kaha adapter
        MultiKahaDBPersistenceAdapter persistenceAdapter = new MultiKahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(kahaDataDir);

        KahaDBPersistenceAdapter kahaStore = new KahaDBPersistenceAdapter();
        kahaStore.setJournalMaxFileLength(1024 * 512);
        kahaStore.setIndexDirectory(kahaIndexDir);

        // set up a store per destination
        FilteredKahaDBPersistenceAdapter filtered = new FilteredKahaDBPersistenceAdapter();
        filtered.setPersistenceAdapter(kahaStore);
        filtered.setPerDestination(true);
        List<FilteredKahaDBPersistenceAdapter> stores = new ArrayList<>();
        stores.add(filtered);

        persistenceAdapter.setFilteredPersistenceAdapters(stores);
        broker.setPersistenceAdapter(persistenceAdapter);

        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(false);
        broker.setPersistenceAdapter(persistenceAdapter);
    }

    @Test
    public void testIndexDirExists() throws Exception {

        produceMessages();

        LOG.info("Index dir is configured as: {}", kahaIndexDir);
        assertTrue(kahaDataDir.exists());
        assertTrue(kahaIndexDir.exists());


        String destName = nameFromDestinationFilter(queue);
        String[] index = new File(kahaIndexDir, destName).list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                LOG.info("Testing index filename: {}", name);
                return name.endsWith("data") || name.endsWith("redo");
            }
        });

        String[] journal = new File(kahaDataDir, destName).list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                LOG.info("Testing log filename: {}", name);
                return name.endsWith("log") || name.equals("lock");
            }
        });


        // Should be db.data and db.redo and nothing else.
        assertNotNull(index);
        assertEquals(2, index.length);

        // Should contain the initial log for the journal
        assertNotNull(journal);
        assertEquals(1, journal.length);

        stopBroker();
        createBroker();
        broker.start();
        broker.waitUntilStarted();

        consume();
    }

    private void consume() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?create=false");
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        for (int i = 0; i < 5; ++i) {
            assertNotNull("message[" + i + "]", consumer.receive(4000));
        }
        connection.close();
    }

    private void produceMessages() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?create=false");
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < 5; ++i) {
            producer.send(session.createTextMessage("test:" + i));
        }
        connection.close();
    }
}
