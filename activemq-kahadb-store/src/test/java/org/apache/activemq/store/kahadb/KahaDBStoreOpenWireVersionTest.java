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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.kahadb.MessageDatabase.Metadata;
import org.apache.activemq.store.kahadb.MessageDatabase.StoredDestination;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KahaDBStoreOpenWireVersionTest {

    private static final Logger LOG = LoggerFactory.getLogger(KahaDBStoreOpenWireVersionTest.class);

    private final String KAHADB_DIRECTORY_BASE = "./target/activemq-data/";
    private final int NUM_MESSAGES = 10;

    private BrokerService broker = null;
    private String storeDir;

    @Rule public TestName name = new TestName();

    protected BrokerService createBroker(int storeOpenWireVersion) throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setDataDirectory(storeDir);
        broker.setStoreOpenWireVersion(storeOpenWireVersion);
        ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).setCheckForCorruptJournalFiles(true);

        broker.start();
        broker.waitUntilStarted();

        return broker;
    }

    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Before
    public void setUp() throws Exception {
        LOG.info("=============== Starting test {} ================", name.getMethodName());
        storeDir = KAHADB_DIRECTORY_BASE + name.getMethodName();
    }

    @After
    public void tearDown() throws Exception {
        File brokerStoreDir = new File(KAHADB_DIRECTORY_BASE);

        if (broker != null) {
            brokerStoreDir = broker.getPersistenceAdapter().getDirectory();
            stopBroker();
        }

        IOHelper.deleteChildren(brokerStoreDir);
        IOHelper.delete(brokerStoreDir);

        LOG.info("=============== Finished test {} ================", name.getMethodName());
    }

    @Test(timeout = 60000)
    public void testConfiguredVersionWorksOnReload() throws Exception {
        final int INITIAL_STORE_VERSION = OpenWireFormat.DEFAULT_STORE_VERSION - 1;
        final int RELOAD_STORE_VERSION = OpenWireFormat.DEFAULT_STORE_VERSION - 1;

        doTestStoreVersionConfigrationOverrides(INITIAL_STORE_VERSION, RELOAD_STORE_VERSION);
    }

    @Test(timeout = 60000)
    public void testOlderVersionWorksWithDefaults() throws Exception {
        final int INITIAL_STORE_VERSION = OpenWireFormat.DEFAULT_LEGACY_VERSION;
        final int RELOAD_STORE_VERSION = OpenWireFormat.DEFAULT_STORE_VERSION;

        doTestStoreVersionConfigrationOverrides(INITIAL_STORE_VERSION, RELOAD_STORE_VERSION);
    }

    @Test(timeout = 60000)
    public void testNewerVersionWorksWhenOlderIsConfigured() throws Exception {
        final int INITIAL_STORE_VERSION = OpenWireFormat.DEFAULT_STORE_VERSION;
        final int RELOAD_STORE_VERSION = OpenWireFormat.DEFAULT_LEGACY_VERSION;

        doTestStoreVersionConfigrationOverrides(INITIAL_STORE_VERSION, RELOAD_STORE_VERSION);
    }

    /**
     * This test shows that a corrupted index/rebuild will still
     * honor the storeOpenWireVersion set on the BrokerService.
     * This wasn't the case before AMQ-6082
     */
    @Test(timeout = 60000)
    public void testStoreVersionCorrupt() throws Exception {
        final int create = 6;
        final int reload = 6;

        createBroker(create);
        populateStore();

        //blow up the index so it has to be recreated
        corruptIndex();
        stopBroker();

        createBroker(reload);
        assertEquals(create, broker.getStoreOpenWireVersion());
        assertStoreIsUsable();
    }


    private void corruptIndex() throws IOException {
        KahaDBStore store = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore();
        final PageFile pageFile = store.getPageFile();
        final Metadata metadata = store.metadata;

        //blow up the index
        try {
            store.indexLock.writeLock().lock();
            pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    for (Iterator<Entry<String, StoredDestination>> iterator = metadata.destinations.iterator(tx); iterator
                            .hasNext();) {
                        Entry<String, StoredDestination> entry = iterator.next();
                        entry.getValue().orderIndex.nextMessageId = -100;
                        entry.getValue().orderIndex.defaultPriorityIndex.clear(tx);
                        entry.getValue().orderIndex.lowPriorityIndex.clear(tx);
                        entry.getValue().orderIndex.highPriorityIndex.clear(tx);
                        entry.getValue().messageReferences.clear();
                    }
                }
            });
        } finally {
            store.indexLock.writeLock().unlock();
        }
    }

    private void doTestStoreVersionConfigrationOverrides(int create, int reload) throws Exception {
        createBroker(create);
        populateStore();
        stopBroker();

        createBroker(reload);
        assertEquals(create, broker.getStoreOpenWireVersion());
        assertStoreIsUsable();
    }

    private void populateStore() throws Exception {

        ConnectionFactory factory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        Connection connection = factory.createConnection();
        connection.setClientID("test");

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.topic");
        Queue queue = session.createQueue("test.queue");
        MessageConsumer consumer = session.createDurableSubscriber(topic, "test");
        consumer.close();

        MessageProducer producer = session.createProducer(topic);
        producer.setPriority(9);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            Message msg = session.createTextMessage("test message:" + i);
            producer.send(msg);
        }
        LOG.info("sent {} to topic", NUM_MESSAGES);

        producer = session.createProducer(queue);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            Message msg = session.createTextMessage("test message:" + i);
            producer.send(msg);
        }
        LOG.info("sent {} to topic", NUM_MESSAGES);

        connection.close();
    }

    private void assertStoreIsUsable() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        connection.setClientID("test");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.topic");
        Queue queue = session.createQueue("test.queue");

        MessageConsumer queueConsumer = session.createConsumer(queue);
        for (int i = 0; i < NUM_MESSAGES; ++i) {
            TextMessage received = (TextMessage) queueConsumer.receive(1000);
            assertNotNull(received);
        }
        LOG.info("Consumed {} from queue", NUM_MESSAGES);

        MessageConsumer topicConsumer = session.createDurableSubscriber(topic, "test");
        for (int i = 0; i < NUM_MESSAGES; ++i) {
            TextMessage received = (TextMessage) topicConsumer.receive(1000);
            assertNotNull(received);
        }
        LOG.info("Consumed {} from topic", NUM_MESSAGES);

        connection.close();
    }
}
