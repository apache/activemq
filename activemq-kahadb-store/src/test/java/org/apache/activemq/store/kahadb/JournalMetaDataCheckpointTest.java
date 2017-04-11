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
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Page;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.IOException;
import java.util.Collection;

import static org.apache.activemq.store.kahadb.JournalCorruptionEofIndexRecoveryTest.drain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JournalMetaDataCheckpointTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalMetaDataCheckpointTest.class);

    private final String KAHADB_DIRECTORY = "target/activemq-data/";
    private final String payload = new String(new byte[1024]);

    private BrokerService broker = null;
    private final Destination destination = new ActiveMQQueue("Test");
    private KahaDBPersistenceAdapter adapter;

    protected void startBroker() throws Exception {
        doStartBroker(true);
    }

    protected void restartBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }

        doStartBroker(false);
    }

    private void doStartBroker(boolean delete) throws Exception {
        doCreateBroker(delete);
        LOG.info("Starting broker..");
        broker.start();
    }

    private void doCreateBroker(boolean delete) throws Exception {

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(delete);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setDataDirectory(KAHADB_DIRECTORY);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setUseCache(false);
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);

        configurePersistence(broker);
    }

    protected void configurePersistence(BrokerService brokerService) throws Exception {
        adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        // ensure there are a bunch of data files but multiple entries in each
        adapter.setJournalMaxFileLength(1024 * 20);

        // manual cleanup
        adapter.setCheckpointInterval(0);
        adapter.setCleanupInterval(0);

        adapter.setCheckForCorruptJournalFiles(true);

    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testRecoveryOnDeleteFailureMetaDataOk() throws Exception {
        startBroker();

        int sent = produceMessagesToConsumeMultipleDataFiles(50);

        int numFilesAfterSend = getNumberOfJournalFiles();
        LOG.info("Sent {}, Num journal files: {} ", sent, numFilesAfterSend);

        assertTrue("more than x files: " + numFilesAfterSend, numFilesAfterSend > 4);


        int received = tryConsume(destination, sent/2);
        assertEquals("all message received", sent/2, received);


        int numFilesAfterRestart = getNumberOfJournalFiles();
        LOG.info("Num journal files before gc: " + numFilesAfterRestart);

        // force gc
        ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().checkpoint(true);

        int numFilesAfterGC = getNumberOfJournalFiles();
        assertEquals("all message received", sent/2, received);
        LOG.info("Num journal files after restart nd gc: " + numFilesAfterGC);
        assertTrue("Gc has happened", numFilesAfterGC < numFilesAfterRestart);

        // verify metadata is correct on disk
        final MessageDatabase.Metadata[] fromDiskMetaData = new MessageDatabase.Metadata[1];
        final KahaDBStore messageStore = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore();

        // need to avoid cache and in-progress writes of existing pageFile
        PageFile fromDiskPageFile = new PageFile(messageStore.getIndexDirectory(), "db");
        fromDiskPageFile.setEnablePageCaching(false);
        fromDiskPageFile.setEnableRecoveryFile(false);
        fromDiskPageFile.load();
        fromDiskPageFile.tx().execute(new Transaction.Closure<IOException>() {
            @Override
            public void execute(Transaction tx) throws IOException {
                    Page<MessageDatabase.Metadata> page = tx.load(0, messageStore.metadataMarshaller);
                fromDiskMetaData[0] = page.get();
            }
        });

        assertEquals("location is uptodate", messageStore.getMetadata().ackMessageFileMapLocation, fromDiskMetaData[0].ackMessageFileMapLocation);
    }

    @Ignore("needs work")
    public void testAckMessageFileMapSyncOnModOnly() throws Exception {
        startBroker();
        // force gc
        ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().checkpoint(true);

        KahaDBStore messageStore = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore();

        Location ackMessageFileModLoc =  messageStore.getMetadata().ackMessageFileMapLocation;
        // force gc
        ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().checkpoint(true);

        assertEquals("location is not changed on no modification", ackMessageFileModLoc, messageStore.getMetadata().ackMessageFileMapLocation);
    }

    private int getNumberOfJournalFiles() throws IOException {
        Collection<DataFile> files = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().values();
        int reality = 0;
        for (DataFile file : files) {
            if (file != null) {
                reality++;
            }
        }
        return reality;
    }

    private int produceMessages(Destination destination, int numToSend) throws Exception {
        int sent = 0;
        Connection connection = new ActiveMQConnectionFactory(broker.getVmConnectorURI()).createConnection();
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(destination);
            for (int i = 0; i < numToSend; i++) {
                producer.send(createMessage(session, i));
                sent++;
            }
        } finally {
            connection.close();
        }

        return sent;
    }

    private int tryConsume(Destination destination, int numToGet) throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        return  drain(cf, destination, numToGet);
    }

    private int produceMessagesToConsumeMultipleDataFiles(int numToSend) throws Exception {
        return produceMessages(destination, numToSend);
    }

    private Message createMessage(Session session, int i) throws Exception {
        return session.createTextMessage(payload + "::" + i);
    }
}
