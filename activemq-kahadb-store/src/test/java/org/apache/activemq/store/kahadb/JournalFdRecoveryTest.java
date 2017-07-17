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
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.Attribute;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.activemq.store.kahadb.JournalCorruptionEofIndexRecoveryTest.drain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class JournalFdRecoveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalFdRecoveryTest.class);

    private final String KAHADB_DIRECTORY = "target/activemq-data/";
    private String payload;
    private ActiveMQConnectionFactory cf = null;
    private BrokerService broker = null;
    private final Destination destination = new ActiveMQQueue("Test");
    private String connectionUri;
    private KahaDBPersistenceAdapter adapter;

    public byte fill = Byte.valueOf("3");
    private int maxJournalSizeBytes;

    protected void startBroker() throws Exception {
        doStartBroker(true);
    }

    protected void restartBroker() throws Exception {
        File dataDir = broker.getPersistenceAdapter().getDirectory();

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }

        whackIndex(dataDir);

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
        broker.addConnector("tcp://localhost:0");

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setUseCache(false);
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);

        configurePersistence(broker);

        connectionUri = "vm://localhost?create=false";
        cf = new ActiveMQConnectionFactory(connectionUri);
    }

    protected void configurePersistence(BrokerService brokerService) throws Exception {
        adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        // ensure there are a bunch of data files but multiple entries in each
        adapter.setJournalMaxFileLength(maxJournalSizeBytes);

        // speed up the test case, checkpoint an cleanup early and often
        adapter.setCheckpointInterval(5000);
        adapter.setCleanupInterval(5000);

        adapter.setCheckForCorruptJournalFiles(true);
        adapter.setIgnoreMissingJournalfiles(true);

        adapter.setPreallocationScope(Journal.PreallocationScope.ENTIRE_JOURNAL_ASYNC.name());
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Before
    public void initPayLoad() {
        payload = new String(new byte[1024]);
        maxJournalSizeBytes = 1024 * 20;
    }


    @Test
    public void testStopOnPageInIOError() throws Exception {
        startBroker();

        int sent = produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();
        LOG.info("Num journal files: " + numFiles);

        assertTrue("more than x files: " + numFiles, numFiles > 4);

        File dataDir = broker.getPersistenceAdapter().getDirectory();

        for (int i=2;i<4;i++) {
            whackDataFile(dataDir, i);
        }

        final CountDownLatch gotShutdown = new CountDownLatch(1);
        broker.addShutdownHook(new Runnable() {
            @Override
            public void run() {
                gotShutdown.countDown();
            }
        });

        int received = tryConsume(destination, sent);
        assertNotEquals("not all message received", sent, received);
        assertTrue("broker got shutdown on page in error", gotShutdown.await(5, TimeUnit.SECONDS));
    }

    private void whackDataFile(File dataDir, int i) throws Exception {
        whackFile(dataDir, "db-" + i + ".log");
    }

    @Test
    public void testRecoveryAfterCorruption() throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();
        LOG.info("Num journal files: " + numFiles);

        assertTrue("more than x files: " + numFiles, numFiles > 4);

        File dataDir = broker.getPersistenceAdapter().getDirectory();

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        long afterStop = totalOpenFileDescriptorCount(broker);
        whackIndex(dataDir);

        LOG.info("Num Open files with broker stopped: " + afterStop);

        doStartBroker(false);

        LOG.info("Journal read pool: " + adapter.getStore().getJournal().getAccessorPool().size());

        assertEquals("one entry in the pool on start", 1, adapter.getStore().getJournal().getAccessorPool().size());

        long afterRecovery = totalOpenFileDescriptorCount(broker);
        LOG.info("Num Open files with broker recovered: " + afterRecovery);

    }

    @Test
    public void testRecoveryWithMissingMssagesWithValidAcks() throws Exception {

        doCreateBroker(true);
        adapter.setCheckpointInterval(50000);
        adapter.setCleanupInterval(50000);
        broker.start();

        int toSend = 50;
        produceMessagesToConsumeMultipleDataFiles(toSend);

        int numFiles = getNumberOfJournalFiles();
        LOG.info("Num files: " + numFiles);
        assertTrue("more than x files: " + numFiles, numFiles > 5);
        assertEquals("Drain", 30, tryConsume(destination, 30));

        LOG.info("Num files after stopped: " + getNumberOfJournalFiles());

        File dataDir = broker.getPersistenceAdapter().getDirectory();
        broker.stop();
        broker.waitUntilStopped();

        whackDataFile(dataDir, 4);

        whackIndex(dataDir);

        doStartBroker(false);

        LOG.info("Num files after restarted: " + getNumberOfJournalFiles());

        assertEquals("Empty?", 18, tryConsume(destination, 20));

        assertEquals("no queue size ", 0l,  ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().getMessages().getCount());

    }

    @Test
    public void testRecoveryCheckSpeedSmallMessages() throws Exception {
        maxJournalSizeBytes = Journal.DEFAULT_MAX_FILE_LENGTH;
        doCreateBroker(true);
        broker.start();

        int toSend = 20000;
        payload = new String(new byte[100]);
        produceMessagesToConsumeMultipleDataFiles(toSend);

        broker.stop();
        broker.waitUntilStopped();

        Instant b = Instant.now();
        doStartBroker(false);
        Instant e = Instant.now();

        Duration timeElapsed = Duration.between(b, e);
        LOG.info("Elapsed: " + timeElapsed);
    }

    private long totalOpenFileDescriptorCount(BrokerService broker) {
        long result = 0;
        try {
            javax.management.AttributeList list = broker.getManagementContext().getMBeanServer().getAttributes(new ObjectName("java.lang:type=OperatingSystem"), new String[]{"OpenFileDescriptorCount"});
            if (!list.isEmpty()) {
                result = ((Long) ((Attribute) list.get(0)).getValue());
            }
        } catch (Exception ignored) {
        }

        return result;
    }

    private void whackIndex(File dataDir) throws Exception {
        whackFile(dataDir, "db.data");
    }

    private void whackFile(File dataDir, String name) throws Exception {
        File indexToDelete = new File(dataDir, name);
        LOG.info("Whacking index: " + indexToDelete);
        indexToDelete.delete();
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
        Connection connection = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri()).createConnection();
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
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri());
        return  drain(cf, destination, numToGet);
    }

    private int produceMessagesToConsumeMultipleDataFiles(int numToSend) throws Exception {
        return produceMessages(destination, numToSend);
    }

    private Message createMessage(Session session, int i) throws Exception {
        return session.createTextMessage(payload + "::" + i);
    }
}
