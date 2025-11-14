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

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collection;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.activemq.store.kahadb.JournalCorruptionEofIndexRecoveryTest.drain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JournalArchiveTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalArchiveTest.class);

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

        // speed up the test case, checkpoint an cleanup early and often
        adapter.setCheckpointInterval(2000);
        adapter.setCleanupInterval(2000);

        adapter.setCheckForCorruptJournalFiles(true);

        adapter.setArchiveDataLogs(true);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }


    @Test
    public void testRecoveryOnArchiveFailure() throws Exception {
        startBroker();
        final Path archivePath = prepareArchivePath();

        // configure a shutdown hook to detect broker shutdown on archive failure
        final CountDownLatch gotShutdown = new CountDownLatch(1);
        broker.addShutdownHook(new Runnable() {
            @Override
            public void run() {
                LOG.info("Broker shutdown hook called");
                gotShutdown.countDown();
            }
        });

        /*
         * It used to be using a fake security manager to simulate the failure but that approach is no longer viable
         * with modern JVMs. Instead, we will produce some messages to create multiple data files, then change the
         * archive directory permissions to read-only, to simulate the failure to archive. Then produce some more
         * messages to trigger the archive attempts.
         */
        final AtomicBoolean permissionsLocked = new AtomicBoolean();
        try {
            LOG.info("Producing messages to create multiple data files");
            int sent = produceMessagesToConsumeMultipleDataFiles(25);

            LOG.info("Number of journal files before archive failure: {}", getNumberOfJournalFiles());
            if (permissionsLocked.compareAndSet(false, true)) {
                try {
                    LOG.info("Making archive directory read-only to simulate archive failure: {}", archivePath);
                    makeReadOnly(archivePath);
                } catch (IOException e) {
                    permissionsLocked.set(false);
                    LOG.warn("Unable to lock archive directory {}", archivePath, e);
                }
            }

            LOG.info("Producing messages to trigger archive attempts");
            sent += produceMessagesToConsumeMultipleDataFiles(25);

            int numFilesAfterSend = getNumberOfJournalFiles();
            LOG.info("Num journal files: {}", numFilesAfterSend);

            assertTrue("more than x files: " + numFilesAfterSend, numFilesAfterSend > 4);

            int received = tryConsume(destination, sent);
            assertEquals("all message received", sent, received);
            assertTrue("broker got shutdown on page in error", gotShutdown.await(10, TimeUnit.SECONDS));

            // remove restrictions to see if it catches up again
            if (permissionsLocked.get()) {
                try {
                    makeWritable(archivePath);
                } catch (IOException e) {
                    LOG.warn("Unable to restore archive directory permissions: {}", archivePath, e);
                }
            }

            int numFilesAfterRestart = 0;
            try {
                // ensure we can restart after failure to archive
                doStartBroker(false);
                numFilesAfterRestart = getNumberOfJournalFiles();
                LOG.info("Num journal files before gc: {}", numFilesAfterRestart);

                // force gc
                ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().checkpoint(true);

            } catch (Exception error) {
                LOG.error("Failed to restart!", error);
                fail("Failed to restart after failure to archive");
            }
            int numFilesAfterGC = getNumberOfJournalFiles();
            LOG.info("Num journal files after restart nd gc: {}", numFilesAfterGC);
            assertTrue("Gc has happened", numFilesAfterGC < numFilesAfterRestart);
            assertTrue("Gc has worked", numFilesAfterGC < 4);

            File archiveDirectory = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore()
                                                                                               .getJournal()
                                                                                               .getDirectoryArchive();
            assertEquals("verify files in archive dir", numFilesAfterSend, archiveDirectory.listFiles().length);
        } finally {
            if (permissionsLocked.get()) {
                try {
                    makeWritable(archivePath);
                } catch (IOException e) {
                    LOG.warn("Unable to restore archive directory permissions: {}", archivePath, e);
                }
            }
        }
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

    private Path prepareArchivePath() throws IOException {
        File archiveDirectory = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getDirectoryArchive();
        Files.createDirectories(archiveDirectory.toPath());
        return archiveDirectory.toPath();
    }

    private void makeReadOnly(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
        } catch (UnsupportedOperationException ex) {
            if (!path.toFile().setWritable(false, false)) {
                throw new IOException("Unable to mark directory as read-only: " + path);
            }
        }
    }

    private void makeWritable(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path, EnumSet.of(PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
        } catch (UnsupportedOperationException ex) {
            if (!path.toFile().setWritable(true, false)) {
                throw new IOException("Unable to restore write permissions to directory: " + path);
            }
        }
    }
}
