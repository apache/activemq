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

import static org.apache.activemq.store.kahadb.JournalCorruptionEofIndexRecoveryTest.drain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class JournalCorruptionIndexRecoveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalCorruptionIndexRecoveryTest.class);

    private final String KAHADB_DIRECTORY = "target/activemq-data/";
    private final String payload = new String(new byte[1024]);

    private ActiveMQConnectionFactory cf = null;
    private BrokerService broker = null;
    private final Destination destination = new ActiveMQQueue("Test");
    private String connectionUri;
    private KahaDBPersistenceAdapter adapter;

    @Parameterized.Parameter(0)
    public byte fill = Byte.valueOf("3");

    @Parameterized.Parameters(name = "fill=#{0}")
    public static Iterable<Object[]> parameters() {
        // corruption can be valid record type values
        return Arrays.asList(new Object[][]{{Byte.valueOf("1")}, {Byte.valueOf("0")}, {Byte.valueOf("2")}, {Byte.valueOf("-1")} });
    }

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
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(delete);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setDataDirectory(KAHADB_DIRECTORY);
        broker.addConnector("tcp://localhost:0");

        configurePersistence(broker);

        connectionUri = "vm://localhost?create=false";
        cf = new ActiveMQConnectionFactory(connectionUri);

        broker.start();
        LOG.info("Starting broker..");
    }

    protected void configurePersistence(BrokerService brokerService) throws Exception {
        adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        // ensure there are a bunch of data files but multiple entries in each
        adapter.setJournalMaxFileLength(1024 * 20);

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

    @Test
    public void testRecoveryAfterCorruptionMiddle() throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();

        assertTrue("more than x files: " + numFiles, numFiles > 4);

        corruptBatchMiddle(3);

        restartBroker();

        assertEquals("missing one message", 49, broker.getAdminView().getTotalMessageCount());
        assertEquals("Drain", 49, drainQueue(49));
    }

    @Test
    public void testRecoveryAfterCorruptionEnd() throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();

        assertTrue("more than x files: " + numFiles, numFiles > 4);

        corruptBatchEnd(4);

        restartBroker();

        assertEquals("missing one message", 49, broker.getAdminView().getTotalMessageCount());
        assertEquals("Drain", 49, drainQueue(49));
    }

    @Test
    public void testRecoveryAfterCorruption() throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();

        assertTrue("more than x files: " + numFiles, numFiles > 4);

        corruptBatchMiddle(3);
        corruptBatchEnd(4);

        restartBroker();

        assertEquals("missing one message", 48, broker.getAdminView().getTotalMessageCount());
        assertEquals("Drain", 48, drainQueue(48));
    }

    private void whackIndex(File dataDir) {
        File indexToDelete = new File(dataDir, "db.data");
        LOG.info("Whacking index: " + indexToDelete);
        indexToDelete.delete();
    }

    private void corruptBatchMiddle(int i) throws IOException {
        corruptBatch(i, false);
    }

    private void corruptBatchEnd(int i) throws IOException {
        corruptBatch(i, true);
    }

    private void corruptBatch(int id, boolean atEnd) throws IOException {

        Collection<DataFile> files = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().values();
        DataFile dataFile = (DataFile) files.toArray()[id];

        RecoverableRandomAccessFile randomAccessFile = dataFile.openRandomAccessFile();

        final ByteSequence header = new ByteSequence(Journal.BATCH_CONTROL_RECORD_HEADER);
        byte data[] = new byte[1024 * 20];

        ByteSequence bs = new ByteSequence(data, 0, randomAccessFile.read(data, 0, data.length));

        int pos = 0;
        int offset = 0;
        int end = atEnd ? Integer.MAX_VALUE : 3;
        for (int i = 0; i < end; i++) {
            int found = bs.indexOf(header, pos);
            if (found == -1) {
                break;
            }
            offset = found;
            pos++;
        }

        LOG.info("Whacking batch record in file:" + id + ", at offset: " + offset + " with fill:" + fill);
        // whack that record
        byte[] bla = new byte[Journal.BATCH_CONTROL_RECORD_HEADER.length];
        Arrays.fill(bla, fill);
        randomAccessFile.seek(offset);
        randomAccessFile.write(bla, 0, bla.length);
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

    private int produceMessagesToConsumeMultipleDataFiles(int numToSend) throws Exception {
        return produceMessages(destination, numToSend);
    }

    private Message createMessage(Session session, int i) throws Exception {
        return session.createTextMessage(payload + "::" + i);
    }

    private int drainQueue(int max) throws Exception {
        return drain(cf, destination, max);
    }

}
