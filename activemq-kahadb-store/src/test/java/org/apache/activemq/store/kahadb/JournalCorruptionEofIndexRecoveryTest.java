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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JournalCorruptionEofIndexRecoveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalCorruptionEofIndexRecoveryTest.class);

    private ActiveMQConnectionFactory cf = null;
    private BrokerService broker = null;
    private String connectionUri;
    private KahaDBPersistenceAdapter adapter;

    private final Destination destination = new ActiveMQQueue("Test");
    private final String KAHADB_DIRECTORY = "target/activemq-data/";
    private final String payload = new String(new byte[1024]);

    protected void startBroker() throws Exception {
        doStartBroker(true, false);
    }

    protected void restartBroker(boolean whackIndex) throws Exception {
        restartBroker(whackIndex, false);
    }

    protected void restartBroker(boolean whackIndex, boolean forceRecoverIndex) throws Exception {
        File dataDir = broker.getPersistenceAdapter().getDirectory();
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }

        if (whackIndex) {
            File indexToDelete = new File(dataDir, "db.data");
            LOG.info("Whacking index: " + indexToDelete);
            indexToDelete.delete();
        }

        doStartBroker(false, forceRecoverIndex);
    }


    private void doStartBroker(boolean delete, boolean forceRecoverIndex) throws Exception {
        broker = new BrokerService();
        broker.setDataDirectory(KAHADB_DIRECTORY);

        if (delete) {
            IOHelper.deleteChildren(broker.getPersistenceAdapter().getDirectory());
            IOHelper.delete(broker.getPersistenceAdapter().getDirectory());
        }

        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:0");

        configurePersistence(broker, forceRecoverIndex);

        connectionUri = "vm://localhost?create=false";
        cf = new ActiveMQConnectionFactory(connectionUri);

        broker.start();
        LOG.info("Starting broker..");
    }

    protected void configurePersistence(BrokerService brokerService, boolean forceRecoverIndex) throws Exception {
        adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        adapter.setForceRecoverIndex(forceRecoverIndex);

        // ensure there are a bunch of data files but multiple entries in each
        adapter.setJournalMaxFileLength(1024 * 20);

        // speed up the test case, checkpoint an cleanup early and often
        adapter.setCheckpointInterval(5000);
        adapter.setCleanupInterval(5000);

        adapter.setCheckForCorruptJournalFiles(true);
        adapter.setIgnoreMissingJournalfiles(true);

        adapter.setPreallocationStrategy("zeros");
        adapter.setPreallocationScope("entire_journal");
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testRecoveryAfterCorruptionEof() throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();

        assertTrue("more than x files: " + numFiles, numFiles > 2);

        corruptBatchEndEof(3);

        restartBroker(false);

        assertEquals("missing one message", 49, broker.getAdminView().getTotalMessageCount());
        assertEquals("Drain", 49, drainQueue(49));
    }

    @Test
    public void testRecoveryAfterCorruptionCheckSum() throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(4);

        corruptBatchCheckSumSplash(1);

        restartBroker(true);

        assertEquals("missing one message", 3, broker.getAdminView().getTotalMessageCount());
        assertEquals("Drain", 3, drainQueue(4));
    }

    @Test
    public void testRecoveryAfterCorruptionCheckSumExistingIndex() throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(4);

        corruptBatchCheckSumSplash(1);

        restartBroker(false);

        assertEquals("unnoticed", 4, broker.getAdminView().getTotalMessageCount());
        assertEquals("Drain", 0, drainQueue(4));

        // force recover index and loose one message
        restartBroker(false, true);

        assertEquals("missing one index recreation", 3, broker.getAdminView().getTotalMessageCount());
        assertEquals("Drain", 3, drainQueue(4));
    }

    @Test
    public void testRecoverIndex() throws Exception {
        startBroker();

        final int numToSend = 4;
        produceMessagesToConsumeMultipleDataFiles(numToSend);

        // force journal replay by whacking the index
        restartBroker(false, true);

        assertEquals("Drain", numToSend, drainQueue(numToSend));
    }

    private void corruptBatchCheckSumSplash(int id) throws Exception{
        Collection<DataFile> files =
                ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().values();
        DataFile dataFile = (DataFile) files.toArray()[0];
        RecoverableRandomAccessFile randomAccessFile = dataFile.openRandomAccessFile();

        ArrayList<Integer> batchPositions = findBatch(randomAccessFile, Integer.MAX_VALUE);
        LOG.info("Batch positions: " + batchPositions);
        int pos = batchPositions.get(1);
        LOG.info("corrupting checksum and size (to push it past eof) of batch record at:" + id + "-" + pos);
        randomAccessFile.seek(pos + Journal.BATCH_CONTROL_RECORD_HEADER.length + 4);
        // whack the batch control record checksum
        randomAccessFile.writeLong(0l);

        // mod the data size in the location header so reading blows
        randomAccessFile.seek(pos + Journal.BATCH_CONTROL_RECORD_SIZE);
        int size = randomAccessFile.readInt();
        byte type = randomAccessFile.readByte();

        LOG.info("Read: size:" + size + ", type:" + type);

        randomAccessFile.seek(pos + Journal.BATCH_CONTROL_RECORD_SIZE);
        size -= 1;
        LOG.info("rewrite incorrect location size @:" + (pos + Journal.BATCH_CONTROL_RECORD_SIZE) + " as: " + size);
        randomAccessFile.writeInt(size);

        randomAccessFile.getChannel().force(true);
    }

    private void corruptBatchEndEof(int id) throws Exception{
        Collection<DataFile> files =
                ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().values();
        DataFile dataFile = (DataFile) files.toArray()[id];
        RecoverableRandomAccessFile randomAccessFile = dataFile.openRandomAccessFile();

        ArrayList<Integer> batchPositions = findBatch(randomAccessFile, Integer.MAX_VALUE);
        int pos = batchPositions.get(batchPositions.size() - 3);
        LOG.info("corrupting checksum and size (to push it past eof) of batch record at:" + id + "-" + pos);
        randomAccessFile.seek(pos + Journal.BATCH_CONTROL_RECORD_HEADER.length);
        randomAccessFile.writeInt(31 * 1024 * 1024);
        randomAccessFile.writeLong(0l);
        randomAccessFile.getChannel().force(true);
    }

    private ArrayList<Integer> findBatch(RecoverableRandomAccessFile randomAccessFile, int where) throws IOException {
        final ArrayList<Integer> batchPositions = new ArrayList<Integer>();
        final ByteSequence header = new ByteSequence(Journal.BATCH_CONTROL_RECORD_HEADER);
        byte data[] = new byte[1024 * 20];

        ByteSequence bs = new ByteSequence(data, 0, randomAccessFile.read(data, 0, data.length));

        int pos = 0;
        for (int i = 0; i < where; i++) {
            int found = bs.indexOf(header, pos);
            if (found == -1) {
                break;
            }
            batchPositions.add(found);
            pos = found + Journal.BATCH_CONTROL_RECORD_HEADER.length - 1;
        }

        return batchPositions;
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
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(destination);
        int count = 0;
        while (count < max && consumer.receive(5000) != null) {
            count++;
        }
        consumer.close();
        connection.close();
        return count;
    }
}
