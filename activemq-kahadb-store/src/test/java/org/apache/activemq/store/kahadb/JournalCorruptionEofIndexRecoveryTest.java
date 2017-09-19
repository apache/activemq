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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.MessageDatabase.MessageKeys;
import org.apache.activemq.store.kahadb.MessageDatabase.StoredDestination;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JournalCorruptionEofIndexRecoveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalCorruptionEofIndexRecoveryTest.class);

    private ActiveMQConnectionFactory cf = null;
    private BrokerService broker = null;
    private String connectionUri;
    private KahaDBPersistenceAdapter adapter;
    private boolean ignoreMissingJournalFiles = false;
    private int journalMaxBatchSize;

    private final Destination destination = new ActiveMQQueue("Test");
    private final String KAHADB_DIRECTORY = "target/activemq-data/";
    private final String payload = new String(new byte[1024]);
    File brokerDataDir = null;

    protected void startBroker() throws Exception {
        doStartBroker(true, false);
    }

    protected void restartBroker(boolean whackIndex) throws Exception {
        restartBroker(whackIndex, false);
    }

    protected void restartBroker(boolean whackIndex, boolean forceRecoverIndex) throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }

        if (whackIndex) {
            File indexToDelete = new File(brokerDataDir, "db.data");
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
        brokerDataDir = broker.getPersistenceAdapter().getDirectory();
        LOG.info("Starting broker..");
    }

    protected void configurePersistence(BrokerService brokerService, boolean forceRecoverIndex) throws Exception {
        adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        adapter.setForceRecoverIndex(forceRecoverIndex);

        // ensure there are a bunch of data files but multiple entries in each
        adapter.setJournalMaxFileLength(1024 * 20);

        adapter.setJournalMaxWriteBatchSize(journalMaxBatchSize);

        // speed up the test case, checkpoint an cleanup early and often
        adapter.setCheckpointInterval(5000);
        adapter.setCleanupInterval(5000);

        adapter.setCheckForCorruptJournalFiles(true);
        adapter.setIgnoreMissingJournalfiles(ignoreMissingJournalFiles);

        adapter.setPreallocationStrategy(Journal.PreallocationStrategy.ZEROS.name());
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
    public void reset() throws Exception {
        ignoreMissingJournalFiles = true;
        journalMaxBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    }

    @Test
    public void testNoRestartOnCorruptJournal() throws Exception {
        ignoreMissingJournalFiles = false;

        startBroker();

        produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();

        assertTrue("more than x files: " + numFiles, numFiles > 2);

        corruptBatchEndEof(3);

        try {
            restartBroker(true);
            fail("Expect failure to start with corrupt journal");
        } catch (Exception expected) {
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
    public void testRecoveryAfterCorruptionMetadataLocation() throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();

        assertTrue("more than x files: " + numFiles, numFiles > 2);

        broker.getPersistenceAdapter().checkpoint(true);
        Location location = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getMetadata().producerSequenceIdTrackerLocation;

        DataFile dataFile = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().get(Integer.valueOf(location.getDataFileId()));
        RecoverableRandomAccessFile randomAccessFile = dataFile.openRandomAccessFile();
        randomAccessFile.seek(location.getOffset());
        randomAccessFile.writeInt(Integer.MAX_VALUE);
        randomAccessFile.getChannel().force(true);

        ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().close();
        try {
            broker.stop();
            broker.waitUntilStopped();
        } catch (Exception expected) {
        } finally {
            broker = null;
        }

        AtomicBoolean trappedExpectedLogMessage = new AtomicBoolean(false);
        DefaultTestAppender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel() == Level.WARN
                        && event.getRenderedMessage().contains("Cannot recover message audit")
                        && event.getThrowableInformation().getThrowable().getLocalizedMessage().contains("Invalid location size")) {
                    trappedExpectedLogMessage.set(true);
                }
            }
        };
        org.apache.log4j.Logger.getRootLogger().addAppender(appender);


        try {
            restartBroker(false);
        } finally {
            org.apache.log4j.Logger.getRootLogger().removeAppender(appender);
        }

        assertEquals("no missing message", 50, broker.getAdminView().getTotalMessageCount());
        assertTrue("Did replay records on invalid location size", trappedExpectedLogMessage.get());
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

    @Test
    public void testRecoverIndexWithSmallBatch() throws Exception {
        journalMaxBatchSize = 2 * 1024;
        startBroker();

        final int numToSend = 4;
        produceMessagesToConsumeMultipleDataFiles(numToSend);

        // force journal replay by whacking the index
        restartBroker(false, true);

        assertEquals("Drain", numToSend, drainQueue(numToSend));
    }


    @Test
    public void testRecoveryAfterProducerAuditLocationCorrupt() throws Exception {
        doTestRecoveryAfterLocationCorrupt(false);
    }

    @Test
    public void testRecoveryAfterAckMapLocationCorrupt() throws Exception {
        doTestRecoveryAfterLocationCorrupt(true);
    }

    private void doTestRecoveryAfterLocationCorrupt(boolean aOrB) throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();

        assertTrue("more than x files: " + numFiles, numFiles > 4);

        KahaDBStore store = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore();
        store.checkpointCleanup(true);
        Location toCorrupt = aOrB ? store.getMetadata().ackMessageFileMapLocation : store.getMetadata().producerSequenceIdTrackerLocation;
        corruptLocation(toCorrupt);

        restartBroker(false, false);

        assertEquals("missing no message", 50, broker.getAdminView().getTotalMessageCount());
        assertEquals("Drain", 50, drainQueue(50));
    }

    private void corruptLocation(Location toCorrupt) throws IOException {

        DataFile dataFile = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().get(new Integer(toCorrupt.getDataFileId()));

        RecoverableRandomAccessFile randomAccessFile = dataFile.openRandomAccessFile();

        randomAccessFile.seek(toCorrupt.getOffset());
        randomAccessFile.writeInt(3);
        dataFile.closeRandomAccessFile(randomAccessFile);
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
        corruptOrderIndex(id, size);

        randomAccessFile.getChannel().force(true);
        dataFile.closeRandomAccessFile(randomAccessFile);
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

    private void corruptOrderIndex(final int num, final int size) throws Exception {
        //This is because of AMQ-6097, now that the MessageOrderIndex stores the size in the Location,
        //we need to corrupt that value as well
        final KahaDBStore kahaDbStore = (KahaDBStore) ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore();
        kahaDbStore.indexLock.writeLock().lock();
        try {
            kahaDbStore.pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    StoredDestination sd = kahaDbStore.getStoredDestination(kahaDbStore.convert(
                            (ActiveMQQueue)destination), tx);
                    int i = 1;
                    for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator.hasNext();) {
                        Entry<Long, MessageKeys> entry = iterator.next();
                        if (i == num) {
                            //change the size value to the wrong size
                            sd.orderIndex.get(tx, entry.getKey());
                            MessageKeys messageKeys = entry.getValue();
                            messageKeys.location.setSize(size);
                            sd.orderIndex.put(tx, sd.orderIndex.lastGetPriority(), entry.getKey(), messageKeys);
                            break;
                        }
                        i++;
                    }
                }
            });
        } finally {
            kahaDbStore.indexLock.writeLock().unlock();
        }
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
        return drain(cf, destination, max);
    }

    public static int drain(ConnectionFactory cf, Destination destination, int max) throws Exception {
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = null;
        int count = 0;
        try {
            consumer = session.createConsumer(destination);
            while (count < max && consumer.receive(4000) != null) {
                count++;
            }
        } catch (JMSException ok) {
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (JMSException ok) {}
            }
            try {
                connection.close();
            } catch (JMSException ok) {}
        }
        return count;
    }
}
