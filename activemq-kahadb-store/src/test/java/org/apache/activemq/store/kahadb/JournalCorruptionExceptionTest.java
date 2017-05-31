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
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.activemq.store.kahadb.JournalCorruptionEofIndexRecoveryTest.drain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class JournalCorruptionExceptionTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalCorruptionExceptionTest.class);

    private final String KAHADB_DIRECTORY = "target/activemq-data/";
    private final String payload = new String(new byte[1024]);

    private ActiveMQConnectionFactory cf = null;
    private BrokerService broker = null;
    private final Destination destination = new ActiveMQQueue("Test");
    private String connectionUri;
    private KahaDBPersistenceAdapter adapter;

    @Parameterized.Parameter(0)
    public byte fill = Byte.valueOf("3");

    @Parameterized.Parameter(1)
    public int fillLength = 10;

    @Parameterized.Parameters(name = "fill=#{0},#{1}")
    public static Iterable<Object[]> parameters() {
        // corruption can be valid record type values
        return Arrays.asList(new Object[][]{
                {Byte.valueOf("0"), 6},
                {Byte.valueOf("1"), 8},
                {Byte.valueOf("-1"), 6},
                {Byte.valueOf("0"), 10},
                {Byte.valueOf("1"), 10},
                {Byte.valueOf("2"), 10},
                {Byte.valueOf("-1"), 10}
        });
    }

    protected void startBroker() throws Exception {
        doStartBroker(true);
    }

    private void doStartBroker(boolean delete) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(delete);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setDataDirectory(KAHADB_DIRECTORY);
        broker.addConnector("tcp://localhost:0");

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setUseCache(false);
        defaultEntry.setExpireMessagesPeriod(0);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);

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

    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testIOExceptionOnCorruptJournalLocationRead() throws Exception {
        startBroker();

        produceMessagesToConsumeMultipleDataFiles(50);

        int numFiles = getNumberOfJournalFiles();

        assertTrue("more than x files: " + numFiles, numFiles > 4);

        corruptLocationAtDataFileIndex(3);

        assertEquals("missing one message", 50, broker.getAdminView().getTotalMessageCount());
        assertEquals("Drain", 0, drainQueue(50));
        assertTrue("broker stopping", broker.isStopping());
    }


    private void corruptLocationAtDataFileIndex(int id) throws IOException {

        Collection<DataFile> files = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().values();
        DataFile dataFile = (DataFile) files.toArray()[id];

        RecoverableRandomAccessFile randomAccessFile = dataFile.openRandomAccessFile();

        final ByteSequence header = new ByteSequence(Journal.BATCH_CONTROL_RECORD_HEADER);
        byte data[] = new byte[1024 * 20];
        ByteSequence bs = new ByteSequence(data, 0, randomAccessFile.read(data, 0, data.length));

        int offset = bs.indexOf(header, 0);

        offset += Journal.BATCH_CONTROL_RECORD_SIZE;

        if (fillLength >= 10) {
            offset += 4; // location size
            offset += 1; // location type
        }

        LOG.info("Whacking batch record in file:" + id + ", at offset: " + offset + " with fill:" + fill);
        // whack that record
        byte[] bla = new byte[fillLength];
        Arrays.fill(bla, fill);
        randomAccessFile.seek(offset);
        randomAccessFile.write(bla, 0, bla.length);
        randomAccessFile.getFD().sync();
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
