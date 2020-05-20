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
package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.RecoveredXATransactionViewMBean;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TransactionIdTransformer;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBTransactionStore;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class MKahaDBTxRecoveryTest {

    static final Logger LOG = LoggerFactory.getLogger(MKahaDBTxRecoveryTest.class);
    private final static int maxFileLength = 1024*1024*32;

    private final static String PREFIX_DESTINATION_NAME = "queue";

    private final static String DESTINATION_NAME = PREFIX_DESTINATION_NAME + ".test";
    private final static String DESTINATION_NAME_2 = PREFIX_DESTINATION_NAME + "2.test";
    private final static int CLEANUP_INTERVAL_MILLIS = 500;

    BrokerService broker;
    private List<KahaDBPersistenceAdapter> kahadbs = new LinkedList<KahaDBPersistenceAdapter>();


    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    protected BrokerService createBroker(PersistenceAdapter kaha) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setBrokerName("localhost");
        broker.setPersistenceAdapter(kaha);
        return broker;
    }

    @Test
    public void testCommitOutcomeDeliveryOnRecovery() throws Exception {

        prepareBrokerWithMultiStore(true);
        broker.start();
        broker.waitUntilStarted();


        // Ensure we have an Admin View.
        assertTrue("Broker doesn't have an Admin View.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return (broker.getAdminView()) != null;
            }
        }));


        final AtomicBoolean injectFailure = new AtomicBoolean(true);

        final AtomicInteger reps = new AtomicInteger();
        final AtomicReference<TransactionIdTransformer> delegate = new AtomicReference<TransactionIdTransformer>();

        TransactionIdTransformer faultInjector  = new TransactionIdTransformer() {
            @Override
            public TransactionId transform(TransactionId txid) {
                if (injectFailure.get() && reps.incrementAndGet() > 5) {
                    throw new RuntimeException("Bla");
                }
                return delegate.get().transform(txid);
            }
        };
        // set up kahadb to fail after N ops
        for (KahaDBPersistenceAdapter pa : kahadbs) {
            if (delegate.get() == null) {
                delegate.set(pa.getStore().getTransactionIdTransformer());
            }
            pa.setTransactionIdTransformer(faultInjector);
        }

        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory("vm://localhost");
        f.setAlwaysSyncSend(true);
        Connection c = f.createConnection();
        c.start();
        Session s = c.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = s.createProducer(new ActiveMQQueue(DESTINATION_NAME  + "," + DESTINATION_NAME_2));
        producer.send(s.createTextMessage("HI"));
        try {
            s.commit();
        } catch (Exception expected) {
            expected.printStackTrace();
        }

        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME)));
        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2)));

        final Destination destination1 = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME));
        final Destination destination2 = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2));

        assertTrue("Partial commit - one dest has message", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return destination2.getMessageStore().getMessageCount() != destination1.getMessageStore().getMessageCount();
            }
        }));

        // check completion on recovery
        injectFailure.set(false);

        // fire in many more local transactions to use N txStore journal files
        for (int i=0; i<100; i++) {
            producer.send(s.createTextMessage("HI"));
            s.commit();
        }

        broker.stop();

        // fail recovery processing on first attempt
        prepareBrokerWithMultiStore(false);
        broker.setPlugins(new BrokerPlugin[] {new BrokerPluginSupport() {

            @Override
            public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
                // longer than CleanupInterval
                TimeUnit.SECONDS.sleep( 2);
                throw new RuntimeException("Sorry");
            }
        }});
        broker.start();

        // second recovery attempt should sort it
        broker.stop();
        prepareBrokerWithMultiStore(false);
        broker.start();
        broker.waitUntilStarted();

        // verify commit completed
        Destination destination = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME));
        assertEquals(101, destination.getMessageStore().getMessageCount());

        destination = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2));
        assertEquals(101, destination.getMessageStore().getMessageCount());
    }


    @Test
    public void testManualRecoveryOnCorruptTxStore() throws Exception {

        prepareBrokerWithMultiStore(true);
        ((MultiKahaDBPersistenceAdapter)broker.getPersistenceAdapter()).setCheckForCorruption(true);
        broker.start();
        broker.waitUntilStarted();

        // Ensure we have an Admin View.
        assertTrue("Broker doesn't have an Admin View.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return (broker.getAdminView()) != null;
            }
        }));

        final AtomicBoolean injectFailure = new AtomicBoolean(true);

        final AtomicInteger reps = new AtomicInteger();
        final AtomicReference<TransactionIdTransformer> delegate = new AtomicReference<TransactionIdTransformer>();

        TransactionIdTransformer faultInjector  = new TransactionIdTransformer() {
            @Override
            public TransactionId transform(TransactionId txid) {
                if (injectFailure.get() && reps.incrementAndGet() > 5) {
                    throw new RuntimeException("Bla2");
                }
                return delegate.get().transform(txid);
            }
        };
        // set up kahadb to fail after N ops
        for (KahaDBPersistenceAdapter pa : kahadbs) {
            if (delegate.get() == null) {
                delegate.set(pa.getStore().getTransactionIdTransformer());
            }
            pa.setTransactionIdTransformer(faultInjector);
        }

        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory("vm://localhost");
        f.setAlwaysSyncSend(true);
        Connection c = f.createConnection();
        c.start();
        Session s = c.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = s.createProducer(new ActiveMQQueue(DESTINATION_NAME  + "," + DESTINATION_NAME_2));
        producer.send(s.createTextMessage("HI"));
        try {
            s.commit();
            fail("Expect commit failure on error injection!");
        } catch (Exception expected) {
            expected.printStackTrace();
        }

        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME)));
        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2)));

        final Destination destination1 = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME));
        final Destination destination2 = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2));

        assertTrue("Partial commit - one dest has message", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return destination2.getMessageStore().getMessageCount() != destination1.getMessageStore().getMessageCount();
            }
        }));

        // check completion on recovery
        injectFailure.set(false);

        // fire in many more local transactions to use N txStore journal files
        for (int i=0; i<100; i++) {
            producer.send(s.createTextMessage("HI"));
            s.commit();
        }

        ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean brokerViewMBean = (BrokerViewMBean) broker.getManagementContext().newProxyInstance(objectName, BrokerViewMBean.class, true);
        String pathToDataDir = brokerViewMBean.getDataDirectory();

        broker.stop();

        // corrupt the journal such that it fails to load
        corruptTxStoreJournal(pathToDataDir);

        // verify failure to load txStore via logging
        org.apache.log4j.Logger log4jLogger =
                org.apache.log4j.Logger.getLogger(MultiKahaDBTransactionStore.class);

        AtomicBoolean foundSomeCorruption = new AtomicBoolean();
        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.ERROR) && event.getMessage().toString().startsWith("Corrupt ")) {
                    LOG.info("received expected log message: " + event.getMessage());
                    foundSomeCorruption.set(true);
                }
            }
        };
        log4jLogger.addAppender(appender);
        try {

            prepareBrokerWithMultiStore(false);
            ((MultiKahaDBPersistenceAdapter) broker.getPersistenceAdapter()).setCheckForCorruption(true);

            broker.start();
            broker.waitUntilStarted();

            {
                final Destination dest1 = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME));
                final Destination dest2 = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2));

                // verify partial commit still present
                assertTrue("Partial commit - one dest has message", Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        return dest1.getMessageStore().getMessageCount() != dest2.getMessageStore().getMessageCount();
                    }
                }));
            }

            assertTrue("broker/store found corruption", foundSomeCorruption.get());
            broker.stop();

            // and without checksum
            LOG.info("Check for journal read failure... no checksum");
            foundSomeCorruption.set(false);
            prepareBrokerWithMultiStore(false);
            ((MultiKahaDBPersistenceAdapter) broker.getPersistenceAdapter()).setCheckForCorruption(false);

            broker.start();
            broker.waitUntilStarted();

            {
                final Destination dest1 = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME));
                final Destination dest2 = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2));

                // verify partial commit still present
                assertTrue("Partial commit - one dest still has message", Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        return dest1.getMessageStore().getMessageCount() != dest2.getMessageStore().getMessageCount();
                    }
                }));
            }

            assertTrue("broker/store found corruption without checksum", foundSomeCorruption.get());

            // force commit outcome via Tx MBeans
            ObjectName matchAllPendingTx = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,transactionType=RecoveredXaTransaction,xid=*");
            Set<ObjectName> pendingTx = broker.getManagementContext().queryNames(matchAllPendingTx, null);
            assertFalse(pendingTx.isEmpty());

            for (ObjectName pendingXAtxOn: pendingTx) {
                RecoveredXATransactionViewMBean proxy = (RecoveredXATransactionViewMBean) broker.getManagementContext().newProxyInstance(pendingXAtxOn,
                        RecoveredXATransactionViewMBean.class, true);
                assertEquals("matches ", proxy.getFormatId(), 61616);

                // force commit outcome, we verify the commit in this test, knowing that one branch has committed already
                proxy.heuristicCommit();
            }

            pendingTx = broker.getManagementContext().queryNames(matchAllPendingTx, null);
            assertTrue(pendingTx.isEmpty());

            // verify commit completed
            Destination destination = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME));
            assertEquals(101, destination.getMessageStore().getMessageCount());

            destination = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2));
            assertEquals(101, destination.getMessageStore().getMessageCount());

        } finally {
            log4jLogger.removeAppender(appender);
        }
    }

    @Test
    public void testCorruptionDetectedOnTruncateAndIgnored() throws Exception {

        prepareBrokerWithMultiStore(true);
        broker.start();
        broker.waitUntilStarted();

        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory("vm://localhost");
        f.setAlwaysSyncSend(true);
        Connection c = f.createConnection();
        c.start();
        Session s = c.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = s.createProducer(new ActiveMQQueue(DESTINATION_NAME  + "," + DESTINATION_NAME_2));
        for (int i=0; i<20; i++) {
            producer.send(s.createTextMessage("HI"));
            s.commit();
        }
        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME)));
        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2)));

        ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean brokerViewMBean = (BrokerViewMBean) broker.getManagementContext().newProxyInstance(objectName, BrokerViewMBean.class, true);
        String pathToDataDir = brokerViewMBean.getDataDirectory();

        broker.stop();

        // corrupt the journal such that it fails to load
        corruptTxStoreJournalAndTruncate(pathToDataDir);

        // verify failure to load txStore via logging
        org.apache.log4j.Logger log4jLogger =
                org.apache.log4j.Logger.getLogger(MultiKahaDBTransactionStore.class);

        AtomicBoolean foundSomeCorruption = new AtomicBoolean();
        AtomicBoolean ignoringCorruption = new AtomicBoolean();

        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.ERROR) && event.getMessage().toString().startsWith("Corrupt ")) {
                    LOG.info("received expected log message: " + event.getMessage());
                    foundSomeCorruption.set(true);
                } else if (event.getLevel().equals(Level.INFO) && event.getMessage().toString().contains("auto resolving")) {
                    ignoringCorruption.set(true);
                }
            }
        };
        log4jLogger.addAppender(appender);
        try {
            prepareBrokerWithMultiStore(false);

            broker.start();
            broker.waitUntilStarted();

            assertTrue("broker/store found corruption", foundSomeCorruption.get());
            assertTrue("broker/store ignored corruption", ignoringCorruption.get());

            broker.stop();

            foundSomeCorruption.set(false);
            ignoringCorruption.set(false);

            prepareBrokerWithMultiStore(false);
            broker.start();
            broker.waitUntilStarted();

            assertFalse("broker/store no corruption", foundSomeCorruption.get());
            assertFalse("broker/store no ignored corruption", ignoringCorruption.get());

            Connection connection = f.createConnection();
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(new ActiveMQQueue(DESTINATION_NAME  + "," + DESTINATION_NAME_2));
            for (int i=0; i<20; i++) {
                messageProducer.send(session.createTextMessage("HI"));
                session.commit();
            }
            assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME)));
            assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2)));

            broker.stop();
        } finally {
            log4jLogger.removeAppender(appender);
        }
    }

    private void corruptTxStoreJournal(String pathToDataDir) throws Exception {
        corruptTxStore(pathToDataDir, false);
    }

    private void corruptTxStoreJournalAndTruncate(String pathToDataDir) throws Exception {
        corruptTxStore(pathToDataDir, true);
    }

    private void corruptTxStore(String pathToDataDir, boolean truncate) throws Exception {
        LOG.info("Path to broker datadir: " + pathToDataDir);

        RandomAccessFile randomAccessFile = new RandomAccessFile(String.format("%s/mKahaDB/txStore/db-1.log", pathToDataDir), "rw");
        final ByteSequence header = new ByteSequence(Journal.BATCH_CONTROL_RECORD_HEADER);
        byte data[] = new byte[1024 * 20];
        ByteSequence bs = new ByteSequence(data, 0, randomAccessFile.read(data, 0, data.length));

        int offset = bs.indexOf(header, 1);
        offset = bs.indexOf(header, offset+1);
        offset = bs.indexOf(header, offset+1);

        // 3rd batch
        LOG.info("3rd batch record in file: 1:" + offset);

        offset += Journal.BATCH_CONTROL_RECORD_SIZE;
        offset += 4; // location size
        offset += 1; // location type

        byte fill = (byte) 0xAF;
        LOG.info("Whacking batch record in file:" + 1 + ", at offset: " + offset + " with fill:" + fill);
        // whack that record
        byte[] bla = new byte[2];
        Arrays.fill(bla, fill);
        randomAccessFile.seek(offset);
        randomAccessFile.write(bla, 0, bla.length);

        if (truncate) {
            // set length to truncate
            randomAccessFile.setLength(randomAccessFile.getFilePointer());
        }
        randomAccessFile.getFD().sync();
    }

    protected KahaDBPersistenceAdapter createStore(boolean delete) throws IOException {
        KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
        kaha.setJournalMaxFileLength(maxFileLength);
        kaha.setCleanupInterval(CLEANUP_INTERVAL_MILLIS);
        if (delete) {
            kaha.deleteAllMessages();
        }
        kahadbs.add(kaha);
        return kaha;
    }

    public void prepareBrokerWithMultiStore(boolean deleteAllMessages) throws Exception {

        MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter = new MultiKahaDBPersistenceAdapter();
        if (deleteAllMessages) {
            multiKahaDBPersistenceAdapter.deleteAllMessages();
        }
        ArrayList<FilteredKahaDBPersistenceAdapter> adapters = new ArrayList<FilteredKahaDBPersistenceAdapter>();

        adapters.add(createFilteredKahaDBByDestinationPrefix(PREFIX_DESTINATION_NAME, deleteAllMessages));
        adapters.add(createFilteredKahaDBByDestinationPrefix(PREFIX_DESTINATION_NAME + "2", deleteAllMessages));

        multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(adapters);
        multiKahaDBPersistenceAdapter.setJournalMaxFileLength(4*1024);
        multiKahaDBPersistenceAdapter.setJournalCleanupInterval(10);

        broker = createBroker(multiKahaDBPersistenceAdapter);
    }

	private FilteredKahaDBPersistenceAdapter createFilteredKahaDBByDestinationPrefix(String destinationPrefix, boolean deleteAllMessages)
			throws IOException {
		FilteredKahaDBPersistenceAdapter template = new FilteredKahaDBPersistenceAdapter();
        template.setPersistenceAdapter(createStore(deleteAllMessages));
        if (destinationPrefix != null) {
            template.setQueue(destinationPrefix + ".>");
        }
		return template;
	}
}