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
package org.apache.activemq.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBTransactionStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StorePerDestinationTest  {
    static final Logger LOG = LoggerFactory.getLogger(StorePerDestinationTest.class);
    final static int maxFileLength = 1024*100;
    final static int numToSend = 10000;
    final Vector<Throwable> exceptions = new Vector<Throwable>();
    BrokerService brokerService;

    protected BrokerService createBroker(PersistenceAdapter kaha) throws Exception {

        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistenceAdapter(kaha);
        return broker;
    }

    private KahaDBPersistenceAdapter createStore(boolean delete) throws IOException {
        KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
        kaha.setJournalMaxFileLength(maxFileLength);
        kaha.setCleanupInterval(5000);
        if (delete) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    @Before
    public void prepareCleanBrokerWithMultiStore() throws Exception {
           prepareBrokerWithMultiStore(true);
    }

    public void prepareBrokerWithMultiStore(boolean deleteAllMessages) throws Exception {

        MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter = new MultiKahaDBPersistenceAdapter();
        if (deleteAllMessages) {
            multiKahaDBPersistenceAdapter.deleteAllMessages();
        }
        ArrayList<FilteredKahaDBPersistenceAdapter> adapters = new ArrayList<FilteredKahaDBPersistenceAdapter>();

        FilteredKahaDBPersistenceAdapter theRest = new FilteredKahaDBPersistenceAdapter();
        theRest.setPersistenceAdapter(createStore(deleteAllMessages));
        // default destination when not set is a match for all
        adapters.add(theRest);

        // separate store for FastQ
        FilteredKahaDBPersistenceAdapter fastQStore = new FilteredKahaDBPersistenceAdapter();
        fastQStore.setPersistenceAdapter(createStore(deleteAllMessages));
        fastQStore.setDestination(new ActiveMQQueue("FastQ"));
        adapters.add(fastQStore);

        multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(adapters);
        brokerService  = createBroker(multiKahaDBPersistenceAdapter);
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
    }

    @Test
    public void testTransactedSendReceive() throws Exception {
        brokerService.start();
        sendMessages(true, "SlowQ", 1, 0);
        assertEquals("got one", 1, receiveMessages(true, "SlowQ", 1));
    }

    @Test
    public void testTransactedSendReceiveAcrossStores() throws Exception {
        brokerService.start();
        sendMessages(true, "SlowQ,FastQ", 1, 0);
        assertEquals("got one", 2, receiveMessages(true, "SlowQ,FastQ", 2));
    }

    @Test
    public void testCommitRecovery() throws Exception {
        doTestRecovery(true);
    }

     @Test
    public void testRollbackRecovery() throws Exception {
        doTestRecovery(false);
    }

    public void doTestRecovery(final boolean haveOutcome) throws Exception {
        final MultiKahaDBPersistenceAdapter persistenceAdapter =
                (MultiKahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();
        MultiKahaDBTransactionStore transactionStore =
                new MultiKahaDBTransactionStore(persistenceAdapter) {
                    @Override
                    public void persistOutcome(Tx tx, TransactionId txid) throws IOException {
                        if (haveOutcome) {
                            super.persistOutcome(tx, txid);
                        }
                        try {
                            // IOExceptions will stop the broker
                            persistenceAdapter.stop();
                        } catch (Exception e) {
                            LOG.error("ex on stop ", e);
                            exceptions.add(e);
                        }
                    }
                };
        persistenceAdapter.setTransactionStore(transactionStore);
        brokerService.start();

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // commit will block
                    sendMessages(true, "SlowQ,FastQ", 1, 0);
                } catch(Exception expected) {
                    LOG.info("expected", expected);
                }
            }
        });

        brokerService.waitUntilStopped();
        // interrupt the send thread
        executorService.shutdownNow();

        // verify auto recovery
        prepareBrokerWithMultiStore(false);
        brokerService.start();

        assertEquals("expect to get the recovered message", haveOutcome ? 2 : 0, receiveMessages(false, "SlowQ,FastQ", 2));
        assertEquals("all transactions are complete", 0, brokerService.getBroker().getPreparedTransactions(null).length);
    }

    @Test
    public void testSlowFastDestinationsStoreUsage() throws Exception {
        brokerService.start();
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sendMessages(false, "SlowQ", 50, 500);
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        });

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sendMessages(false, "FastQ", numToSend, 0);
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        });

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals("Got all sent", numToSend, receiveMessages(false, "FastQ", numToSend));
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        });

        executorService.shutdown();
        assertTrue("consumers executor finished on time", executorService.awaitTermination(60, TimeUnit.SECONDS));
        final SystemUsage usage = brokerService.getSystemUsage();
        assertTrue("Store is not hogged", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                long storeUsage = usage.getStoreUsage().getUsage();
                LOG.info("Store Usage: " + storeUsage);
                return storeUsage < 5 * maxFileLength;
            }
        }));
        assertTrue("no exceptions", exceptions.isEmpty());
    }

    private void sendMessages(boolean transacted, String destName, int count, long sleep) throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        try {
            Session session = transacted ? connection.createSession(true, Session.SESSION_TRANSACTED) : connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(new ActiveMQQueue(destName));
            for (int i = 0; i < count; i++) {
                if (sleep > 0) {
                    TimeUnit.MILLISECONDS.sleep(sleep);
                }
                producer.send(session.createTextMessage(createContent(i)));
            }
            if (transacted) {
                session.commit();
            }
        } finally {
            connection.close();
        }
    }

    private int receiveMessages(boolean transacted, String destName, int max) throws JMSException {
        int rc = 0;
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        try {
            connection.start();
            Session session = transacted ? connection.createSession(true, Session.SESSION_TRANSACTED) : connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(new ActiveMQQueue(destName));
            while (rc < max && messageConsumer.receive(4000) != null) {
                rc++;

                if (transacted && rc % 200 == 0) {
                    session.commit();
                }
            }
            if (transacted) {
                session.commit();
            }
            return rc;
        } finally {
            connection.close();
        }
    }

    private String createContent(int i) {
        StringBuilder sb = new StringBuilder(i + ":");
        while (sb.length() < 1024) {
            sb.append("*");
        }
        return sb.toString();
    }

}