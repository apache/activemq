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
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TransactionIdTransformer;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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