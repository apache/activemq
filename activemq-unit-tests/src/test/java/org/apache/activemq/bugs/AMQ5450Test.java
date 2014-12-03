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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;

public class AMQ5450Test {

    static final Logger LOG = LoggerFactory.getLogger(AMQ5450Test.class);
    private final static int maxFileLength = 1024*1024*32;

    private final static String POSTFIX_DESTINATION_NAME = ".dlq";

    private final static String DESTINATION_NAME = "test" + POSTFIX_DESTINATION_NAME;
    private final static String DESTINATION_NAME_2 = "2.test" + POSTFIX_DESTINATION_NAME;
    private final static String DESTINATION_NAME_3 = "3.2.test" + POSTFIX_DESTINATION_NAME;

    private final static String[] DESTS = new String[] {DESTINATION_NAME, DESTINATION_NAME_2, DESTINATION_NAME_3, DESTINATION_NAME, DESTINATION_NAME};


    BrokerService broker;
    private HashMap<Object, PersistenceAdapter> adapters = new HashMap();

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    protected BrokerService createAndStartBroker(PersistenceAdapter persistenceAdapter) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setBrokerName("localhost");
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }

    @Test
    public void testPostFixMatch() throws Exception {
        doTestPostFixMatch(false);
    }

    @Test
    public void testPostFixCompositeMatch() throws Exception {
        doTestPostFixMatch(true);
    }

    private void doTestPostFixMatch(boolean useComposite) throws Exception {
        prepareBrokerWithMultiStore(useComposite);

        sendMessage(DESTINATION_NAME, "test 1");
        sendMessage(DESTINATION_NAME_2, "test 1");
        sendMessage(DESTINATION_NAME_3, "test 1");

        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME)));
        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2)));
        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_3)));

        for (String dest: DESTS)  {
            Destination destination2 = broker.getDestination(new ActiveMQQueue(dest));
            assertNotNull(destination2);
            assertEquals(1, destination2.getMessageStore().getMessageCount());
        }

        HashMap numDests = new HashMap();
        for (PersistenceAdapter pa : adapters.values()) {
            numDests.put(pa.getDestinations().size(), pa);
        }

        // ensure wildcard does not match any
        assertTrue("0 in wildcard matcher", adapters.get(null).getDestinations().isEmpty());

        assertEquals("only two values", 2, numDests.size());
        assertTrue("0 in others", numDests.containsKey(0));

        if (useComposite) {
            assertTrue("3 in one", numDests.containsKey(3));
        } else {
            assertTrue("1 in some", numDests.containsKey(1));
        }

    }

    protected KahaDBPersistenceAdapter createStore(boolean delete) throws IOException {
        KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
        kaha.setJournalMaxFileLength(maxFileLength);
        kaha.setCleanupInterval(5000);
        if (delete) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    public void prepareBrokerWithMultiStore(boolean compositeMatch) throws Exception {

        MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter = new MultiKahaDBPersistenceAdapter();
        multiKahaDBPersistenceAdapter.deleteAllMessages();
        ArrayList<FilteredKahaDBPersistenceAdapter> adapters = new ArrayList<FilteredKahaDBPersistenceAdapter>();

        if (compositeMatch) {
            StringBuffer compositeDestBuf = new StringBuffer();
            for (int i=1; i<=DESTS.length;i++) {
                for (int j=0;j<i;j++) {
                    compositeDestBuf.append("*");
                    if ((j+1 == i)) {
                        compositeDestBuf.append(POSTFIX_DESTINATION_NAME);
                    } else {
                        compositeDestBuf.append(".");
                    }
                }
                if (! (i+1 > DESTS.length)) {
                    compositeDestBuf.append(",");
                }
            }
            adapters.add(createFilteredKahaDBByDestinationPrefix(compositeDestBuf.toString(), true));

        } else {
            // destination map does not do post fix wild card matches on paths, so we need to cover
            // each path length
            adapters.add(createFilteredKahaDBByDestinationPrefix("*" + POSTFIX_DESTINATION_NAME, true));
            adapters.add(createFilteredKahaDBByDestinationPrefix("*.*" + POSTFIX_DESTINATION_NAME, true));
            adapters.add(createFilteredKahaDBByDestinationPrefix("*.*.*" + POSTFIX_DESTINATION_NAME, true));
            adapters.add(createFilteredKahaDBByDestinationPrefix("*.*.*.*" + POSTFIX_DESTINATION_NAME, true));
        }

        // ensure wildcard matcher is there for other dests
        adapters.add(createFilteredKahaDBByDestinationPrefix(null, true));

        multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(adapters);
        broker = createAndStartBroker(multiKahaDBPersistenceAdapter);
    }

	private FilteredKahaDBPersistenceAdapter createFilteredKahaDBByDestinationPrefix(String destinationPrefix, boolean deleteAllMessages)
			throws IOException {
		FilteredKahaDBPersistenceAdapter template = new FilteredKahaDBPersistenceAdapter();
        template.setPersistenceAdapter(createStore(deleteAllMessages));
        if (destinationPrefix != null) {
            template.setQueue(destinationPrefix);
        }
        adapters.put(destinationPrefix, template.getPersistenceAdapter());
		return template;
	}

	private void sendMessage(String destinationName, String message) throws JMSException {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory("vm://localhost");
        f.setAlwaysSyncSend(true);
        Connection c = f.createConnection();
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = s.createProducer(new ActiveMQQueue(destinationName));
        producer.send(s.createTextMessage(message));
        producer.close();
        s.close();
        c.stop();
	}

}