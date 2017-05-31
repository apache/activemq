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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

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
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4407Test {

    static final Logger LOG = LoggerFactory.getLogger(AMQ4407Test.class);
    private final static int maxFileLength = 1024*1024*32;

    private final static String PREFIX_DESTINATION_NAME = "queue";

    private final static String DESTINATION_NAME = PREFIX_DESTINATION_NAME + ".test";
    private final static String DESTINATION_NAME_2 = PREFIX_DESTINATION_NAME + "2.test";
    private final static String DESTINATION_NAME_3 = PREFIX_DESTINATION_NAME + "3.test";

    BrokerService broker;

    @Before
    public void setUp() throws Exception {
        prepareBrokerWithMultiStore(true);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    protected BrokerService createBroker(PersistenceAdapter kaha) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setBrokerName("localhost");
        broker.setPersistenceAdapter(kaha);
        return broker;
    }

    @Test
    public void testRestartAfterQueueDelete() throws Exception {

        // Ensure we have an Admin View.
        assertTrue("Broker doesn't have an Admin View.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return (broker.getAdminView()) != null;
            }
        }));


        LOG.info("Adding destinations: {}, {}, {}", new Object[] {DESTINATION_NAME, DESTINATION_NAME_3, DESTINATION_NAME_3});
        sendMessage(DESTINATION_NAME, "test 1");
        sendMessage(DESTINATION_NAME_2, "test 1");
        sendMessage(DESTINATION_NAME_3, "test 1");

        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME)));
        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2)));
        assertNotNull(broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_3)));


        LOG.info("Removing destination: {}", DESTINATION_NAME_2);
        broker.getAdminView().removeQueue(DESTINATION_NAME_2);

        LOG.info("Recreating destination: {}", DESTINATION_NAME_2);
        sendMessage(DESTINATION_NAME_2, "test 1");

        Destination destination2 = broker.getDestination(new ActiveMQQueue(DESTINATION_NAME_2));
        assertNotNull(destination2);
        assertEquals(1, destination2.getMessageStore().getMessageCount());
    }


    @Test
    public void testRemoveOfOneDestFromSharedPa() throws Exception {
        // Ensure we have an Admin View.
        assertTrue("Broker doesn't have an Admin View.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return (broker.getAdminView()) != null;
            }
        }));

        // will both use first persistence adapter
        sendMessage("queue.A", "test 1");
        sendMessage("queue.B", "test 1");

        broker.getAdminView().removeQueue("queue.A");

        sendMessage("queue.B", "test 1");

        Destination destination2 = broker.getDestination(new ActiveMQQueue("queue.B"));
        assertNotNull(destination2);
        assertEquals(2, destination2.getMessageStore().getMessageCount());
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

    public void prepareBrokerWithMultiStore(boolean deleteAllMessages) throws Exception {

        MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter = new MultiKahaDBPersistenceAdapter();
        if (deleteAllMessages) {
            multiKahaDBPersistenceAdapter.deleteAllMessages();
        }
        ArrayList<FilteredKahaDBPersistenceAdapter> adapters = new ArrayList<FilteredKahaDBPersistenceAdapter>();

        adapters.add(createFilteredKahaDBByDestinationPrefix(PREFIX_DESTINATION_NAME, deleteAllMessages));
        adapters.add(createFilteredKahaDBByDestinationPrefix(PREFIX_DESTINATION_NAME + "2", deleteAllMessages));
        adapters.add(createFilteredKahaDBByDestinationPrefix(null, deleteAllMessages));

        multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(adapters);
        broker = createBroker(multiKahaDBPersistenceAdapter);
    }

    /**
     * Create filtered KahaDB adapter by destination prefix.
     *
     * @param destinationPrefix
     * @param deleteAllMessages
     * @return
     * @throws IOException
     */
	private FilteredKahaDBPersistenceAdapter createFilteredKahaDBByDestinationPrefix(String destinationPrefix, boolean deleteAllMessages)
			throws IOException {
		FilteredKahaDBPersistenceAdapter template = new FilteredKahaDBPersistenceAdapter();
        template.setPersistenceAdapter(createStore(deleteAllMessages));
        if (destinationPrefix != null) {
        	template.setQueue(destinationPrefix + ".>");
        }
		return template;
	}


	/**
	 * Send message to particular destination.
	 *
	 * @param destinationName
	 * @param message
	 * @throws JMSException
	 */
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