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

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MultiKahaDBMultipleFilteredAdapterTest {

    private final static int maxFileLength = 1024*1024*32;
    private static final String QUEUE_NAME = "QUEUE.amqMultiKahadbMultiFilteredAdapter";
    private static final String TOPIC_NAME = "TOPIC.amqMultiKahadbMultiFilteredAdapter";
    private static final int MESSAGE_COUNT = 100;

    private  BrokerService broker;
    private ActiveMQConnectionFactory cf;

    @Before
    public void setUp() throws Exception {
        prepareBrokerWithMultiStore(true);
        TransportConnector connector = broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        cf = new ActiveMQConnectionFactory("failover://(" + connector.getConnectUri() + ")");
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test
    public void testTopicWildcardAndPerDestinationFilteredAdapter() throws Exception {
        //create one topic and queue and send messages
        sendMessages(false);
        sendMessages(true);

        //the adapters will persist the data, topic will be by wildcard filtered adapter
        //and queue will be by per destionation adapter

        //stop broker and restart, the bug is that on restart wildcard filtered adapter adds one KahaDBPersistenceAdapter
        // when calling MultiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters
        //and on the call to doStart of MultiKahaDBPersistenceAdapter it uses per destination filtered adapter to add
        //another KahaDBPersistenceAdapter and fails on jmx bean registration
        // the fix is to make sure to check the persistent adapter already exists or not when we do per destination
        // filtered adapter processing and only add if one does not exists already
        var multiKaha = (MultiKahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        // Verify 2 adapters created and not 3
        assertEquals(2, multiKaha.getAdapters().size());
        Set<File> dirs = multiKaha.getAdapters().stream().map(adapter -> adapter.getDirectory()).collect(
            Collectors.toSet());
        broker.stop();
        broker.waitUntilStopped();

        //restart should succeed with fix
        prepareBrokerWithMultiStore(false);
        broker.start();
        broker.waitUntilStarted();

        // Verify 2 adapters created and not 3 and same dirs
        multiKaha = (MultiKahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        assertEquals(2, multiKaha.getAdapters().size());
        assertEquals(dirs, multiKaha.getAdapters().stream().map(adapter -> adapter.getDirectory()).collect(
            Collectors.toSet()));
    }

    private void sendMessages(boolean isTopic) throws Exception {
        Connection connection = cf.createConnection();
        Session session = connection.createSession();
        MessageProducer producer = isTopic ? session.createProducer(session
                .createTopic(TOPIC_NAME)) : session.createProducer(session
                .createQueue(QUEUE_NAME));

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            TextMessage message = session
                    .createTextMessage("Test message " + i);
            producer.send(message);
        }

        producer.close();
        session.close();
        connection.close();

        if(!isTopic) {
            assertQueueLength(MESSAGE_COUNT);
        }
    }

    private void assertQueueLength(int len) throws Exception, IOException {
        Set<org.apache.activemq.broker.region.Destination> destinations = broker.getBroker().getDestinations(
                new ActiveMQQueue(QUEUE_NAME));
        org.apache.activemq.broker.region.Queue queue = (Queue) destinations.iterator().next();
        assertEquals(len, queue.getMessageStore().getMessageCount());
    }

    protected BrokerService createBroker(PersistenceAdapter kaha) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setBrokerName("localhost");
        broker.setPersistenceAdapter(kaha);
        return broker;
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
        ArrayList<FilteredKahaDBPersistenceAdapter> adapters = new ArrayList<>();

        //have a topic wildcard filtered adapter
        FilteredKahaDBPersistenceAdapter adapter1 = new FilteredKahaDBPersistenceAdapter();
        adapter1.setPersistenceAdapter(createStore(deleteAllMessages));
        adapter1.setTopic(">");
        adapters.add(adapter1);

        //have another per destination filtered adapter
        FilteredKahaDBPersistenceAdapter adapter2 = new FilteredKahaDBPersistenceAdapter();
        adapter2.setPersistenceAdapter(createStore(deleteAllMessages));
        adapter2.setPerDestination(true);
        adapters.add(adapter2);

        multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(adapters);
        broker = createBroker(multiKahaDBPersistenceAdapter);
    }
}
