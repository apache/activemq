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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQ-5875
 *
 * This test shows that when multiple destinations share a single KahaDB
 * instance when using mKahaDB, that the deletion of one Topic will no longer
 * cause an IllegalStateException and the store will be properly kept around
 * until all destinations associated with the store are gone.
 *
 * */
@RunWith(Parameterized.class)
public class MultiKahaDBTopicDeletionTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(MultiKahaDBTopicDeletionTest.class);

    protected BrokerService brokerService;
    protected Broker broker;
    protected URI brokerConnectURI;
    protected File storeDir;
    protected ActiveMQTopic topic1;
    protected ActiveMQTopic topic2;

    protected static ActiveMQTopic TOPIC1 = new ActiveMQTopic("test.>");
    protected static ActiveMQTopic TOPIC2 = new ActiveMQTopic("test.t.topic");


    @Parameters
    public static Collection<Object[]> data() {

        //Test with topics created in different orders
        return Arrays.asList(new Object[][] {
                {TOPIC1, TOPIC2},
                {TOPIC2, TOPIC1}
        });
    }

    public MultiKahaDBTopicDeletionTest(ActiveMQTopic topic1, ActiveMQTopic topic2) {
        this.topic1 = topic1;
        this.topic2 = topic2;
    }

    @Rule
    public TemporaryFolder tempTestDir = new TemporaryFolder();

    @Before
    public void startBroker() throws Exception {
        setUpBroker(true);
    }

    protected void setUpBroker(boolean clearDataDir) throws Exception {
        brokerService = new BrokerService();
        this.initPersistence(brokerService);
        // set up a transport
        TransportConnector connector = brokerService
                .addConnector(new TransportConnector());
        connector.setUri(new URI("tcp://0.0.0.0:0"));
        connector.setName("tcp");

        brokerService.start();
        brokerService.waitUntilStarted();
        brokerConnectURI = brokerService.getConnectorByName("tcp")
                .getConnectUri();
        broker = brokerService.getBroker();
    }

    @After
    public void stopBroker() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    protected void initPersistence(BrokerService brokerService)
            throws IOException {
        storeDir = tempTestDir.getRoot();
        brokerService.setPersistent(true);

        // setup multi-kaha adapter
        MultiKahaDBPersistenceAdapter persistenceAdapter = new MultiKahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(storeDir);

        KahaDBPersistenceAdapter kahaStore = new KahaDBPersistenceAdapter();
        kahaStore.setJournalMaxFileLength(1024 * 512);

        // set up a store per destination
        FilteredKahaDBPersistenceAdapter filtered = new FilteredKahaDBPersistenceAdapter();
        filtered.setPersistenceAdapter(kahaStore);
        filtered.setPerDestination(true);
        List<FilteredKahaDBPersistenceAdapter> stores = new ArrayList<>();
        stores.add(filtered);

        persistenceAdapter.setFilteredPersistenceAdapters(stores);
        brokerService.setPersistenceAdapter(persistenceAdapter);
    }

    /**
     * Test that a topic can be deleted and the other topic can still be subscribed to
     * @throws Exception
     */
    @Test
    public void testTopic1Deletion() throws Exception {
        LOG.info("Creating {} first, {} second", topic1, topic2);
        LOG.info("Removing {}, subscribing to {}", topic1, topic2);

        // Create two topics
        broker.addDestination(brokerService.getAdminConnectionContext(), topic1, false);
        broker.addDestination(brokerService.getAdminConnectionContext(), topic2, false);

        // remove topic2
        broker.removeDestination(brokerService.getAdminConnectionContext(), topic1, 100);

        // try and create a subscription on topic2, before AMQ-5875 this
        //would cause an IllegalStateException
        createSubscriber(topic2);
    }


    @Test
    public void testTopic2Deletion() throws Exception {
        LOG.info("Creating {} first, {} second", topic1, topic2);
        LOG.info("Removing {}, subscribing to {}", topic2, topic1);

        // Create two topics
        broker.addDestination(brokerService.getAdminConnectionContext(), topic1, false);
        broker.addDestination(brokerService.getAdminConnectionContext(), topic2, false);

        // remove topic2
        broker.removeDestination(brokerService.getAdminConnectionContext(), topic2, 100);

        // try and create a subscription on topic1, before AMQ-5875 this
        //would cause an IllegalStateException
        createSubscriber(topic1);
    }


    @Test
    public void testStoreCleanupDeleteTopic1First() throws Exception {
        LOG.info("Creating {} first, {} second", topic1, topic2);
        LOG.info("Deleting {} first, {} second", topic1, topic2);

        // Create two topics
        broker.addDestination(brokerService.getAdminConnectionContext(), topic1, false);
        broker.addDestination(brokerService.getAdminConnectionContext(), topic2, false);

        // remove both topics
        broker.removeDestination(brokerService.getAdminConnectionContext(), topic1, 100);
        broker.removeDestination(brokerService.getAdminConnectionContext(), topic2, 100);

        //Assert that with no more destinations attached to a store that it has been cleaned up
        Collection<File> storeFiles = FileUtils.listFiles(storeDir, new WildcardFileFilter("db*"), new WildcardFileFilter("topic*"));
        assertEquals("Store files should be deleted", 0, storeFiles.size());

    }

    @Test
    public void testStoreCleanupDeleteTopic2First() throws Exception {
        LOG.info("Creating {} first, {} second", topic1, topic2);
        LOG.info("Deleting {} first, {} second", topic2, topic1);

        // Create two topics
        broker.addDestination(brokerService.getAdminConnectionContext(), topic1, false);
        broker.addDestination(brokerService.getAdminConnectionContext(), topic2, false);

        // remove both topics
        broker.removeDestination(brokerService.getAdminConnectionContext(), topic2, 100);
        broker.removeDestination(brokerService.getAdminConnectionContext(), topic1, 100);

        //Assert that with no more destinations attached to a store that it has been cleaned up
        Collection<File> storeFiles = FileUtils.listFiles(storeDir, new WildcardFileFilter("db*"), new WildcardFileFilter("topic*"));
        assertEquals("Store files should be deleted", 0, storeFiles.size());

    }


    protected void createSubscriber(ActiveMQTopic topic) throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                brokerConnectURI);
        Connection connection = factory.createConnection();
        connection.setClientID("client1");
        connection.start();
        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "sub1");
    }

}
