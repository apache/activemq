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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractMultiKahaDBDeletionTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(MultiKahaDBTopicDeletionTest.class);

    protected BrokerService brokerService;
    protected Broker broker;
    protected URI brokerConnectURI;
    protected File storeDir;
    protected ActiveMQDestination dest1;
    protected ActiveMQDestination dest2;

    public AbstractMultiKahaDBDeletionTest(ActiveMQDestination dest1, ActiveMQDestination dest2) {
        this.dest1 = dest1;
        this.dest2 = dest2;
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
     * Test that a destination can be deleted and the other destination can still be subscribed to
     * @throws Exception
     */
    @Test
    public void testDest1Deletion() throws Exception {
        LOG.info("Creating {} first, {} second", dest1, dest2);
        LOG.info("Removing {}, subscribing to {}", dest1, dest2);

        // Create two destinations
        broker.addDestination(brokerService.getAdminConnectionContext(), dest1, false);
        broker.addDestination(brokerService.getAdminConnectionContext(), dest2, false);

        // remove destination2
        broker.removeDestination(brokerService.getAdminConnectionContext(), dest1, 100);

        // try and create a consumer on dest2, before AMQ-5875 this
        //would cause an IllegalStateException for Topics
        createConsumer(dest2);
        Collection<File> storeFiles = FileUtils.listFiles(storeDir, new WildcardFileFilter("db*"), getStoreFileFilter());
        assertTrue("Store index should still exist", storeFiles.size() >= 1);
    }


    @Test
    public void testDest2Deletion() throws Exception {
        LOG.info("Creating {} first, {} second", dest1, dest2);
        LOG.info("Removing {}, subscribing to {}", dest2, dest1);

        // Create two destinations
        broker.addDestination(brokerService.getAdminConnectionContext(), dest1, false);
        broker.addDestination(brokerService.getAdminConnectionContext(), dest2, false);

        // remove destination2
        broker.removeDestination(brokerService.getAdminConnectionContext(), dest2, 100);

        // try and create a consumer on dest1, before AMQ-5875 this
        //would cause an IllegalStateException for Topics
        createConsumer(dest1);
        Collection<File> storeFiles = FileUtils.listFiles(storeDir, new WildcardFileFilter("db*"), getStoreFileFilter());
        assertTrue("Store index should still exist", storeFiles.size() >= 1);
    }


    @Test
    public void testStoreCleanupDeleteDest1First() throws Exception {
        LOG.info("Creating {} first, {} second", dest1, dest2);
        LOG.info("Deleting {} first, {} second", dest1, dest2);

        // Create two destinations
        broker.addDestination(brokerService.getAdminConnectionContext(), dest1, false);
        broker.addDestination(brokerService.getAdminConnectionContext(), dest2, false);

        // remove both destinations
        broker.removeDestination(brokerService.getAdminConnectionContext(), dest1, 100);
        broker.removeDestination(brokerService.getAdminConnectionContext(), dest2, 100);

        //Assert that with no more destinations attached to a store that it has been cleaned up
        Collection<File> storeFiles = FileUtils.listFiles(storeDir, new WildcardFileFilter("db*"), getStoreFileFilter());
        assertEquals("Store files should be deleted", 0, storeFiles.size());

    }

    @Test
    public void testStoreCleanupDeleteDest2First() throws Exception {
        LOG.info("Creating {} first, {} second", dest1, dest2);
        LOG.info("Deleting {} first, {} second", dest2, dest1);

        // Create two destinations
        broker.addDestination(brokerService.getAdminConnectionContext(), dest1, false);
        broker.addDestination(brokerService.getAdminConnectionContext(), dest2, false);

        // remove both destinations
        broker.removeDestination(brokerService.getAdminConnectionContext(), dest2, 100);
        broker.removeDestination(brokerService.getAdminConnectionContext(), dest1, 100);

        //Assert that with no more destinations attached to a store that it has been cleaned up
        Collection<File> storeFiles = FileUtils.listFiles(storeDir, new WildcardFileFilter("db*"), getStoreFileFilter());
        assertEquals("Store files should be deleted", 0, storeFiles.size());

    }


    protected abstract void createConsumer(ActiveMQDestination dest) throws JMSException;

    protected abstract WildcardFileFilter getStoreFileFilter();

}
