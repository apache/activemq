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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuplicatedAdapterOnRestartTest {

    private static final Logger LOG = LoggerFactory.getLogger(DuplicatedAdapterOnRestartTest.class);

    private static final String KAHADB_DIRECTORY = "target/activemq-data/";

    private BrokerService broker = null;

    @Test
    public void shouldNotFailOnBrokerRestartOnDuplicatedEntries() throws Exception {
        startBroker();
        restartBroker();

        assertNotNull(broker);
        assertTrue(broker.isStarted());
    }

    @After
    public void tearDown() throws Exception {
        if ( broker != null ) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private void startBroker() throws Exception {
        doStartBroker(true);
    }

    private void restartBroker() throws Exception {
        if ( broker != null ) {
            broker.stop();
            broker.waitUntilStopped();
        }

        doStartBroker(false);
    }

    private void doStartBroker(boolean delete) throws Exception {
        doCreateBroker(delete);
        LOG.info("Starting broker..");
        broker.start();
    }

    private void doCreateBroker(boolean delete) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(delete);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setDataDirectory(KAHADB_DIRECTORY);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setUseCache(false);
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);
        broker.setDestinations(new ActiveMQDestination[] { //
                ActiveMQDestination.createDestination("TEST_QUEUE_1", ActiveMQDestination.QUEUE_TYPE), //
                ActiveMQDestination.createDestination("TEST_QUEUE_2", ActiveMQDestination.QUEUE_TYPE), //
                ActiveMQDestination.createDestination("TEST_QUEUE_3", ActiveMQDestination.QUEUE_TYPE), //
                ActiveMQDestination.createDestination("TEST_QUEUE_PREFIX.queue1", ActiveMQDestination.QUEUE_TYPE), //
                ActiveMQDestination.createDestination("TEST_TOPIC_PREFIX.topic1", ActiveMQDestination.TOPIC_TYPE), //
                ActiveMQDestination.createDestination("TEST_TOPIC_PREFIX.topic2", ActiveMQDestination.TOPIC_TYPE), //
        });

        broker.setPersistenceAdapter(getPersistenceConfiguration());
    }

    private MultiKahaDBPersistenceAdapter getPersistenceConfiguration() {
        KahaDBPersistenceAdapter queueAdapter = new KahaDBPersistenceAdapter();
        queueAdapter.setCheckForCorruptJournalFiles(true);
        queueAdapter.setCheckpointInterval(0);
        queueAdapter.setCleanupInterval(0);
        FilteredKahaDBPersistenceAdapter queueFilteredAdapter = new FilteredKahaDBPersistenceAdapter();
        queueFilteredAdapter.setQueue("TEST_QUEUE_PREFIX.>");
        queueFilteredAdapter.setPersistenceAdapter(queueAdapter);

        KahaDBPersistenceAdapter topicAdapter = new KahaDBPersistenceAdapter();
        topicAdapter.setCheckForCorruptJournalFiles(true);
        topicAdapter.setCheckpointInterval(0);
        topicAdapter.setCleanupInterval(0);
        FilteredKahaDBPersistenceAdapter topicFilteredAdapter = new FilteredKahaDBPersistenceAdapter();
        topicFilteredAdapter.setTopic("TEST_TOPIC_PREFIX.>");
        topicFilteredAdapter.setPersistenceAdapter(topicAdapter);

        KahaDBPersistenceAdapter perDestinationAdapter = new KahaDBPersistenceAdapter();
        perDestinationAdapter.setCheckForCorruptJournalFiles(true);
        perDestinationAdapter.setCheckpointInterval(0);
        perDestinationAdapter.setCleanupInterval(0);
        FilteredKahaDBPersistenceAdapter perDestinationFilteredAdapter = new FilteredKahaDBPersistenceAdapter();
        perDestinationFilteredAdapter.setPerDestination(true);
        perDestinationFilteredAdapter.setPersistenceAdapter(perDestinationAdapter);

        MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter = new MultiKahaDBPersistenceAdapter();
        multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(Stream.of(queueFilteredAdapter, topicFilteredAdapter, perDestinationFilteredAdapter)
                .collect(Collectors.toList()));

        return multiKahaDBPersistenceAdapter;
    }
}
