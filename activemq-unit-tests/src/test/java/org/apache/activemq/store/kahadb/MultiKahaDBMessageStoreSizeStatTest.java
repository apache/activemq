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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.store.AbstractMessageStoreSizeStatTest;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks that KahaDB properly sets the new storeMessageSize
 * statistic.
 *
 * AMQ-5748
 *
 */
@RunWith(Parameterized.class)
public class MultiKahaDBMessageStoreSizeStatTest extends
        AbstractMessageStoreSizeStatTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(MultiKahaDBMessageStoreSizeStatTest.class);

    @Parameters(name = "subStatsEnabled={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            // Subscription stats on
            {true},
            // Subscription stats off
            {false}
        });
    }

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    public MultiKahaDBMessageStoreSizeStatTest(boolean subStatsEnabled) {
        super(subStatsEnabled);
    }

    @Override
    protected void setUpBroker(boolean clearDataDir) throws Exception {
        if (clearDataDir && dataFileDir.getRoot().exists())
            FileUtils.cleanDirectory(dataFileDir.getRoot());
        super.setUpBroker(clearDataDir);
    }

    @Override
    protected void initPersistence(BrokerService brokerService)
            throws IOException {
        broker.setPersistent(true);

        //setup multi-kaha adapter
        MultiKahaDBPersistenceAdapter persistenceAdapter = new MultiKahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(dataFileDir.getRoot());

        KahaDBPersistenceAdapter kahaStore = new KahaDBPersistenceAdapter();
        kahaStore.setJournalMaxFileLength(1024 * 512);
        kahaStore.setEnableSubscriptionStatistics(subStatsEnabled);

        //set up a store per destination
        FilteredKahaDBPersistenceAdapter filtered = new FilteredKahaDBPersistenceAdapter();
        filtered.setPersistenceAdapter(kahaStore);
        filtered.setPerDestination(true);
        List<FilteredKahaDBPersistenceAdapter> stores = new ArrayList<>();
        stores.add(filtered);

        persistenceAdapter.setFilteredPersistenceAdapters(stores);
        broker.setPersistenceAdapter(persistenceAdapter);
    }

    /**
     * Test that the the counter restores size and works after restart and more
     * messages are published
     *
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testMessageSizeAfterRestartAndPublish() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Destination dest = publishTestQueueMessages(200, publishedMessageSize);

        // verify the count and size
        verifyStats(dest, 200, publishedMessageSize.get());

        // stop, restart broker and publish more messages
        stopBroker();
        this.setUpBroker(false);
        dest = publishTestQueueMessages(200, publishedMessageSize);

        // verify the count and size
        verifyStats(dest, 400, publishedMessageSize.get());

    }

    @Test(timeout=60000)
    public void testMessageSizeAfterRestartAndPublishMultiQueue() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();
        AtomicLong publishedMessageSize2 = new AtomicLong();

        Destination dest = publishTestQueueMessages(200, publishedMessageSize);

        // verify the count and size
        verifyStats(dest, 200, publishedMessageSize.get());
        assertTrue(broker.getPersistenceAdapter().size() > publishedMessageSize.get());

        Destination dest2 = publishTestQueueMessages(200, "test.queue2", publishedMessageSize2);

        // verify the count and size
        verifyStats(dest2, 200, publishedMessageSize2.get());
        assertTrue(broker.getPersistenceAdapter().size() > publishedMessageSize.get() + publishedMessageSize2.get());

        // stop, restart broker and publish more messages
        stopBroker();
        this.setUpBroker(false);
        dest = publishTestQueueMessages(200, publishedMessageSize);
        dest2 = publishTestQueueMessages(200, "test.queue2", publishedMessageSize2);

        // verify the count and size after publishing messages
        verifyStats(dest, 400, publishedMessageSize.get());
        verifyStats(dest2, 400, publishedMessageSize2.get());

        assertTrue(broker.getPersistenceAdapter().size() > publishedMessageSize.get() + publishedMessageSize2.get());
        assertTrue(broker.getPersistenceAdapter().size() >=
                (dest.getMessageStore().getMessageSize() + dest2.getMessageStore().getMessageSize()));

    }

}
