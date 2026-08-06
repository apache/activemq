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
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.kahadb.MessageDatabase.StoredDestination;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.PageFile.PageFileCompactionStrategy;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KahaDBIndexCompactionTest {

    private static final Logger LOG = LoggerFactory.getLogger(KahaDBIndexCompactionTest.class);

    @Rule
    public TemporaryFolder dataDir = new TemporaryFolder(new File("target"));
    private final String payload = new String(new byte[1024]);

    private BrokerService broker = null;
    private final ActiveMQQueue destination = new ActiveMQQueue("Test");
    private KahaDBPersistenceAdapter adapter;


    protected void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setDataDirectory(dataDir.getRoot().getAbsolutePath());
        adapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        adapter.setJournalDiskSyncStrategy(JournalDiskSyncStrategy.NEVER.name());
        adapter.setEnableIndexDiskSyncs(true);
        adapter.setIndexCompactionStrategy(PageFileCompactionStrategy.TRUNCATION);

        // disable cleanup task so we can control it int ests
        adapter.setCheckpointInterval(0);
        adapter.setCleanupInterval(0);
        broker.start();
        LOG.info("Starting broker..");
    }

    @Before
    public void start() throws Exception {
        startBroker();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testTruncate() throws Exception {
        final KahaDBStore store = adapter.getStore();
        var pageFile = store.getPageFile();

        //Add a single message and update once so we can compare the size consistently
        MessageStore messageStore = store.createQueueMessageStore(destination);
        messageStore.start();

        for (int i = 0; i < 1000; i++) {
            ActiveMQTextMessage textMessage = getMessage(new MessageId("111:222:" + i));
            messageStore.addMessage(broker.getAdminConnectionContext(), textMessage);
        }

        long diskSize = pageFile.getDiskSize();
        store.checkpointCleanup(true);
        // should not be able to truncate as we added a bunch of data to the index
        // but have not cleared it yet
        assertEquals(diskSize, store.getPageFile().getDiskSize());

        for (int i = 0; i < 1000; i++) {
            MessageId messageId = new MessageId("111:222:" + i);
            MessageAck ack = new MessageAck();
            ack.setMessageID(messageId);
            ack.setDestination(messageStore.getDestination());
            ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);
            ack.setMessageCount(1);
            messageStore.removeMessage(broker.getAdminConnectionContext(), ack);
        }

        // get free page size before cleanup
        long freePages = pageFile.getFreePageCount();
        long totalPages = pageFile.getPageCount();

        // After consuming all messages cleanup again, we should be able to truncate
        // as there are > 30% free pages in the page file
        store.checkpointCleanup(true);
        assertTrue(store.getPageFile().getDiskSize() < diskSize);
        assertTrue(store.getPageFile().getFreePageCount() < freePages);
        assertEquals(totalPages * store.getPageFile().getMinFreePageCompactionRatio(),
            pageFile.getFreePageCount(), 1);
    }


    private ActiveMQTextMessage getMessage(final MessageId messageId) throws Exception {
        ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setMessageId(messageId);
        textMessage.setText(payload);

        return textMessage;
    }

}
