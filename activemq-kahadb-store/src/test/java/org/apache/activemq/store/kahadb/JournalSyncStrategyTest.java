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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.kahadb.disk.journal.FileAppender;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class JournalSyncStrategyTest  {

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    @Rule
    public Timeout globalTimeout= new Timeout(10, TimeUnit.SECONDS);

    private KahaDBStore store;
    private int defaultJournalLength = 10 * 1024;

    @After
    public void after() throws Exception {
        if (store != null) {
            store.stop();
        }
    }

    @Test
    public void testPeriodicSync()throws Exception {
        store = configureStore(JournalDiskSyncStrategy.PERIODIC);
        store.setJournalDiskSyncInterval(800);
        store.start();
        final Journal journal = store.getJournal();
        assertTrue(journal.isJournalDiskSyncPeriodic());
        assertFalse(store.isEnableJournalDiskSyncs());
        assertEquals(store.getJournalDiskSyncStrategy(), JournalDiskSyncStrategy.PERIODIC.name());
        assertEquals(store.getJournalDiskSyncStrategyEnum(), JournalDiskSyncStrategy.PERIODIC);
        assertEquals(store.getJournal().getJournalDiskSyncStrategy(), JournalDiskSyncStrategy.PERIODIC);
        assertEquals(store.getJournalDiskSyncInterval(), 800);

        Location l = store.lastAsyncJournalUpdate.get();

        //write a message to the store
        MessageStore messageStore = store.createQueueMessageStore(new ActiveMQQueue("test"));
        writeMessage(messageStore, 1);

        //make sure message write causes the lastAsyncJournalUpdate to be set with a new value
        assertFalse(store.lastAsyncJournalUpdate.get().equals(l));
    }

    @Test
    public void testAlwaysSync()throws Exception {
        store = configureStore(JournalDiskSyncStrategy.ALWAYS);
        store.start();
        assertFalse(store.getJournal().isJournalDiskSyncPeriodic());
        assertTrue(store.isEnableJournalDiskSyncs());
        assertEquals(store.getJournalDiskSyncStrategy(), JournalDiskSyncStrategy.ALWAYS.name());
        assertEquals(store.getJournalDiskSyncStrategyEnum(), JournalDiskSyncStrategy.ALWAYS);
        assertEquals(store.getJournal().getJournalDiskSyncStrategy(), JournalDiskSyncStrategy.ALWAYS);

        MessageStore messageStore = store.createQueueMessageStore(new ActiveMQQueue("test"));
        writeMessage(messageStore, 1);
        assertNull(store.lastAsyncJournalUpdate.get());
    }

    @Test
    public void testNeverSync() throws Exception {
        store = configureStore(JournalDiskSyncStrategy.NEVER);
        store.start();
        assertFalse(store.getJournal().isJournalDiskSyncPeriodic());
        assertFalse(store.isEnableJournalDiskSyncs());
        assertEquals(store.getJournalDiskSyncStrategy(), JournalDiskSyncStrategy.NEVER.name());
        assertEquals(store.getJournalDiskSyncStrategyEnum(), JournalDiskSyncStrategy.NEVER);
        assertEquals(store.getJournal().getJournalDiskSyncStrategy(), JournalDiskSyncStrategy.NEVER);

        MessageStore messageStore = store.createQueueMessageStore(new ActiveMQQueue("test"));
        writeMessage(messageStore, 1);
        assertNull(store.lastAsyncJournalUpdate.get());
    }

    private KahaDBStore configureStore(JournalDiskSyncStrategy strategy) throws Exception {
        KahaDBStore store = new KahaDBStore();
        store.setJournalMaxFileLength(defaultJournalLength);
        store.deleteAllMessages();
        store.setDirectory(dataFileDir.getRoot());
        if (strategy != null) {
            store.setJournalDiskSyncStrategy(strategy.name());
        }

        return store;
    }

    private void writeMessage(final MessageStore messageStore, int num) throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("testtesttest");
        MessageId messageId = new MessageId("ID:localhost-56913-1254499826208-0:0:1:1:" + num);
        messageId.setBrokerSequenceId(num);
        message.setMessageId(messageId);
        messageStore.addMessage(new ConnectionContext(), message);
    }


}
