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
package org.apache.activemq.store.kahadb.disk.journal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
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
        store.start();
        final Journal journal = store.getJournal();
        assertTrue(journal.isJournalDiskSyncPeriodic());
        assertFalse(store.isEnableJournalDiskSyncs());

        MessageStore messageStore = store.createQueueMessageStore(new ActiveMQQueue("test"));

        //write a message to the store
        writeMessage(messageStore, 1);

        //Make sure the flag was set to true
        assertTrue(Wait.waitFor(new Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return journal.currentFileNeedSync.get();
            }
        }));

        //Make sure a disk sync was done by the executor because a message was added
        //which will cause the flag to be set to false
        assertTrue(Wait.waitFor(new Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return !journal.currentFileNeedSync.get();
            }
        }));

    }

    @Test
    public void testSyncRotate()throws Exception {
        store = configureStore(JournalDiskSyncStrategy.PERIODIC);
        //Set a long interval to make sure it isn't called in this test
        store.setJournalDiskSyncInterval(10 * 1000);
        store.start();

        final Journal journal = store.getJournal();
        assertTrue(journal.isJournalDiskSyncPeriodic());
        assertFalse(store.isEnableJournalDiskSyncs());
        assertEquals(10 * 1000, store.getJournalDiskSyncInterval());
        journal.currentFileNeedSync.set(true);        //Make sure a disk sync was done by the executor because a message was added

        //get the current file but pass in a size greater than the
        //journal length to trigger a rotation so we can verify that it was synced
        journal.getCurrentDataFile(2 * defaultJournalLength);

        //verify a sync was called (which will set this flag to false)
        assertFalse(journal.currentFileNeedSync.get());
    }

    @Test
    public void testAlwaysSync()throws Exception {
        store = configureStore(JournalDiskSyncStrategy.ALWAYS);
        store.start();
        assertFalse(store.getJournal().isJournalDiskSyncPeriodic());
        assertTrue(store.isEnableJournalDiskSyncs());
    }

    @Test
    public void testNeverSync() throws Exception {
        store = configureStore(JournalDiskSyncStrategy.NEVER);
        store.start();
        assertFalse(store.getJournal().isJournalDiskSyncPeriodic());
        assertFalse(store.isEnableJournalDiskSyncs());
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
