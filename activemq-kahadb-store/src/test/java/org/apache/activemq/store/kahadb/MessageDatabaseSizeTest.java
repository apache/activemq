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
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.kahadb.MessageDatabase.StoredDestination;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaUpdateMessageCommand;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
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

@RunWith(Parameterized.class)
public class MessageDatabaseSizeTest {

    private static final Logger LOG = LoggerFactory.getLogger(MessageDatabaseSizeTest.class);

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
    public TemporaryFolder dataDir = new TemporaryFolder(new File("target"));
    private final String payload = new String(new byte[1024]);

    private BrokerService broker = null;
    private final ActiveMQQueue destination = new ActiveMQQueue("Test");
    private KahaDBPersistenceAdapter adapter;
    private boolean subStatsEnabled;

    public MessageDatabaseSizeTest(boolean subStatsEnabled) {
        super();
        this.subStatsEnabled = subStatsEnabled;
    }


    protected void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setDataDirectory(dataDir.getRoot().getAbsolutePath());
        adapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        adapter.setEnableSubscriptionStatistics(subStatsEnabled);
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

    /**
     * Test that when only updating the index and not rewriting the message to the journal
     * that the size doesn't change
     *
     * This was broken before AMQ-6356
     */
    @Test
    public void testUpdateMessageSameLocation() throws Exception {
        final KahaDBStore store = adapter.getStore();
        MessageId messageId = new MessageId("111:222:333");
        ActiveMQTextMessage textMessage = getMessage(new MessageId("111:222:333"));

        //Add a single message and update once so we can compare the size consistently
        MessageStore messageStore = store.createQueueMessageStore(destination);
        messageStore.start();
        messageStore.addMessage(broker.getAdminConnectionContext(), textMessage);
        messageStore.updateMessage(textMessage);

        Location location = findMessageLocation(messageId.toString(), store.convert(destination));
        long existingSize = messageStore.getMessageSize();

        //Process the update command for the index and verify the size doesn't change
        KahaUpdateMessageCommand updateMessageCommand = (KahaUpdateMessageCommand) store.load(location);
        store.process(updateMessageCommand, location);
        assertEquals(existingSize, messageStore.getMessageSize());
    }

    @Test
    public void testUpdateMessageSameLocationDifferentSize() throws Exception {
        final KahaDBStore store = adapter.getStore();
        MessageId messageId = new MessageId("111:222:333");
        ActiveMQTextMessage textMessage = getMessage(new MessageId("111:222:333"));

        //Add a single message and update once so we can compare the size consistently
        MessageStore messageStore = store.createQueueMessageStore(destination);
        messageStore.start();
        messageStore.addMessage(broker.getAdminConnectionContext(), textMessage);
        textMessage.setText("new size of message");
        messageStore.updateMessage(textMessage);

        assertNotNull(findMessageLocation(messageId.toString(), store.convert(destination)));

    }

    /**
     * Test that when updating an existing message to a different location in the
     * journal that the index size doesn't change
     */
    @Test
    public void testUpdateMessageDifferentLocation() throws Exception {
        final KahaDBStore store = adapter.getStore();
        ActiveMQTextMessage textMessage = getMessage(new MessageId("111:222:333"));

        //Add a single message and update once so we can compare the size consistently
        MessageStore messageStore = store.createQueueMessageStore(destination);
        messageStore.addMessage(broker.getAdminConnectionContext(), textMessage);
        messageStore.updateMessage(textMessage);

        //Update again and make sure the size is the same
        long existingSize = messageStore.getMessageSize();
        messageStore.updateMessage(textMessage);
        assertEquals(existingSize, messageStore.getMessageSize());
    }

    private ActiveMQTextMessage getMessage(final MessageId messageId) throws Exception {
        ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setMessageId(messageId);
        textMessage.setText(payload);

        return textMessage;
    }

    private Location findMessageLocation(final String key, final KahaDestination destination) throws IOException {
        final KahaDBStore store = adapter.getStore();
        return store.pageFile.tx().execute(new Transaction.CallableClosure<Location, IOException>() {
            @Override
            public Location execute(Transaction tx) throws IOException {
                StoredDestination sd = store.getStoredDestination(destination, tx);
                Long sequence = sd.messageIdIndex.get(tx, key);
                if (sequence == null) {
                    return null;
                }
                return sd.orderIndex.get(tx, sequence).location;
            }
        });
    }

}
