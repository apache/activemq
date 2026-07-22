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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryContext;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Proves that recoverMessages(MessageRecoveryContext) with a isolated cursor
 * is fully isolated from the destination's live cursor state.
 *
 * The MessageOrderIterator records the last visited sequence keys
 * (lastDefaultKey/lastHighKey/lastLowKey) on the shared MessageOrderIndex.
 * Before the fix, a isolated-cursor visit also wrote those bookmarks, and
 * the next zero-entry live batch (recoverNextMessages) committed them into
 * the destination cursor via stoppedIterating() — rewinding the cursor
 * (duplicates from store) or jumping it forward (missed messages).
 *
 * Also proves that a isolated-cursor vsite does not consume rolled-back
 * transactional acks that the live cursor must redeliver.
 */
public class KahaDBRecoverMessagesIsolatedCursorTest {

    protected BrokerService brokerService = null;
    private KahaDBStore kahaDBStore = null;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void beforeEach() throws Exception {
        kahaDBStore = createStore(true);
        brokerService = createBroker(kahaDBStore);
    }

    @After
    public void afterEach() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
        kahaDBStore = null;
    }

    protected BrokerService createBroker(KahaDBStore kaha) throws Exception {
        var broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setPersistenceAdapter(kaha);

        // keep the broker-side queue completely passive so only this test
        // drives the store cursor
        var policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(0);
        var policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);

        broker.start();
        broker.waitUntilStarted(10_000L);
        return broker;
    }

    private KahaDBStore createStore(boolean delete) throws Exception {
        var kaha = new KahaDBStore();
        kaha.setJournalMaxFileLength(1024 * 100);
        kaha.setDirectory(new File(IOHelper.getDefaultDataDirectory(), "kahadb-isolated-cursor-tests"));
        if (delete) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    /**
     * A isolated-cursor visit between two live batches must not leave state
     * behind that a later zero-entry live batch commits into the destination
     * cursor. Before the fix the final recoverNextMessages() re-delivered the
     * tail of the queue (duplicates from store).
     */
    @Test
    public void testIsolatedCursorVisitDoesNotCorruptLiveCursor() throws Exception {
        var queueName = testName.getMethodName();
        sendMessages(10, queueName);
        var messageStore = kahaDBStore.createQueueMessageStore(new ActiveMQQueue(queueName));

        // live batch 1: page all 10 messages through the destination cursor
        var liveBatch1 = new TestMessageRecoveryListener();
        messageStore.recoverNextMessages(10, liveBatch1);
        assertEquals(10, liveBatch1.getRecoveredMessages().size());

        // isolated-cursor visit from the head of the store (messages are
        // still in the index — dispatched but unacked)
        var visit = new TestMessageRecoveryListener();
        messageStore.recoverMessages(new MessageRecoveryContext.Builder()
                .messageRecoveryListener(visit)
                .offset(0L)
                .maxMessageCountReturned(5)
                .build());
        assertEquals(5, visit.getRecoveredMessages().size());

        // live batch 2: nothing new to page in — a zero-entry batch. Before
        // the fix, stoppedIterating() committed the visit bookmark here,
        // rewinding the destination cursor.
        var liveBatch2 = new TestMessageRecoveryListener();
        messageStore.recoverNextMessages(10, liveBatch2);
        assertEquals(0, liveBatch2.getRecoveredMessages().size());

        // live batch 3: must still be empty. Before the fix this re-delivered
        // the 5 messages beyond the visit window ("duplicate from store").
        var liveBatch3 = new TestMessageRecoveryListener();
        messageStore.recoverNextMessages(10, liveBatch3);
        assertEquals("live cursor re-delivered store messages after a isolated-cursor visit",
                0, liveBatch3.getRecoveredMessages().size());
    }

    /**
     * A isolated-cursor visit ahead of the live cursor must not cause the
     * live cursor to skip messages. Before the fix the visit bookmark was
     * committed by a zero-entry live batch, jumping the cursor past messages
     * that were never dispatched (stuck queue).
     */
    @Test
    public void testIsolatedCursorVisitDoesNotSkipLiveMessages() throws Exception {
        var queueName = testName.getMethodName();
        sendMessages(10, queueName);
        var messageStore = kahaDBStore.createQueueMessageStore(new ActiveMQQueue(queueName));

        // live batch 1: page in only the first 2 messages
        var liveBatch1 = new TestMessageRecoveryListener();
        messageStore.recoverNextMessages(2, liveBatch1);
        assertEquals(2, liveBatch1.getRecoveredMessages().size());

        // isolated-cursor visit across the whole store — reads sequences
        // ahead of the live cursor
        var visit = new TestMessageRecoveryListener();
        messageStore.recoverMessages(new MessageRecoveryContext.Builder()
                .messageRecoveryListener(visit)
                .offset(0L)
                .maxMessageCountReturned(10)
                .build());
        assertEquals(10, visit.getRecoveredMessages().size());

        // live batch 2: must continue exactly where batch 1 stopped and
        // deliver the remaining 8 messages. Before the fix a zero-entry
        // batch here would have committed the visit bookmark instead,
        // skipping messages 3..10 entirely.
        var liveBatch2 = new TestMessageRecoveryListener();
        messageStore.recoverNextMessages(10, liveBatch2);
        assertEquals("live cursor skipped messages after a isolated-cursor visit",
                8, liveBatch2.getRecoveredMessages().size());
    }

    /**
     * Shared-cursor mode (useIsolatedCursor=false) intentionally advances
     * the destination cursor — successive calls continue where the previous
     * one stopped. This documents the contract and guards it from regressing.
     */
    @Test
    public void testSharedCursorModeAdvancesLiveCursor() throws Exception {
        var queueName = testName.getMethodName();
        sendMessages(10, queueName);
        var messageStore = kahaDBStore.createQueueMessageStore(new ActiveMQQueue(queueName));

        var firstPage = new TestMessageRecoveryListener();
        messageStore.recoverMessages(new MessageRecoveryContext.Builder()
                .messageRecoveryListener(firstPage)
                .useIsolatedCursor(false)
                .maxMessageCountReturned(5)
                .build());
        assertEquals(5, firstPage.getRecoveredMessages().size());
        assertEquals(0, firstPage.getRecoveredMessages().get(0).getProperty("index"));

        var secondPage = new TestMessageRecoveryListener();
        messageStore.recoverMessages(new MessageRecoveryContext.Builder()
                .messageRecoveryListener(secondPage)
                .useIsolatedCursor(false)
                .maxMessageCountReturned(5)
                .build());
        assertEquals(5, secondPage.getRecoveredMessages().size());
        assertEquals(5, secondPage.getRecoveredMessages().get(0).getProperty("index"));

        // the shared cursor is now at the tail — the live batch sees nothing
        var liveBatch = new TestMessageRecoveryListener();
        messageStore.recoverNextMessages(10, liveBatch);
        assertEquals(0, liveBatch.getRecoveredMessages().size());
    }

    /**
     * Rolled-back transactional acks are queued for redelivery through the
     * live cursor. A isolated-cursor visit must not consume them: before
     * the fix the visit both received the rolled-back message (duplicating
     * it in its own scan results) and permanently removed it from the
     * redelivery map, so the live cursor never redelivered it.
     */
    @Test
    public void testIsolatedCursorVisitDoesNotConsumeRolledBackAcks() throws Exception {
        var queueName = testName.getMethodName();
        sendMessages(3, queueName);
        var messageStore = kahaDBStore.createQueueMessageStore(new ActiveMQQueue(queueName));

        // live batch 1: page all 3 messages through the destination cursor
        var liveBatch1 = new TestMessageRecoveryListener();
        messageStore.recoverNextMessages(3, liveBatch1);
        assertEquals(3, liveBatch1.getRecoveredMessages().size());
        MessageId rolledBackId = liveBatch1.getRecoveredMessages().get(1).getMessageId();

        // simulate an XA prepare + rollback outcome for the second message:
        // the store queues it for redelivery via the live cursor
        KahaDBStore.KahaDBMessageStore kahaMessageStore =
                (KahaDBStore.KahaDBMessageStore) ((ProxyMessageStore) messageStore).getDelegate();
        var rolledBackAck = new MessageAck();
        rolledBackAck.setLastMessageId(rolledBackId);
        var acks = new ArrayList<MessageAck>();
        acks.add(rolledBackAck);
        kahaMessageStore.trackRecoveredAcks(acks);
        kahaMessageStore.forgetRecoveredAcks(acks, true);

        // isolated-cursor visit: must see exactly the 3 messages in the
        // index — no rolled-back-ack replay mixed into visit results
        var visit = new TestMessageRecoveryListener();
        messageStore.recoverMessages(new MessageRecoveryContext.Builder()
                .messageRecoveryListener(visit)
                .offset(0L)
                .maxMessageCountReturned(10)
                .build());
        assertEquals("isolated-cursor visit consumed rolled-back ack redeliveries",
                3, visit.getRecoveredMessages().size());

        // the live cursor must still redeliver the rolled-back message
        var liveBatch2 = new TestMessageRecoveryListener();
        messageStore.recoverNextMessages(10, liveBatch2);
        assertEquals("rolled-back ack was not redelivered through the live cursor",
                1, liveBatch2.getRecoveredMessages().size());
        assertEquals(rolledBackId, liveBatch2.getRecoveredMessages().get(0).getMessageId());
    }

    private void sendMessages(int count, String queueName) throws JMSException {
        var cf = new ActiveMQConnectionFactory("vm://localhost");
        cf.setWatchTopicAdvisories(false);

        try (var connection = cf.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(new ActiveMQQueue(queueName))) {

            for (int i = 0; i < count; i++) {
                var textMessage = session.createTextMessage("message:" + i);
                textMessage.setIntProperty("index", i);
                producer.send(textMessage);
            }
        }
    }

    static class TestMessageRecoveryListener implements MessageRecoveryListener {

        final List<MessageId> recoveredMessageIds = new LinkedList<>();
        final List<Message> recoveredMessages = new LinkedList<>();

        @Override
        public boolean hasSpace() {
            return true;
        }

        @Override
        public boolean isDuplicate(MessageId messageId) {
            return recoveredMessageIds.contains(messageId);
        }

        @Override
        public boolean recoverMessage(Message message) throws Exception {
            return recoveredMessages.add(message);
        }

        @Override
        public boolean recoverMessageReference(MessageId messageId) throws Exception {
            return recoveredMessageIds.add(messageId);
        }

        public List<Message> getRecoveredMessages() {
            return recoveredMessages;
        }
    }
}
