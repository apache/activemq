/*
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
package org.apache.activemq.store.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.transaction.xa.XAResource;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.XAConnection;
import jakarta.jms.XASession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.SubscriptionKey;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Test for {@link JDBCTopicMessageStore#recoverExpired(Set, int, MessageRecoveryListener)}.
 * The JDBC store only keeps the last acked id of a durable subscription, so unlike KahaDB
 * only the run of expired messages that directly follows it is returned.
 */
@Category(ParallelTest.class)
public class JDBCRecoverExpiredTest {

    @Rule
    public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    private BrokerService broker;
    private Connection connection;
    private final ActiveMQTopic topic = new ActiveMQTopic("test.topic");
    private final SubscriptionKey subKey1 = new SubscriptionKey("clientId", "sub1");
    private final SubscriptionKey subKey2 = new SubscriptionKey("clientId", "sub2");

    private void startBroker(boolean prioritizedMessages) throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setSchedulerSupport(false);
        broker.setDataDirectoryFile(dataFileDir.getRoot());
        broker.setPersistenceAdapter(new JDBCPersistenceAdapter());
        broker.setDeleteAllMessagesOnStartup(true);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        // the test drives recoverExpired() itself
        policy.setExpireMessagesPeriod(0);
        policy.setPrioritizedMessages(prioritizedMessages);
        policyMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private Session initializeSubs() throws Exception {
        connection = new ActiveMQConnectionFactory("vm://localhost").createConnection();
        connection.setClientID("clientId");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "sub1").close();
        session.createDurableSubscriber(topic, "sub2").close();
        return session;
    }

    private TopicMessageStore store() throws Exception {
        Destination dest = broker.getDestination(topic);
        return (TopicMessageStore) dest.getMessageStore();
    }

    private List<MessageId> send(MessageProducer prod, int count, int priority, long ttl) throws Exception {
        List<MessageId> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText("message" + i);
            prod.send(message, Message.DEFAULT_DELIVERY_MODE, priority, ttl);
            ids.add(message.getMessageId());
        }
        return ids;
    }

    private void ack(SubscriptionKey sub, MessageId id) throws Exception {
        MessageAck ack = new MessageAck();
        ack.setLastMessageId(id);
        ack.setAckType(MessageAck.EXPIRED_ACK_TYPE);
        ack.setDestination(topic);
        store().acknowledge(broker.getAdminConnectionContext(), sub.getClientId(),
            sub.getSubscriptionName(), id, ack);
    }

    private static List<MessageId> ids(List<org.apache.activemq.command.Message> messages) {
        List<MessageId> ids = new ArrayList<>();
        for (org.apache.activemq.command.Message message : messages) {
            ids.add(message.getMessageId());
        }
        return ids;
    }

    // only the expired messages before the first non-expired one are returned, so that
    // acking them can never implicitly ack a message that has not expired
    @Test
    public void testRecoverExpiredStopsAtFirstNonExpired() throws Exception {
        startBroker(false);
        try (Session session = initializeSubs()) {
            MessageProducer prod = session.createProducer(topic);
            TopicMessageStore store = store();

            // nothing should be expired yet, no messages
            assertTrue(store.recoverExpired(Set.of(subKey1, subKey2), 100, listener).isEmpty());

            List<MessageId> first = send(prod, 3, Message.DEFAULT_PRIORITY, 1000);
            List<MessageId> noTtl = send(prod, 1, Message.DEFAULT_PRIORITY, 0);
            List<MessageId> last = send(prod, 3, Message.DEFAULT_PRIORITY, 1000);

            // wait for the time to pass the point of needing expiration
            Thread.sleep(1500);

            var expired = store.recoverExpired(Set.of(subKey1, subKey2), 100, listener);
            assertEquals(2, expired.size());
            assertEquals(first, ids(expired.get(subKey1)));
            assertEquals(first, ids(expired.get(subKey2)));

            // once sub1 acked the first run, the non-expired message blocks the rest
            for (MessageId id : first) {
                ack(subKey1, id);
            }
            expired = store.recoverExpired(Set.of(subKey1, subKey2), 100, listener);
            assertEquals(1, expired.size());
            assertEquals(first, ids(expired.get(subKey2)));

            // after it is consumed, the next run of expired messages is returned
            ack(subKey1, noTtl.get(0));
            expired = store.recoverExpired(Set.of(subKey1, subKey2), 100, listener);
            assertEquals(2, expired.size());
            assertEquals(last, ids(expired.get(subKey1)));
            assertEquals(first, ids(expired.get(subKey2)));
        }
    }

    @Test
    public void testNonExpiredHeadBlocksExpiry() throws Exception {
        startBroker(false);
        try (Session session = initializeSubs()) {
            MessageProducer prod = session.createProducer(topic);

            send(prod, 50, Message.DEFAULT_PRIORITY, 0);
            send(prod, 50, Message.DEFAULT_PRIORITY, 1000);
            Thread.sleep(1500);

            assertTrue(store().recoverExpired(Set.of(subKey1, subKey2), 100, listener).isEmpty());
        }
    }

    // test max number of messages to load works
    @Test
    public void testRecoverExpiredMax() throws Exception {
        startBroker(false);
        try (Session session = initializeSubs()) {
            MessageProducer prod = session.createProducer(topic);
            TopicMessageStore store = store();

            List<MessageId> sent = send(prod, 100, Message.DEFAULT_PRIORITY, 1000);
            Thread.sleep(1500);

            // both subs share the same 30 messages
            var expired = store.recoverExpired(Set.of(subKey1, subKey2), 30, listener);
            assertEquals(2, expired.size());
            assertEquals(sent.subList(0, 30), ids(expired.get(subKey1)));
            assertEquals(sent.subList(0, 30), ids(expired.get(subKey2)));

            for (int i = 0; i < 25; i++) {
                ack(subKey1, sent.get(i));
            }

            expired = store.recoverExpired(Set.of(subKey1, subKey2), 100, listener);
            assertEquals(2, expired.size());
            assertEquals(sent.subList(25, 100), ids(expired.get(subKey1)));
            assertEquals(sent, ids(expired.get(subKey2)));
        }
    }

    // Test that filtering works by the set of subscriptions
    @Test
    public void testRecoverExpiredSubSet() throws Exception {
        startBroker(false);
        try (Session session = initializeSubs()) {
            MessageProducer prod = session.createProducer(topic);
            TopicMessageStore store = store();

            List<MessageId> sent = send(prod, 10, Message.DEFAULT_PRIORITY, 1000);
            Thread.sleep(1500);

            var expired = store.recoverExpired(Set.of(subKey2), 100, listener);
            assertEquals(1, expired.size());
            assertEquals(10, expired.get(subKey2).size());

            ack(subKey2, sent.get(0));

            expired = store.recoverExpired(Set.of(subKey2), 100, listener);
            assertEquals(1, expired.size());
            assertEquals(9, expired.get(subKey2).size());

            expired = store.recoverExpired(Set.of(subKey1), 100, listener);
            assertEquals(1, expired.size());
            assertEquals(10, expired.get(subKey1).size());

            // verify passing in unmatched sub leaves it out of the result set
            var unmatched = new SubscriptionKey("clientId", "sub3");
            assertTrue(store.recoverExpired(Set.of(unmatched), 100, listener).isEmpty());

            expired = store.recoverExpired(Set.of(subKey1, subKey2, unmatched), 100, listener);
            assertEquals(2, expired.size());
        }
    }

    // with prioritized messages the last acked id is kept per priority,
    // so a non-expired message only blocks expiry of messages with the same priority
    @Test
    public void testRecoverExpiredPrioritized() throws Exception {
        startBroker(true);
        try (Session session = initializeSubs()) {
            MessageProducer prod = session.createProducer(topic);

            send(prod, 1, 4, 0);
            List<MessageId> high = send(prod, 2, 9, 1000);
            send(prod, 2, 4, 1000);
            Thread.sleep(1500);

            var expired = store().recoverExpired(Set.of(subKey1, subKey2), 100, listener);
            assertEquals(2, expired.size());
            assertEquals(high, ids(expired.get(subKey1)));
            assertEquals(high, ids(expired.get(subKey2)));
        }
    }

    // a message of a prepared XA transaction may still be rolled back or committed,
    // so it blocks expiry of the messages after it
    @Test
    public void testPreparedTransactionBlocksExpiry() throws Exception {
        startBroker(false);
        try (Session session = initializeSubs()) {
            MessageProducer prod = session.createProducer(topic);

            List<MessageId> before = send(prod, 2, Message.DEFAULT_PRIORITY, 1000);

            XAConnection xaConnection = new ActiveMQXAConnectionFactory("vm://localhost").createXAConnection();
            try {
                xaConnection.start();
                XASession xaSession = xaConnection.createXASession();
                XAResource resource = xaSession.getXAResource();
                XATransactionId xid = createXid();
                resource.start(xid, XAResource.TMNOFLAGS);
                MessageProducer xaProd = xaSession.createProducer(topic);
                xaProd.send(xaSession.createTextMessage("in-tx"), Message.DEFAULT_DELIVERY_MODE,
                    Message.DEFAULT_PRIORITY, 1000);
                resource.end(xid, XAResource.TMSUCCESS);
                resource.prepare(xid);

                send(prod, 2, Message.DEFAULT_PRIORITY, 1000);
                Thread.sleep(1500);

                var expired = store().recoverExpired(Set.of(subKey1, subKey2), 100, listener);
                assertEquals(2, expired.size());
                assertEquals(before, ids(expired.get(subKey1)));
                assertEquals(before, ids(expired.get(subKey2)));

                resource.commit(xid, false);

                expired = store().recoverExpired(Set.of(subKey1, subKey2), 100, listener);
                assertEquals(5, expired.get(subKey1).size());
                assertEquals(5, expired.get(subKey2).size());
            } finally {
                xaConnection.close();
            }
        }
    }

    // test recovery listener works with hasSpace()
    @Test
    public void testRecoverExpiredRecoveryListener() throws Exception {
        startBroker(false);
        try (Session session = initializeSubs()) {
            MessageProducer prod = session.createProducer(topic);
            TopicMessageStore store = store();

            send(prod, 10, Message.DEFAULT_PRIORITY, 1000);
            Thread.sleep(1500);

            // don't return any, has space is false
            final AtomicBoolean hasSpaceCalled = new AtomicBoolean();
            var expired = store.recoverExpired(Set.of(subKey1, subKey2), 100, new MessageRecoveryListener() {
                @Override
                public boolean recoverMessage(org.apache.activemq.command.Message message) {
                    return true;
                }

                @Override
                public boolean recoverMessageReference(MessageId ref) {
                    return false;
                }

                @Override
                public boolean hasSpace() {
                    hasSpaceCalled.set(true);
                    return false;
                }

                @Override
                public boolean isDuplicate(MessageId ref) {
                    return false;
                }
            });

            assertTrue(expired.isEmpty());
            assertTrue(hasSpaceCalled.get());

            // check we only call recoverMessage() once for each unique id
            Set<MessageId> ids = new HashSet<>();
            Map<SubscriptionKey, List<org.apache.activemq.command.Message>> all =
                store.recoverExpired(Set.of(subKey1, subKey2), 100, new MessageRecoveryListener() {
                @Override
                public boolean recoverMessage(org.apache.activemq.command.Message message) {
                    assertTrue("duplicate message passed to listener", ids.add(message.getMessageId()));
                    return true;
                }

                @Override
                public boolean recoverMessageReference(MessageId ref) {
                    return false;
                }

                @Override
                public boolean hasSpace() {
                    return true;
                }

                @Override
                public boolean isDuplicate(MessageId ref) {
                    return false;
                }
            });
            assertEquals(10, ids.size());
            assertEquals(10, all.get(subKey1).size());
            assertEquals(10, all.get(subKey2).size());
        }
    }

    private static XATransactionId createXid() {
        XATransactionId xid = new XATransactionId();
        xid.setFormatId(1);
        xid.setGlobalTransactionId(new byte[] {1, 2, 3});
        xid.setBranchQualifier(new byte[] {4, 5, 6});
        return xid;
    }

    private final MessageRecoveryListener listener = new MessageRecoveryListener() {

        @Override
        public boolean recoverMessage(org.apache.activemq.command.Message message) {
            return true;
        }

        @Override
        public boolean recoverMessageReference(MessageId ref) {
            return true;
        }

        @Override
        public boolean hasSpace() {
            return true;
        }

        @Override
        public boolean isDuplicate(MessageId ref) {
            return false;
        }
    };
}
