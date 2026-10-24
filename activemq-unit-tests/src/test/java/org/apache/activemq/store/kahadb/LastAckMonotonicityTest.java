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

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Demonstrates recovery cost when KahaDB's lastAckedSequence regresses
 * due to blind overwrite during out-of-order acks.
 *
 * <p>Two durable subscriptions share a topic. The "fast" subscription acks
 * 499 of 500 messages in reverse sequence order (worst case for blind
 * overwrite). The "slow" subscription never acks, keeping all messages
 * in the orderIndex so GC cannot remove them.
 *
 * <p>After broker restart, recovery for the fast subscription should find
 * its single pending message. With blind overwrite the lastAck cursor
 * regresses to sequence 1, forcing recovery to scan all 499 entries.
 * With monotonic max-tracking, lastAck stays at 499 and recovery jumps
 * directly to the one pending message.
 */
public class LastAckMonotonicityTest {

    private static final String BROKER_NAME = "lastack-test";
    private static final String DATA_DIR = "target/test-data/lastack-monotonicity";
    private static final String TOPIC_NAME = "test.lastack.monotonic";
    private static final String FAST_CLIENT = "fast-client";
    private static final String SLOW_CLIENT = "slow-client";
    private static final String FAST_SUB = "fast-sub";
    private static final String SLOW_SUB = "slow-sub";
    private static final int MESSAGE_COUNT = 500;

    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        IOHelper.deleteFile(new File(DATA_DIR));
    }

    @After
    public void tearDown() throws Exception {
        stopBroker();
        IOHelper.deleteFile(new File(DATA_DIR));
    }

    @Test
    public void testOutOfOrderAckRecoveryCost() throws Exception {
        startBroker();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://" + BROKER_NAME);
        Topic topic;

        // Create slow-sub FIRST — it never acks, keeping messages in the orderIndex.
        Connection slowConn = factory.createConnection();
        slowConn.setClientID(SLOW_CLIENT);
        slowConn.start();
        Session slowSession = slowConn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        topic = slowSession.createTopic(TOPIC_NAME);
        MessageConsumer slowConsumer = slowSession.createDurableSubscriber(topic, SLOW_SUB);

        // Create fast-sub with INDIVIDUAL_ACKNOWLEDGE for out-of-order acking.
        Connection fastConn = factory.createConnection();
        fastConn.setClientID(FAST_CLIENT);
        fastConn.start();
        Session fastSession = fastConn.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        topic = fastSession.createTopic(TOPIC_NAME);
        MessageConsumer fastConsumer = fastSession.createDurableSubscriber(topic, FAST_SUB);

        // Publish messages — both subscriptions receive all of them.
        MessageProducer producer = fastSession.createProducer(topic);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            producer.send(fastSession.createTextMessage("msg-" + i));
        }

        // slow-sub: receive but don't ack — messages stay in orderIndex.
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = slowConsumer.receive(5000);
            assertNotNull("slow-sub should receive message " + i, msg);
        }

        // fast-sub: receive all messages.
        List<Message> fastReceived = new ArrayList<>();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = fastConsumer.receive(5000);
            assertNotNull("fast-sub should receive message " + i, msg);
            fastReceived.add(msg);
        }

        // Ack messages 0..498 in REVERSE order (worst case for blind overwrite).
        // Message 499 (the last) stays un-acked — the single pending message.
        // With blind overwrite, the last ack (for sequence ~1) regresses lastAckedSequence.
        List<Message> toAck = new ArrayList<>(fastReceived.subList(0, MESSAGE_COUNT - 1));
        Collections.reverse(toAck);
        for (Message msg : toAck) {
            msg.acknowledge();
        }

        slowConsumer.close();
        fastConsumer.close();
        slowConn.close();
        fastConn.close();

        // Restart broker — KahaDB data persists.
        stopBroker();
        startBroker();

        // Recover fast-sub and count how many entries the store yields.
        KahaDBPersistenceAdapter adapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        TopicMessageStore store = adapter.createTopicMessageStore(new ActiveMQTopic(TOPIC_NAME));

        CountingRecoveryListener counter = new CountingRecoveryListener();
        store.recoverSubscription(FAST_CLIENT, FAST_SUB, counter);

        int recovered = counter.recovered.get();

        assertEquals("Should recover exactly the 1 pending message", 1, recovered);
    }

    private void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(true);
        broker.setUseJmx(false);
        broker.setBrokerName(BROKER_NAME);
        broker.setDataDirectory(DATA_DIR);
        broker.setDeleteAllMessagesOnStartup(false);
        broker.addConnector("vm://" + BROKER_NAME);
        broker.start();
        broker.waitUntilStarted();
    }

    private void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    static class CountingRecoveryListener implements MessageRecoveryListener {
        final AtomicInteger recovered = new AtomicInteger();
        String lastRecoveredId;

        @Override
        public boolean recoverMessage(org.apache.activemq.command.Message message) {
            recovered.incrementAndGet();
            lastRecoveredId = message.getMessageId().toString();
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
    }
}
