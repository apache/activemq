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

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.jdbc.adapter.H2JDBCAdapter;
import org.apache.activemq.store.jdbc.h2.H2DB;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * A topic browse, also used by the topic expiry task for the JDBC store, must not read more
 * rows than the browse page size: some drivers (PostgreSQL by default) read the whole result
 * set in executeQuery(), so a large durable subscription backlog could exhaust the heap.
 */
@Category(ParallelTest.class)
public class JDBCTopicBrowseLimitTest {

    private static final int MESSAGE_COUNT = 50;
    private static final int PAGE_SIZE = 10;

    @Rule
    public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    private final ActiveMQTopic topic = new ActiveMQTopic("test.topic");
    private final List<Integer> recoverLimits = new CopyOnWriteArrayList<>();
    private BrokerService broker;
    private Connection connection;

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setSchedulerSupport(false);
        broker.setDataDirectoryFile(dataFileDir.getRoot());
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        jdbc.setDataSource(H2DB.createDataSource("JDBCTopicBrowseLimitTest"));
        // records the row limit of each recover query
        jdbc.setAdapter(new H2JDBCAdapter() {
            @Override
            public void doRecover(TransactionContext c, ActiveMQDestination destination, int maxReturned,
                    JDBCMessageRecoveryListener listener) throws Exception {
                recoverLimits.add(maxReturned);
                super.doRecover(c, destination, maxReturned, listener);
            }
        });
        broker.setPersistenceAdapter(jdbc);
        broker.setDeleteAllMessagesOnStartup(true);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        // the test drives the browse itself
        policy.setExpireMessagesPeriod(0);
        policy.setMaxBrowsePageSize(PAGE_SIZE);
        policyMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();

        connection = new ActiveMQConnectionFactory("vm://localhost").createConnection();
        connection.setClientID("clientId");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // an offline durable subscription keeps the messages in the store
        session.createDurableSubscriber(topic, "sub1").close();
        MessageProducer producer = session.createProducer(topic);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            producer.send(session.createTextMessage("message" + i));
        }
        session.close();
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

    @Test
    public void testRecoverReadsAtMostMaxReturnedRows() throws Exception {
        MessageStore store = broker.getDestination(topic).getMessageStore();

        // a listener that never stops the recovery: only the query can bound it
        AtomicInteger count = new AtomicInteger();
        store.recover(new CountingListener(count), PAGE_SIZE);
        assertEquals(PAGE_SIZE, count.get());

        count.set(0);
        store.recover(new CountingListener(count));
        assertEquals(MESSAGE_COUNT, count.get());
    }

    @Test
    public void testBrowseLimitsTheQuery() throws Exception {
        Destination destination = broker.getDestination(topic);
        recoverLimits.clear();

        assertEquals(PAGE_SIZE, destination.browse().length);
        assertEquals(List.of(PAGE_SIZE), recoverLimits);
    }

    private static class CountingListener implements MessageRecoveryListener {
        private final AtomicInteger count;

        CountingListener(AtomicInteger count) {
            this.count = count;
        }

        @Override
        public boolean recoverMessage(Message message) {
            count.incrementAndGet();
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
