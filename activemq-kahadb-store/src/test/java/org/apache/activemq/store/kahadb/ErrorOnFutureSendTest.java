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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.StoreQueueCursor;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.TransactionIdTransformer;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class ErrorOnFutureSendTest {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorOnFutureSendTest.class);

    @Rule
    public TemporaryFolder dataDir = new TemporaryFolder(new File("target"));

    private BrokerService broker = null;
    private final ActiveMQQueue destination = new ActiveMQQueue("Test");
    private KahaDBPersistenceAdapter adapter;

    protected void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setDataDirectory(dataDir.getRoot().getAbsolutePath());
        adapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
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


    @Test(timeout = 30000)
    public void testSendErrorBubblesBackFromStoreTask() throws Exception {

        adapter.setTransactionIdTransformer(new TransactionIdTransformer() {
            @Override
            public TransactionId transform(TransactionId txid) {
                throw new RuntimeException("Bla");
            }
        });

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        connectionFactory.setWatchTopicAdvisories(false);
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(destination);
        Message message = session.createMessage();

        try {
            producer.send(message);
            fail("Expect exaception");
        } catch (JMSException expected) {
            expected.printStackTrace();
        }

        adapter.setTransactionIdTransformer(new TransactionIdTransformer() {
            @Override
            public TransactionId transform(TransactionId txid) {
                throw new java.lang.OutOfMemoryError("Bla");
            }
        });

        try {
            producer.send(message);
            fail("Expect exaception");
        } catch (JMSException expected) {
            expected.printStackTrace();
        }


        connection.close();
    }

    @Test(timeout = 30000)
    public void testSuccessiveFailedSendsDoesNotConsumeMemInError() throws Exception {

        adapter.setTransactionIdTransformer(new TransactionIdTransformer() {
            @Override
            public TransactionId transform(TransactionId txid) {
                throw new RuntimeException("Bla");
            }
        });

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        connectionFactory.setWatchTopicAdvisories(false);
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(destination);
        Message message = session.createMessage();

        final AtomicInteger received = new AtomicInteger();
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                received.incrementAndGet();
            }
        });

        final int numIterations = 10;
        for (int i=0; i<numIterations; i++) {
            try {
                producer.send(message);
                fail("Expect exception");
            } catch (JMSException expected) {}
        }

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return received.get() == numIterations;
            }
        });
        consumer.close();
        connection.close();

        RegionBroker regionBroker = (RegionBroker) broker.getRegionBroker();
        Queue queue = (Queue) regionBroker.getQueueRegion().getDestinationMap().get(destination);
        StoreQueueCursor storeQueueCursor = (StoreQueueCursor) queue.getMessages();
        PendingMessageCursor queueStorePrefetch = storeQueueCursor.getPersistent();
        LOG.info("QueueStorePrefetch {}", queueStorePrefetch);
        String toString = queueStorePrefetch.toString();
        assertTrue("contains pendingCachedIds.size:1", toString.contains("pendingCachedIds.size:1"));
    }
}
