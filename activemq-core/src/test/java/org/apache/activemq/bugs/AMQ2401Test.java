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
package org.apache.activemq.bugs;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AMQ-2401 Test
 */
public class AMQ2401Test extends TestCase implements MessageListener {
    private BrokerService broker;
    private ActiveMQConnectionFactory factory;
    private static final int SEND_COUNT = 500;
    private static final int CONSUMER_COUNT = 50;
    private static final int PRODUCER_COUNT = 1;
    private static final int LOG_INTERVAL = 10;

    private static final Logger LOG = LoggerFactory.getLogger(AMQ2401Test.class);

    private final ArrayList<Service> services = new ArrayList<Service>(CONSUMER_COUNT + PRODUCER_COUNT);
    private int count = 0;
    private CountDownLatch latch;

    @Override
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "test-data" + File.separator + "AMQ2401Test");
        broker.setDeleteAllMessagesOnStartup(true);
        String connectionUri = broker.addConnector("tcp://0.0.0.0:0").getPublishableConnectString();
        PolicyMap policies = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setMemoryLimit(1024 * 100);
        entry.setProducerFlowControl(true);
        entry.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        entry.setQueue(">");
        policies.setDefaultEntry(entry);
        broker.setDestinationPolicy(policies);
        broker.setUseJmx(false);
        broker.start();
        broker.waitUntilStarted();

        factory = new ActiveMQConnectionFactory(connectionUri);
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    public void testDupsOk() throws Exception {

        TestProducer p = null;
        TestConsumer c = null;
        try {
            latch = new CountDownLatch(SEND_COUNT);

            for (int i = 0; i < CONSUMER_COUNT; i++) {
                TestConsumer consumer = new TestConsumer();
                consumer.start();
                services.add(consumer);
            }
            for (int i = 0; i < PRODUCER_COUNT; i++) {
                TestProducer producer = new TestProducer();
                producer.start();
                services.add(producer);
            }

            waitForMessageReceipt(TimeUnit.SECONDS.toMillis(30));
        } finally {
            if (p != null) {
                p.close();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    @Override
    public void onMessage(Message message) {
        latch.countDown();
        if (++count % LOG_INTERVAL == 0) {
            LOG.debug("Received message " + count);
        }

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * @throws InterruptedException
     * @throws TimeoutException
     */
    private void waitForMessageReceipt(long timeout) throws InterruptedException, TimeoutException {
        if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException(String.format("Consumner didn't receive expected # of messages, %d of %d received.", latch.getCount(), SEND_COUNT));
        }
    }

    private interface Service {
        public void start() throws Exception;
        public void close();
    }

    private class TestProducer implements Runnable, Service {
        Thread thread;
        BytesMessage message;

        Connection connection;
        Session session;
        MessageProducer producer;

        TestProducer() throws Exception {
            thread = new Thread(this, "TestProducer");
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            producer = session.createProducer(session.createQueue("AMQ2401Test"));
        }

        @Override
        public void start() {
            thread.start();
        }

        @Override
        public void run() {

            int count = SEND_COUNT / PRODUCER_COUNT;
            for (int i = 1; i <= count; i++) {
                try {
                    if ((i % LOG_INTERVAL) == 0) {
                        LOG.debug("Sending: " + i);
                    }
                    message = session.createBytesMessage();
                    message.writeBytes(new byte[1024]);
                    producer.send(message);
                } catch (JMSException jmse) {
                    jmse.printStackTrace();
                    break;
                }
            }
        }

        @Override
        public void close() {
            try {
                connection.close();
            } catch (JMSException e) {
            }
        }
    }

    private class TestConsumer implements Runnable, Service {
        ActiveMQConnection connection;
        Session session;
        MessageConsumer consumer;

        TestConsumer() throws Exception {
            factory.setOptimizeAcknowledge(false);
            connection = (ActiveMQConnection) factory.createConnection();

            session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            consumer = session.createConsumer(session.createQueue("AMQ2401Test"));

            consumer.setMessageListener(AMQ2401Test.this);
        }

        @Override
        public void start() throws Exception {
            connection.start();
        }

        @Override
        public void close() {
            try {
                connection.close();
            } catch (JMSException e) {
            }
        }

        @Override
        public void run() {
            while (latch.getCount() > 0) {
                try {
                    onMessage(consumer.receive());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
