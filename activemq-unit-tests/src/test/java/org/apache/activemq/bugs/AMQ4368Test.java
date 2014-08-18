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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4368Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ4368Test.class);

    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    private final Destination destination = new ActiveMQQueue("large_message_queue");
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();
        broker.start();
        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();

        PolicyEntry policy = new PolicyEntry();
        policy.setUseCache(false);
        broker.setDestinationPolicy(new PolicyMap());
        broker.getDestinationPolicy().setDefaultEntry(policy);

        KahaDBPersistenceAdapter kahadb = new KahaDBPersistenceAdapter();
        kahadb.setCheckForCorruptJournalFiles(true);
        kahadb.setCleanupInterval(1000);

        kahadb.deleteAllMessages();
        broker.setPersistenceAdapter(kahadb);
        broker.getSystemUsage().getMemoryUsage().setLimit(1024*1024*100);
        broker.setUseJmx(false);

        return broker;
    }

    abstract class Client implements Runnable   {
        private final String name;
        final AtomicBoolean done = new AtomicBoolean();
        CountDownLatch startedLatch;
        CountDownLatch doneLatch = new CountDownLatch(1);
        Connection connection;
        Session session;
        final AtomicLong size = new AtomicLong();

        Client(String name, CountDownLatch startedLatch) {
            this.name = name;
            this.startedLatch = startedLatch;
        }

        public void start() {
            LOG.info("Starting: " + name);
            new Thread(this, name).start();
        }

        public void stopAsync() {
            done.set(true);
        }

        public void stop() throws InterruptedException {
            stopAsync();
            if (!doneLatch.await(20, TimeUnit.MILLISECONDS)) {
                try {
                    connection.close();
                    doneLatch.await();
                } catch (Exception e) {
                }
            }
        }

        @Override
        public void run() {
            try {
                connection = createConnection();
                connection.start();
                try {
                    session = createSession();
                    work();
                } finally {
                    try {
                        connection.close();
                    } catch (JMSException ignore) {
                    }
                    LOG.info("Stopped: " + name);
                }
            } catch (Exception e) {
                e.printStackTrace();
                done.set(true);
            } finally {
                doneLatch.countDown();
            }
        }

        protected Session createSession() throws JMSException {
            return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }

        protected Connection createConnection() throws JMSException {
            return connectionFactory.createConnection();
        }

        abstract protected void work() throws Exception;
    }

    class ProducingClient extends Client {

        ProducingClient(String name, CountDownLatch startedLatch) {
            super(name, startedLatch);
        }

        private String createMessage() {
            StringBuffer stringBuffer = new StringBuffer();
            for (long i = 0; i < 1000000; i++) {
                stringBuffer.append("1234567890");
            }
            return stringBuffer.toString();
        }

        @Override
        protected void work() throws Exception {
            String data = createMessage();
            MessageProducer producer = session.createProducer(destination);
            startedLatch.countDown();
            while (!done.get()) {
                producer.send(session.createTextMessage(data));
                long i = size.incrementAndGet();
                if ((i % 1000) == 0) {
                    LOG.info("produced " + i + ".");
                }
            }
        }
    }

    class ConsumingClient extends Client {
        public ConsumingClient(String name, CountDownLatch startedLatch) {
            super(name, startedLatch);
        }

        @Override
        protected void work() throws Exception {
            MessageConsumer consumer = session.createConsumer(destination);
            startedLatch.countDown();
            while (!done.get()) {
                Message msg = consumer.receive(100);
                if (msg != null) {
                    size.incrementAndGet();
                }
            }
        }
    }

    @Test
    public void testENTMQ220() throws Exception {
        LOG.info("Start test.");
        CountDownLatch producer1Started = new CountDownLatch(1);
        CountDownLatch producer2Started = new CountDownLatch(1);
        CountDownLatch listener1Started = new CountDownLatch(1);

        final ProducingClient producer1 = new ProducingClient("1", producer1Started);
        final ProducingClient producer2 = new ProducingClient("2", producer2Started);
        final ConsumingClient listener1 = new ConsumingClient("subscriber-1", listener1Started);
        final AtomicLong lastSize = new AtomicLong();

        try {

            producer1.start();
            producer2.start();
            listener1.start();

            producer1Started.await(15, TimeUnit.SECONDS);
            producer2Started.await(15, TimeUnit.SECONDS);
            listener1Started.await(15, TimeUnit.SECONDS);

            lastSize.set(listener1.size.get());
            for (int i = 0; i < 10; i++) {
                Wait.waitFor(new Wait.Condition() {

                    @Override
                    public boolean isSatisified() throws Exception {
                        return listener1.size.get() > lastSize.get();
                    }
                });
                long size = listener1.size.get();
                LOG.info("Listener 1: consumed: " + (size - lastSize.get()));
                assertTrue("No messages received on iteration: " + i, size > lastSize.get());
                lastSize.set(size);
            }
        } finally {
            LOG.info("Stopping clients");
            producer1.stop();
            producer2.stop();
            listener1.stop();
        }
    }
}