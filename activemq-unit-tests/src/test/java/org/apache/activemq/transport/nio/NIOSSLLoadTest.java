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
package org.apache.activemq.transport.nio;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.ConsumerThread;
import org.apache.activemq.util.ProducerThread;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NIOSSLLoadTest {

    private static final Logger LOG = LoggerFactory.getLogger(NIOSSLLoadTest.class);

    BrokerService broker;
    Connection connection;
    Session session;

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    public static final int PRODUCER_COUNT = 10;
    public static final int CONSUMER_COUNT = 10;
    public static final int MESSAGE_COUNT = 1000;

    final ConsumerThread[] consumers = new ConsumerThread[CONSUMER_COUNT];
    TransportConnector connector;

    @Before
    public void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);

        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        connector = broker.addConnector("nio+ssl://localhost:0?transport.needClientAuth=true&transport.enabledCipherSuites=TLS_RSA_WITH_AES_256_CBC_SHA256");
        broker.start();
        broker.waitUntilStarted();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("nio+ssl://localhost:" + connector.getConnectUri().getPort());
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testLoad() throws Exception {
        Queue dest = session.createQueue("TEST");
        for (int i = 0; i < PRODUCER_COUNT; i++) {
            ProducerThread producer = new ProducerThread(session, dest);
            producer.setMessageCount(MESSAGE_COUNT);
            producer.start();
        }

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            ConsumerThread consumer = new ConsumerThread(session, dest);
            consumer.setMessageCount(MESSAGE_COUNT);
            consumer.start();
            consumers[i] = consumer;
        }

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getReceived() == PRODUCER_COUNT * MESSAGE_COUNT;
            }
        }, 60000);

        assertEquals(PRODUCER_COUNT * MESSAGE_COUNT, getReceived());

    }

    @Test(timeout=360000)
    @Ignore
    public void testConnectionHandshakeLoad() throws Exception {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("nio+ssl://localhost:" + connector.getConnectUri().getPort());
        int threadNumber = 500;
        final CountDownLatch latch = new CountDownLatch(threadNumber);
        final AtomicInteger errors = new AtomicInteger(0);
        final Random rand = new Random();
        for (int i = 0; i < threadNumber; i++) {
            Thread thread = new Thread("thread " + i) {
                @Override
                public void run() {
                    for (int i = 0; i < 100; i++) {
                        Connection conn = null;
                        try {
                            conn = factory.createConnection();
                            conn.start();
                            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                            int sleepTime = rand.nextInt((3000 - 1000) + 1) + 1000;
                            LOG.info(getName() + " sleeping " + sleepTime);
                            Thread.sleep(sleepTime);
                        } catch (Exception e) {
                            e.printStackTrace();
                            errors.incrementAndGet();
                        }  finally {
                            try {
                                conn.close();
                            } catch (Exception e) {}
                            LOG.info(getName() + " iteration " + i);
                        }
                    }

                    LOG.info(getName() + " finished");
                    latch.countDown();
                }
            };
            thread.start();
        }

        latch.await(5, TimeUnit.MINUTES);

        LOG.info("errors " + errors.get());

    }

    protected int getReceived() {
        int received = 0;
        for (ConsumerThread consumer : consumers) {
            received += consumer.getReceived();
        }
        return received;
    }

}
