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
package org.apache.activemq;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Benchmarks the broker by starting many consumer and producers against the
 * same destination.
 * 
 * Make sure you run with jvm option -server (makes a big difference). The tests
 * simulate storing 1000 1k jms messages to see the rate of processing msg/sec.
 * 
 * @version $Revision$
 */
public class JmsBenchmark extends JmsTestSupport {
    private static final transient Log log = LogFactory.getLog(JmsBenchmark.class);

    private static final long SAMPLE_DELAY = Integer.parseInt(System.getProperty("SAMPLE_DELAY",
                                                                                 "" + 1000 * 5));
    private static final long SAMPLES = Integer.parseInt(System.getProperty("SAMPLES", "10"));
    private static final long SAMPLE_DURATION = Integer.parseInt(System.getProperty("SAMPLES_DURATION",
                                                                                    "" + 1000 * 60));
    private static final int PRODUCER_COUNT = Integer.parseInt(System.getProperty("PRODUCER_COUNT", "10"));
    private static final int CONSUMER_COUNT = Integer.parseInt(System.getProperty("CONSUMER_COUNT", "10"));

    public ActiveMQDestination destination;

    public static Test suite() {
        return suite(JmsBenchmark.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(JmsBenchmark.class);
    }

    public void initCombos() {
        addCombinationValues("destination", new Object[] {
        // new ActiveMQTopic("TEST"),
                             new ActiveMQQueue("TEST"),});
    }

    protected BrokerService createBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("broker://(tcp://localhost:0)?persistent=false"));
    }

    protected ConnectionFactory createConnectionFactory() throws URISyntaxException, IOException {
        return new ActiveMQConnectionFactory(((TransportConnector)broker.getTransportConnectors().get(0))
            .getServer().getConnectURI());
    }

    /**
     * @throws Throwable
     */
    public void testConcurrentSendReceive() throws Throwable {

        final Semaphore connectionsEstablished = new Semaphore(1 - (CONSUMER_COUNT + PRODUCER_COUNT));
        final Semaphore workerDone = new Semaphore(1 - (CONSUMER_COUNT + PRODUCER_COUNT));
        final CountDownLatch sampleTimeDone = new CountDownLatch(1);

        final AtomicInteger producedMessages = new AtomicInteger(0);
        final AtomicInteger receivedMessages = new AtomicInteger(0);

        final Callable producer = new Callable() {
            public Object call() throws JMSException, InterruptedException {
                Connection connection = factory.createConnection();
                connections.add(connection);
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                BytesMessage message = session.createBytesMessage();
                message.writeBytes(new byte[1024]);
                connection.start();
                connectionsEstablished.release();

                while (!sampleTimeDone.await(0, TimeUnit.MILLISECONDS)) {
                    producer.send(message);
                    producedMessages.incrementAndGet();
                }

                connection.close();
                workerDone.release();
                return null;
            }
        };

        final Callable consumer = new Callable() {
            public Object call() throws JMSException, InterruptedException {
                Connection connection = factory.createConnection();
                connections.add(connection);
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(destination);

                consumer.setMessageListener(new MessageListener() {
                    public void onMessage(Message msg) {
                        receivedMessages.incrementAndGet();
                    }
                });
                connection.start();

                connectionsEstablished.release();
                sampleTimeDone.await();

                connection.close();
                workerDone.release();
                return null;
            }
        };

        final Throwable workerError[] = new Throwable[1];
        for (int i = 0; i < PRODUCER_COUNT; i++) {
            new Thread("Producer:" + i) {
                public void run() {
                    try {
                        producer.call();
                    } catch (Throwable e) {
                        e.printStackTrace();
                        workerError[0] = e;
                    }
                }
            }.start();
        }

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            new Thread("Consumer:" + i) {
                public void run() {
                    try {
                        consumer.call();
                    } catch (Throwable e) {
                        e.printStackTrace();
                        workerError[0] = e;
                    }
                }
            }.start();
        }

        log.info(getName() + ": Waiting for Producers and Consumers to startup.");
        connectionsEstablished.acquire();
        log.info("Producers and Consumers are now running.  Waiting for system to reach steady state: "
                 + (SAMPLE_DELAY / 1000.0f) + " seconds");
        Thread.sleep(1000 * 10);

        log.info("Starting sample: " + SAMPLES + " each lasting " + (SAMPLE_DURATION / 1000.0f) + " seconds");

        long now = System.currentTimeMillis();
        for (int i = 0; i < SAMPLES; i++) {

            long start = System.currentTimeMillis();
            producedMessages.set(0);
            receivedMessages.set(0);

            Thread.sleep(SAMPLE_DURATION);

            long end = System.currentTimeMillis();
            int r = receivedMessages.get();
            int p = producedMessages.get();

            log.info("published: " + p + " msgs at " + (p * 1000f / (end - start)) + " msgs/sec, "
                     + "consumed: " + r + " msgs at " + (r * 1000f / (end - start)) + " msgs/sec");
        }

        log.info("Sample done.");
        sampleTimeDone.countDown();

        workerDone.acquire();
        if (workerError[0] != null) {
            throw workerError[0];
        }

    }

}
