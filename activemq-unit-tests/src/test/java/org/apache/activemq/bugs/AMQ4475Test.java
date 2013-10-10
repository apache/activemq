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

import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.util.TimeStampingBrokerPlugin;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ4475Test {

    private final Log LOG = LogFactory.getLog(AMQ4475Test.class);

    private final int NUM_MSGS = 1000;
    private final int MAX_THREADS = 20;

    private BrokerService broker;
    private String connectionUri;

    private final ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);
    private final ActiveMQQueue original = new ActiveMQQueue("jms/AQueue");
    private final ActiveMQQueue rerouted = new ActiveMQQueue("jms/AQueue_proxy");

    @Before
    public void setUp() throws Exception {
        TimeStampingBrokerPlugin tsbp = new TimeStampingBrokerPlugin();
        tsbp.setZeroExpirationOverride(432000000);
        tsbp.setTtlCeiling(432000000);
        tsbp.setFutureOnly(true);

        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setPlugins(new BrokerPlugin[] {tsbp});
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();

        // Configure Dead Letter Strategy
        DeadLetterStrategy strategy = new IndividualDeadLetterStrategy();
        strategy.setProcessExpired(true);
        ((IndividualDeadLetterStrategy)strategy).setUseQueueForQueueMessages(true);
        ((IndividualDeadLetterStrategy)strategy).setQueuePrefix("DLQ.");
        strategy.setProcessNonPersistent(true);

        // Add policy and individual DLQ strategy
        PolicyEntry policy = new PolicyEntry();
        policy.setTimeBeforeDispatchStarts(3000);
        policy.setDeadLetterStrategy(strategy);

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void after() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testIndividualDeadLetterAndTimeStampPlugin() {
        LOG.info("Starting test ..");

        long startTime = System.nanoTime();

        // Produce to network
        List<Future<ProducerTask>> tasks = new ArrayList<Future<ProducerTask>>();

        for (int index = 0; index < 1; index++) {
            ProducerTask p = new ProducerTask(connectionUri, original, NUM_MSGS);
            Future<ProducerTask> future = executor.submit(p, p);
            tasks.add(future);
        }

        ForwardingConsumerThread f1 = new ForwardingConsumerThread(original, rerouted, NUM_MSGS);
        f1.start();
        ConsumerThread c1 = new ConsumerThread(connectionUri, rerouted, NUM_MSGS);
        c1.start();

        LOG.info("Waiting on consumers and producers to exit");

        try {
            for (Future<ProducerTask> future : tasks) {
                ProducerTask e = future.get();
                LOG.info("[Completed] " + e.dest.getPhysicalName());
            }
            executor.shutdown();
            LOG.info("Producing threads complete, waiting on ACKs");
            f1.join(TimeUnit.MINUTES.toMillis(2));
            c1.join(TimeUnit.MINUTES.toMillis(2));
        } catch (ExecutionException e) {
            LOG.warn("Caught unexpected exception: {}", e);
            throw new RuntimeException(e);
        } catch (InterruptedException ie) {
            LOG.warn("Caught unexpected exception: {}", ie);
            throw new RuntimeException(ie);
        }

        assertFalse(f1.isFailed());
        assertFalse(c1.isFailed());

        long estimatedTime = System.nanoTime() - startTime;

        LOG.info("Testcase duration (seconds): " + estimatedTime / 1000000000.0);
        LOG.info("Consumers and producers exited, all msgs received as expected");
    }

    public class ProducerTask implements Runnable {
        private final String uri;
        private final ActiveMQQueue dest;
        private final int count;

        public ProducerTask(String uri, ActiveMQQueue dest, int count) {
            this.uri = uri;
            this.dest = dest;
            this.count = count;
        }

        @Override
        public void run() {

            Connection connection = null;
            try {
                String destName = "";

                try {
                    destName = dest.getQueueName();
                } catch (JMSException e) {
                    LOG.warn("Caught unexpected exception: {}", e);
                }

                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uri);

                connection = connectionFactory.createConnection();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(dest);
                connection.start();

                producer.setDeliveryMode(DeliveryMode.PERSISTENT);

                String msg = "Test Message";

                for (int i = 0; i < count; i++) {
                    producer.send(session.createTextMessage(msg + dest.getQueueName() + " " + i));
                }

                LOG.info("[" + destName + "] Sent " + count + " msgs");
            } catch (Exception e) {
                LOG.warn("Caught unexpected exception: {}", e);
            } finally {
                try {
                    connection.close();
                } catch (Throwable e) {
                    LOG.warn("Caught unexpected exception: {}", e);
                }
            }
        }
    }

    public class ForwardingConsumerThread extends Thread {

        private final ActiveMQQueue original;
        private final ActiveMQQueue forward;
        private int blockSize = 0;
        private final int PARALLEL = 1;
        private boolean failed;

        public ForwardingConsumerThread(ActiveMQQueue original, ActiveMQQueue forward, int total) {
            this.original = original;
            this.forward = forward;
            this.blockSize = total / PARALLEL;
        }

        public boolean isFailed() {
            return failed;
        }

        @Override
        public void run() {
            Connection connection = null;
            try {

                for (int index = 0; index < PARALLEL; index++) {

                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");

                    connection = factory.createConnection();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer = session.createConsumer(original);
                    MessageProducer producer = session.createProducer(forward);
                    connection.start();
                    int count = 0;

                    while (count < blockSize) {

                        Message msg1 = consumer.receive(10000);
                        if (msg1 != null) {
                            if (msg1 instanceof ActiveMQTextMessage) {
                                if (count % 100 == 0) {
                                    LOG.info("Consuming -> " + ((ActiveMQTextMessage) msg1).getDestination() + " count=" + count);
                                }

                                producer.send(msg1);

                                count++;
                            } else {
                                LOG.info("Skipping unknown msg type " + msg1);
                            }
                        } else {
                            break;
                        }
                    }

                    LOG.info("[" + original.getQueueName() + "] completed segment (" + index + " of " + blockSize + ")");
                    connection.close();
                }
            } catch (Exception e) {
                LOG.warn("Caught unexpected exception: {}", e);
            } finally {
                LOG.debug(getName() + ": is stopping");
                try {
                    connection.close();
                } catch (Throwable e) {
                }
            }
        }
    }

    public class ConsumerThread extends Thread {

        private final String uri;
        private final ActiveMQQueue dest;
        private int blockSize = 0;
        private final int PARALLEL = 1;
        private boolean failed;

        public ConsumerThread(String uri, ActiveMQQueue dest, int total) {
            this.uri = uri;
            this.dest = dest;
            this.blockSize = total / PARALLEL;
        }

        public boolean isFailed() {
            return failed;
        }

        @Override
        public void run() {
            Connection connection = null;
            try {

                for (int index = 0; index < PARALLEL; index++) {

                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri);

                    connection = factory.createConnection();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer = session.createConsumer(dest);
                    connection.start();
                    int count = 0;

                    while (count < blockSize) {

                        Object msg1 = consumer.receive(10000);
                        if (msg1 != null) {
                            if (msg1 instanceof ActiveMQTextMessage) {
                                if (count % 100 == 0) {
                                    LOG.info("Consuming -> " + ((ActiveMQTextMessage) msg1).getDestination() + " count=" + count);
                                }

                                count++;
                            } else {
                                LOG.info("Skipping unknown msg type " + msg1);
                            }
                        } else {
                            failed = true;
                            break;
                        }
                    }

                    LOG.info("[" + dest.getQueueName() + "] completed segment (" + index + " of " + blockSize + ")");
                    connection.close();
                }
            } catch (Exception e) {
                LOG.warn("Caught unexpected exception: {}", e);
            } finally {
                LOG.debug(getName() + ": is stopping");
                try {
                    connection.close();
                } catch (Throwable e) {
                }
            }
        }
    }
}
