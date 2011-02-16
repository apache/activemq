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
package org.apache.activemq.broker;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BrokerBenchmark is used to get an idea of the raw performance of a broker.
 * Since the broker data structures using in message dispatching are under high
 * contention from client requests, it's performance should be monitored closely
 * since it typically is the biggest bottleneck in a high performance messaging
 * fabric. The benchmarks are run under all the following combinations options:
 * Queue vs. Topic, 1 vs. 10 producer threads, 1 vs. 10 consumer threads, and
 * Persistent vs. Non-Persistent messages. Message Acking uses client ack style
 * batch acking since that typically has the best ack performance.
 * 
 * 
 */
public class BrokerBenchmark extends BrokerTestSupport {
    private static final transient Logger LOG = LoggerFactory.getLogger(BrokerBenchmark.class);

    public int produceCount = Integer.parseInt(System.getProperty("PRODUCE_COUNT", "10000"));
    public ActiveMQDestination destination;
    public int prodcuerCount;
    public int consumerCount;
    public boolean deliveryMode;

    public void initCombosForTestPerformance() {
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST")});
        addCombinationValues("PRODUCER_COUNT", new Object[] {new Integer("1"), new Integer("10")});
        addCombinationValues("CONSUMER_COUNT", new Object[] {new Integer("1"), new Integer("10")});
        addCombinationValues("CONSUMER_COUNT", new Object[] {new Integer("1"), new Integer("10")});
        addCombinationValues("deliveryMode", new Object[] {Boolean.TRUE});
    }

    public void testPerformance() throws Exception {

        LOG.info("Running Benchmark for destination=" + destination + ", producers=" + prodcuerCount + ", consumers=" + consumerCount + ", deliveryMode=" + deliveryMode);
        final int consumeCount = destination.isTopic() ? consumerCount * produceCount : produceCount;

        final Semaphore consumersStarted = new Semaphore(1 - consumerCount);
        final Semaphore producersFinished = new Semaphore(1 - prodcuerCount);
        final Semaphore consumersFinished = new Semaphore(1 - consumerCount);
        final ProgressPrinter printer = new ProgressPrinter(produceCount + consumeCount, 10);

        // Start a producer and consumer

        profilerPause("Benchmark ready.  Start profiler ");

        long start = System.currentTimeMillis();

        final AtomicInteger receiveCounter = new AtomicInteger(0);
        for (int i = 0; i < consumerCount; i++) {
            new Thread() {
                public void run() {
                    try {

                        // Consume the messages
                        StubConnection connection = new StubConnection(broker);
                        ConnectionInfo connectionInfo = createConnectionInfo();
                        connection.send(connectionInfo);

                        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
                        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
                        consumerInfo.setPrefetchSize(1000);
                        connection.send(sessionInfo);
                        connection.send(consumerInfo);

                        consumersStarted.release();

                        while (receiveCounter.get() < consumeCount) {

                            int counter = 0;
                            // Get a least 1 message.
                            Message msg = receiveMessage(connection, 2000);
                            if (msg != null) {
                                printer.increment();
                                receiveCounter.incrementAndGet();

                                counter++;

                                // Try to piggy back a few extra message acks if
                                // they are ready.
                                Message extra = null;
                                while ((extra = receiveMessage(connection, 0)) != null) {
                                    msg = extra;
                                    printer.increment();
                                    receiveCounter.incrementAndGet();
                                    counter++;
                                }
                            }

                            if (msg != null) {
                                connection.send(createAck(consumerInfo, msg, counter, MessageAck.STANDARD_ACK_TYPE));
                            } else if (receiveCounter.get() < consumeCount) {
                                LOG.info("Consumer stall, waiting for message #" + receiveCounter.get() + 1);
                            }
                        }

                        connection.send(closeConsumerInfo(consumerInfo));
                    } catch (Throwable e) {
                        e.printStackTrace();
                    } finally {
                        consumersFinished.release();
                    }
                }

            }.start();
        }

        // Make sure that the consumers are started first to avoid sending
        // messages
        // before a topic is subscribed so that those messages are not missed.
        consumersStarted.acquire();

        // Send the messages in an async thread.
        for (int i = 0; i < prodcuerCount; i++) {
            new Thread() {
                public void run() {
                    try {
                        StubConnection connection = new StubConnection(broker);
                        ConnectionInfo connectionInfo = createConnectionInfo();
                        connection.send(connectionInfo);

                        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
                        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
                        connection.send(sessionInfo);
                        connection.send(producerInfo);

                        for (int i = 0; i < produceCount / prodcuerCount; i++) {
                            Message message = createMessage(producerInfo, destination);
                            message.setPersistent(deliveryMode);
                            message.setResponseRequired(false);
                            connection.send(message);
                            printer.increment();
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    } finally {
                        producersFinished.release();
                    }
                };
            }.start();
        }

        producersFinished.acquire();
        long end1 = System.currentTimeMillis();
        consumersFinished.acquire();
        long end2 = System.currentTimeMillis();

        LOG.info("Results for destination=" + destination + ", producers=" + prodcuerCount + ", consumers=" + consumerCount + ", deliveryMode=" + deliveryMode);
        LOG.info("Produced at messages/sec: " + (produceCount * 1000.0 / (end1 - start)));
        LOG.info("Consumed at messages/sec: " + (consumeCount * 1000.0 / (end2 - start)));
        profilerPause("Benchmark done.  Stop profiler ");
    }

    public static Test suite() {
        return suite(BrokerBenchmark.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
