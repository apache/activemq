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
package org.apache.activemq.broker.virtual;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualTopicDestinationMapAccessTest {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualTopicDestinationMapAccessTest.class);

    BrokerService brokerService;
    ConnectionFactory connectionFactory;

    @Before
    public void createBroker() throws Exception {
        createBroker(true);
    }

    public void createBroker(boolean delete) throws Exception  {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(delete);
        brokerService.setAdvisorySupport(false);
        brokerService.start();

        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        ActiveMQPrefetchPolicy zeroPrefetch = new ActiveMQPrefetchPolicy();
        zeroPrefetch.setAll(0);
        activeMQConnectionFactory.setPrefetchPolicy(zeroPrefetch);
        connectionFactory = activeMQConnectionFactory;
    }

    @After
    public void stopBroker() throws Exception  {
        brokerService.stop();
    }

    @Test
    @Ignore("perf test that needs manual comparator")
    public void testX() throws Exception {

        final int numConnections = 200;
        final int numDestinations = 10000;
        final AtomicInteger numConsumers = new AtomicInteger(numDestinations);
        final AtomicInteger numProducers = new AtomicInteger(numDestinations);

        ExecutorService executorService = Executors.newFixedThreadPool(numConnections);

        // precreate dests to accentuate read access
        for (int i=0; i<numDestinations; i++ ) {
            brokerService.getRegionBroker().addDestination(
                    brokerService.getAdminConnectionContext(),
                    new ActiveMQQueue("Consumer." + i + ".VirtualTopic.TEST-" + i),
                    false);
            brokerService.getRegionBroker().addDestination(
                    brokerService.getAdminConnectionContext(), new ActiveMQTopic("VirtualTopic.TEST-" + i), false);

        }

        Runnable runnable = new Runnable() {
            @Override
            public void run() {

                try {
                    int opsCount = 0;

                    Connection connection1 = connectionFactory.createConnection();
                    connection1.start();

                    Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(null);

                    do {
                        boolean consumerOrProducer = opsCount++ % 2 == 0;
                        int i = consumerOrProducer ? numConsumers.decrementAndGet() : numProducers.decrementAndGet();
                        if (i > 0) {
                            if (consumerOrProducer) {
                                session.createConsumer(new ActiveMQQueue("Consumer." + i + ".VirtualTopic.TEST-" + i));
                            } else {
                                producer.send(new ActiveMQTopic("VirtualTopic.TEST-" + i), new ActiveMQMessage());
                            }
                        }
                    } while (numConsumers.get() > 0 || numProducers.get() > 0);
                    connection1.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        for (int i = 0; i < numConnections; i++) {
            executorService.execute(runnable);
        }

        long start = System.currentTimeMillis();
        LOG.info("Starting timer: " + start);
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);
        LOG.info("Done, duration: " + (System.currentTimeMillis() - start));

    }
}
