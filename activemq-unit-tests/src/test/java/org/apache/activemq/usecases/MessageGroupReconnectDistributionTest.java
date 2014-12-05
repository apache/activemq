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
package org.apache.activemq.usecases;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(BlockJUnit4ClassRunner.class)
public class MessageGroupReconnectDistributionTest {
    public static final Logger LOG = LoggerFactory.getLogger(MessageGroupReconnectDistributionTest.class);
    protected Connection connection;
    protected Session session;
    protected MessageProducer producer;
    protected Destination destination;

    BrokerService broker;
    protected TransportConnector connector;

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        ActiveMQConnectionFactory connFactory = new ActiveMQConnectionFactory(connector.getConnectUri() + "?jms.prefetchPolicy.all=30");
        connection = connFactory.createConnection();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = new ActiveMQQueue("GroupQ");
        producer = session.createProducer(destination);
        connection.start();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(true);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setUseConsumerPriority(true);
        policy.setMessageGroupMapFactoryType("cached");
        policyMap.setDefaultEntry(policy);
        service.setDestinationPolicy(policyMap);

        connector = service.addConnector("tcp://localhost:0");
        return service;
    }

    @After
    public void tearDown() throws Exception {
        producer.close();
        session.close();
        connection.close();
        broker.stop();
    }

    final Random random = new Random();
    public int getBatchSize(int bound) throws Exception {
        return bound + random.nextInt(bound);
    }

    @Test(timeout = 20 * 60 * 1000)
    public void testReconnect() throws Exception {

        final int numMessages = 50000;
        final int numConsumers = 10;
        final AtomicLong totalConsumed = new AtomicLong(0);

        produceMessages(numMessages);

        ExecutorService executorService = Executors.newCachedThreadPool();
        final ArrayList<AtomicLong> consumedCounters = new ArrayList<AtomicLong>(numConsumers);
        for (int i=0;i<numConsumers; i++) {
            consumedCounters.add(new AtomicLong(0l));
            final int id = i;
            executorService.submit(new Runnable() {
                Session connectionSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

                @Override
                public void run() {
                    try {
                        MessageConsumer messageConsumer = connectionSession.createConsumer(destination);

                        long batchSize = getBatchSize(numConsumers);
                        Message message;
                        AtomicLong consumed = consumedCounters.get(id);

                        LOG.info("Consumer: " + id + ", batchSize:" + batchSize + ", totalConsumed:" + totalConsumed.get() + ", consumed:" + consumed.get());

                        while (totalConsumed.get() < numMessages) {

                            message = messageConsumer.receive(10000);

                            if (message == null) {
                                LOG.info("Consumer: " + id + ", batchSize:" + batchSize + ", null message (totalConsumed:" + totalConsumed.get() + ") consumed:" + consumed.get());
                                messageConsumer.close();

                                if (totalConsumed.get() == numMessages) {
                                    break;
                                } else {
                                    messageConsumer = connectionSession.createConsumer(destination);
                                    continue;
                                }
                            }

                            message.acknowledge();
                            consumed.incrementAndGet();
                            totalConsumed.incrementAndGet();

                            if (consumed.get() > 0 && consumed.longValue() % batchSize == 0) {
                                messageConsumer.close();
                                messageConsumer = connectionSession.createConsumer(destination);
                                batchSize = getBatchSize(numConsumers);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();
        assertTrue("threads done on time", executorService.awaitTermination(10, TimeUnit.MINUTES));

        assertEquals("All consumed", numMessages, totalConsumed.intValue());

        LOG.info("Distribution: " + consumedCounters);

        double max = consumedCounters.get(0).longValue() * 1.5;
        double min = consumedCounters.get(0).longValue() * 0.5;

        for (AtomicLong l : consumedCounters) {
            assertTrue("Even +/- 50% distribution on consumed:" + consumedCounters + ", outlier:" + l.get(),
                    l.longValue() < max && l.longValue() > min);
        }
    }

    private void produceMessages(int numMessages) throws JMSException {
        for (int i = 0; i < numMessages; i++) {
            TextMessage msga = session.createTextMessage("hello " +i);
            msga.setStringProperty("JMSXGroupID", msga.getText());
            producer.send(msga);
        }
    }
}
