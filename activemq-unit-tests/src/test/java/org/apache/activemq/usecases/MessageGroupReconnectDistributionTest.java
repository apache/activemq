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
import java.util.Arrays;
import java.util.Random;
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
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MessageGroupReconnectDistributionTest {
    public static final Logger LOG = LoggerFactory.getLogger(MessageGroupReconnectDistributionTest.class);
    final Random random = new Random();
    protected Connection connection;
    protected Session session;
    protected MessageProducer producer;
    protected ActiveMQQueue destination = new ActiveMQQueue("GroupQ");
    protected TransportConnector connector;
    ActiveMQConnectionFactory connFactory;
    BrokerService broker;
    int numMessages = 10000;
    int groupSize = 10;
    int batchSize = 20;

    @Parameterized.Parameter(0)
    public int numConsumers = 4;

    @Parameterized.Parameter(1)
    public boolean consumerPriority = true;

    @Parameterized.Parameters(name="numConsumers={0},consumerPriority={1}")
    public static Iterable<Object[]> combinations() {
        return Arrays.asList(new Object[][]{{4, true}, {10, true}});
    }

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        connFactory = new ActiveMQConnectionFactory(connector.getConnectUri() + "?jms.prefetchPolicy.all=200");
        connFactory.setWatchTopicAdvisories(false);
        connection = connFactory.createConnection();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        producer = session.createProducer(destination);
        connection.start();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setAdvisorySupport(false);
        service.setPersistent(false);
        service.setUseJmx(true);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setUseConsumerPriority(consumerPriority);
        policy.setMessageGroupMapFactoryType("cached?cacheSize=" + (numConsumers - 1));
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

    @Test(timeout = 5 * 60 * 1000)
    public void testReconnect() throws Exception {

        final AtomicLong totalConsumed = new AtomicLong(0);

        ExecutorService executorService = Executors.newFixedThreadPool(numConsumers);
        final ArrayList<AtomicLong> consumedCounters = new ArrayList<AtomicLong>(numConsumers);
        final ArrayList<AtomicLong> batchCounters = new ArrayList<AtomicLong>(numConsumers);

        for (int i = 0; i < numConsumers; i++) {
            consumedCounters.add(new AtomicLong(0l));
            batchCounters.add(new AtomicLong(0l));

            final int id = i;
            executorService.submit(new Runnable() {
                int getBatchSize() {
                    return (id + 1) * batchSize;
                }

                @Override
                public void run() {
                    try {
                        Session connectionSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        int batchSize = getBatchSize();
                        MessageConsumer messageConsumer = connectionSession.createConsumer(destWithPrefetch(destination));

                        Message message;
                        AtomicLong consumed = consumedCounters.get(id);
                        AtomicLong batches = batchCounters.get(id);

                        LOG.info("Consumer: " + id + ", batchSize:" + batchSize + ", totalConsumed:" + totalConsumed.get() + ", consumed:" + consumed.get());

                        while (totalConsumed.get() < numMessages) {

                            message = messageConsumer.receive(10000);

                            if (message == null) {
                                LOG.info("Consumer: " + id + ", batchSize:" + batchSize + ", null message (totalConsumed:" + totalConsumed.get() + ") consumed:" + consumed.get());
                                messageConsumer.close();

                                if (totalConsumed.get() == numMessages) {
                                    break;
                                } else {
                                    batchSize = getBatchSize();
                                    messageConsumer = connectionSession.createConsumer(destWithPrefetch(destination));
                                    batches.incrementAndGet();
                                    continue;
                                }
                            }

                            consumed.incrementAndGet();
                            totalConsumed.incrementAndGet();

                            if (consumed.get() > 0 && consumed.intValue() % batchSize == 0) {
                                messageConsumer.close();
                                batchSize = getBatchSize();
                                messageConsumer = connectionSession.createConsumer(destWithPrefetch(destination));
                                batches.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            TimeUnit.MILLISECONDS.sleep(200);
        }

        TimeUnit.SECONDS.sleep(1);
        produceMessages(numMessages);

        executorService.shutdown();
        assertTrue("threads done on time", executorService.awaitTermination(10, TimeUnit.MINUTES));

        assertEquals("All consumed", numMessages, totalConsumed.intValue());

        LOG.info("Distribution: " + consumedCounters);
        LOG.info("Batches: " + batchCounters);

        double max = consumedCounters.get(0).longValue() * 1.5;
        double min = consumedCounters.get(0).longValue() * 0.5;

        for (AtomicLong l : consumedCounters) {
            assertTrue("Even +/- 50% distribution on consumed:" + consumedCounters + ", outlier:" + l.get(),
                    l.longValue() < max && l.longValue() > min);
        }
    }

    private Destination destWithPrefetch(ActiveMQQueue destination) throws Exception {
        return destination;
    }

    private void produceMessages(int numMessages) throws JMSException {
        int groupID=0;
        for (int i = 0; i < numMessages; i++) {
            if (i>0 && i%groupSize==0) {
                groupID++;
            }
            TextMessage msga = session.createTextMessage("hello " + i);
            msga.setStringProperty("JMSXGroupID", "Group-"+groupID);
            producer.send(msga);
        }
    }
}
