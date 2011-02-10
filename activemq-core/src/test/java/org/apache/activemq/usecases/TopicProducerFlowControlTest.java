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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicProducerFlowControlTest extends TestCase implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(TopicProducerFlowControlTest.class);
    private static final String brokerName = "testBroker";
    private static final String brokerUrl = "vm://" + brokerName;
    private static final int destinationMemLimit = 2097152; // 2MB
    private static final AtomicLong produced = new AtomicLong();
    private static final AtomicLong consumed = new AtomicLong();
    private static final int numMessagesToSend = 50000;

    private BrokerService broker;

    protected void setUp() throws Exception {
        // Setup and start the broker
        broker = new BrokerService();
        broker.setBrokerName(brokerName);
        broker.setPersistent(false);
        broker.setSchedulerSupport(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.addConnector(brokerUrl);

        // Setup the destination policy
        PolicyMap pm = new PolicyMap();

        // Setup the topic destination policy
        PolicyEntry tpe = new PolicyEntry();
        tpe.setTopic(">");
        tpe.setMemoryLimit(destinationMemLimit);
        tpe.setProducerFlowControl(true);

        // Setup the topic destination policy
        PolicyEntry qpe = new PolicyEntry();
        qpe.setQueue(">");
        qpe.setMemoryLimit(destinationMemLimit);
        qpe.setProducerFlowControl(true);
        qpe.setQueuePrefetch(1);

        pm.setPolicyEntries(Arrays.asList(new PolicyEntry[]{tpe, qpe}));

        broker.setDestinationPolicy(pm);

        // Start the broker
        broker.start();
        broker.waitUntilStarted();
    }


    protected void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    public void testTopicProducerFlowControl() throws Exception {
        Destination destination = new ActiveMQTopic("test");

        // Create the connection factory
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        connectionFactory.setAlwaysSyncSend(true);
        connectionFactory.setProducerWindowSize(1024);

        // Start the test destination listener
        Connection c = connectionFactory.createConnection();
        c.start();
        c.createSession(false, 1).createConsumer(destination).setMessageListener(new TopicProducerFlowControlTest());

        // Start producing the test messages
        final Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageProducer producer = session.createProducer(destination);

        Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                    for (long i = 0; i < numMessagesToSend; i++) {
                        producer.send(session.createTextMessage("test"));

                        long count = produced.incrementAndGet();
                        if (count % 100 == 0) {
                            LOG.info("Produced " + count + " messages");
                        }
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                } finally {
                    try {
                        producer.close();
                        session.close();
                    } catch (Exception e) {
                    }
                }
            }
        };

        producingThread.start();

        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return consumed.get() == numMessagesToSend;
            }
        }, 60 * 1000);

        assertEquals("Didn't produce all messages", numMessagesToSend, produced.get());
        assertEquals("Didn't consume all messages", numMessagesToSend, consumed.get());

    }

    @Override
    public void onMessage(Message message) {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
        }

        long count = consumed.incrementAndGet();
        if (count % 100 == 0) {
            LOG.info("\tConsumed " + count + " messages");
        }
    }
}
