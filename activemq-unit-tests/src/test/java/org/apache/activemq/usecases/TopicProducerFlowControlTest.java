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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicProducerFlowControlTest extends TestCase implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(TopicProducerFlowControlTest.class);
    private static final String brokerName = "testBroker";
    private static final String brokerUrl = "vm://" + brokerName;
    protected static final int destinationMemLimit = 2097152; // 2MB
    private static final AtomicLong produced = new AtomicLong();
    private static final AtomicLong consumed = new AtomicLong();
    private static final int numMessagesToSend = 50000;

    private BrokerService broker;

    protected void setUp() throws Exception {
        produced.set(0);
        consumed.set(0);
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
        tpe.setAdvisoryWhenFull(true);
        tpe.setBlockedProducerWarningInterval(2000);


        pm.setPolicyEntries(Arrays.asList(new PolicyEntry[]{tpe}));

        setDestinationPolicy(broker, pm);

        // Start the broker
        broker.start();
        broker.waitUntilStarted();
    }

    protected void setDestinationPolicy(BrokerService broker, PolicyMap pm) {
        broker.setDestinationPolicy(pm);
    }

    protected void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    public void testTopicProducerFlowControl() throws Exception {

        // Create the connection factory
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        connectionFactory.setAlwaysSyncSend(true);
        connectionFactory.setProducerWindowSize(1024);

        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setAll(5000);
        connectionFactory.setPrefetchPolicy(prefetchPolicy);
        // Start the test destination listener
        Connection c = connectionFactory.createConnection();
        c.start();
        Session listenerSession = c.createSession(false, 1);
        Destination destination = createDestination(listenerSession);

        listenerSession.createConsumer(destination).setMessageListener(new TopicProducerFlowControlTest());
        final AtomicInteger blockedCounter = new AtomicInteger(0);
        listenerSession.createConsumer(new ActiveMQTopic(AdvisorySupport.FULL_TOPIC_PREFIX + ">")).setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    if (blockedCounter.get() % 100 == 0) {
                        LOG.info("Got full advisory, usageName: " +
                                message.getStringProperty(AdvisorySupport.MSG_PROPERTY_USAGE_NAME) +
                                ", usageCount: " +
                                message.getLongProperty(AdvisorySupport.MSG_PROPERTY_USAGE_COUNT)
                                + ", blockedCounter: " + blockedCounter.get());
                    }
                    blockedCounter.incrementAndGet();

                } catch (Exception error) {
                    error.printStackTrace();
                    LOG.error("missing advisory property", error);
                }
            }
        });

        final AtomicInteger warnings = new AtomicInteger();
        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.WARN) && event.getMessage().toString().contains("Usage Manager memory limit reached")) {
                    LOG.info("received  log message: " + event.getMessage());
                    warnings.incrementAndGet();
                }
            }
        };
        org.apache.log4j.Logger log4jLogger =
                org.apache.log4j.Logger.getLogger(Topic.class);
        log4jLogger.addAppender(appender);
        try {

            // Start producing the test messages
            final Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(destination);

            Thread producingThread = new Thread("Producing Thread") {
                public void run() {
                    try {
                        for (long i = 0; i < numMessagesToSend; i++) {
                            producer.send(session.createTextMessage("test"));

                            long count = produced.incrementAndGet();
                            if (count % 10000 == 0) {
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
            }, 5 * 60 * 1000); // give it plenty of time before failing

            assertEquals("Didn't produce all messages", numMessagesToSend, produced.get());
            assertEquals("Didn't consume all messages", numMessagesToSend, consumed.get());

            assertTrue("Producer got blocked", Wait.waitFor(new Wait.Condition() {
                public boolean isSatisified() throws Exception {
                    return blockedCounter.get() > 0;
                }
            }, 5 * 1000));

            LOG.info("BlockedCount: " + blockedCounter.get() + ", Warnings:" + warnings.get());
            assertTrue("got a few warnings", warnings.get() > 1);
            assertTrue("warning limited", warnings.get() < blockedCounter.get());

        } finally {
            log4jLogger.removeAppender(appender);
        }
    }


    public void testTransactedProducerBlockedAndClosedWillRelease() throws Exception {
        doTestTransactedProducerBlockedAndClosedWillRelease(false);
    }

    public void testTransactedSyncSendProducerBlockedAndClosedWillRelease() throws Exception {
        doTestTransactedProducerBlockedAndClosedWillRelease(true);
    }

    public void doTestTransactedProducerBlockedAndClosedWillRelease(final boolean alwaysSyncSend) throws Exception {

        // Create the connection factory
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        connectionFactory.setWatchTopicAdvisories(false);
        connectionFactory.setAlwaysSyncSend(alwaysSyncSend);
        Connection c = connectionFactory.createConnection();
        c.start();


        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setAll(5000);
        connectionFactory.setPrefetchPolicy(prefetchPolicy);
        // Start the test destination listener
        Session listenerSession = c.createSession(false, 1);
        Destination destination = createDestination(listenerSession);


        final AtomicInteger warnings = new AtomicInteger();
        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.WARN) && event.getMessage().toString().contains("Usage Manager memory limit reached")) {
                    LOG.info("received  log message: " + event.getMessage());
                    warnings.incrementAndGet();
                }
            }
        };
        org.apache.log4j.Logger log4jLogger =
                org.apache.log4j.Logger.getLogger(Topic.class);
        log4jLogger.addAppender(appender);
        try {

            // Start producing the test messages
            final Session session = connectionFactory.createConnection().createSession(true, Session.SESSION_TRANSACTED);
            final MessageProducer producer = session.createProducer(destination);

            Thread producingThread = new Thread("Producing Thread") {
                public void run() {
                    try {
                        for (long i = 0; i < numMessagesToSend; i++) {
                            producer.send(session.createTextMessage("test"));

                            long count = produced.incrementAndGet();
                            if (count % 10000 == 0) {
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


            assertTrue("Producer got blocked", Wait.waitFor(new Wait.Condition() {
                public boolean isSatisified() throws Exception {
                    return warnings.get() > 0;
                }
            }, 5 * 1000));


            LOG.info("Produced: " + produced.get() + ", Warnings:" + warnings.get());

            assertTrue("Producer got blocked", Wait.waitFor(new Wait.Condition() {
                public boolean isSatisified() throws Exception {
                    return warnings.get() > 0;
                }
            }, 5 * 1000));


            final long enqueueCountWhenBlocked = broker.getDestination(ActiveMQDestination.transform(destination)).getDestinationStatistics().getEnqueues().getCount();

            // now whack the hung connection broker side (mimic jmx), and verify usage gone b/c of rollback
            for (TransportConnection transportConnection : broker.getTransportConnectors().get(0).getConnections()) {
                transportConnection.serviceException(new IOException("forcing close for hung connection"));
            }

            assertTrue("Usage gets released on close", Wait.waitFor(new Wait.Condition() {
                public boolean isSatisified() throws Exception {
                    LOG.info("Usage: " + broker.getSystemUsage().getMemoryUsage().getUsage());

                    return broker.getSystemUsage().getMemoryUsage().getUsage() == 0;
                }
            }, 5 * 1000));

            c.close();

            // verify no pending sends completed in rolledback tx
            // temp dest should not exist
            if (!ActiveMQDestination.transform(destination).isTemporary()) {
                assertEquals("nothing sent during close", enqueueCountWhenBlocked, broker.getDestination(ActiveMQDestination.transform(destination)).getDestinationStatistics().getEnqueues().getCount());
            }
        } finally {
            log4jLogger.removeAppender(appender);
        }
    }

    protected Destination createDestination(Session listenerSession) throws Exception {
        return new ActiveMQTopic("test");
    }

    @Override
    public void onMessage(Message message) {
        long count = consumed.incrementAndGet();
        if (count % 100 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        if (count % 10000 == 0) {
            LOG.info("\tConsumed " + count + " messages");
        }

    }
}
