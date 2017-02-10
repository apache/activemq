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
package org.apache.activemq.broker.region;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DestinationGCStressTest {

    protected static final Logger logger = LoggerFactory.getLogger(DestinationGCStressTest.class);


    private BrokerService brokerService;

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setSchedulePeriodForDestinationPurge(1);
        broker.setMaxPurgedDestinationsPerSweep(100);
        broker.setAdvisorySupport(false);

        PolicyEntry entry = new PolicyEntry();
        entry.setGcInactiveDestinations(true);
        entry.setInactiveTimeoutBeforeGC(1);
        PolicyMap map = new PolicyMap();
        map.setDefaultEntry(entry);
        broker.setDestinationPolicy(map);

        return broker;
    }

    @Test(timeout = 60000)
    public void testClashWithPublishAndGC() throws Exception {

        org.apache.log4j.Logger log4jLogger =
                org.apache.log4j.Logger.getLogger(RegionBroker.class);
        final AtomicBoolean failed = new AtomicBoolean(false);

        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.ERROR) && event.getMessage().toString().startsWith("Failed to remove inactive")) {
                    logger.info("received unexpected log message: " + event.getMessage());
                    failed.set(true);
                }
            }
        };
        log4jLogger.addAppender(appender);
        try {

            final AtomicInteger max = new AtomicInteger(20000);

            final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
            factory.setWatchTopicAdvisories(false);
            Connection connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            final MessageConsumer messageConsumer = session.createConsumer(new ActiveMQTopic(">"));

            ExecutorService executorService = Executors.newCachedThreadPool();
            for (int i = 0; i < 1; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Connection c = factory.createConnection();
                            c.start();
                            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                            MessageProducer producer = s.createProducer(null);
                            Message message = s.createTextMessage();
                            int j;
                            while ((j = max.decrementAndGet()) > 0) {
                                producer.send(new ActiveMQTopic("A." + j), message);
                            }
                        } catch (Exception ignored) {
                            ignored.printStackTrace();
                        }
                    }
                });
            }

            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);

            logger.info("Done");

            connection.close();

        } finally {
            log4jLogger.removeAppender(appender);
        }
        assertFalse("failed on unexpected log event", failed.get());

    }

    @Test(timeout = 60000)
    public void testAddRemoveWildcardWithGc() throws Exception {

        org.apache.log4j.Logger log4jLogger =
                org.apache.log4j.Logger.getLogger(RegionBroker.class);
        final AtomicBoolean failed = new AtomicBoolean(false);

        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.ERROR) && event.getMessage().toString().startsWith("Failed to remove inactive")) {
                    logger.info("received unexpected log message: " + event.getMessage());
                    failed.set(true);
                }
            }
        };
        log4jLogger.addAppender(appender);
        try {

            final AtomicInteger max = new AtomicInteger(20000);

            final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
            factory.setWatchTopicAdvisories(false);
            Connection connection = factory.createConnection();
            connection.start();
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            ExecutorService executorService = Executors.newCachedThreadPool();
            for (int i = 0; i < 1; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Connection c = factory.createConnection();
                            c.start();
                            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                            MessageProducer producer = s.createProducer(null);
                            Message message = s.createTextMessage();
                            int j;
                            while ((j = max.decrementAndGet()) > 0) {
                                producer.send(new ActiveMQTopic("A." + j), message);
                            }
                        } catch (Exception ignored) {
                            ignored.printStackTrace();
                        }
                    }
                });
            }

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        try {
                            MessageConsumer messageConsumer = session.createConsumer(new ActiveMQTopic(">"));
                            messageConsumer.close();

                        } catch (Exception ignored) {
                        }
                    }
                }
            });

            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);

            logger.info("Done");

            connection.close();

        } finally {
            log4jLogger.removeAppender(appender);
        }
        assertFalse("failed on unexpected log event", failed.get());

    }

    @Test(timeout = 60000)
    public void testAllDestsSeeSub() throws Exception {

        final AtomicInteger foundDestWithMissingSub = new AtomicInteger(0);

        final AtomicInteger max = new AtomicInteger(20000);

        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        factory.setWatchTopicAdvisories(false);
        Connection connection = factory.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 1; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection c = factory.createConnection();
                        c.start();
                        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        MessageProducer producer = s.createProducer(null);
                        Message message = s.createTextMessage();
                        int j;
                        while ((j = max.decrementAndGet()) > 0) {
                            producer.send(new ActiveMQTopic("A." + j), message);
                        }
                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                    }
                }
            });
        }

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    try {
                        MessageConsumer messageConsumer = session.createConsumer(new ActiveMQTopic(">"));
                        if (destMissingSub(foundDestWithMissingSub)) {
                            break;
                        }
                        messageConsumer.close();

                    } catch (Exception ignored) {
                    }
                }
            }
        });

        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);
        connection.close();

        assertEquals("no dests missing sub", 0, foundDestWithMissingSub.get());

    }

    private boolean destMissingSub(AtomicInteger tally) {
        for (Destination destination :
                ((RegionBroker)brokerService.getRegionBroker()).getTopicRegion().getDestinationMap().values()) {
            if (destination.getConsumers().isEmpty()) {
                tally.incrementAndGet();
                return true;
            }
        }
        return false;
    }
}
