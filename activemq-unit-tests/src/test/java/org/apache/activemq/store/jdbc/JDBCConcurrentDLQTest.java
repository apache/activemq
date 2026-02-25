/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.jdbc;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.DefaultIOExceptionHandler;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.*;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;


@Category(ParallelTest.class)
public class JDBCConcurrentDLQTest {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCConcurrentDLQTest.class);

    BrokerService broker;
    JDBCPersistenceAdapter jdbcPersistenceAdapter;
    Appender appender = null;
    final AtomicBoolean gotError = new AtomicBoolean(false);

    @Before
    public void setUp() throws Exception {
        gotError.set(false);
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();

        final var jdbcLogger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getLogger(JDBCPersistenceAdapter.class));
        final var regionLogger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getLogger(RegionBroker.class));

        appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                //isMoreSpecificThan is actually greather or equal so poorly named
                if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
                    LOG.error("Got error from log:" + event.getMessage().getFormattedMessage());
                    gotError.set(true);
                }
            }
        };
        appender.start();

        jdbcLogger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        jdbcLogger.addAppender(appender);
        
        regionLogger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        regionLogger.addAppender(appender);
    }

    @After
    public void tearDown() throws Exception {
        ((org.apache.logging.log4j.core.Logger)LogManager.getLogger(RegionBroker.class)).removeAppender(appender);
        ((org.apache.logging.log4j.core.Logger)LogManager.getLogger(JDBCPersistenceAdapter.class)).removeAppender(appender);
        broker.stop();
    }

    protected BrokerService createBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        jdbcPersistenceAdapter = new JDBCPersistenceAdapter();
        jdbcPersistenceAdapter.setUseLock(false);
        broker.setPersistenceAdapter(jdbcPersistenceAdapter);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("tcp://0.0.0.0:0");
        return broker;
    }


    @Test
    public void testConcurrentDlqOk() throws Exception {

        final Destination dest = new ActiveMQQueue("DD");

        final ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        amq.setWatchTopicAdvisories(false);

        broker.setIoExceptionHandler(new DefaultIOExceptionHandler() {
            @Override
            public void handle(IOException exception) {
                LOG.error("handle IOException from store", exception);
                gotError.set(true);
            }
        });

        final var loggerRB = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getLogger(RegionBroker.class));
        final var loggerJDBC = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getLogger(JDBCPersistenceAdapter.class));

        loggerRB.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        loggerRB.addAppender(appender);

        loggerJDBC.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        loggerJDBC.addAppender(appender);

        final int numMessages = 100;
        final AtomicInteger consumed = new AtomicInteger(numMessages);
        produceMessages(amq, dest, numMessages);
        ExecutorService executorService = Executors.newCachedThreadPool();

        for (int i = 0; i < 50; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    Connection connection = null;
                    Session session = null;
                    MessageConsumer consumer = null;

                    try {
                        connection = amq.createConnection();
                        connection.setExceptionListener(new jakarta.jms.ExceptionListener() {
                            public void onException(jakarta.jms.JMSException e) {
                                e.printStackTrace();
                            }
                        });

                        //set custom redelivery policy with 0 retries to force move to DLQ
                        RedeliveryPolicy queuePolicy = new RedeliveryPolicy();
                        queuePolicy.setMaximumRedeliveries(0);
                        ((ActiveMQConnection) connection).setRedeliveryPolicy(queuePolicy);
                        connection.start();

                        session = connection.createSession(true, Session.SESSION_TRANSACTED);

                        consumer = session.createConsumer(dest);

                        while (consumed.get() > 0 && !gotError.get()) {
                            Message message = consumer.receive(4000);
                            if (message != null) {
                                consumed.decrementAndGet();
                                session.rollback();
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error on consumption", e);
                        gotError.set(true);
                    } finally {
                        try {
                            if (connection != null) {
                                connection.close();
                            }
                        } catch (Exception ignored) {}
                    }

                }
            });
        }

        executorService.shutdown();
        boolean allComplete = executorService.awaitTermination(60, TimeUnit.SECONDS);
        executorService.shutdownNow();
        LOG.info("Total messages: " + broker.getAdminView().getTotalMessageCount());
        LOG.info("Total enqueues: " + broker.getAdminView().getTotalEnqueueCount());
        LOG.info("Total deueues: " + broker.getAdminView().getTotalDequeueCount());

        assertTrue(allComplete);
        assertEquals("all consumed", 0l, consumed.get());
        assertEquals("all messages get to the dlq", numMessages * 2, broker.getAdminView().getTotalEnqueueCount());
        assertEquals("all messages acked", numMessages, broker.getAdminView().getTotalDequeueCount());
        assertFalse("no error", gotError.get());

    }

    private void produceMessages(ActiveMQConnectionFactory amq, Destination dest, int numMessages) throws JMSException {
        Connection connection = amq.createConnection();

        connection.setExceptionListener(new jakarta.jms.ExceptionListener() {
            public void onException(jakarta.jms.JMSException e) {
                e.printStackTrace();
            }
        });
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(dest);
        long counter = 0;
        TextMessage message = session.createTextMessage();

        for (int i = 0; i < numMessages; i++) {
            producer.send(message);
            counter++;

            if ((counter % 50) == 0) {
                LOG.info("sent " + counter + " messages");
            }
        }

        if (connection != null) {
            connection.close();
        }
    }
}
