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

package org.apache.activemq.usage;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.ConfigurationException;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Test;

import org.apache.activemq.broker.BrokerService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StoreUsageLimitsTest {

    final int WAIT_TIME_MILLS = 20 * 1000;
    private static final String limitsLogLevel = "warn";
    final String toMatch = new String(Long.toString(Long.MAX_VALUE / (1024 * 1024)));

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.getSystemUsage().getMemoryUsage().setLimit(Long.MAX_VALUE);
        broker.getSystemUsage().setCheckLimitsLogLevel(limitsLogLevel);
        broker.deleteAllMessages();
        return broker;
    }

    @Test
    public void testCheckLimitsLogLevel() throws Exception {

        final CountDownLatch foundMessage = new CountDownLatch(1);
        final var logger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getRootLogger());
        final var appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                String message = event.getMessage().getFormattedMessage();
                if (message.contains(toMatch) && event.getLevel().equals(Level.WARN)) {
                    foundMessage.countDown();
                }
            }
        };
        appender.start();

        logger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        logger.addAppender(appender);

        BrokerService brokerService = createBroker();
        brokerService.start();
        brokerService.stop();

        assertTrue("Fount log message", foundMessage.await(WAIT_TIME_MILLS, TimeUnit.MILLISECONDS));

        logger.removeAppender(appender);
    }

    @Test
    public void testCheckLimitsFailStart() throws Exception {

        final CountDownLatch foundMessage = new CountDownLatch(1);
        final var logger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getRootLogger());
        final var appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                String message = event.getMessage().getFormattedMessage();
                if (message.contains(toMatch) && event.getLevel().equals(Level.ERROR)) {
                    foundMessage.countDown();
                }
            }
        };
        appender.start();

        logger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        logger.addAppender(appender);

         BrokerService brokerService = createBroker();
        brokerService.setAdjustUsageLimits(false);
        try {
            brokerService.start();
            fail("expect ConfigurationException");
        } catch (ConfigurationException expected) {
            assertTrue("exception message match", expected.getLocalizedMessage().contains(toMatch));
        }
        brokerService.stop();

        assertTrue("Fount log message", foundMessage.await(WAIT_TIME_MILLS, TimeUnit.MILLISECONDS));

        logger.removeAppender(appender);
    }

    @Test
    public void testUsageLimitUpdatesAppliedToDests() throws Exception {
        BrokerService brokerService = createBroker();
        try {
            // prevent any other dests from being created
            brokerService.setAdvisorySupport(false);
            final SystemUsage systemUsage = brokerService.getSystemUsage();
            final StoreUsage storeUsage = systemUsage.getStoreUsage();
            final TempUsage tempUsage = systemUsage.getTempUsage();
            storeUsage.setLimit(4096);
            tempUsage.setLimit(1024);
            brokerService.start();
            brokerService.waitUntilStarted();

            // On startup there will be no destinations created so storeUsage and
            // systemUsage for the broker should have no children
            assertTrue(storeUsage.getChildren().isEmpty());
            assertTrue(tempUsage.getChildren().isEmpty());

            // Create a topic and a queue. This should cause both destinations to call start()
            // and create child usage trackers that will get registered with the parent
            ActiveMQTopic topic = new ActiveMQTopic("test.topic");
            ActiveMQQueue queue = new ActiveMQQueue("test.queue");
            brokerService.setDestinations(new ActiveMQDestination[]{topic, queue});
            List<BaseDestination> dests = List.of((BaseDestination) brokerService.getDestination(topic),
                    (BaseDestination) brokerService.getDestination(queue));

            // verify both broker usage trackers have 2 children now
            assertEquals(2, storeUsage.getChildren().size());
            assertEquals(2, tempUsage.getChildren().size());

            // the limits for each destination should match the parent
            for (BaseDestination dest : dests) {
                assertEquals(4096, dest.getSystemUsage().getStoreUsage().getLimit());
                assertEquals(1024, dest.getSystemUsage().getTempUsage().getLimit());
            }

            // Update the parent, this will trigger a callback to update to tell any
            // registered children usage trackers to update their limit
            storeUsage.setLimit(8192);
            tempUsage.setLimit(2048);

            // Verify the children trackers on each dest were correctly updated
            for (BaseDestination dest : dests) {
                assertEquals(8192, dest.getSystemUsage().getStoreUsage().getLimit());
                assertEquals(2048, dest.getSystemUsage().getTempUsage().getLimit());
            }

            // Remove each dest and verify that after removal the stop() method properly
            // unregisters the tracker from the parent so we prevent leaks
            brokerService.removeDestination(topic);
            assertEquals(1, storeUsage.getChildren().size());
            assertEquals(1, tempUsage.getChildren().size());

            brokerService.removeDestination(queue);
            assertTrue(storeUsage.getChildren().isEmpty());
            assertTrue(tempUsage.getChildren().isEmpty());
        } finally {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }
}
