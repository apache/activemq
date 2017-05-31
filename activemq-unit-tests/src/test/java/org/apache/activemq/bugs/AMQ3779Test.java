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

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.util.LoggingBrokerPlugin;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AMQ3779Test {

    private static final Logger LOG = Logger.getLogger(AMQ3779Test.class);
    private static final String qName = "QNameToFind";

    private BrokerService brokerService;
    private Appender appender;
    private final AtomicBoolean ok = new AtomicBoolean(false);
    private final AtomicBoolean gotZeroSize = new AtomicBoolean(false);
    
    @Before
    public void setUp() throws Exception {
        ok.set(false);

        appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLoggerName().toString().contains(qName)) {
                    ok.set(true);
                }

                if (event.getMessage().toString().contains("Sending") && event.getMessage().toString().contains("size = 0")) {
                    gotZeroSize.set(true);
                }
            }
        };

        Logger.getRootLogger().addAppender(appender);

        try {
            brokerService = new BrokerService();
            LoggingBrokerPlugin loggingBrokerPlugin = new LoggingBrokerPlugin();
            loggingBrokerPlugin.setPerDestinationLogger(true);
            loggingBrokerPlugin.setLogAll(true);
            brokerService.setPlugins(new LoggingBrokerPlugin[]{loggingBrokerPlugin});

            brokerService.setPersistent(false);
            brokerService.setUseJmx(false);
            brokerService.start();
        } finally {
            LOG.removeAppender(appender);
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (brokerService != null) {
                brokerService.stop();
                brokerService.waitUntilStopped();
            }
        } finally {
           Logger.getRootLogger().removeAppender(appender);
        }

    }

    @Test(timeout = 60000)
    public void testLogPerDest() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerService.getVmConnectorURI()).createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer messageProducer = session.createProducer(session.createQueue(qName));
        messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();

        messageProducer.send(session.createTextMessage("Hi"));
        connection.close();

        assertTrue("got expected log message", ok.get());

        assertFalse("did not get zero size in send message", gotZeroSize.get());
    }
}
