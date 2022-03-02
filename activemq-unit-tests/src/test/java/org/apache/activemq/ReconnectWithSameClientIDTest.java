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
package org.apache.activemq;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.Session;

import junit.framework.Test;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconnectWithSameClientIDTest extends EmbeddedBrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ReconnectWithSameClientIDTest.class);

    protected Connection connection;
    protected boolean transacted;
    protected int authMode = Session.AUTO_ACKNOWLEDGE;
    public boolean useFailover = false;

    public static Test suite() {
        return suite(ReconnectWithSameClientIDTest.class);
    }

    public void initCombosForTestReconnectMultipleTimesWithSameClientID() {
        addCombinationValues("useFailover", new Object[]{Boolean.FALSE, Boolean.TRUE});
    }

    public void testReconnectMultipleTimesWithSameClientID() throws Exception {

        final AtomicBoolean failed = new AtomicBoolean(false);
        final var logger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getLogger((org.apache.activemq.broker.jmx.ManagedTransportConnection.class)));
        final var appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                if (event.getMessage().getFormattedMessage().startsWith("Failed to register MBean")) {
                    LOG.info("received unexpected log message: " + event.getMessage());
                    failed.set(true);
                }
            }
        };
        appender.start();

        logger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        logger.addAppender(appender);

        try {
            connection = connectionFactory.createConnection();
            useConnection(connection);

            // now lets create another which should fail
            for (int i = 1; i < 11; i++) {
                Connection connection2 = connectionFactory.createConnection();
                try {
                    useConnection(connection2);
                    fail("Should have thrown InvalidClientIDException on attempt" + i);
                } catch (InvalidClientIDException e) {
                    LOG.info("Caught expected: " + e);
                } finally {
                    connection2.close();
                }
            }

            // now lets try closing the original connection and creating a new
            // connection with the same ID
            connection.close();
            connection = connectionFactory.createConnection();
            useConnection(connection);
        } finally {
            logger.removeAppender(appender);
        }
        assertFalse("failed on unexpected log event", failed.get());
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory((useFailover ? "failover:" : "") +
                broker.getTransportConnectors().get(0).getPublishableConnectString());
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:0";
        super.setUp();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    protected void useConnection(Connection connection) throws JMSException {
        connection.setClientID("foo");
        connection.start();
    }
}
