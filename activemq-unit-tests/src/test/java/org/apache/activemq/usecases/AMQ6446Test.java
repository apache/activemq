/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AMQ6446Test {

    private BrokerService brokerService;
    LinkedList<Connection> connections = new LinkedList<>();

    @Test
    public void test2Connections() throws Exception {
        final String urlTraceParam = "?trace=true";
        startBroker(urlTraceParam);
        final HashSet<String> loggers = new HashSet<String>();
        final HashSet<String> messages = new HashSet<String>();

        DefaultTestAppender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                loggers.add(event.getLoggerName());
                messages.add(event.getRenderedMessage());
            }
        };

        Logger.getRootLogger().addAppender(appender);
        Logger.getRootLogger().setLevel(Level.DEBUG);

        String brokerUrlWithTrace = brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString() +
                urlTraceParam;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrlWithTrace);

        for (int i=0; i<2; i++) {
            Connection c = factory.createConnection();
            c.start();
            connections.add(c);
        }

        Logger.getRootLogger().removeAppender(appender);

        // no logger ends with :2
        assertFalse(foundMatch(loggers, ".*:2$"));

        // starts with 000000x:
        assertTrue(foundMatch(messages, "^0+\\d:.*"));
    }

    public boolean foundMatch(Collection<String> values, String regex) {
        boolean found = false;
        Pattern p = Pattern.compile(regex);

        for (String input: values) {
            Matcher m = p.matcher(input);
            found =  m.matches();
            if (found) {
                break;
            }
        }
        return found;
    }

    @Test
    public void test2ConnectionsLegacy() throws Exception {
        final String legacySupportParam = "?trace=true&jmxPort=22";
        startBroker(legacySupportParam);

        final HashSet<String> loggers = new HashSet<String>();
        final HashSet<String> messages = new HashSet<String>();

        DefaultTestAppender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                loggers.add(event.getLoggerName());
                messages.add(event.getRenderedMessage());
            }
        };

        Logger.getRootLogger().addAppender(appender);
        Logger.getRootLogger().setLevel(Level.TRACE);

        String brokerUrlWithTrace = brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString() +
                legacySupportParam;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrlWithTrace);

        for (int i=0; i<2; i++) {
            Connection c = factory.createConnection();
            c.start();
            connections.add(c);
        }

        Logger.getRootLogger().removeAppender(appender);

        // logger ends with :2
        assertTrue(foundMatch(loggers, ".*:2$"));

        // starts with 000000x:
        assertFalse(foundMatch(messages, "^0+\\d:.*"));

    }

    @After
    public void tearDown() throws Exception {
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (Exception ignored) {}
        }
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    public void startBroker(String urlParam) throws Exception {
        brokerService = BrokerFactory.createBroker("broker:(tcp://0.0.0.0:0" + urlParam + ")/localhost?useJmx=false&persistent=false");
        brokerService.start();
        brokerService.waitUntilStarted();
    }

}
