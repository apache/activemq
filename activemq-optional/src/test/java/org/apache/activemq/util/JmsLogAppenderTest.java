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
package org.apache.activemq.util;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Level;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.JMSException;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class JmsLogAppenderTest extends TestCase {
    protected BrokerService broker;

    public void testLoggingWithJMS() throws IOException, JMSException {
        // Setup the consumers
        MessageConsumer info, debug, warn;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection conn = factory.createConnection();
        conn.start();

        warn  = conn.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQTopic("log4j.MAIN.WARN"));
        info  = conn.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQTopic("log4j.MAIN.INFO"));
        debug = conn.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(new ActiveMQTopic("log4j.MAIN.DEBUG"));

        // lets try configure log4j
        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("test-log4j.properties"));
        PropertyConfigurator.configure(properties);

        Logger warnLog, infoLog, debugLog;

        warnLog = Logger.getLogger("MAIN.WARN");
        warnLog.setLevel(Level.WARN);
        warnLog.warn("Warn Message");
        warnLog.info("Info Message");
        warnLog.debug("Debug Message");

        infoLog = Logger.getLogger("MAIN.INFO");
        infoLog.setLevel(Level.INFO);
        infoLog.warn("Warn Message");
        infoLog.info("Info Message");
        infoLog.debug("Debug Message");

        debugLog = Logger.getLogger("MAIN.DEBUG");
        debugLog.setLevel(Level.DEBUG);
        debugLog.warn("Warn Message");
        debugLog.info("Info Message");
        debugLog.debug("Debug Message");

        TextMessage msg;

        // Test warn level
        msg = (TextMessage)warn.receive(1000);
        assertNotNull(msg);
        assertEquals("Warn Message", msg.getText());

        msg = (TextMessage)warn.receive(1000);
        assertNull(msg); // We should not receive anymore message because our level is warning only

        // Test info level
        msg = (TextMessage)info.receive(1000);
        assertNotNull(msg);
        assertEquals("Warn Message", msg.getText());

        msg = (TextMessage)info.receive(1000);
        assertNotNull(msg);
        assertEquals("Info Message", msg.getText());

        msg = (TextMessage)info.receive(1000);
        assertNull(msg); // We should not receive the debug message

        // Test debug level
        msg = (TextMessage)debug.receive(1000);
        assertNotNull(msg);
        assertEquals("Warn Message", msg.getText());

        msg = (TextMessage)debug.receive(1000);
        assertNotNull(msg);
        assertEquals("Info Message", msg.getText());

        msg = (TextMessage)debug.receive(1000);
        assertNotNull(msg);
    }

}
