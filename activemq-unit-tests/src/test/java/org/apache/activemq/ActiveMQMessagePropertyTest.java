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

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.Session;

import java.util.HashMap;
import java.util.Map;

import static org.apache.activemq.command.DataStructureTestSupport.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ActiveMQMessagePropertyTest {

    private BrokerService broker;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("vm://localhost");
        broker.start();
        broker.waitUntilStarted();
        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testStrictComplianceMasterSwitch() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");

        // Turn on strict compliance
        factory.setStrictCompliance(true);
        // Explicitly turn on the legacy flag to prove strict mode overrides it
        factory.setNestedMapAndListEnabled(true);

        Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Message message = session.createMessage();

        // Verify standard types work
        message.setStringProperty("validString", "test"); // Should pass

        // Verify Character is rejected
        try {
            message.setObjectProperty("invalidChar", 'A');
            fail("Should have rejected Character under strict compliance");
        } catch (MessageFormatException e) {
            // Expected
        }

        // Verify Map is rejected (even though nestedMapAndListEnabled is true)
        try {
            Map<String, String> map = new HashMap<>();
            message.setObjectProperty("invalidMap", map);
            fail("Should have rejected Map under strict compliance");
        } catch (MessageFormatException e) {
            // Expected
        }

        connection.close();
    }

    @Test
    public void testLegacyModeStillAllowsCharacter() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // Default is strictCompliance = false
        try (Connection connection = factory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            Message message = session.createMessage();
            message.setObjectProperty("charProp", 'A'); // Should pass
            assertEquals('A', message.getObjectProperty("charProp"));
        }
    }
}
