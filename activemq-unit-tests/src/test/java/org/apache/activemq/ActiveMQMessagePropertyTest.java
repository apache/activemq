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
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // Enable master switch
        factory.setStrictCompliance(true);

        // Verify side-effect
        assertFalse("nestedMapAndListEnabled must be false when strictCompliance is true",
                factory.isNestedMapAndListEnabled());

        try (Connection connection = factory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            Message message = session.createMessage();
            try {
                message.setObjectProperty("charProp", 'A');
                fail("Should have rejected Character under strictCompliance=true");
            } catch (MessageFormatException e) {
                // Success
            }
        }
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
