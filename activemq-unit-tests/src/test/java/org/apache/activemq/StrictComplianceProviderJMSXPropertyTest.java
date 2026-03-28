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

import jakarta.jms.Connection;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StrictComplianceProviderJMSXPropertyTest {

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
    public void testLegacyModeAllowsProviderSetJMSXProperties() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // Legacy mode (default)
        factory.setStrictCompliance(false);

        try (Connection connection = factory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            connection.start();
            Message message = (Message) session.createMessage();

            try {
                // ActiveMQ allows clients to overwrite the delivery count
                message.setIntProperty("JMSXDeliveryCount", 5);
                assertEquals(5, message.getIntProperty("JMSXDeliveryCount"));
                // PASS: Should not throw an exception in legacy mode
            } catch (Exception e) {
                fail("Legacy mode should not restrict setting provider-only JMSX properties");
            }
        }
    }

    @Test
    public void testStrictModeRejectsProviderSetJMSXProperties() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // Enforce Jakarta 3.1 compliance
        factory.setStrictCompliance(true);

        try (Connection connection = factory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            connection.start();
            Message message = (Message) session.createMessage();

            String[] providerOnlyProperties = {
                    "JMSXDeliveryCount",
                    "JMSXRcvTimestamp",
                    "JMSXState",
                    "JMSXProducerTXID",
                    "JMSXConsumerTXID"
            };

            for (String property : providerOnlyProperties) {
                try {
                    // Attempting to set these should fail
                    message.setObjectProperty(property, 1);
                    fail("Strict mode must reject client attempt to set provider-only property: " + property);
                } catch (MessageNotWriteableException e) {
                    // PASS: Expected Jakarta Messaging 3.1 behavior
                }
            }
        }
    }

    @Test
    public void testStrictModeAllowsClientSetJMSXProperties() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // Enforce Jakarta 3.1 compliance
        factory.setStrictCompliance(true);

        try (Connection connection = factory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            connection.start();
            Message message = (Message) session.createMessage();

            try {
                // The spec explicitly allows clients to set these specific JMSX properties
                message.setStringProperty("JMSXGroupID", "Group-A");
                message.setIntProperty("JMSXGroupSeq", 1);

                assertEquals("Group-A", message.getStringProperty("JMSXGroupID"));
                assertEquals(1, message.getIntProperty("JMSXGroupSeq"));
                // PASS: Valid client-set properties succeeded
            } catch (MessageNotWriteableException e) {
                fail("Strict mode incorrectly rejected a valid client-set JMSX property");
            }
        }
    }
}
