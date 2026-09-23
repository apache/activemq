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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import jakarta.jms.Connection;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSContext;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A client identifier configured on the connection factory is administratively
 * configured; the specification forbids the application from overriding it.
 * ActiveMQ has always allowed the override, so enforcement is tied to
 * strictCompliance.
 */
public class StrictComplianceClientIDTest {

    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.addConnector("vm://localhost");
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testStrictRejectsOverridingAdminConfiguredClientID() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setStrictCompliance(true);
        factory.setClientID("admin-configured");

        try (Connection connection = factory.createConnection()) {
            try {
                connection.setClientID("application-override");
                fail("Expected IllegalStateException overriding an administratively configured clientID");
            } catch (jakarta.jms.IllegalStateException expected) {
            }
            assertEquals("admin-configured", connection.getClientID());
        }

        try (JMSContext context = factory.createContext()) {
            try {
                context.setClientID("application-override");
                fail("Expected IllegalStateRuntimeException overriding an administratively configured clientID");
            } catch (IllegalStateRuntimeException expected) {
            }
            assertEquals("admin-configured", context.getClientID());
        }
    }

    @Test(timeout = 60000)
    public void testLegacyAllowsOverridingAdminConfiguredClientID() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setClientID("admin-configured");

        // Default strictCompliance = false keeps the historical override behavior
        try (Connection connection = factory.createConnection()) {
            connection.setClientID("application-override");
            assertEquals("application-override", connection.getClientID());
        }
    }
}
