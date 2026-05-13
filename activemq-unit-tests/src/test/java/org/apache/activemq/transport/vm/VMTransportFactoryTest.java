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
package org.apache.activemq.transport.vm;

import static org.apache.activemq.util.VmTransportTestUtils.resetVmTransportFactory;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class VMTransportFactoryTest {

    @Before
    public void setUp() throws Exception {
        // reset before each test
        resetVmTransportFactory();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        resetVmTransportFactory();
    }

    @Test
    public void testDefaults() throws Exception {
        // broker and properties allowed by default
        assertBrokerCreated("vm:localhost?persistent=false");
        assertBrokerCreated("vm:(broker:(tcp://localhost:0)?persistent=false)");
        assertBrokerCreated("vm://localhost?brokerConfig=properties:org/apache/activemq/config/broker.properties");

        // xbean not allowed by default
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:activemq.xml");
    }

    @Test
    public void testAllowAll() throws Exception {
        resetVmTransportFactory("broker,properties,xbean");

        // broker and properties allowed by default
        assertBrokerCreated("vm:localhost?persistent=false");
        assertBrokerCreated("vm:(broker:(tcp://localhost:0)?persistent=false)");
        assertBrokerCreated("vm://localhost?brokerConfig=properties:org/apache/activemq/config/broker.properties");

        // xbean now allowed
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:activemq.xml");
    }

    @Test
    public void testAllowAllWildcard() throws Exception {
        resetVmTransportFactory("*");

        // all allowed
        assertBrokerCreated("vm:localhost?persistent=false");
        assertBrokerCreated("vm:(broker:(tcp://localhost:0)?persistent=false)");
        assertBrokerCreated("vm://localhost?brokerConfig=properties:org/apache/activemq/config/broker.properties");
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:activemq.xml");
    }

    @Test
    public void testNullSchemes() throws Exception {
        // should set to defaults
        resetVmTransportFactory(null);

        // broker and properties allowed by default
        assertBrokerCreated("vm:localhost?persistent=false");
        assertBrokerCreated("vm:(broker:(tcp://localhost:0)?persistent=false)");
        assertBrokerCreated("vm://localhost?brokerConfig=properties:org/apache/activemq/config/broker.properties");

        // xbean not allowed by default
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:activemq.xml");
    }

    @Test
    public void testNoneAllowed() throws Exception {
        // deny all
        resetVmTransportFactory("");

        // nothing allowed
        assertBrokerStartError("vm:localhost?persistent=false");
        assertBrokerStartError("vm:(broker:(tcp://localhost:0)?persistent=false)");
        assertBrokerStartError("vm://localhost?brokerConfig=properties:org/apache/activemq/config/broker.properties");
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:activemq.xml");
    }

    @Test
    public void testOneAllowed() throws Exception {
        // deny all
        resetVmTransportFactory("properties");

        // only properties allowed
        assertBrokerStartError("vm:localhost?persistent=false");
        assertBrokerStartError("vm:(broker:(tcp://localhost:0)?persistent=false)");
        assertBrokerCreated("vm://localhost?brokerConfig=properties:org/apache/activemq/config/broker.properties");
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:activemq.xml");
    }

    private void assertBrokerCreated(String url) throws JMSException {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        try (Connection connection = factory.createConnection()) {
            assertNotNull(connection);
        }
    }

    private void assertBrokerStartError(String url) {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        try (Connection ignored = factory.createConnection()) {
            fail("Should have failed with an exception");
        } catch (JMSException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            assertTrue(cause instanceof IllegalArgumentException);
        }
    }

}
