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
package org.apache.activemq.security;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class SimpleAuthenticationPluginNoUsersTest extends SecurityTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleAuthenticationPluginNoUsersTest.class);

    @Override
    protected void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        return createBroker("org/apache/activemq/security/simple-auth-broker-no-users.xml");
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

    public void testConnectionStartThrowsJMSSecurityException() throws Exception {
        final CountDownLatch exceptionLatch = new CountDownLatch(1);

        try (final Connection connection = factory.createConnection("user", "password")) {
            connection.setExceptionListener(e -> {
                LOG.info("Connection received exception: {}", e.getMessage());
                assertTrue(e instanceof JMSSecurityException);
                exceptionLatch.countDown();
            });

            try {
                connection.start();

                // If start() doesn't throw synchronously, wait for async exception
                assertTrue("Should receive security exception via listener", exceptionLatch.await(5, TimeUnit.SECONDS));

            } catch (final JMSSecurityException jmsEx) {
                // Synchronous security exception - expected
            } catch (final JMSException e) {
                // with the latch, we should always pass first into the listener and assert the right exception
                LOG.info("Expected JMSSecurityException but was: {}", e.getClass());
                fail("Should throw JMSSecurityException");
            }
        }
    }
}
