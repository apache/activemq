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

package org.apache.activemq.proxy;


import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.util.Wait;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.activemq.util.TestUtils.findOpenPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(ParallelTest.class)
public class AMQ4889Test {

    protected static final Logger LOG = LoggerFactory.getLogger(AMQ4889Test.class);

    public static final String USER = "user";
    public static final String GOOD_USER_PASSWORD = "password";
    public static final String WRONG_PASSWORD = "wrongPassword";
    public static final String PROXY_URI_PREFIX = "tcp://localhost:";
    public static final String LOCAL_URI_PREFIX = "tcp://localhost:";

    private String proxyURI;
    private String localURI;

    private BrokerService brokerService;
    private ProxyConnector proxyConnector;
    private ConnectionFactory connectionFactory;

    private static final Integer ITERATIONS = 100;

    protected BrokerService createBroker() throws Exception {
        proxyURI = PROXY_URI_PREFIX + findOpenPort();
        localURI = LOCAL_URI_PREFIX + findOpenPort();

        brokerService = new BrokerService();
        brokerService.setPersistent(false);

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();
        BrokerPlugin authenticationPlugin = configureAuthentication();
        plugins.add(authenticationPlugin);
        BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
        brokerService.setPlugins(plugins.toArray(array));

        brokerService.addConnector(localURI);
        proxyConnector = new ProxyConnector();
        proxyConnector.setName("proxy");
        proxyConnector.setBind(new URI(proxyURI));
        proxyConnector.setRemote(new URI(localURI));
        brokerService.addProxyConnector(proxyConnector);

        brokerService.start();
        brokerService.waitUntilStarted();

        return brokerService;
    }

    protected BrokerPlugin configureAuthentication() throws Exception {
        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser(USER, GOOD_USER_PASSWORD, "users"));
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);

        return authenticationPlugin;
    }

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        connectionFactory = new ActiveMQConnectionFactory(proxyURI);
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test(timeout = 60000)
    public void testForConnectionLeak() throws Exception {
        int successfulConnections = 0;
        int failedConnections = 0;

        for (int i = 0; i < ITERATIONS; i++) {
            if (i % 2 == 0) {
                // Expected to fail - bad password
                // Use try-with-resources to ensure failed connections are cleaned up
                LOG.debug("Iteration {} adding bad connection", i);
                try (final Connection connection = connectionFactory.createConnection(USER, WRONG_PASSWORD)) {
                    connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    fail("createSession should fail with bad password");
                } catch (final JMSException e) {
                    // Authentication failure can be JMSSecurityException (synchronous)
                    // or JMSException "Disposed due to prior exception" (asynchronous)
                    // we don't care much here as long as it fails, but we could add an ExceptionListener to the connection
                    // to check more closely if desired.
                    failedConnections++;
                }
            } else {
                // Expected to succeed - good password
                // Do NOT close - test verifies these connections persist
                LOG.debug("Iteration {} adding good connection", i);
                try { // do not use try-with-resources here, because we want to keep the connection open and assert at the end
                    final Connection connection = connectionFactory.createConnection(USER, GOOD_USER_PASSWORD);
                    connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    successfulConnections++;
                } catch (final JMSException e) {
                    fail("Good connection should not fail: " + e.getMessage());
                }
            }
            LOG.debug("Iteration {} Connections? {}", i, proxyConnector.getConnectionCount());
        }

        // Verify we had the expected number of failures and successes
        assertEquals("Failed connections", ITERATIONS / 2, failedConnections);
        assertEquals("Successful connections", ITERATIONS / 2, successfulConnections);

        // Wait for proxy connector to reflect only the successful (still open) connections
        final int expectedConnections = successfulConnections;
        Wait.waitFor(() -> expectedConnections == proxyConnector.getConnectionCount(), TimeUnit.SECONDS.toMillis(20));
        assertEquals("Proxy connection count", expectedConnections, (int) proxyConnector.getConnectionCount());
    }

}
