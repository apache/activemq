/*
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
package org.apache.activemq.transport.amqp.interop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.sasl.PlainMechanism;
import org.junit.Test;

/**
 * Test broker behaviour when creating AMQP connections with SASL PLAIN mechanism.
 */
public class AmqpSaslPlainTest extends AmqpClientTestSupport {

    private static final String ADMIN = "admin";
    private static final String USER = "user";
    private static final String USER_PASSWORD = "password";

    @Override
    protected void performAdditionalConfiguration(org.apache.activemq.broker.BrokerService brokerService) throws Exception {
        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser(USER, USER_PASSWORD, "users"));
        users.add(new AuthenticationUser(ADMIN, ADMIN, "admins"));

        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);

        brokerService.setPlugins(new BrokerPlugin[] { authenticationPlugin});
    };

    @Test(timeout = 30000)
    public void testSaslPlainWithValidUsernameAndPassword() throws Exception {
        AmqpClient client = createAmqpClient(USER, USER_PASSWORD);

        doSucessfullConnectionTestImpl(client);
    }

    @Test(timeout = 30000)
    public void testSaslPlainWithValidUsernameAndPasswordAndAuthzidAsUser() throws Exception {
        AmqpClient client = createAmqpClient(USER, USER_PASSWORD);
        client.setAuthzid(USER);

        doSucessfullConnectionTestImpl(client);
    }

    @Test(timeout = 30000)
    public void testSaslPlainWithValidUsernameAndPasswordAndAuthzidAsUnkown() throws Exception {
        AmqpClient client = createAmqpClient(USER, USER_PASSWORD);
        client.setAuthzid("unknown");

        doSucessfullConnectionTestImpl(client);
    }

    private void doSucessfullConnectionTestImpl(AmqpClient client) throws Exception {
        client.setMechanismRestriction(PlainMechanism.MECH_NAME);

        // Expect connection to succeed
        AmqpConnection connection = trackConnection(client.connect());

        // Exercise it for verification
        exerciseConnection(connection);

        connection.close();
    }

    private void exerciseConnection(AmqpConnection connection)throws Exception{
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        AmqpSender sender = session.createSender("queue://" + getTestName());

        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertNotNull(getProxyToQueue(getTestName()));
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);
        sender.close();
        assertEquals(0, brokerService.getAdminView().getQueueProducers().length);
    }

    @Test(timeout = 30000)
    public void testSaslPlainWithInvalidUsername() throws Exception {
        AmqpClient client = createAmqpClient("not-user", USER_PASSWORD);
        doFailedConnectionTestImpl(client);
    }

    @Test(timeout = 30000)
    public void testSaslPlainWithInvalidPassword() throws Exception {
        AmqpClient client = createAmqpClient(USER, "not-user-password");
        doFailedConnectionTestImpl(client);
    }

    @Test(timeout = 30000)
    public void testSaslPlainWithInvalidUsernameAndAuthzid() throws Exception {
        AmqpClient client = createAmqpClient("not-user", USER_PASSWORD);
        client.setAuthzid(USER);
        doFailedConnectionTestImpl(client);
    }

    @Test(timeout = 30000)
    public void testSaslPlainWithInvalidPasswordAndAuthzid() throws Exception {
        AmqpClient client = createAmqpClient(USER, "not-user-password");
        client.setAuthzid(USER);
        doFailedConnectionTestImpl(client);
    }

    private void doFailedConnectionTestImpl(AmqpClient client) throws Exception {
        client.setMechanismRestriction(PlainMechanism.MECH_NAME);

        // Expect connection to fail
        try {
            client.connect();
            fail("exected connection to fail");
        } catch (Exception e){
            // Expected
            Throwable cause = e.getCause();
            assertNotNull("Expected security exception cause", cause);
            assertTrue("Expected security exception cause", cause instanceof SecurityException);
        }
    }
}