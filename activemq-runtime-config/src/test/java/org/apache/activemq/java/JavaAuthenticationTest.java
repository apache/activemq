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
package org.apache.activemq.java;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RuntimeConfigTestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationBroker;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationPlugin;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.junit.Test;

public class JavaAuthenticationTest extends RuntimeConfigTestSupport {

    public static final int SLEEP = 2; // seconds
    private JavaRuntimeConfigurationBroker javaConfigBroker;
    private SimpleAuthenticationPlugin authenticationPlugin;

    public void startBroker(BrokerService brokerService) throws Exception {
        this.brokerService = brokerService;

        authenticationPlugin = new SimpleAuthenticationPlugin();
        authenticationPlugin.setAnonymousAccessAllowed(false);
        authenticationPlugin.setAnonymousGroup("ag");
        authenticationPlugin.setAnonymousUser("au");
        List<AuthenticationUser> users = new ArrayList<>();
        users.add(new AuthenticationUser("test_user_password", "test_user_password", "users"));
        authenticationPlugin.setUsers(users);

        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin();
        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap();
        authorizationPlugin.setMap(authorizationMap);
        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> entries = new ArrayList<>();
        entries.add(buildQueueAuthorizationEntry(">", "admins", "admins", "admins"));
        entries.add(buildQueueAuthorizationEntry("USERS.>", "users", "users", "users"));

        entries.add(buildTopicAuthorizationEntry(">", "admins", "admins", "admins"));
        entries.add(buildTopicAuthorizationEntry("USERS.>", "users", "users", "users"));

        entries.add(buildTopicAuthorizationEntry("ActiveMQ.Advisory.>", "guests,users", "guests,users", "guests,users"));

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("tempDestinationAdmins");
        tempEntry.setWrite("tempDestinationAdmins");
        tempEntry.setAdmin("tempDestinationAdmins");

        authorizationMap.setAuthorizationEntries(entries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);

        brokerService.setPlugins(new BrokerPlugin[]{new JavaRuntimeConfigurationPlugin(),
                authenticationPlugin, authorizationPlugin});
        brokerService.setPersistent(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        javaConfigBroker =
                (JavaRuntimeConfigurationBroker) brokerService.getBroker().getAdaptor(JavaRuntimeConfigurationBroker.class);
    }

    @Test
    public void testMod() throws Exception {
        BrokerService brokerService = new BrokerService();
        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        assertAllowed("test_user_password", "USERS.A");
        assertDenied("another_test_user_password", "USERS.A");

        // anonymous
        assertDenied(null, "USERS.A");

        List<AuthenticationUser> users = new ArrayList<>();
        users.add(new AuthenticationUser("test_user_password", "test_user_password", "users"));
        users.add(new AuthenticationUser("another_test_user_password", "another_test_user_password", "users"));
        authenticationPlugin.setAnonymousGroup("users");
        authenticationPlugin.setUsers(users);
        authenticationPlugin.setAnonymousAccessAllowed(true);
        javaConfigBroker.updateSimpleAuthenticationPlugin(authenticationPlugin);

        TimeUnit.SECONDS.sleep(SLEEP);

        assertAllowed("test_user_password", "USERS.A");
        assertAllowed("another_test_user_password", "USERS.A");
        assertAllowed(null, "USERS.A");

    }

    private void assertDenied(String userPass, String destination) {
        try {
            assertAllowed(userPass, destination);
            fail("Expected not allowed exception");
        } catch (JMSException expected) {
            LOG.debug("got:" + expected, expected);
        }
    }

    private void assertAllowed(String userPass, String dest) throws JMSException {
        ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory("vm://localhost").createConnection(userPass, userPass);
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(dest));
        } finally {
            connection.close();
        }
    }

    private AuthorizationEntry buildQueueAuthorizationEntry(String queue, String read, String write, String admin) throws Exception {
        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(queue);
        entry.setRead(read);
        entry.setWrite(write);
        entry.setAdmin(admin);
        return entry;
    }

    private AuthorizationEntry buildTopicAuthorizationEntry(String topic, String read, String write, String admin) throws Exception {
        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setTopic(topic);
        entry.setRead(read);
        entry.setWrite(write);
        entry.setAdmin(admin);
        return entry;
    }

}
