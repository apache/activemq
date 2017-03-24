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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.AbstractAuthorizationTest;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationBroker;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationPlugin;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.JaasAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.junit.Test;

public class JavaAuthorizationTest extends AbstractAuthorizationTest {

    public static final int SLEEP = 2; // seconds
    String configurationSeed = "authorizationTest";

    private JavaRuntimeConfigurationBroker javaConfigBroker;

    public void startBroker(BrokerService brokerService) throws Exception {
        this.brokerService = brokerService;

        JaasAuthenticationPlugin authenticationPlugin = new JaasAuthenticationPlugin();
        authenticationPlugin.setConfiguration("activemq-domain");


        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin();
        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap();
        authorizationPlugin.setMap(authorizationMap);

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
        DefaultAuthorizationMap authorizationMap = buildUsersMap();
        BrokerService brokerService = new BrokerService();
        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        javaConfigBroker.updateAuthorizationMap(authorizationMap);
        assertAllowed("user", "USERS.A");
        assertDenied("user", "GUESTS.A");

        assertDeniedTemp("guest");

       // applyNewConfig(brokerConfig, configurationSeed + "-users-guests", SLEEP);

        authorizationMap = buildUsersGuestsMap();
        javaConfigBroker.updateAuthorizationMap(authorizationMap);
        TimeUnit.SECONDS.sleep(SLEEP);

        assertAllowed("user", "USERS.A");
        assertAllowed("guest", "GUESTS.A");
        assertDenied("user", "GUESTS.A");

        assertAllowedTemp("guest");
    }

    @Test
    public void testModRm() throws Exception {
        DefaultAuthorizationMap authorizationMap = buildUsersGuestsMap();
        BrokerService brokerService = new BrokerService();
        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        javaConfigBroker.updateAuthorizationMap(authorizationMap);
        TimeUnit.SECONDS.sleep(SLEEP);
        assertAllowed("user", "USERS.A");
        assertAllowed("guest", "GUESTS.A");
        assertDenied("user", "GUESTS.A");
        assertAllowedTemp("guest");

        authorizationMap = buildUsersMap();
        javaConfigBroker.updateAuthorizationMap(authorizationMap);
        TimeUnit.SECONDS.sleep(SLEEP);

        assertAllowed("user", "USERS.A");
        assertDenied("user", "GUESTS.A");
        assertDeniedTemp("guest");
    }

    @Test
    public void testWildcard() throws Exception {
        DefaultAuthorizationMap authorizationMap = buildWildcardUsersGuestsMap();
        BrokerService brokerService = new BrokerService();
        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        javaConfigBroker.updateAuthorizationMap(authorizationMap);
        TimeUnit.SECONDS.sleep(SLEEP);

        final String ALL_USERS = "ALL.USERS.>";
        final String ALL_GUESTS = "ALL.GUESTS.>";

        assertAllowed("user", ALL_USERS);
        assertAllowed("guest", ALL_GUESTS);
        assertDenied("user", ALL_USERS + "," + ALL_GUESTS);
        assertDenied("guest", ALL_GUESTS + "," + ALL_USERS);

        final String ALL_PREFIX = "ALL.>";

        assertDenied("user", ALL_PREFIX);
        assertDenied("guest", ALL_PREFIX);

        assertAllowed("user", "ALL.USERS.A");
        assertAllowed("user", "ALL.USERS.A,ALL.USERS.B");
        assertAllowed("guest", "ALL.GUESTS.A");
        assertAllowed("guest", "ALL.GUESTS.A,ALL.GUESTS.B");

        assertDenied("user", "USERS.>");
        assertDenied("guest", "GUESTS.>");


        assertAllowedTemp("guest");
    }

    /**
     * @return
     * @throws Exception
     */
    private DefaultAuthorizationMap buildWildcardUsersGuestsMap() throws Exception {
        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap();
        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> entries = new ArrayList<>();
        entries.add(buildQueueAuthorizationEntry(">", "admins", "admins", "admins"));
        entries.add(buildQueueAuthorizationEntry("ALL.USERS.>", "users", "users", "users"));
        entries.add(buildQueueAuthorizationEntry("ALL.GUESTS.>", "guests", "guests,users", "guests,users"));

        entries.add(buildTopicAuthorizationEntry(">", "admins", "admins", "admins"));
        entries.add(buildTopicAuthorizationEntry("ALL.USERS.>", "users", "users", "users"));
        entries.add(buildTopicAuthorizationEntry("ALL.GUESTS.>", "guests", "guests,users", "guests,users"));

        entries.add(buildTopicAuthorizationEntry("ActiveMQ.Advisory.>", "guests,users", "guests,users", "guests,users"));

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("tempDestinationAdmins,guests");
        tempEntry.setWrite("tempDestinationAdmins,guests");
        tempEntry.setAdmin("tempDestinationAdmins,guests");

        authorizationMap.setAuthorizationEntries(entries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);
        return authorizationMap;
    }

    private DefaultAuthorizationMap buildUsersMap() throws Exception {
        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap();
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

        return authorizationMap;
    }

    private DefaultAuthorizationMap buildUsersGuestsMap() throws Exception {
        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap();
        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> entries = new ArrayList<>();
        entries.add(buildQueueAuthorizationEntry(">", "admins", "admins", "admins"));
        entries.add(buildQueueAuthorizationEntry("USERS.>", "users", "users", "users"));
        entries.add(buildQueueAuthorizationEntry("GUESTS.>", "guests", "guests,users", "guests,users"));

        entries.add(buildTopicAuthorizationEntry(">", "admins", "admins", "admins"));
        entries.add(buildTopicAuthorizationEntry("USERS.>", "users", "users", "users"));
        entries.add(buildTopicAuthorizationEntry("GUESTS.>", "guests", "guests,users", "guests,users"));

        entries.add(buildTopicAuthorizationEntry("ActiveMQ.Advisory.>", "guests,users", "guests,users", "guests,users"));

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("tempDestinationAdmins,guests");
        tempEntry.setWrite("tempDestinationAdmins,guests");
        tempEntry.setAdmin("tempDestinationAdmins,guests");

        authorizationMap.setAuthorizationEntries(entries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);
        return authorizationMap;
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
