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
package org.apache.activemq.transport.mqtt;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;

/**
 * Used as a base class for MQTT tests that require Authentication and Authorization
 * to be configured on the Broker.
 */
public class MQTTAuthTestSupport extends MQTTTestSupport {

    public MQTTAuthTestSupport() {
        super();
    }

    public MQTTAuthTestSupport(String connectorScheme, boolean useSSL) {
        super(connectorScheme, useSSL);
    }

    @Override
    protected BrokerPlugin configureAuthentication() throws Exception {
        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser("admin", "admin", "users,admins"));
        users.add(new AuthenticationUser("user", "password", "users"));
        users.add(new AuthenticationUser("guest", "password", "guests"));
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);
        authenticationPlugin.setAnonymousAccessAllowed(true);

        return authenticationPlugin;
    }

    @Override
    protected BrokerPlugin configureAuthorization() throws Exception {

        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> authorizationEntries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(">");
        entry.setRead("admins");
        entry.setWrite("admins");
        entry.setAdmin("admins");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic(">");
        entry.setRead("admins");
        entry.setWrite("admins");
        entry.setAdmin("admins");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("anonymous");
        entry.setRead("guests,anonymous");
        entry.setWrite("guests,users,anonymous");
        entry.setAdmin("guests,users,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("ActiveMQ.Advisory.>");
        entry.setRead("guests,users,anonymous");
        entry.setWrite("guests,users,anonymous");
        entry.setAdmin("guests,users,anonymous");
        authorizationEntries.add(entry);

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("admins");
        tempEntry.setWrite("admins");
        tempEntry.setAdmin("admins");

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(authorizationEntries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(authorizationMap);

        return authorizationPlugin;
    }
}
