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

import java.util.LinkedList;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class MQTTNioTest extends MQTTTest {

    @Override
    protected String getProtocolScheme() {
        return "mqtt+nio";
    }

    @Test
    public void testPingOnMQTTNIO() throws Exception {
        addMQTTConnector("maxInactivityDuration=-1");
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("test-mqtt");
        mqtt.setKeepAlive((short)2);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));

        connection.disconnect();
    }

    @Test
    public void testAnonymousUserConnect() throws Exception {
        addMQTTConnector();
        configureAuthentication(brokerService);
        brokerService.start();
        brokerService.waitUntilStarted();
        MQTT mqtt = createMQTTConnection();
        mqtt.setCleanSession(true);
        mqtt.setUserName((String)null);
        mqtt.setPassword((String)null);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        System.out.println("Connected!");

        connection.disconnect();

    }

    private void configureAuthentication(BrokerService brokerService) throws Exception {
        LinkedList<AuthenticationUser> users = new LinkedList<AuthenticationUser>();
        users.add(new AuthenticationUser("user1", "user1", "anonymous,user1group"));
        final SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);

        DefaultAuthorizationMap map = new DefaultAuthorizationMap();
        LinkedList<DestinationMapEntry> authz = new LinkedList<DestinationMapEntry>();
        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setDestination(new ActiveMQTopic(">"));
        entry.setAdmin("admins");
        entry.setRead("admins,anonymous");
        entry.setWrite("admins");
        authz.add(entry);
        map.setAuthorizationEntries(authz);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(map);
        authenticationPlugin.setAnonymousAccessAllowed(true);

        brokerService.setPlugins(new BrokerPlugin[]{
                authenticationPlugin, authorizationPlugin
        });
    }

}
