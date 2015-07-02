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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This test asserts that AMQ-5814 has been fixed.  It tests that
 * the correct ConnectionContext is used for new subscriptions.  The
 * issue previously was that the producer's context was being used instead
 * of the consumers so ACL permission checks were failing when the producer didn't have permission
 * to create a subscription.
 *
 * Thanks to hongphu8790@gmail.com for this test.
 *
 * */
public class MQTTPubSubWithAuthorizationTest {

    BrokerService brokerService;
    String mqttConnectorUrl = "mqtt://0.0.0.0:1883";
    String mqttClientUrl = "tcp://0.0.0.0:1883";

    public BrokerPlugin configureAuthentication() throws Exception {
        List<AuthenticationUser> users = new ArrayList<>();
        users.add(new AuthenticationUser("publisher", "123", "publisher"));
        users.add(new AuthenticationUser("subscriber", "123", "subscriber"));
        users.add(new AuthenticationUser("admin", "123", "publisher,subscriber"));

        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(
                users);
        authenticationPlugin.setAnonymousAccessAllowed(true);

        return authenticationPlugin;
    }

    public BrokerPlugin configureAuthorization() throws Exception {

        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> authorizationEntries = new ArrayList<>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setTopic("dcu.>");
        entry.setRead("subscriber");
        entry.setWrite("publisher");
        entry.setAdmin("publisher,subscriber");
        authorizationEntries.add(entry);

        entry = new AuthorizationEntry();
        entry.setTopic("ActiveMQ.Advisory.>");
        entry.setRead("publisher,subscriber");
        entry.setWrite("publisher,subscriber");
        entry.setAdmin("publisher,subscriber");
        authorizationEntries.add(entry);

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(
                authorizationEntries);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(
                authorizationMap);

        return authorizationPlugin;
    }

    @Before
    public void setup() throws Exception {
        if (brokerService == null) {
            brokerService = new BrokerService();
            brokerService.setPersistent(false);
            TransportConnector connector = new TransportConnector();
            connector.setUri(new URI(mqttConnectorUrl));
            connector.setName("mqtt");
            brokerService.addConnector(connector);
            ArrayList<BrokerPlugin> plugins = new ArrayList<>();
            plugins.add(configureAuthentication());
            plugins.add(configureAuthorization());
            if (!plugins.isEmpty()) {
                BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
                brokerService.setPlugins(plugins.toArray(array));
            }
            brokerService.start();
            brokerService.waitUntilStarted();
        }
    }

    @After
    public void shutdown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }

    /**
     * * Test with right permission, publisher has permission to write *
     * topic://dcu.> But it hasn't permission to read topic://dcu.> * * Test
     * result failed
     * */
    @Test()
    public void doTestWithNormalPermission() throws Exception {
        doTestChangeTopic("publisher", "123");
    }

    /**
     * * Test with super permission, publisher has permission both write and
     * read * topic://dcu.> * * Test result OK
     * */
    @Test()
    public void doTestWithSuperPermission() throws Exception {
        doTestChangeTopic("admin", "123");
    }

    private BlockingConnection getBlockingConnection(String host, String user,
            String password) throws URISyntaxException, Exception {
        BlockingConnection conn;
        MQTT mqttPub = new MQTT();
        mqttPub.setHost(host);
        mqttPub.setUserName(user);
        mqttPub.setPassword(password);
        mqttPub.setConnectAttemptsMax(0);
        mqttPub.setReconnectAttemptsMax(0);
        conn = mqttPub.blockingConnection();
        conn.connect();
        return conn;
    }


    public void doTestChangeTopic(String publishUser, String publishPassword)
            throws Exception {
        String payload = "This is test payload";
        // Create two instance mqtt client: publisher & subscriber
        BlockingConnection connectionPub = getBlockingConnection(mqttClientUrl,
                publishUser, publishPassword);
        BlockingConnection connectionSub = getBlockingConnection(mqttClientUrl,
                "subscriber", "123");
        // Doing with topic dcu/#
        org.fusesource.mqtt.client.Topic[] topics = { new org.fusesource.mqtt.client.Topic(
                "dcu/#", QoS.values()[1]) };
        // Subscribe topic dcu/#
        connectionSub.subscribe(topics);
        // Publish message
        connectionPub.publish("dcu/id", payload.getBytes(), QoS.AT_LEAST_ONCE,
                false);
        // Received message
        byte[] message = receive(connectionSub, 5000);
        assertNotNull("Should get a message", message);
        assertEquals("Payload not valid", payload, new String(message));

        connectionPub.disconnect();
        connectionSub.disconnect();
    }

    public byte[] receive(BlockingConnection connection, int timeout)
            throws Exception {
        byte[] result = null;
        org.fusesource.mqtt.client.Message message = connection.receive(
                timeout, TimeUnit.MILLISECONDS);
        if (message != null) {
            result = message.getPayload();
            message.ack();
        }
        return result;
    }

}
