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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MQTTOverlapedSubscriptionsTest {

    private BrokerService brokerService;
    private String mqttClientUrl;

    @Before
    public void setup() throws Exception {
        initializeBroker(true);
    }

    @After
    public void shutdown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    protected void initializeBroker(boolean deleteAllMessagesOnStart) throws Exception {

        brokerService = new BrokerService();
        brokerService.setPersistent(true);
        brokerService.setDeleteAllMessagesOnStartup(deleteAllMessagesOnStart);
        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI("mqtt://localhost:0"));
        connector.setName("mqtt");
        brokerService.addConnector(connector);
        brokerService.start();
        brokerService.waitUntilStarted();

        mqttClientUrl = connector.getPublishableConnectString().replace("mqtt", "tcp");
    }

    @Test
    public void testMqttResubscribe() throws Exception {
        // inactive durable consumer on test/1 will be left on the broker after restart
        doTest("test/1");

        shutdown();
        initializeBroker(false);

        // new consumer on test/# will match all messages sent to the inactive sub
        doTest("test/#");
    }

    private BlockingConnection getConnection(String host, String clientId) throws URISyntaxException, Exception {
        BlockingConnection conn;
        MQTT mqttPub = new MQTT();
        mqttPub.setHost(host);
        mqttPub.setConnectAttemptsMax(0);
        mqttPub.setReconnectAttemptsMax(0);
        mqttPub.setClientId(clientId);
        mqttPub.setCleanSession(false);
        conn = mqttPub.blockingConnection();
        conn.connect();
        return conn;
    }

    public void doTest(String subscribe) throws Exception {
        String payload = "This is test payload";
        BlockingConnection connectionPub = getConnection(mqttClientUrl, "client1");
        BlockingConnection connectionSub = getConnection(mqttClientUrl, "client2");
        Topic[] topics = { new Topic(subscribe, QoS.values()[1]) };
        connectionSub.subscribe(topics);
        connectionPub.publish("test/1", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
        receive(connectionSub, 3000);

        //Unsubscribe and resubscribe
        connectionSub.unsubscribe(new String[]{subscribe});
        connectionSub.subscribe(topics);
        connectionPub.publish("test/1", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
        receive(connectionSub, 3000);

        connectionPub.disconnect();
        connectionSub.disconnect();
    }

    public byte[] receive(BlockingConnection connection, int timeout) throws Exception {
        byte[] result = null;
        org.fusesource.mqtt.client.Message message = connection.receive(timeout, TimeUnit.MILLISECONDS);
        if (message != null) {
            result = message.getPayload();
            message.ack();
        }
        return result;
    }
}
