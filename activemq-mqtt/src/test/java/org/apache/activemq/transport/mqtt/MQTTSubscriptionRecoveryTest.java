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

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.util.Wait;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that all previous QoS 2 subscriptions are recovered on Broker restart.
 */
@RunWith(Parameterized.class)
public class MQTTSubscriptionRecoveryTest extends MQTTTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTSubscriptionRecoveryTest.class);

    protected boolean defaultStrategy = false;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { "mqtt-virtual-topic-subscriptions", false},
            { "mqtt-default-subscriptions", true} });
    }

    public MQTTSubscriptionRecoveryTest(String subscriptionStrategy, boolean defaultStrategy) {
        this.defaultStrategy = defaultStrategy;
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public String getProtocolConfig() {
        if (defaultStrategy) {
            return "transport.subscriptionStrategy=mqtt-default-subscriptions";
        } else {
            return "transport.subscriptionStrategy=mqtt-virtual-topic-subscriptions";
        }
    }

    @Test
    public void testDurableSubscriptionsAreRecovered() throws Exception {

        MqttClient connection = createClient(getTestName());

        final String[] topics = { "TopicA/", "TopicB/", "TopicC/" };
        for (int i = 0; i < topics.length; i++) {
            LOG.debug("Subscribing to Topic:{}", topics[i]);
            connection.subscribe(topics[i], EXACTLY_ONCE);
        }

        assertStatsForConnectedClient(topics.length);

        disconnect(connection);

        assertStatsForDisconnectedClient(topics.length);

        restartBroker();

        assertStatsForDisconnectedClient(topics.length);

        connection = createClient(getTestName());

        assertStatsForConnectedClient(topics.length);
    }

    private void assertStatsForConnectedClient(final int numDestinations) throws Exception {
        if (defaultStrategy) {
            assertTopicStatsForConnectedClient(numDestinations);
        } else {
            assertQueueStatsForConnectedClient(numDestinations);
        }
    }

    private void assertStatsForDisconnectedClient(final int numDestinations) throws Exception {
        if (defaultStrategy) {
            assertTopicStatsForDisconnectedClient(numDestinations);
        } else {
            assertQueueStatsForDisconnectedClient(numDestinations);
        }
    }

    //----- Assert implementations based on subscription strategy ------------//

    private void assertQueueStatsForConnectedClient(final int numDestinations) throws Exception {
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getQueueSubscribers().length == numDestinations;
            }
        }));
    }

    private void assertQueueStatsForDisconnectedClient(final int numDestinations) throws Exception {
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getQueueSubscribers().length == 0;
            }
        }));
    }

    private void assertTopicStatsForConnectedClient(final int numDestinations) throws Exception {
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getDurableTopicSubscribers().length == numDestinations;
            }
        }));

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getInactiveDurableTopicSubscribers().length == 0;
            }
        }));
    }

    private void assertTopicStatsForDisconnectedClient(final int numDestinations) throws Exception {
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getDurableTopicSubscribers().length == 0;
            }
        }));

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getInactiveDurableTopicSubscribers().length == numDestinations;
            }
        }));
    }

    //----- Client Connect and Disconnect using Paho MQTT --------------------//

    protected MqttClient createClient(String clientId) throws Exception {
        return createClient(false, clientId, null);
    }

    protected MqttClient createClient(boolean cleanSession, String clientId, MqttCallback listener) throws Exception {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(cleanSession);
        options.setKeepAliveInterval(30);

        final MqttClient client = new MqttClient("tcp://localhost:" + getPort(), clientId, new MemoryPersistence());
        client.setCallback(listener);
        client.connect(options);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return client.isConnected();
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(100));

        return client;
    }

    protected void disconnect(final MqttClient client) throws Exception {
        client.disconnect();
        client.close();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return !client.isConnected();
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(100));
    }
}
