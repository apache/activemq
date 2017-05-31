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

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.RegionBroker;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.Before;
import org.junit.Test;

import javax.jms.MessageConsumer;
import javax.jms.Session;

import static org.junit.Assert.assertEquals;

public class PahoVirtualTopicMQTTTest extends PahoMQTTTest {

    @Override
    @Before
    public void setUp() throws Exception {
        protocolConfig = "transport.subscriptionStrategy=mqtt-virtual-topic-subscriptions";
        super.setUp();
    }

    @Override
    protected MessageConsumer createConsumer(Session s, String topic) throws Exception {
        return s.createConsumer(s.createQueue("Consumer.X.VirtualTopic." + topic));
    }

    @Test(timeout = 300000)
    public void testVirtualTopicQueueRestore() throws Exception {
        String user10 = "user10";
        String password10 = "user10";
        String clientId10 = "client-10";
        String topic10 = "user10/";
        MqttConnectOptions options10 = new MqttConnectOptions();
        options10.setCleanSession(false);
        options10.setUserName(user10);
        options10.setPassword(password10.toCharArray());
        MqttClient client10 = createClient(false, clientId10, null);
        client10.subscribe(topic10 + clientId10 + "/#", 1);
        client10.subscribe(topic10 + "#", 1);

        String user1 = "user1";
        String password1 = "user1";
        String clientId1 = "client-1";
        String topic1 = "user1/";
        MqttConnectOptions options1 = new MqttConnectOptions();
        options1.setCleanSession(false);
        options1.setUserName(user1);
        options1.setPassword(password1.toCharArray());

        MqttClient client1 = createClient(false, clientId1, null);
        client1.subscribe(topic1 + clientId1 + "/#", 1);
        client1.subscribe(topic1 + "#", 1);

        RegionBroker regionBroker = (RegionBroker) brokerService.getBroker().getAdaptor(RegionBroker.class);

        for (Destination queue : regionBroker.getQueueRegion().getDestinationMap().values()) {
            assertEquals("Queue " + queue.getActiveMQDestination() + " have more than one consumer", 1, queue.getConsumers().size());
        }
    }

}
