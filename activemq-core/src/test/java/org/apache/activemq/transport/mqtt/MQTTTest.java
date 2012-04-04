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

import java.util.Vector;

import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.ByteSequence;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.fusesource.hawtbuf.UTF8Buffer.utf8;
import static org.junit.Assert.assertEquals;


public class MQTTTest {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTConnectTest.class);
    BrokerService brokerService;
    Vector<Throwable> exceptions = new Vector<Throwable>();

    @Before
    public void startBroker() throws Exception {
        exceptions.clear();
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void testSendAndReceiveAtLeastOnce() throws Exception {

        brokerService.addConnector("mqtt://localhost:1883");
        brokerService.start();
        MQTT mqtt = new MQTT();
        mqtt.setHost("localhost", 1883);
        BlockingConnection connection = mqtt.blockingConnection();

        connection.connect();

        Topic[] topics = {new Topic(utf8("foo"), QoS.AT_LEAST_ONCE)};
        connection.subscribe(topics);
        for (int i = 0; i < 10000; i++) {
            String payload = "Test Message: " + i;
            connection.publish("foo", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
            Message message = connection.receive();
            message.ack();
            assertEquals(payload, new String(message.getPayload()));
        }
        connection.disconnect();
    }

    @Test
    public void testSendMQTTReceiveJMS() throws Exception {

        brokerService.addConnector("mqtt://localhost:1883");
        brokerService.addConnector(ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL);
        brokerService.start();
        MQTT mqtt = new MQTT();
        mqtt.setHost("localhost", 1883);
        BlockingConnection connection = mqtt.blockingConnection();
        final String DESTINATION_NAME = "foo";
        connection.connect();

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory().createConnection();
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic jmsTopic = s.createTopic(DESTINATION_NAME);
        MessageConsumer consumer = s.createConsumer(jmsTopic);

        for (int i = 0; i < 10000; i++) {
            String payload = "Test Message: " + i;
            connection.publish("foo", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
            ActiveMQMessage message = (ActiveMQMessage) consumer.receive();
            ByteSequence bs = message.getContent();
            assertEquals(payload, new String(bs.data, bs.offset, bs.length));
        }


        activeMQConnection.close();
        connection.disconnect();
    }


}