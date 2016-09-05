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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AmqpAndMqttTest {

    protected BrokerService broker;
    private TransportConnector amqpConnector;
    private TransportConnector mqttConnector;

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(false);

        amqpConnector = broker.addConnector("amqp://0.0.0.0:0");
        mqttConnector = broker.addConnector("mqtt://0.0.0.0:0");

        return broker;
    }

    @Test(timeout = 60000)
    public void testFromMqttToAmqp() throws Exception {
        Connection amqp = createAmqpConnection();
        Session session = amqp.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(session.createTopic("topic://FOO"));

        final BlockingConnection mqtt = createMQTTConnection().blockingConnection();
        mqtt.connect();
        byte[] payload = bytes("Hello World");
        mqtt.publish("FOO", payload, QoS.AT_LEAST_ONCE, false);
        mqtt.disconnect();

        Message msg = consumer.receive(1000 * 5);
        assertNotNull(msg);
        assertTrue(msg instanceof BytesMessage);

        BytesMessage bmsg = (BytesMessage) msg;
        byte[] actual = new byte[(int) bmsg.getBodyLength()];
        bmsg.readBytes(actual);
        assertTrue(Arrays.equals(actual, payload));
        amqp.close();
    }

    private byte[] bytes(String value) {
        try {
            return value.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    protected MQTT createMQTTConnection() throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setConnectAttemptsMax(1);
        mqtt.setReconnectAttemptsMax(0);
        mqtt.setHost("localhost", mqttConnector.getConnectUri().getPort());
        return mqtt;
    }

    public Connection createAmqpConnection() throws Exception {

        String amqpURI = "amqp://localhost:" + amqpConnector.getConnectUri().getPort();

        final JmsConnectionFactory factory = new JmsConnectionFactory(amqpURI);

        factory.setUsername("admin");
        factory.setPassword("password");

        final Connection connection = factory.createConnection();
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                exception.printStackTrace();
            }
        });

        connection.start();
        return connection;
    }
}
