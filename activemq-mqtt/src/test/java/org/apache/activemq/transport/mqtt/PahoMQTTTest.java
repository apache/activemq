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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.Wait;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class PahoMQTTTest extends MQTTTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PahoMQTTTest.class);

    @Override
    @Before
    public void setUp() throws Exception {
        protocolConfig = "transport.activeMQSubscriptionPrefetch=32766";
        super.setUp();
    }

    @Test(timeout = 300000)
    public void testLotsOfClients() throws Exception {

        final int CLIENTS = Integer.getInteger("PahoMQTTTest.CLIENTS", 100);
        LOG.info("Using: {} clients", CLIENTS);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) cf.createConnection();
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = s.createConsumer(s.createTopic("test"));

        final AtomicInteger receiveCounter = new AtomicInteger();
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                receiveCounter.incrementAndGet();
            }
        });

        final AtomicReference<Throwable> asyncError = new AtomicReference<Throwable>();
        final CountDownLatch connectedDoneLatch = new CountDownLatch(CLIENTS);
        final CountDownLatch disconnectDoneLatch = new CountDownLatch(CLIENTS);
        final CountDownLatch sendBarrier = new CountDownLatch(1);
        for (int i = 0; i < CLIENTS; i++) {
            Thread.sleep(10);
            new Thread(null, null, "client:" + i) {
                @Override
                public void run() {
                    try {
                        MqttClient client = new MqttClient("tcp://localhost:" + getPort(),
                                                           Thread.currentThread().getName(),
                                                           new MemoryPersistence());
                        client.connect();
                        connectedDoneLatch.countDown();
                        sendBarrier.await();
                        for (int i = 0; i < 10; i++) {
                            Thread.sleep(1000);
                            client.publish("test", "hello".getBytes(), 1, false);
                        }
                        client.disconnect();
                        client.close();
                    } catch (Throwable e) {
                        e.printStackTrace();
                        asyncError.set(e);
                    } finally {
                        disconnectDoneLatch.countDown();
                    }
                }
            }.start();
        }

        connectedDoneLatch.await();
        assertNull("Async error: " + asyncError.get(), asyncError.get());
        sendBarrier.countDown();

        LOG.info("All clients connected... waiting to receive sent messages...");

        // We should eventually get all the messages.
        within(30, TimeUnit.SECONDS, new Task() {
            @Override
            public void run() throws Exception {
                assertTrue(receiveCounter.get() == CLIENTS * 10);
            }
        });

        LOG.info("All messages received.");

        disconnectDoneLatch.await();
        assertNull("Async error: " + asyncError.get(), asyncError.get());
    }

    @Test(timeout=300000)
    public void testSendAndReceiveMQTT() throws Exception {

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) cf.createConnection();
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = s.createConsumer(s.createTopic("test"));

        MqttClient client = new MqttClient("tcp://localhost:" + getPort(), "clientid", new MemoryPersistence());
        client.connect();
        client.publish("test", "hello".getBytes(), 1, false);

        Message msg = consumer.receive(100 * 5);
        assertNotNull(msg);

        client.disconnect();
        client.close();
    }

    @Test(timeout = 300000)
    public void testCleanSession() throws Exception {
        String topic = "test";
        final DefaultListener listener = new DefaultListener();

        // subscriber connects and creates durable sub
        LOG.info("Connecting durable subscriber...");
        MqttClient client = createClient(false, "receive", listener);
        // subscribe and wait for the retain message to arrive
        LOG.info("Subscribing durable subscriber...");
        client.subscribe(topic, 1);
        assertTrue(client.getPendingDeliveryTokens().length == 0);
        disconnect(client);
        LOG.info("Disconnected durable subscriber.");

        // Publish message with QoS 1
        MqttClient client2 = createClient(true, "publish", listener);

        LOG.info("Publish message with QoS 1...");
        String expectedResult = "QOS 1 message";
        client2.publish(topic, expectedResult.getBytes(), 1, false);
        waitForDelivery(client2);

        // Publish message with QoS 0
        LOG.info("Publish message with QoS 0...");
        expectedResult = "QOS 0 message";
        client2.publish(topic, expectedResult.getBytes(), 0, false);
        waitForDelivery(client2);

        // subscriber reconnects
        LOG.info("Reconnecting durable subscriber...");
        MqttClient client3 = createClient(false, "receive", listener);

        LOG.info("Subscribing durable subscriber...");
        client3.subscribe(topic, 1);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return listener.received == 2;
            }
        });
        assertEquals(2, listener.received);
        disconnect(client3);
        LOG.info("Disconnected durable subscriber.");

        // make sure we consumed everything
        listener.received = 0;

        LOG.info("Reconnecting durable subscriber...");
        MqttClient client4 = createClient(false, "receive", listener);

        LOG.info("Subscribing durable subscriber...");
        client4.subscribe(topic, 1);
        Thread.sleep(3 * 1000);
        assertEquals(0, listener.received);
    }

    protected MqttClient createClient(boolean cleanSession, String clientId, MqttCallback listener) throws Exception {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(cleanSession);
        final MqttClient client = new MqttClient("tcp://localhost:" + getPort(), clientId, new MemoryPersistence());
        client.setCallback(listener);
        client.connect(options);
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return client.isConnected();
            }
        });
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
        });
    }

    protected void waitForDelivery(final MqttClient client) throws Exception {
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return client.getPendingDeliveryTokens().length == 0;
            }
        });
        assertTrue(client.getPendingDeliveryTokens().length == 0);
    }

    static class DefaultListener implements MqttCallback {

        int received = 0;

        @Override
        public void connectionLost(Throwable cause) {

        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            LOG.info("Received: " + message);
            received++;
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {

        }
    }

}