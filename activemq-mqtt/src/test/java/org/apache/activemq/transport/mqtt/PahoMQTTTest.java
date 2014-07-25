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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class PahoMQTTTest extends MQTTTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PahoMQTTTest.class);

    @Parameters(name= "{index}: scheme({0})")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"mqtt", false},
                {"mqtt+nio", false}
            });
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
}