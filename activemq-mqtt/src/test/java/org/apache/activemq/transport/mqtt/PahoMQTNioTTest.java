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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.TransportConnector;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PahoMQTNioTTest extends PahoMQTTTest {

    private static final Logger LOG = LoggerFactory.getLogger(PahoMQTNioTTest.class);

    @Override
    protected String getProtocolScheme() {
        return "mqtt+nio";
    }

    @Test(timeout=300000)
    public void testLotsOfClients() throws Exception {

        final int CLIENTS = Integer.getInteger("PahoMQTNioTTest.CLIENTS", 100);
        LOG.info("Using: "+CLIENTS+" clients");
        addMQTTConnector();
        TransportConnector openwireTransport = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(openwireTransport.getConnectUri()).createConnection();
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
        for( int i=0; i < CLIENTS; i++ ) {
            Thread.sleep(10);
            new Thread(null, null, "client:"+i) {
                @Override
                public void run() {
                    try {
                        MqttClient client = new MqttClient("tcp://localhost:" + mqttConnector.getConnectUri().getPort(), Thread.currentThread().getName(), new MemoryPersistence());
                        client.connect();
                        connectedDoneLatch.countDown();
                        sendBarrier.await();
                        for( int i=0; i < 10; i++) {
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
        assertNull("Async error: "+asyncError.get(),asyncError.get());
        sendBarrier.countDown();

        LOG.info("All clients connected... waiting to receive sent messages...");

        // We should eventually get all the messages.
        within(30, TimeUnit.SECONDS, new Task() {
            @Override
            public void run() throws Exception {
                assertTrue(receiveCounter.get() == CLIENTS*10);
            }
        });

        LOG.info("All messages received.");

        disconnectDoneLatch.await();
        assertNull("Async error: "+asyncError.get(),asyncError.get());

    }


}
