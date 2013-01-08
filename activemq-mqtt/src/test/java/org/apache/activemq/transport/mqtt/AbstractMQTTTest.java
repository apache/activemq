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

import java.io.File;
import java.io.IOException;
import java.security.ProtectionDomain;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.ByteSequence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


public abstract class AbstractMQTTTest {
    protected TransportConnector mqttConnector;

    public static final int AT_MOST_ONCE =0;
    public static final int AT_LEAST_ONCE = 1;
    public static final int EXACTLY_ONCE =2;

    public File basedir() throws IOException {
        ProtectionDomain protectionDomain = getClass().getProtectionDomain();
        return new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalFile();
    }

    protected BrokerService brokerService;
    protected LinkedList<Throwable> exceptions = new LinkedList<Throwable>();
    protected int numberOfMessages;
    AutoFailTestSupport autoFailTestSupport = new AutoFailTestSupport() {};

    @Before
    public void startBroker() throws Exception {
        autoFailTestSupport.startAutoFailThread();
        exceptions.clear();
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setUseJmx(false);
        this.numberOfMessages = 3000;
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
        autoFailTestSupport.stopAutoFailThread();
    }

    @Test
    public void testSendAndReceiveMQTT() throws Exception {
        addMQTTConnector();
        brokerService.start();
        final MQTTClientProvider subscriptionProvider = getMQTTClientProvider();
        initializeConnection(subscriptionProvider);


        subscriptionProvider.subscribe("foo/bah",AT_MOST_ONCE);

        final CountDownLatch latch = new CountDownLatch(numberOfMessages);

        Thread thread = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < numberOfMessages; i++){
                    try {
                        byte[] payload = subscriptionProvider.receive(10000);
                        assertNotNull("Should get a message", payload);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }

                }
            }
        });
        thread.start();

        final MQTTClientProvider publishProvider = getMQTTClientProvider();
        initializeConnection(publishProvider);

        for (int i = 0; i < numberOfMessages; i++){
            String payload = "Message " + i;
            publishProvider.publish("foo/bah",payload.getBytes(),AT_LEAST_ONCE);
        }

        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
        subscriptionProvider.disconnect();
        publishProvider.disconnect();
    }

    @Test
    public void testSendAtMostOnceReceiveExactlyOnce() throws Exception {
        /**
         * Although subscribing with EXACTLY ONCE, the message gets published
         * with AT_MOST_ONCE - in MQTT the QoS is always determined by the message
         * as published - not the wish of the subscriber
         */
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo",EXACTLY_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_MOST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test
    public void testSendAtLeastOnceReceiveExactlyOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo",EXACTLY_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_LEAST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test
    public void testSendAtLeastOnceReceiveAtMostOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo",AT_MOST_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_LEAST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }


    @Test
    public void testSendAndReceiveAtMostOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo",AT_MOST_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_MOST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test
    public void testSendAndReceiveAtLeastOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo",AT_LEAST_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_LEAST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test
    public void testSendAndReceiveExactlyOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();
        final MQTTClientProvider publisher = getMQTTClientProvider();
        initializeConnection(publisher);

        final MQTTClientProvider subscriber = getMQTTClientProvider();
        initializeConnection(subscriber);


            subscriber.subscribe("foo",EXACTLY_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            publisher.publish("foo", payload.getBytes(), EXACTLY_ONCE);
            byte[] message = subscriber.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        subscriber.disconnect();
        publisher.disconnect();
    }

    @Test
    public void testSendAndReceiveLargeMessages() throws Exception {
        byte[] payload = new byte[1024 * 32];
        for (int i = 0; i < payload.length; i++){
            payload[i] = '2';
        }
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider publisher = getMQTTClientProvider();
        initializeConnection(publisher);

        final MQTTClientProvider subscriber = getMQTTClientProvider();
        initializeConnection(subscriber);

        subscriber.subscribe("foo",AT_LEAST_ONCE);
        for (int i = 0; i < 10; i++) {
            publisher.publish("foo", payload, AT_LEAST_ONCE);
            byte[] message = subscriber.receive(5000);
            assertNotNull("Should get a message", message);

            assertArrayEquals(payload, message);
        }
        subscriber.disconnect();
        publisher.disconnect();
    }


    @Test
    public void testSendMQTTReceiveJMS() throws Exception {
        addMQTTConnector();
        TransportConnector openwireTransport = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        final String DESTINATION_NAME = "foo.*";


        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(openwireTransport.getConnectUri()).createConnection();
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic jmsTopic = s.createTopic(DESTINATION_NAME);
        MessageConsumer consumer = s.createConsumer(jmsTopic);

        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo/bah", payload.getBytes(), AT_LEAST_ONCE);
            ActiveMQMessage message = (ActiveMQMessage) consumer.receive(5000);
            assertNotNull("Should get a message", message);
            ByteSequence bs = message.getContent();
            assertEquals(payload, new String(bs.data, bs.offset, bs.length));
        }


        activeMQConnection.close();
        provider.disconnect();
    }

    @Test
    public void testSendJMSReceiveMQTT() throws Exception {
        addMQTTConnector();
        TransportConnector openwireTransport = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(openwireTransport.getConnectUri()).createConnection();
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic jmsTopic = s.createTopic("foo.far");
        MessageProducer producer = s.createProducer(jmsTopic);

        provider.subscribe("foo/+",AT_MOST_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "This is Test Message: " + i;
            TextMessage sendMessage = s.createTextMessage(payload);
            producer.send(sendMessage);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);

            assertEquals(payload, new String(message));
        }
        provider.disconnect();
        activeMQConnection.close();
    }

    protected String getProtocolScheme() {
        return "mqtt";
    }

    protected void addMQTTConnector() throws Exception {
        addMQTTConnector("");
    }

    protected void addMQTTConnector(String config) throws Exception {
        mqttConnector= brokerService.addConnector(getProtocolScheme()+"://localhost:0" + config);
    }

    protected void initializeConnection(MQTTClientProvider provider) throws Exception {
        provider.connect("tcp://localhost:"+mqttConnector.getConnectUri().getPort());
    }

    protected abstract MQTTClientProvider getMQTTClientProvider();
}