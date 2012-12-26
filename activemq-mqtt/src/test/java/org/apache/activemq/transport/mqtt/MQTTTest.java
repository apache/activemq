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
import java.net.URI;
import java.net.URISyntaxException;
import java.security.ProtectionDomain;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.net.SocketFactory;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.Wait;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.codec.CONNECT;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.fusesource.hawtbuf.UTF8Buffer.utf8;
import static org.junit.Assert.*;


public class MQTTTest {

    public File basedir() throws IOException {
        ProtectionDomain protectionDomain = getClass().getProtectionDomain();
        return new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalFile();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(MQTTTest.class);
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
        this.numberOfMessages = 1000;
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
        MQTT mqtt = createMQTTConnection();
        final BlockingConnection subscribeConnection = mqtt.blockingConnection();
        subscribeConnection.connect();
        Topic topic = new Topic("foo/bah",QoS.AT_MOST_ONCE);
        Topic[] topics = {topic};
        subscribeConnection.subscribe(topics);
        final CountDownLatch latch = new CountDownLatch(numberOfMessages);

        Thread thread = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < numberOfMessages; i++){
                    try {
                        Message message = subscribeConnection.receive(5, TimeUnit.SECONDS);
                        assertNotNull("Should get a message", message);
                        message.ack();
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }

                }
            }
        });
        thread.start();

        BlockingConnection publisherConnection = mqtt.blockingConnection();
        publisherConnection.connect();
        for (int i = 0; i < numberOfMessages; i++){
            String payload = "Message " + i;
            publisherConnection.publish(topic.name().toString(),payload.getBytes(),QoS.AT_LEAST_ONCE,false);
        }

        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
        subscribeConnection.disconnect();
        publisherConnection.disconnect();
    }

    @Test
    public void testSendAndReceiveAtMostOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive(Short.MAX_VALUE);
        BlockingConnection connection = mqtt.blockingConnection();

        connection.connect();

        Topic[] topics = {new Topic(utf8("foo"), QoS.AT_MOST_ONCE)};
        connection.subscribe(topics);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            connection.publish("foo", payload.getBytes(), QoS.AT_MOST_ONCE, false);
            Message message = connection.receive(5, TimeUnit.SECONDS);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message.getPayload()));
        }
        connection.disconnect();
    }

    @Test
    public void testSendAndReceiveAtLeastOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive(Short.MAX_VALUE);
        BlockingConnection connection = mqtt.blockingConnection();

        connection.connect();

        Topic[] topics = {new Topic(utf8("foo"), QoS.AT_LEAST_ONCE)};
        connection.subscribe(topics);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            connection.publish("foo", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
            Message message = connection.receive(5, TimeUnit.SECONDS);
            assertNotNull("Should get a message", message);
            message.ack();
            assertEquals(payload, new String(message.getPayload()));
        }
        connection.disconnect();
    }

    @Test
    public void testSendAndReceiveExactlyOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();
        MQTT publisher = createMQTTConnection();
        BlockingConnection pubConnection = publisher.blockingConnection();

        pubConnection.connect();

        MQTT subscriber = createMQTTConnection();
        BlockingConnection subConnection = subscriber.blockingConnection();

        subConnection.connect();

        Topic[] topics = {new Topic(utf8("foo"), QoS.EXACTLY_ONCE)};
        subConnection.subscribe(topics);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            pubConnection.publish("foo", payload.getBytes(), QoS.EXACTLY_ONCE, false);
            Message message = subConnection.receive(5, TimeUnit.SECONDS);
            assertNotNull("Should get a message", message);
            LOG.debug(payload);
            message.ack();
            assertEquals(payload, new String(message.getPayload()));
        }
        subConnection.disconnect();
        pubConnection.disconnect();
    }

    @Test
    public void testSendAndReceiveLargeMessages() throws Exception {
        byte[] payload = new byte[1024 * 32];
        for (int i = 0; i < payload.length; i++){
            payload[i] = '2';
        }
        addMQTTConnector();
        brokerService.start();

        MQTT publisher = createMQTTConnection();
        BlockingConnection pubConnection = publisher.blockingConnection();

        pubConnection.connect();

        MQTT subscriber = createMQTTConnection();
        BlockingConnection subConnection = subscriber.blockingConnection();

        subConnection.connect();

        Topic[] topics = {new Topic(utf8("foo"), QoS.AT_LEAST_ONCE)};
        subConnection.subscribe(topics);
        for (int i = 0; i < 10; i++) {
            pubConnection.publish("foo", payload, QoS.AT_LEAST_ONCE, false);
            Message message = subConnection.receive(5, TimeUnit.SECONDS);
            assertNotNull("Should get a message", message);
            message.ack();
            assertArrayEquals(payload, message.getPayload());
        }
        subConnection.disconnect();
        pubConnection.disconnect();
    }


    @Test
    public void testSendMQTTReceiveJMS() throws Exception {
        addMQTTConnector();
        TransportConnector openwireTransport = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        BlockingConnection connection = mqtt.blockingConnection();
        final String DESTINATION_NAME = "foo.*";
        connection.connect();

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(openwireTransport.getConnectUri()).createConnection();
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic jmsTopic = s.createTopic(DESTINATION_NAME);
        MessageConsumer consumer = s.createConsumer(jmsTopic);

        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            connection.publish("foo/bah", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
            ActiveMQMessage message = (ActiveMQMessage) consumer.receive(5000);
            assertNotNull("Should get a message", message);
            ByteSequence bs = message.getContent();
            assertEquals(payload, new String(bs.data, bs.offset, bs.length));
        }


        activeMQConnection.close();
        connection.disconnect();
    }

    @Test
    public void testSendJMSReceiveMQTT() throws Exception {
        addMQTTConnector();
        TransportConnector openwireTransport = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive(Short.MAX_VALUE);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(openwireTransport.getConnectUri()).createConnection();
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic jmsTopic = s.createTopic("foo.far");
        MessageProducer producer = s.createProducer(jmsTopic);

        Topic[] topics = {new Topic(utf8("foo/+"), QoS.AT_MOST_ONCE)};
        connection.subscribe(topics);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "This is Test Message: " + i;
            TextMessage sendMessage = s.createTextMessage(payload);
            producer.send(sendMessage);
            Message message = connection.receive(5, TimeUnit.SECONDS);
            assertNotNull("Should get a message", message);
            message.ack();
            assertEquals(payload, new String(message.getPayload()));
        }
        connection.disconnect();
    }

    public void testInactivityTimeoutDisconnectsClient() throws Exception{

        addMQTTConnector();
        brokerService.start();

        // manually need to create the client so we don't send keep alive (PINGREQ) frames to keep the conn
        // from timing out
        Transport clientTransport = createManualMQTTClient();
        clientTransport.start();
        CONNECT connectFrame = new CONNECT().clientId(new UTF8Buffer("testClient")).keepAlive((short)2);
        clientTransport.oneway(connectFrame.encode());

        // wait for broker to register the MQTT connection
        assertTrue("MQTT Connection should be registered.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return mqttConnector.getConnections().size() > 0;
            }
        }));

        // wait for broker to time out the MQTT connection due to inactivity
        assertTrue("MQTT Connection should be timed out.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return mqttConnector.getConnections().size() == 0;
            }
        }));

        assertTrue("Should have seen client transport exception", exceptions.size() > 0);

        clientTransport.stop();
    }

    private Transport createManualMQTTClient() throws IOException, URISyntaxException {
        Transport clientTransport = new TcpTransport(new MQTTWireFormat(), SocketFactory.getDefault(),
                new URI("tcp://localhost:"+mqttConnector.getConnectUri().getPort()), null);
        clientTransport.setTransportListener(new TransportListener() {
            @Override
            public void onCommand(Object command) {
            }

            @Override
            public void onException(IOException error) {
                exceptions.add(error);
            }

            @Override
            public void transportInterupted() {
            }

            @Override
            public void transportResumed() {
            }
        });
        return clientTransport;
    }

    @Test
    public void testPingKeepsInactivityMonitorAlive() throws Exception {
        addMQTTConnector();
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive((short)2);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));

        connection.disconnect();
    }

    @Test
    public void testTurnOffInactivityMonitor()throws Exception{
        addMQTTConnector("?transport.useInactivityMonitor=false");
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive((short)2);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));

        connection.disconnect();
    }


    @Test
    public void testDefaultKeepAliveWhenClientSpecifiesZero() throws Exception {
        // default keep alive in milliseconds
        addMQTTConnector("?transport.defaultKeepAlive=2000");
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive((short)0);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));
    }

    TransportConnector mqttConnector;

    protected String getProtocolScheme() {
        return "mqtt";
    }

    protected void addMQTTConnector() throws Exception {
        addMQTTConnector("");
    }

    protected void addMQTTConnector(String config) throws Exception {
        mqttConnector= brokerService.addConnector(getProtocolScheme()+"://localhost:0" + config);
    }

    protected MQTT createMQTTConnection() throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setHost("localhost", mqttConnector.getConnectUri().getPort());
        // shut off connect retry
        mqtt.setConnectAttemptsMax(0);
        mqtt.setReconnectAttemptsMax(0);
        return mqtt;
    }
}