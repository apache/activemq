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
package org.apache.activemq.transport.amqp;

import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.spring.SpringSslContext;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Vector;

import static org.fusesource.hawtbuf.UTF8Buffer.utf8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(AmqpTestSupport.class);
    protected BrokerService brokerService;
    protected Vector<Throwable> exceptions = new Vector<Throwable>();
    protected int numberOfMessages;
    AutoFailTestSupport autoFailTestSupport = new AutoFailTestSupport() {};
    protected int port;
    protected int sslPort;


    public static void main(String[] args) throws Exception {
        final AmqpTestSupport s = new AmqpTestSupport();
        s.sslPort = 5671;
        s.port = 5672;
        s.startBroker();
        while(true) {
            Thread.sleep(100000);
        }
    }

    @Before
    public void setUp() throws Exception {
        autoFailTestSupport.startAutoFailThread();
        exceptions.clear();
        startBroker();
    }

    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);

        // Setup SSL context...
        final File classesDir = new File(AmqpProtocolConverter.class.getProtectionDomain().getCodeSource().getLocation().getFile());
        File keystore = new File(classesDir, "../../src/test/resources/keystore");
        final SpringSslContext sslContext = new SpringSslContext();
        sslContext.setKeyStore(keystore.getCanonicalPath());
        sslContext.setKeyStorePassword("password");
        sslContext.setTrustStore(keystore.getCanonicalPath());
        sslContext.setTrustStorePassword("password");
        sslContext.afterPropertiesSet();
        brokerService.setSslContext(sslContext);

        addAMQPConnector();
        brokerService.start();
        this.numberOfMessages = 2000;
    }

    protected void addAMQPConnector() throws Exception {
        TransportConnector connector =brokerService.addConnector("amqp+ssl://0.0.0.0:"+sslPort);
        sslPort = connector.getConnectUri().getPort();
        connector = brokerService.addConnector("amqp://0.0.0.0:"+port);
        port = connector.getConnectUri().getPort();
    }


    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService = null;
        }
        autoFailTestSupport.stopAutoFailThread();
    }



//    @Test
//    public void testSendAndReceiveAMQP() throws Exception {
//        addAMQPConnector(brokerService);
//        brokerService.start();
//        AMQP amqp = new AMQP();
//        final BlockingConnection subscribeConnection = amqp.blockingConnection();
//        subscribeConnection.connect();
//        Topic topic = new Topic("foo/bah",QoS.AT_MOST_ONCE);
//        Topic[] topics = {topic};
//        subscribeConnection.subscribe(topics);
//        final CountDownLatch latch = new CountDownLatch(numberOfMessages);
//
//        Thread thread = new Thread(new Runnable() {
//            public void run() {
//                for (int i = 0; i < numberOfMessages; i++){
//                    try {
//                        Message message = subscribeConnection.receive();
//                        message.ack();
//                        latch.countDown();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        break;
//                    }
//
//                }
//            }
//        });
//        thread.start();
//
//        BlockingConnection publisherConnection = amqp.blockingConnection();
//        publisherConnection.connect();
//        for (int i = 0; i < numberOfMessages; i++){
//            String payload = "Message " + i;
//            publisherConnection.publish(topic.name().toString(),payload.getBytes(),QoS.AT_LEAST_ONCE,false);
//        }
//
//        latch.await(10, TimeUnit.SECONDS);
//        assertEquals(0, latch.getCount());
//    }
//
//    @Test
//    public void testSendAndReceiveAtMostOnce() throws Exception {
//        addAMQPConnector(brokerService);
//        brokerService.start();
//        AMQP amqp = createAMQPConnection();
//        amqp.setKeepAlive(Short.MAX_VALUE);
//        BlockingConnection connection = amqp.blockingConnection();
//
//        connection.connect();
//
//
//        Topic[] topics = {new Topic(utf8("foo"), QoS.AT_MOST_ONCE)};
//        connection.subscribe(topics);
//        for (int i = 0; i < numberOfMessages; i++) {
//            String payload = "Test Message: " + i;
//            connection.publish("foo", payload.getBytes(), QoS.AT_MOST_ONCE, false);
//            Message message = connection.receive();
//            assertEquals(payload, new String(message.getPayload()));
//        }
//        connection.disconnect();
//    }
//
//    @Test
//    public void testSendAndReceiveAtLeastOnce() throws Exception {
//        addAMQPConnector(brokerService);
//        brokerService.start();
//        AMQP amqp = createAMQPConnection();
//        amqp.setKeepAlive(Short.MAX_VALUE);
//        BlockingConnection connection = amqp.blockingConnection();
//
//        connection.connect();
//
//        Topic[] topics = {new Topic(utf8("foo"), QoS.AT_LEAST_ONCE)};
//        connection.subscribe(topics);
//        for (int i = 0; i < numberOfMessages; i++) {
//            String payload = "Test Message: " + i;
//            connection.publish("foo", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
//            Message message = connection.receive();
//            message.ack();
//            assertEquals(payload, new String(message.getPayload()));
//        }
//        connection.disconnect();
//    }
//
//    @Test
//    public void testSendAndReceiveExactlyOnce() throws Exception {
//        addAMQPConnector(brokerService);
//        brokerService.start();
//        AMQP publisher = createAMQPConnection();
//        BlockingConnection pubConnection = publisher.blockingConnection();
//
//        pubConnection.connect();
//
//        AMQP subscriber = createAMQPConnection();
//        BlockingConnection subConnection = subscriber.blockingConnection();
//
//        subConnection.connect();
//
//        Topic[] topics = {new Topic(utf8("foo"), QoS.EXACTLY_ONCE)};
//        subConnection.subscribe(topics);
//        for (int i = 0; i < numberOfMessages; i++) {
//            String payload = "Test Message: " + i;
//            pubConnection.publish("foo", payload.getBytes(), QoS.EXACTLY_ONCE, false);
//            Message message = subConnection.receive();
//            message.ack();
//            assertEquals(payload, new String(message.getPayload()));
//        }
//        subConnection.disconnect();
//        pubConnection.disconnect();
//    }
//
//    @Test
//    public void testSendAndReceiveLargeMessages() throws Exception {
//        byte[] payload = new byte[1024 * 32];
//        for (int i = 0; i < payload.length; i++){
//            payload[i] = '2';
//        }
//        addAMQPConnector(brokerService);
//        brokerService.start();
//
//        AMQP publisher = createAMQPConnection();
//        BlockingConnection pubConnection = publisher.blockingConnection();
//
//        pubConnection.connect();
//
//        AMQP subscriber = createAMQPConnection();
//        BlockingConnection subConnection = subscriber.blockingConnection();
//
//        subConnection.connect();
//
//        Topic[] topics = {new Topic(utf8("foo"), QoS.AT_LEAST_ONCE)};
//        subConnection.subscribe(topics);
//        for (int i = 0; i < 10; i++) {
//            pubConnection.publish("foo", payload, QoS.AT_LEAST_ONCE, false);
//            Message message = subConnection.receive();
//            message.ack();
//            assertArrayEquals(payload, message.getPayload());
//        }
//        subConnection.disconnect();
//        pubConnection.disconnect();
//    }
//
//
//    @Test
//    public void testSendAMQPReceiveJMS() throws Exception {
//        addAMQPConnector(brokerService);
//        brokerService.addConnector(ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL);
//        brokerService.start();
//        AMQP amqp = createAMQPConnection();
//        BlockingConnection connection = amqp.blockingConnection();
//        final String DESTINATION_NAME = "foo.*";
//        connection.connect();
//
//        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory().createConnection();
//        activeMQConnection.start();
//        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//        javax.jms.Topic jmsTopic = s.createTopic(DESTINATION_NAME);
//        MessageConsumer consumer = s.createConsumer(jmsTopic);
//
//        for (int i = 0; i < numberOfMessages; i++) {
//            String payload = "Test Message: " + i;
//            connection.publish("foo/bah", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
//            ActiveMQMessage message = (ActiveMQMessage) consumer.receive();
//            ByteSequence bs = message.getContent();
//            assertEquals(payload, new String(bs.data, bs.offset, bs.length));
//        }
//
//
//        activeMQConnection.close();
//        connection.disconnect();
//    }
//
//    @Test
//    public void testSendJMSReceiveAMQP() throws Exception {
//        addAMQPConnector(brokerService);
//        brokerService.addConnector(ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL);
//        brokerService.start();
//        AMQP amqp = createAMQPConnection();
//        amqp.setKeepAlive(Short.MAX_VALUE);
//        BlockingConnection connection = amqp.blockingConnection();
//        connection.connect();
//
//        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory().createConnection();
//        activeMQConnection.start();
//        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//        javax.jms.Topic jmsTopic = s.createTopic("foo.far");
//        MessageProducer producer = s.createProducer(jmsTopic);
//
//        Topic[] topics = {new Topic(utf8("foo/+"), QoS.AT_MOST_ONCE)};
//        connection.subscribe(topics);
//        for (int i = 0; i < numberOfMessages; i++) {
//            String payload = "This is Test Message: " + i;
//            TextMessage sendMessage = s.createTextMessage(payload);
//            producer.send(sendMessage);
//            Message message = connection.receive();
//            message.ack();
//            assertEquals(payload, new String(message.getPayload()));
//        }
//        connection.disconnect();
//    }
//
//


}