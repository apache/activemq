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
package org.apache.activemq.transport.nio;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "javadoc" })
public class NIOSSLConcurrencyTest extends TestCase {

    BrokerService broker;
    Connection connection;

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    public static final int PRODUCER_COUNT = 10;
    public static final int CONSUMER_COUNT = 10;
    public static final int MESSAGE_COUNT = 10000;
    public static final int MESSAGE_SIZE = 4096;

    final ConsumerThread[] consumers = new ConsumerThread[CONSUMER_COUNT];
    final Session[] producerSessions = new Session[PRODUCER_COUNT];
    final Session[] consumerSessions = new Session[CONSUMER_COUNT];

    byte[] messageData;
    volatile boolean failed;

    @Override
    protected void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);

        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        TransportConnector connector = broker.addConnector("nio+ssl://localhost:0?transport.needClientAuth=true&transport.enabledCipherSuites=SSL_RSA_WITH_RC4_128_SHA,SSL_DH_anon_WITH_3DES_EDE_CBC_SHA");
        broker.start();
        broker.waitUntilStarted();

        failed = false;
        messageData = new byte[MESSAGE_SIZE];
        for (int i = 0; i < MESSAGE_SIZE;  i++)
        {
            messageData[i] = (byte) (i & 0xff);
        }

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("nio+ssl://localhost:" + connector.getConnectUri().getPort());
        connection = factory.createConnection();

        for (int i = 0; i < PRODUCER_COUNT; i++) {
            producerSessions[i] = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            consumerSessions[i] = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }

        connection.start();
    }

    @Override
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    public void testLoad() throws Exception {
        for (int i = 0; i < PRODUCER_COUNT; i++) {
            Queue dest = producerSessions[i].createQueue("TEST" + i);
            ProducerThread producer = new ProducerThread(producerSessions[i], dest);
            producer.setMessageCount(MESSAGE_COUNT);
            producer.start();
        }

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            Queue dest = consumerSessions[i].createQueue("TEST" + i);
            ConsumerThread consumer = new ConsumerThread(consumerSessions[i], dest);
            consumer.setMessageCount(MESSAGE_COUNT);
            consumer.start();
            consumers[i] = consumer;
        }

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return failed || getReceived() == PRODUCER_COUNT * MESSAGE_COUNT;
            }
        }, 120000);

        assertEquals(PRODUCER_COUNT * MESSAGE_COUNT, getReceived());

    }

    protected int getReceived() {
        int received = 0;
        for (ConsumerThread consumer : consumers) {
            received += consumer.getReceived();
        }
        return received;
    }

    private class ConsumerThread extends Thread {

        private final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

        int messageCount = 1000;
        int received = 0;
        Destination dest;
        Session sess;
        boolean breakOnNull = true;

        public ConsumerThread(Session sess, Destination dest) {
            this.dest = dest;
            this.sess = sess;
        }

        @Override
        public void run() {
          MessageConsumer consumer = null;

            try {
                consumer = sess.createConsumer(dest);
                while (received < messageCount) {
                    Message msg = consumer.receive(3000);
                    if (msg != null) {
                        LOG.info("Received test message: " + received++);
                    } else {
                        if (breakOnNull) {
                            break;
                        }
                    }
                }
            } catch (JMSException e) {
                e.printStackTrace();
                failed = true;
            } finally {
                if (consumer != null) {
                    try {
                        consumer.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public int getReceived() {
            return received;
        }

        public void setMessageCount(int messageCount) {
            this.messageCount = messageCount;
        }
    }

    private class ProducerThread extends Thread {

        private final Logger LOG = LoggerFactory.getLogger(ProducerThread.class);

        int messageCount = 1000;
        Destination dest;
        protected Session sess;
        int sleep = 0;
        int sentCount = 0;

        public ProducerThread(Session sess, Destination dest) {
            this.dest = dest;
            this.sess = sess;
        }

        @Override
        public void run() {
            MessageProducer producer = null;
            try {
                producer = sess.createProducer(dest);
                for (sentCount = 0; sentCount < messageCount; sentCount++) {
                    producer.send(createMessage(sentCount));
                    LOG.info("Sent 'test message: " + sentCount + "'");
                    if (sleep > 0) {
                        Thread.sleep(sleep);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                failed = true;
            } finally {
                if (producer != null) {
                    try {
                        producer.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        protected Message createMessage(int i) throws Exception {
            BytesMessage b = sess.createBytesMessage();
            b.writeBytes(messageData);
            return b;
        }

        public void setMessageCount(int messageCount) {
            this.messageCount = messageCount;
        }

    }
}
