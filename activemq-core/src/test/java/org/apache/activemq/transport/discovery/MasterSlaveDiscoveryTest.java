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
package org.apache.activemq.transport.discovery;

import java.io.File;
import java.net.URI;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.IOHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MasterSlaveDiscoveryTest extends TestCase {
    private static final Log LOG = LogFactory.getLog(MasterSlaveDiscoveryTest.class);

    private static final int NUMBER = 10;

    private static final String BROKER_A_DIRECTORY = "target/activemq-data/kahadbA";

    private static final String BROKER_A1_NAME = "BROKERA1";
    private static final String BROKER_A1_BIND_ADDRESS = "tcp://127.0.0.1:61616";

    private static final String BROKER_A2_NAME = "BROKERA2";
    private static final String BROKER_A2_BIND_ADDRESS = "tcp://127.0.0.1:61617";

    private static final String BROKER_B_DIRECTORY = "target/activemq-data/kahadbB";

    private static final String BROKER_B1_NAME = "BROKERB1";
    private static final String BROKER_B1_BIND_ADDRESS = "tcp://127.0.0.1:61626";

    private static final String BROKER_B2_NAME = "BROKERB2";
    private static final String BROKER_B2_BIND_ADDRESS = "tcp://127.0.0.1:61627";

    private BrokerService brokerA1;
    private BrokerService brokerA2;
    private BrokerService brokerB1;
    private BrokerService brokerB2;

    private String clientUrlA;
    private String clientUrlB;

    public void testNetworkFailback() throws Exception {
        final long timeout = 5000; // 5 seconds
        final String queueName = getClass().getName();

        ActiveMQConnectionFactory factoryA = new ActiveMQConnectionFactory(clientUrlA);
        ActiveMQConnection connectionA = (ActiveMQConnection) factoryA.createConnection();
        connectionA.start();
        Session sessionA = connectionA.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queueA = sessionA.createQueue(queueName);
        MessageProducer producerA = sessionA.createProducer(queueA);

        ActiveMQConnectionFactory factoryB = new ActiveMQConnectionFactory(clientUrlB);
        ActiveMQConnection connectionB = (ActiveMQConnection) factoryB.createConnection();
        connectionB.start();
        Session sessionB = connectionB.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queueB = sessionB.createQueue(queueName);
        MessageConsumer consumerB = sessionA.createConsumer(queueB);

        // Test initial configuration is working
        String msgStr = queueName + "-" + System.currentTimeMillis();
        Message msgSent = sessionA.createTextMessage(msgStr);
        producerA.send(msgSent);

        Message msgReceived = null;

        try {
            msgReceived = consumerB.receive(timeout);
        } catch (JMSException e) {
            fail("Message Timeout");
        }

        assertTrue(msgReceived instanceof TextMessage);
        assertEquals(((TextMessage) msgReceived).getText(), msgStr);

        // Test Failover
        assertTrue(brokerB2.isSlave());

        brokerB1.stop();

        brokerB2.waitUntilStarted();
        assertFalse(brokerB2.isSlave());

        msgStr = queueName + "-" + System.currentTimeMillis();
        msgSent = sessionA.createTextMessage(msgStr);
        producerA.send(msgSent);

        try {
            msgReceived = consumerB.receive(timeout);
        } catch (JMSException e) {
            fail("Message Timeout");
        }

        assertTrue(msgReceived instanceof TextMessage);
        assertEquals(((TextMessage)msgReceived).getText(), msgStr);

        // Test Failback
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    brokerB1.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Failed to start broker");
                }
            }
        }, "BrokerB1 Restarting").start();

        brokerB1.waitUntilStarted();
        assertTrue(brokerB1.isSlave());

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    brokerB2.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Failed to stop broker");
                }
            }
        }, "BrokerB2 Stopping").start();

        brokerB2.waitUntilStopped();
        brokerB1.waitUntilStarted();

        msgStr = queueName + "-" + System.currentTimeMillis();
        msgSent = sessionA.createTextMessage(msgStr);
        producerA.send(msgSent);

        try {
            msgReceived = consumerB.receive(timeout);
        } catch (JMSException e) {
            fail("Message Timeout");
        }

        assertTrue(msgReceived instanceof TextMessage);
        assertEquals(((TextMessage)msgReceived).getText(), msgStr);

        connectionA.close();
        connectionB.close();
    }

    @Override
    protected void setUp() throws Exception {
        brokerA1 = createBrokerA1();
        brokerA1.waitUntilStarted(); // wait to ensure A1 is master
        brokerA2 = createBrokerA2();

        String connectStringA1 = brokerA1.getTransportConnectors().get(0).getPublishableConnectString();
        String connectStringA2 = brokerA2.getTransportConnectors().get(0).getPublishableConnectString();

        clientUrlA = "failover:(" + connectStringA1 + "," + connectStringA2 + ")?randomize=false&updateURIsSupported=false";

        brokerB1 = createBrokerB1();
        brokerB1.waitUntilStarted(); // wait to ensure B1 is master
        brokerB2 = createBrokerB2();

        String connectStringB1 = brokerB1.getTransportConnectors().get(0).getPublishableConnectString();
        String connectStringB2 = brokerB2.getTransportConnectors().get(0).getPublishableConnectString();

        clientUrlB = "failover:(" + connectStringB1 + "," + connectStringB2 + ")?randomize=false&updateURIsSupported=false";
    }

    @Override
    protected void tearDown() throws Exception {
        if (brokerB2 != null) {
            brokerB2.stop();
            brokerB2 = null;
        }
        if (brokerB1 != null) {
            brokerB1.stop();
            brokerB1 = null;
        }
        if (brokerA1 != null) {
            brokerA1.stop();
            brokerA1 = null;
        }
        if (brokerA2 != null) {
            brokerA2.stop();
            brokerA2 = null;
        }
    }

    protected BrokerService createBrokerA1() throws Exception {
        final BrokerService answer = new BrokerService();
        answer.setUseJmx(false);
        answer.setBrokerName(BROKER_A1_NAME);

        File directory = new File(BROKER_A_DIRECTORY);
        IOHelper.deleteChildren(directory);

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(directory);
        answer.setPersistent(true);
        answer.setPersistenceAdapter(kaha);

        NetworkConnector network = answer.addNetworkConnector("masterslave:(" + BROKER_B1_BIND_ADDRESS + "," + BROKER_B2_BIND_ADDRESS + ")?useExponentialBackOff=false&discovered.randomize=true&discovered.maxReconnectAttempts=0");
        network.setDuplex(false);

        // lazy create
        TransportConnector transportConnector = new TransportConnector();
        transportConnector.setUri(new URI(BROKER_A1_BIND_ADDRESS));
        answer.addConnector(transportConnector);
        answer.setUseShutdownHook(false);

        answer.start();

        return answer;
    }

    protected BrokerService createBrokerA2() throws Exception {
        final BrokerService answer = new BrokerService();
        answer.setUseJmx(false);
        answer.setBrokerName(BROKER_A2_NAME);

        File directory = new File(BROKER_A_DIRECTORY);

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(directory);
        answer.setPersistent(true);
        answer.setPersistenceAdapter(kaha);

        // it is possible to *replace* the default implied failover options  via..
        NetworkConnector network = answer.addNetworkConnector("masterslave:(" + BROKER_B1_BIND_ADDRESS + "," + BROKER_B2_BIND_ADDRESS + ")");
        network.setDuplex(false);

        // lazy create
        TransportConnector transportConnector = new TransportConnector();
        transportConnector.setUri(new URI(BROKER_A2_BIND_ADDRESS));
        answer.addConnector(transportConnector);
        answer.setUseShutdownHook(false);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    answer.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Failed to start broker");
                }
            }
        }, "BrokerA2 Starting").start();

        return answer;
    }

    protected BrokerService createBrokerB1() throws Exception {
        final BrokerService answer = new BrokerService();
        answer.setUseJmx(false);
        answer.setBrokerName(BROKER_B1_NAME);

        File directory = new File(BROKER_B_DIRECTORY);
        IOHelper.deleteChildren(directory);

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(directory);
        answer.setPersistent(true);
        answer.setPersistenceAdapter(kaha);

        NetworkConnector network = answer.addNetworkConnector("masterslave:(" + BROKER_A1_BIND_ADDRESS + "," + BROKER_A2_BIND_ADDRESS + ")");
        network.setDuplex(false);

        // lazy create
        TransportConnector transportConnector = new TransportConnector();
        transportConnector.setUri(new URI(BROKER_B1_BIND_ADDRESS));
        answer.addConnector(transportConnector);
        answer.setUseShutdownHook(false);

        answer.start();

        return answer;
    }

    protected BrokerService createBrokerB2() throws Exception {
        final BrokerService answer = new BrokerService();
        answer.setUseJmx(false);
        answer.setBrokerName(BROKER_B2_NAME);

        File directory = new File(BROKER_B_DIRECTORY);

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(directory);
        answer.setPersistent(true);
        answer.setPersistenceAdapter(kaha);

        NetworkConnector network = answer.addNetworkConnector("masterslave:(" + BROKER_A1_BIND_ADDRESS + "," + BROKER_A2_BIND_ADDRESS + ")");
        network.setDuplex(false);

        // lazy create
        TransportConnector transportConnector = new TransportConnector();
        transportConnector.setUri(new URI(BROKER_B2_BIND_ADDRESS));
        answer.addConnector(transportConnector);
        answer.setUseShutdownHook(false);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    answer.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Failed to start broker");
                }
            }
        }, "BrokerB2 Starting").start();

        return answer;
    }
}
