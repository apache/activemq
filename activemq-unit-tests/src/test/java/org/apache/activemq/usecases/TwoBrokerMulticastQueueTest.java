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
package org.apache.activemq.usecases;

import java.net.URI;
import java.util.Arrays;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.xbean.XBeanBrokerFactory;

public class TwoBrokerMulticastQueueTest extends CombinationTestSupport {

    public static final int MESSAGE_COUNT = 100;
    public static final int BROKER_COUNT = 2;
    public static final int CONSUMER_COUNT = 20;

    public String sendUri;
    public String recvUri;
    private BrokerService[] brokers;
    private String groupId;

    public static Test suite() {
        return suite(TwoBrokerMulticastQueueTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void setUp() throws Exception {
    	groupId = getClass().getName()+"-"+System.currentTimeMillis();
    	System.setProperty("groupId", groupId);
        super.setAutoFail(true);
        super.setUp();
    }

    public void tearDown() throws Exception {
        if (brokers != null) {
            for (int i = 0; i < BROKER_COUNT; i++) {
                if (brokers[i] != null) {
                    brokers[i].stop();
                }
            }
            super.tearDown();
        }
    }

    private void doSendReceiveTest() throws Exception {
        Destination dest = new ActiveMQQueue("TEST.FOO");

        ConnectionFactory sendFactory = createConnectionFactory(sendUri);

        Connection conn = createConnection(sendFactory);
        sendMessages(conn, dest, MESSAGE_COUNT);

        Thread.sleep(500);

        ConnectionFactory recvFactory = createConnectionFactory(recvUri);
        assertEquals(MESSAGE_COUNT, receiveMessages(createConnection(recvFactory), dest, 0));
    }

    private void doMultipleConsumersConnectTest() throws Exception {
        Destination dest = new ActiveMQQueue("TEST.FOO");

        ConnectionFactory sendFactory = createConnectionFactory(sendUri);

        Connection conn = createConnection(sendFactory);
        sendMessages(conn, dest, MESSAGE_COUNT);

        Thread.sleep(500);

        ConnectionFactory recvFactory = createConnectionFactory(recvUri);
        assertEquals(MESSAGE_COUNT, receiveMessages(createConnection(recvFactory), dest, 0));

        for (int i = 0; i < (CONSUMER_COUNT - 1); i++) {
            assertEquals(0, receiveMessages(createConnection(recvFactory), dest, 200));
        }
    }

    public void initCombosForTestSendReceive() {
        addCombinationValues("sendUri", new Object[] {"tcp://localhost:61616", "tcp://localhost:61617"});
        addCombinationValues("recvUri", new Object[] {"tcp://localhost:61616", "tcp://localhost:61617"});
    }

    public void testSendReceive() throws Exception {
        createMulticastBrokerNetwork();
        doSendReceiveTest();
    }

    public void initCombosForTestMultipleConsumersConnect() {
        addCombinationValues("sendUri", new Object[] {"tcp://localhost:61616", "tcp://localhost:61617"});
        addCombinationValues("recvUri", new Object[] {"tcp://localhost:61616", "tcp://localhost:61617"});
    }

    public void testMultipleConsumersConnect() throws Exception {
        createMulticastBrokerNetwork();
        doMultipleConsumersConnectTest();
    }

    public void testSendReceiveUsingFailover() throws Exception {
        sendUri = "failover:(tcp://localhost:61616,tcp://localhost:61617)";
        recvUri = "failover:(tcp://localhost:61616,tcp://localhost:61617)";
        createMulticastBrokerNetwork();
        doSendReceiveTest();
    }

    public void testMultipleConsumersConnectUsingFailover() throws Exception {
        sendUri = "failover:(tcp://localhost:61616,tcp://localhost:61617)";
        recvUri = "failover:(tcp://localhost:61616,tcp://localhost:61617)";
        createMulticastBrokerNetwork();
        doMultipleConsumersConnectTest();
    }

    public void testSendReceiveUsingDiscovery() throws Exception {
        sendUri = "discovery:multicast://default?group="+groupId;
        recvUri = "discovery:multicast://default?group="+groupId;
        createMulticastBrokerNetwork();
        doSendReceiveTest();
    }

    public void testMultipleConsumersConnectUsingDiscovery() throws Exception {
        sendUri = "discovery:multicast://default?group="+groupId;
        recvUri = "discovery:multicast://default?group="+groupId;
        createMulticastBrokerNetwork();
        doMultipleConsumersConnectTest();
    }

    public void testSendReceiveUsingAutoAssignFailover() throws Exception {
        sendUri = "failover:(discovery:multicast:default?group=//"+groupId+")";
        recvUri = "failover:(discovery:multicast:default?group=//"+groupId+")";
        createAutoAssignMulticastBrokerNetwork();
        doSendReceiveTest();
    }

    public void testMultipleConsumersConnectUsingAutoAssignFailover() throws Exception {
        sendUri = "failover:(discovery:multicast:default?group=//"+groupId+")";
        recvUri = "failover:(discovery:multicast:default?group=//"+groupId+")";
        createAutoAssignMulticastBrokerNetwork();
        doMultipleConsumersConnectTest();
    }

    public void testSendReceiveUsingAutoAssignDiscovery() throws Exception {
        sendUri = "discovery:multicast://default?group="+groupId;
        recvUri = "discovery:multicast://default?group="+groupId;
        createAutoAssignMulticastBrokerNetwork();
        doSendReceiveTest();
    }

    public void testMultipleConsumersConnectUsingAutoAssignDiscovery() throws Exception {
        sendUri = "discovery:multicast://default?group="+groupId;
        recvUri = "discovery:multicast://default?group="+groupId;
        createAutoAssignMulticastBrokerNetwork();
        doMultipleConsumersConnectTest();
    }

    protected void createMulticastBrokerNetwork() throws Exception {
        brokers = new BrokerService[BROKER_COUNT];
        for (int i = 0; i < BROKER_COUNT; i++) {
            brokers[i] = createBroker("org/apache/activemq/usecases/multicast-broker-" + (i + 1) + ".xml");
            brokers[i].start();
        }

        // Let the brokers discover each other first
        Thread.sleep(1000);
    }

    protected void createAutoAssignMulticastBrokerNetwork() throws Exception {
        brokers = new BrokerService[BROKER_COUNT];
        for (int i = 0; i < BROKER_COUNT; i++) {
            brokers[i] = createBroker("org/apache/activemq/usecases/multicast-broker-auto.xml");
            brokers[i].start();
        }

        // Let the brokers discover each other first
        Thread.sleep(1000);
    }

    protected BrokerService createBroker(String uri) throws Exception {
        return (new XBeanBrokerFactory()).createBroker(new URI(uri));
    }

    protected ConnectionFactory createConnectionFactory(String uri) {
        return new ActiveMQConnectionFactory(uri);
    }

    protected Connection createConnection(ConnectionFactory factory) throws JMSException {
        Connection conn = factory.createConnection();
        return conn;
    }

    protected int receiveMessages(Connection conn, Destination dest, int waitTime) throws JMSException, InterruptedException {
        conn.start();
        MessageIdList list = new MessageIdList();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = sess.createConsumer(dest);
        consumer.setMessageListener(list);

        if (waitTime > 0) {
            Thread.sleep(waitTime);
        } else {
            list.waitForMessagesToArrive(MESSAGE_COUNT);
        }

        conn.close();

        return list.getMessageCount();
    }

    protected void sendMessages(Connection conn, Destination dest, int count) throws JMSException {
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer prod = sess.createProducer(dest);

        for (int i = 0; i < count; i++) {
            prod.send(createTextMessage(sess, "Message " + i, 1024));
        }

        conn.close();
    }

    protected TextMessage createTextMessage(Session session, String initText, int messageSize) throws JMSException {
        TextMessage msg = session.createTextMessage();

        // Pad message text
        if (initText.length() < messageSize) {
            char[] data = new char[messageSize - initText.length()];
            Arrays.fill(data, '*');
            String str = new String(data);
            msg.setText(initText + str);

            // Do not pad message text
        } else {
            msg.setText(initText);
        }

        return msg;
    }

}
