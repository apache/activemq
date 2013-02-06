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
package org.apache.activemq.network.jms;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TopicBridgeStandaloneReconnectTest {

    private SimpleJmsTopicConnector jmsTopicConnector;

    private BrokerService localBroker;
    private BrokerService foreignBroker;

    private ActiveMQConnectionFactory localConnectionFactory;
    private ActiveMQConnectionFactory foreignConnectionFactory;

    private Destination outbound;
    private Destination inbound;

    private final ArrayList<Connection> connections = new ArrayList<Connection>();

    @Test
    public void testSendAndReceiveOverConnectedBridges() throws Exception {

        startLocalBroker();
        startForeignBroker();

        jmsTopicConnector.start();

        final MessageConsumer local = createConsumerForLocalBroker();
        final MessageConsumer foreign = createConsumerForForeignBroker();

        sendMessageToForeignBroker("to.foreign.broker");
        sendMessageToLocalBroker("to.local.broker");

        assertTrue("Should have received a Message.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Message message = local.receive(100);
                if (message != null && ((TextMessage) message).getText().equals("to.local.broker")) {
                    return true;
                }
                return false;
            }
        }));

        assertTrue("Should have received a Message.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Message message = foreign.receive(100);
                if (message != null && ((TextMessage) message).getText().equals("to.foreign.broker")) {
                    return true;
                }
                return false;
            }
        }));
    }

    @Test
    public void testSendAndReceiveOverBridgeWhenStartedBeforeBrokers() throws Exception {

        jmsTopicConnector.start();

        startLocalBroker();
        startForeignBroker();

        assertTrue("Should have Connected.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return jmsTopicConnector.isConnected();
            }
        }));

        final MessageConsumer local = createConsumerForLocalBroker();
        final MessageConsumer foreign = createConsumerForForeignBroker();

        sendMessageToForeignBroker("to.foreign.broker");
        sendMessageToLocalBroker("to.local.broker");

        assertTrue("Should have received a Message.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Message message = local.receive(100);
                if (message != null && ((TextMessage) message).getText().equals("to.local.broker")) {
                    return true;
                }
                return false;
            }
        }));

        assertTrue("Should have received a Message.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Message message = foreign.receive(100);
                if (message != null && ((TextMessage) message).getText().equals("to.foreign.broker")) {
                    return true;
                }
                return false;
            }
        }));
    }

    @Test
    public void testSendAndReceiveOverBridgeWithRestart() throws Exception {

        startLocalBroker();
        startForeignBroker();

        jmsTopicConnector.start();

        assertTrue("Should have Connected.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return jmsTopicConnector.isConnected();
            }
        }));

        stopLocalBroker();
        stopForeignBroker();

        assertTrue("Should have detected connection drop.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return !jmsTopicConnector.isConnected();
            }
        }));

        startLocalBroker();
        startForeignBroker();

        assertTrue("Should have Re-Connected.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return jmsTopicConnector.isConnected();
            }
        }));

        final MessageConsumer local = createConsumerForLocalBroker();
        final MessageConsumer foreign = createConsumerForForeignBroker();

        sendMessageToForeignBroker("to.foreign.broker");
        sendMessageToLocalBroker("to.local.broker");

        assertTrue("Should have received a Message.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Message message = local.receive(100);
                if (message != null && ((TextMessage) message).getText().equals("to.local.broker")) {
                    return true;
                }
                return false;
            }
        }));

        assertTrue("Should have received a Message.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Message message = foreign.receive(100);
                if (message != null && ((TextMessage) message).getText().equals("to.foreign.broker")) {
                    return true;
                }
                return false;
            }
        }));
    }

    @Before
    public void setUp() throws Exception {

        localConnectionFactory = createLocalConnectionFactory();
        foreignConnectionFactory = createForeignConnectionFactory();

        outbound = new ActiveMQTopic("RECONNECT.TEST.OUT.TOPIC");
        inbound = new ActiveMQTopic("RECONNECT.TEST.IN.TOPIC");

        jmsTopicConnector = new SimpleJmsTopicConnector();

        // Wire the bridges.
        jmsTopicConnector.setOutboundTopicBridges(
            new OutboundTopicBridge[] {new OutboundTopicBridge("RECONNECT.TEST.OUT.TOPIC")});
        jmsTopicConnector.setInboundTopicBridges(
                new InboundTopicBridge[] {new InboundTopicBridge("RECONNECT.TEST.IN.TOPIC")});

        // Tell it how to reach the two brokers.
        jmsTopicConnector.setOutboundTopicConnectionFactory(
            new ActiveMQConnectionFactory("tcp://localhost:61617"));
        jmsTopicConnector.setLocalTopicConnectionFactory(
                new ActiveMQConnectionFactory("tcp://localhost:61616"));
    }

    @After
    public void tearDown() throws Exception {
        disposeConsumerConnections();

        try {
            jmsTopicConnector.stop();
            jmsTopicConnector = null;
        } catch (Exception e) {
        }

        try {
            stopLocalBroker();
        } catch (Throwable e) {
        }
        try {
            stopForeignBroker();
        } catch (Throwable e) {
        }
    }

    protected void disposeConsumerConnections() {
        for (Iterator<Connection> iter = connections.iterator(); iter.hasNext();) {
            Connection connection = iter.next();
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
        }
    }

    protected void startLocalBroker() throws Exception {
        if (localBroker == null) {
            localBroker = createFirstBroker();
            localBroker.start();
            localBroker.waitUntilStarted();
        }
    }

    protected void stopLocalBroker() throws Exception {
        if (localBroker != null) {
            localBroker.stop();
            localBroker.waitUntilStopped();
            localBroker = null;
        }
    }

    protected void startForeignBroker() throws Exception {
        if (foreignBroker == null) {
            foreignBroker = createSecondBroker();
            foreignBroker.start();
            foreignBroker.waitUntilStarted();
        }
    }

    protected void stopForeignBroker() throws Exception {
        if (foreignBroker != null) {
            foreignBroker.stop();
            foreignBroker.waitUntilStopped();
            foreignBroker = null;
        }
    }

    protected BrokerService createFirstBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("broker1");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:61616");

        return broker;
    }

    protected BrokerService createSecondBroker() throws Exception {

        BrokerService broker = new BrokerService();
        broker.setBrokerName("broker2");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:61617");

        return broker;
    }

    protected ActiveMQConnectionFactory createLocalConnectionFactory() {
        return new ActiveMQConnectionFactory("tcp://localhost:61616");
    }

    protected ActiveMQConnectionFactory createForeignConnectionFactory() {
        return new ActiveMQConnectionFactory("tcp://localhost:61617");
    }

    protected void sendMessageToForeignBroker(String text) throws JMSException {
        Connection connection = null;
        try {
            connection = localConnectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(outbound);
            TextMessage message = session.createTextMessage();
            message.setText(text);
            producer.send(message);
        } finally {
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
        }
    }

    protected void sendMessageToLocalBroker(String text) throws JMSException {
        Connection connection = null;
        try {
            connection = foreignConnectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(inbound);
            TextMessage message = session.createTextMessage();
            message.setText(text);
            producer.send(message);
        } finally {
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
        }
    }

    protected MessageConsumer createConsumerForLocalBroker() throws JMSException {
        Connection connection = localConnectionFactory.createConnection();
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createConsumer(inbound);
    }

    protected MessageConsumer createConsumerForForeignBroker() throws JMSException {
        Connection connection = foreignConnectionFactory.createConnection();
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createConsumer(outbound);
    }
}
