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
package org.apache.activemq.broker.virtual;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Collections;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.CompositeQueue;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *  https://issues.apache.org/jira/browse/AMQ-5898
 */
public class MultipleCompositeToPhysicalQueueTest {

    private final ActiveMQQueue SUB1 = new ActiveMQQueue("SUB1");
    private final CompositeQueue PUB_BROADCAST = newCompositeQueue("PUB.ALL", SUB1);
    private final CompositeQueue PUB_INDIVIDUAL = newCompositeQueue("PUB.SUB1", SUB1);
    private String url;;

    private BrokerService broker;

    @Before
    public void before() throws Exception {
        broker = createBroker(false);
        VirtualDestinationInterceptor virtualDestinationInterceptor = new VirtualDestinationInterceptor();
        virtualDestinationInterceptor.setVirtualDestinations(
                new VirtualDestination[]{
                        PUB_BROADCAST,
                        PUB_INDIVIDUAL
                }
        );
        broker.setDestinationInterceptors(new DestinationInterceptor[]{virtualDestinationInterceptor});
        broker.start();
        broker.waitUntilStarted();
        url = broker.getConnectorByName("tcp").getConnectUri().toString();
    }

    @After
    public void after() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 60000)
    public void testManyToOne() throws Exception {

        Session consumerSession = buildSession("Consumer", url);

        MessageConsumer consumer = createSubscriber(consumerSession, SUB1, null);

        // Producer
        Session publisherSession = buildSession("Producer", url);

        createPublisher(publisherSession, PUB_BROADCAST.getVirtualDestination()).send(publisherSession.createTextMessage("BROADCAST"));
        ActiveMQMessage broadcastMessage = (ActiveMQMessage) consumer.receive();
        ActiveMQDestination originalDestination = broadcastMessage.getOriginalDestination();

        assertEquals("BROADCAST", ((TextMessage) broadcastMessage).getText());
        assertEquals( PUB_BROADCAST.getName(), broadcastMessage.getOriginalDestination().getPhysicalName());

        createPublisher(publisherSession, PUB_INDIVIDUAL.getVirtualDestination()).send(publisherSession.createTextMessage("INDIVIDUAL"));
        ActiveMQMessage individualMessage = (ActiveMQMessage)consumer.receive();

        assertEquals("INDIVIDUAL", ((TextMessage)individualMessage).getText());
        assertEquals( PUB_INDIVIDUAL.getName(), individualMessage.getOriginalDestination().getPhysicalName());
    }

    private BrokerService createBroker(boolean persistent) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("TestBroker");
        broker.setPersistent(persistent);
        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI("tcp://localhost:0"));
        connector.setName("tcp");;
        broker.addConnector(connector);

        return broker;
    }

    private MessageConsumer createSubscriber(Session session, Destination destination, MessageListener messageListener) throws JMSException {
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(messageListener);
        return consumer;
    }

    private MessageProducer createPublisher(Session session, Destination destination) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return producer;
    }

    private Session buildSession(String clientId, String url) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);

        connectionFactory.setClientIDPrefix(clientId);
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        return session;
    }

    private CompositeQueue newCompositeQueue(String name, ActiveMQDestination forwardTo) {
        CompositeQueue queue = new CompositeQueue();
        queue.setName(name);
        queue.setForwardTo(Collections.singleton(forwardTo));
        return queue;
    }
}