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
package org.apache.activemq.camel.component.broker;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.jms.*;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BrokerComponentXMLConfigTest {

    protected static final String CONF_ROOT = "src/test/resources/org/apache/activemq/camel/component/broker/";
    protected static final String TOPIC_NAME = "test.broker.component.topic";
    protected static final String QUEUE_NAME = "test.broker.component.queue";
    protected static final String ROUTE_QUEUE_NAME = "test.broker.component.route";
    protected static final String DIVERTED_QUEUE_NAME = "test.broker.component.ProcessLater";
    protected static final int DIVERT_COUNT = 100;

    protected BrokerService brokerService;
    protected ActiveMQConnectionFactory factory;
    protected Connection producerConnection;
    protected Connection consumerConnection;
    protected Session consumerSession;
    protected Session producerSession;

    protected int messageCount = 1000;
    protected int timeOutInSeconds = 10;

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker(new FileSystemResource(CONF_ROOT + "broker-camel.xml"));

        factory = new ActiveMQConnectionFactory(BrokerRegistry.getInstance().findFirst().getVmConnectorURI());
        consumerConnection = factory.createConnection();
        consumerConnection.start();
        producerConnection = factory.createConnection();
        producerConnection.start();
        consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected BrokerService createBroker(String resource) throws Exception {
        return createBroker(new ClassPathResource(resource));
    }

    protected BrokerService createBroker(Resource resource) throws Exception {
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();

        BrokerService broker = factory.getBroker();

        assertTrue("Should have a broker!", broker != null);

        // Broker is already started by default when using the XML file
        // broker.start();

        return broker;
    }

    @After
    public void tearDown() throws Exception {
        if (producerConnection != null) {
            producerConnection.close();
        }
        if (consumerConnection != null) {
            consumerConnection.close();
        }
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void testReRouteAll() throws Exception {
        final ActiveMQQueue queue = new ActiveMQQueue(QUEUE_NAME);

        Topic topic = consumerSession.createTopic(TOPIC_NAME);

        final CountDownLatch latch = new CountDownLatch(messageCount);
        MessageConsumer consumer = consumerSession.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                try {
                    assertEquals(9, message.getJMSPriority());
                    latch.countDown();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        MessageProducer producer = producerSession.createProducer(topic);

        for (int i = 0; i < messageCount; i++) {
            javax.jms.Message message = producerSession.createTextMessage("test: " + i);
            producer.send(message);
        }

        latch.await(timeOutInSeconds, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void testRouteWithDestinationLimit() throws Exception {
        final ActiveMQQueue routeQueue = new ActiveMQQueue(ROUTE_QUEUE_NAME);

        final CountDownLatch routeLatch = new CountDownLatch(DIVERT_COUNT);
        MessageConsumer messageConsumer = consumerSession.createConsumer(routeQueue);
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                try {
                    routeLatch.countDown();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });

        final CountDownLatch divertLatch = new CountDownLatch(messageCount - DIVERT_COUNT);
        MessageConsumer divertConsumer = consumerSession.createConsumer(new ActiveMQQueue(DIVERTED_QUEUE_NAME));
        divertConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                try {
                    divertLatch.countDown();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });

        MessageProducer producer = producerSession.createProducer(routeQueue);

        for (int i = 0; i < messageCount; i++) {
            javax.jms.Message message = producerSession.createTextMessage("test: " + i);
            producer.send(message);
        }

        routeLatch.await(timeOutInSeconds, TimeUnit.SECONDS);
        divertLatch.await(timeOutInSeconds, TimeUnit.SECONDS);
        assertEquals(0, routeLatch.getCount());
        assertEquals(0, divertLatch.getCount());
    }

    @Test
    public void testPreserveOriginalHeaders() throws Exception {
        final ActiveMQQueue queue = new ActiveMQQueue(QUEUE_NAME);

        Topic topic = consumerSession.createTopic(TOPIC_NAME);

        final CountDownLatch latch = new CountDownLatch(messageCount);
        MessageConsumer consumer = consumerSession.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                try {
                    assertEquals("321", message.getStringProperty("JMSXGroupID"));
                    assertEquals("custom", message.getStringProperty("CustomHeader"));
                    latch.countDown();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        MessageProducer producer = producerSession.createProducer(topic);

        for (int i = 0; i < messageCount; i++) {
            javax.jms.Message message = producerSession.createTextMessage("test: " + i);
            message.setStringProperty("JMSXGroupID", "123");
            producer.send(message);
        }

        latch.await(timeOutInSeconds, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
    }
}
