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

package org.apache.activemq.camel.camelplugin;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.xbean.XBeanBrokerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class CamelPluginConfigTest {

    protected static final String CONF_ROOT = "src/test/resources/org/apache/activemq/camel/camelplugin/";
    protected static final String TOPIC_NAME = "test.topic";
    protected static final String QUEUE_NAME = "test.queue";

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
        brokerService = createBroker(new FileSystemResource(CONF_ROOT + "camel-routes-activemq.xml"));

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

        XBeanBrokerFactory factory = new XBeanBrokerFactory();
        BrokerService broker = factory.createBroker(resource.getURI());
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
        Thread.sleep(2000);
        final ActiveMQQueue queue = new ActiveMQQueue(QUEUE_NAME);

        Topic topic = consumerSession.createTopic(TOPIC_NAME);

        final CountDownLatch latch = new CountDownLatch(messageCount);
        MessageConsumer consumer = consumerSession.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                try {
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
}
