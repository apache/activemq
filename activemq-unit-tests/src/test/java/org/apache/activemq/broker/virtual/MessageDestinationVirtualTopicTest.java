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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import javax.jms.*;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "virtual-topic-network-test.xml" })
public class MessageDestinationVirtualTopicTest {

    private static final Logger LOG = LoggerFactory.getLogger(MessageDestinationVirtualTopicTest.class);

    private SimpleMessageListener listener1;

    private SimpleMessageListener listener2;

    @Resource(name = "broker1")
    private BrokerService broker1;

    @Resource(name = "broker2")
    private BrokerService broker2;

    private MessageProducer producer;

    private Session session1;

    public void init() throws JMSException {
        // Create connection on Broker B2
        ConnectionFactory broker2ConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:62616");
        Connection connection2 = broker2ConnectionFactory.createConnection();
        connection2.start();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue consumerDQueue = session2.createQueue("Consumer.D.VirtualTopic.T1");

        // Bind listener on queue for consumer D
        MessageConsumer consumer = session2.createConsumer(consumerDQueue);
        listener2 = new SimpleMessageListener();
        consumer.setMessageListener(listener2);

        // Create connection on Broker B1
        ConnectionFactory broker1ConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection1 = broker1ConnectionFactory.createConnection();
        connection1.start();
        session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue consumerCQueue = session1.createQueue("Consumer.C.VirtualTopic.T1");

        // Bind listener on queue for consumer D
        MessageConsumer consumer1 = session1.createConsumer(consumerCQueue);
        listener1 = new SimpleMessageListener();
        consumer1.setMessageListener(listener1);

        // Create producer for topic, on B1
        Topic virtualTopicT1 = session1.createTopic("VirtualTopic.T1");
        producer = session1.createProducer(virtualTopicT1);
    }

    @Test
    public void testDestinationNames() throws Exception {

        LOG.info("Started waiting for broker 1 and 2");
        broker1.waitUntilStarted();
        broker2.waitUntilStarted();
        LOG.info("Broker 1 and 2 have started");

        init();

        // Create a monitor
        CountDownLatch monitor = new CountDownLatch(2);
        listener1.setCountDown(monitor);
        listener2.setCountDown(monitor);

        LOG.info("Sending message");
        // Send a message on the topic
        TextMessage message = session1.createTextMessage("Hello World !");
        producer.send(message);
        LOG.info("Waiting for message reception");
        // Wait the two messages in the related queues
        monitor.await();

        // Get the message destinations
        String lastJMSDestination2 = listener2.getLastJMSDestination();
        System.err.println(lastJMSDestination2);
        String lastJMSDestination1 = listener1.getLastJMSDestination();
        System.err.println(lastJMSDestination1);

        // The destination names
        assertEquals("queue://Consumer.D.VirtualTopic.T1", lastJMSDestination2);
        assertEquals("queue://Consumer.C.VirtualTopic.T1", lastJMSDestination1);

    }
}