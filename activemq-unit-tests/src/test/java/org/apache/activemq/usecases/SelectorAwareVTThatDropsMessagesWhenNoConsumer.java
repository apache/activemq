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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class SelectorAwareVTThatDropsMessagesWhenNoConsumer {
    protected static final Logger LOG = LoggerFactory.getLogger(SelectorAwareVTThatDropsMessagesWhenNoConsumer.class);
    private static final String QUEUE_NAME="TestQ";
    private static final String CONSUMER_QUEUE="Consumer.Orders.VirtualOrders." + QUEUE_NAME;
    private static final String PRODUCER_DESTINATION_NAME = "VirtualOrders." + QUEUE_NAME;

    final AtomicInteger receivedCount = new AtomicInteger(0);

    private BrokerService broker;

    @Before
    public void setUp() {
        setupBroker("broker://()/localhost?");
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60 * 1000)
    public void verifyNoDispatchDuringDisconnect() throws Exception{
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue consumerQueue = session.createQueue(CONSUMER_QUEUE);
        MessageListener listenerA = new CountingListener(receivedCount);
        MessageConsumer consumerA = session.createConsumer(consumerQueue);
        consumerA.setMessageListener(listenerA);

        Destination producerDestination = session.createTopic(PRODUCER_DESTINATION_NAME);
        MessageProducer producer = session.createProducer(producerDestination);
        TextMessage message = session.createTextMessage("bla");
        producer.send(message);
        producer.send(message);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return receivedCount.get() == 2;
            }
        });

        consumerA.close();

        producer.send(message);
        producer.send(message);

        assertEquals(2, receivedCount.get());

        LOG.debug("Restarting consumerA");
        consumerA = session.createConsumer(consumerQueue);
        consumerA.setMessageListener(listenerA);

        producer.send(message);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return receivedCount.get() == 3;
            }
        });

        assertEquals(3, receivedCount.get());
        connection.close();
    }

    /**
     * Setup broker with VirtualTopic configured
     */
    private void setupBroker(String uri) {
        try {
            broker = BrokerFactory.createBroker(uri);

            VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
            VirtualTopic virtualTopic = new VirtualTopic();
            virtualTopic.setName("VirtualOrders.>");
            virtualTopic.setSelectorAware(true);
            VirtualDestination[] virtualDestinations = { virtualTopic };
            interceptor.setVirtualDestinations(virtualDestinations);
            broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});

            broker.setUseJmx(false);
            broker.start();
            broker.waitUntilStarted();
        } catch (Exception e) {
            LOG.error("Failed creating broker", e);
        }
    }

    class CountingListener implements MessageListener {
        AtomicInteger counter;

        public CountingListener(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void onMessage(Message message) {
            counter.incrementAndGet();
        }
    }
}

