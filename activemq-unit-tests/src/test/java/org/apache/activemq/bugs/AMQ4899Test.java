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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.plugin.SubQueueSelectorCacheBrokerPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4899Test {
    protected static final Logger LOG = LoggerFactory.getLogger(AMQ4899Test.class);
    private static final String QUEUE_NAME="AMQ4899TestQueue";
    private static final String CONSUMER_QUEUE="Consumer.Orders.VirtualOrders." + QUEUE_NAME;
    private static final String PRODUCER_DESTINATION_NAME = "VirtualOrders." + QUEUE_NAME;

    private static final Integer MESSAGE_LIMIT = 20;
    public static final String CONSUMER_A_SELECTOR = "Order < " + 10;
    public static  String CONSUMER_B_SELECTOR = "Order >= " + 10;
    private final CountDownLatch consumersStarted = new CountDownLatch(2);
    private final CountDownLatch consumerAtoConsumeCount= new CountDownLatch(10);
    private final CountDownLatch consumerBtoConsumeCount = new CountDownLatch(10);

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
    public void testVirtualTopicMultipleSelectors() throws Exception{
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue consumerQueue = session.createQueue(CONSUMER_QUEUE);

        MessageListener listenerA = new AMQ4899Listener("A", consumersStarted, consumerAtoConsumeCount);
        MessageConsumer consumerA = session.createConsumer(consumerQueue, CONSUMER_A_SELECTOR);
        consumerA.setMessageListener(listenerA);

        MessageListener listenerB = new AMQ4899Listener("B", consumersStarted, consumerBtoConsumeCount);
        MessageConsumer consumerB = session.createConsumer(consumerQueue, CONSUMER_B_SELECTOR);
        consumerB.setMessageListener(listenerB);

        consumersStarted.await(10, TimeUnit.SECONDS);
        assertEquals("Not all consumers started in time", 0, consumersStarted.getCount());

        Destination producerDestination = session.createTopic(PRODUCER_DESTINATION_NAME);
        MessageProducer producer = session.createProducer(producerDestination);
        int messageIndex = 0;
        for (int i=0; i < MESSAGE_LIMIT; i++) {
            if (i==3) {
                LOG.debug("Stopping consumerA");
                consumerA.close();
            }

            if (i == 14) {
                LOG.debug("Stopping consumer B");
                consumerB.close();
            }
            String messageText = "hello " + messageIndex++ + " sent at " + new java.util.Date().toString();
            TextMessage message = session.createTextMessage(messageText);
            message.setIntProperty("Order", i);
            LOG.debug("Sending message [{}]", messageText);
            producer.send(message);
        }

        // restart consumerA
        LOG.debug("Restarting consumerA");
        consumerA = session.createConsumer(consumerQueue, CONSUMER_A_SELECTOR);
        consumerA.setMessageListener(listenerA);

        // restart consumerB
        LOG.debug("restarting consumerB");
        consumerB = session.createConsumer(consumerQueue, CONSUMER_B_SELECTOR);
        consumerB.setMessageListener(listenerB);

        consumerAtoConsumeCount.await(5, TimeUnit.SECONDS);
        consumerBtoConsumeCount.await(5, TimeUnit.SECONDS);

        LOG.debug("Unconsumed messages for consumerA {} consumerB {}", consumerAtoConsumeCount.getCount(), consumerBtoConsumeCount.getCount());

        assertEquals("Consumer A did not consume all messages", 0, consumerAtoConsumeCount.getCount());
        assertEquals("Consumer B did not consume all messages", 0, consumerBtoConsumeCount.getCount());

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

            SubQueueSelectorCacheBrokerPlugin subQueueSelectorCacheBrokerPlugin = new SubQueueSelectorCacheBrokerPlugin();
            BrokerPlugin[] updatedPlugins = {subQueueSelectorCacheBrokerPlugin};
            broker.setPlugins(updatedPlugins);

            broker.setUseJmx(false);
            broker.start();
            broker.waitUntilStarted();
        } catch (Exception e) {
            LOG.error("Failed creating broker", e);
        }
    }
}

class AMQ4899Listener implements MessageListener {
    Logger LOG = LoggerFactory.getLogger(AMQ4899Listener.class);
    CountDownLatch toConsume;
    String id;

    public AMQ4899Listener(String id, CountDownLatch started, CountDownLatch toConsume) {
        this.id = id;
        this.toConsume = toConsume;
        started.countDown();
    }

    @Override
    public void onMessage(Message message) {
        toConsume.countDown();
        try {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                LOG.debug("Listener {} received [{}]", id, textMessage.getText());
            } else {
                LOG.error("Listener {} Expected a TextMessage, got {}", id, message.getClass().getCanonicalName());
            }
        } catch (JMSException e) {
            LOG.error("Unexpected JMSException in Listener " + id, e);
        }
    }
}
