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
package org.apache.activemq.broker.interceptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.inteceptor.MessageInterceptor;
import org.apache.activemq.broker.inteceptor.MessageInterceptorRegistry;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;

public class MessageInterceptorTest extends TestCase {
    protected BrokerService brokerService;
    protected ActiveMQConnectionFactory factory;
    protected Connection producerConnection;
    protected Connection consumerConnection;
    protected Session consumerSession;
    protected Session producerSession;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected Topic topic;
    protected int messageCount = 10000;
    protected int timeOutInSeconds = 10;



    @Override
    protected void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.start();

        factory =  new ActiveMQConnectionFactory(BrokerRegistry.getInstance().findFirst().getVmConnectorURI());
        consumerConnection = factory.createConnection();
        consumerConnection.start();
        producerConnection = factory.createConnection();
        producerConnection.start();
        consumerSession = consumerConnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        topic = consumerSession.createTopic(getName());
        producerSession = producerConnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        consumer = consumerSession.createConsumer(topic);
        producer = producerSession.createProducer(topic);
    }

    @Override
    protected void tearDown() throws Exception {
        if (producerConnection != null){
            producerConnection.close();
        }
        if (consumerConnection != null){
            consumerConnection.close();
        }
        if (brokerService != null) {
            brokerService.stop();
        }
    }



    public void testNoIntercept() throws Exception {
        final CountDownLatch latch = new CountDownLatch(messageCount);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                latch.countDown();

            }
        });
        for (int i  = 0; i < messageCount; i++){
            javax.jms.Message message = producerSession.createTextMessage("test: " + i);
            producer.send(message);
        }

        latch.await(timeOutInSeconds, TimeUnit.SECONDS);
        assertEquals(0,latch.getCount());

    }

    public void testNoStackOverFlow() throws Exception {


        final MessageInterceptorRegistry registry = MessageInterceptorRegistry.getInstance().get(BrokerRegistry.getInstance().findFirst());
        registry.addMessageInterceptorForTopic(topic.getTopicName(), new MessageInterceptor() {
            @Override
            public void intercept(ProducerBrokerExchange producerExchange, Message message) {

                try {
                    registry.injectMessage(producerExchange, message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        final CountDownLatch latch = new CountDownLatch(messageCount);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                latch.countDown();

            }
        });
        for (int i  = 0; i < messageCount; i++){
            javax.jms.Message message = producerSession.createTextMessage("test: " + i);
            producer.send(message);
        }

        latch.await(timeOutInSeconds, TimeUnit.SECONDS);
        assertEquals(0,latch.getCount());
    }

    public void testInterceptorAll() throws Exception {
        MessageInterceptorRegistry registry = MessageInterceptorRegistry.getInstance().get(BrokerRegistry.getInstance().findFirst());
        registry.addMessageInterceptorForTopic(topic.getTopicName(), new MessageInterceptor() {
            @Override
            public void intercept(ProducerBrokerExchange producerExchange, Message message) {
                //just ignore
            }
        });

        final CountDownLatch latch = new CountDownLatch(messageCount);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                latch.countDown();

            }
        });
        for (int i  = 0; i < messageCount; i++){
            javax.jms.Message message = producerSession.createTextMessage("test: " + i);
            producer.send(message);
        }

        latch.await(timeOutInSeconds, TimeUnit.SECONDS);
        assertEquals(messageCount,latch.getCount());

    }

    public void testReRouteAll() throws Exception {
        final ActiveMQQueue queue = new ActiveMQQueue("Reroute.From."+topic.getTopicName());

        final MessageInterceptorRegistry registry = MessageInterceptorRegistry.getInstance().get(BrokerRegistry.getInstance().findFirst());
        registry.addMessageInterceptorForTopic(topic.getTopicName(), new MessageInterceptor() {
            @Override
            public void intercept(ProducerBrokerExchange producerExchange, Message message) {
                message.setDestination(queue);
                try {
                    registry.injectMessage(producerExchange, message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        final CountDownLatch latch = new CountDownLatch(messageCount);
        consumer = consumerSession.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                latch.countDown();

            }
        });
        for (int i  = 0; i < messageCount; i++){
            javax.jms.Message message = producerSession.createTextMessage("test: " + i);
            producer.send(message);
        }

        latch.await(timeOutInSeconds, TimeUnit.SECONDS);
        assertEquals(0,latch.getCount());

    }

    public void testReRouteAllWithNullProducerExchange() throws Exception {
        final ActiveMQQueue queue = new ActiveMQQueue("Reroute.From."+topic.getTopicName());

        final MessageInterceptorRegistry registry = MessageInterceptorRegistry.getInstance().get(BrokerRegistry.getInstance().findFirst());
        registry.addMessageInterceptorForTopic(topic.getTopicName(), new MessageInterceptor() {
            @Override
            public void intercept(ProducerBrokerExchange producerExchange, Message message) {
                message.setDestination(queue);
                try {
                    registry.injectMessage(producerExchange, message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        final CountDownLatch latch = new CountDownLatch(messageCount);
        consumer = consumerSession.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                latch.countDown();

            }
        });
        for (int i  = 0; i < messageCount; i++){
            javax.jms.Message message = producerSession.createTextMessage("test: " + i);
            producer.send(message);
        }

        latch.await(timeOutInSeconds, TimeUnit.SECONDS);
        assertEquals(0,latch.getCount());

    }

    public void testReRouteAllowWildCards() throws Exception {

        final ActiveMQQueue testQueue = new ActiveMQQueue("testQueueFor."+getName());

        final MessageInterceptorRegistry registry = MessageInterceptorRegistry.getInstance().get(BrokerRegistry.getInstance().findFirst());
        registry.addMessageInterceptorForTopic(">", new MessageInterceptor() {
            @Override
            public void intercept(ProducerBrokerExchange producerExchange, Message message) {

                try {
                    message.setDestination(testQueue);
                    registry.injectMessage(producerExchange,message);
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        });

        final CountDownLatch latch = new CountDownLatch(messageCount);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                latch.countDown();

            }
        });

        MessageConsumer consumer1 = consumerSession.createConsumer(testQueue);

        consumer1.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(javax.jms.Message message) {
                latch.countDown();

            }
        });
        for (int i  = 0; i < messageCount; i++){
            javax.jms.Message message = producerSession.createTextMessage("test: " + i);
            producer.send(message);
        }

        latch.await(timeOutInSeconds, TimeUnit.SECONDS);
        assertEquals(0,latch.getCount());

    }



}
