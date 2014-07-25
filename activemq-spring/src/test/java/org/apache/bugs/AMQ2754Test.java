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
package org.apache.bugs;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

public class AMQ2754Test extends TestCase {

    public void testNetworkOfBrokers() throws Exception {
        BrokerService brokerService1 = null;
        BrokerService brokerService2 = null;

        String broker1Uri;
        String broker2Uri;

        final int total = 100;
        final CountDownLatch latch = new CountDownLatch(total);
        final boolean conduitSubscriptions = true;
        try {

            {
                brokerService1 = new BrokerService();
                brokerService1.setBrokerName("consumer");
                brokerService1.setUseJmx(false);
                brokerService1.setPersistenceAdapter(new MemoryPersistenceAdapter());
                broker1Uri = brokerService1.addConnector("tcp://0.0.0.0:0").getPublishableConnectString();
                brokerService1.start();
            }

            {
                brokerService2 = new BrokerService();
                brokerService2.setBrokerName("producer");
                brokerService2.setUseJmx(false);
                brokerService2.setPersistenceAdapter(new MemoryPersistenceAdapter());
                broker2Uri = brokerService2.addConnector("tcp://0.0.0.0:0").getPublishableConnectString();
                NetworkConnector network2 = brokerService2.addNetworkConnector("static:("+broker1Uri+")");
                network2.setName("network1");
                network2.setDynamicOnly(true);
                network2.setConduitSubscriptions(conduitSubscriptions);
                network2.setNetworkTTL(3);
                network2.setPrefetchSize(1);
                brokerService2.start();
            }

            ExecutorService pool = Executors.newSingleThreadExecutor();

            ActiveMQConnectionFactory connectionFactory1 =
                new ActiveMQConnectionFactory("failover:("+broker1Uri+")");

            connectionFactory1.setWatchTopicAdvisories(false);
            final DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
            container.setConnectionFactory(connectionFactory1);
            container.setMaxConcurrentConsumers(10);
            container.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
            container.setCacheLevel(DefaultMessageListenerContainer.CACHE_CONSUMER);
            container.setDestination(new ActiveMQQueue("testingqueue"));
            container.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    latch.countDown();
                }
            });
            container.setMaxMessagesPerTask(1);
            container.afterPropertiesSet();
            container.start();

            final String finalBroker2Uri = broker2Uri;

            pool.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    try {
                        final int batch = 10;
                        ActiveMQConnectionFactory connectionFactory2 =
                            new ActiveMQConnectionFactory("failover:("+finalBroker2Uri+")");
                        PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory(connectionFactory2);
                        connectionFactory2.setWatchTopicAdvisories(false);
                        JmsTemplate template = new JmsTemplate(pooledConnectionFactory);
                        ActiveMQQueue queue = new ActiveMQQueue("testingqueue");
                        for(int b = 0; b < batch; b++) {
                            for(int i = 0; i < (total / batch); i++) {
                                final String id = ":batch=" + b + "i=" + i;
                                template.send(queue, new MessageCreator() {
                                    @Override
                                    public Message createMessage(Session session) throws JMSException {
                                        TextMessage message = session.createTextMessage();
                                        message.setText("Hello World!" + id);
                                        return message;
                                    }
                                });
                            }
                            // give spring time to scale back again
                            while(container.getActiveConsumerCount() > 1) {
                                System.out.println("active consumer count:" + container.getActiveConsumerCount());
                                System.out.println("concurrent consumer count: " + container.getConcurrentConsumers());
                                Thread.sleep(1000);
                            }
                        }
                        //pooledConnectionFactory.stop();
                    } catch(Throwable t) {
                        t.printStackTrace();
                    }
                    return null;
                }
            });

            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);

            int count = 0;

            // give it 20 seconds
            while(!latch.await(1, TimeUnit.SECONDS) && count++ < 20) {
                System.out.println("count " + latch.getCount());
            }

            container.destroy();

        } finally {
            try { if(brokerService1 != null) {
                brokerService1.stop();
            }} catch(Throwable t) { t.printStackTrace(); }
            try { if(brokerService2 != null) {
                brokerService2.stop();
            }} catch(Throwable t) { t.printStackTrace(); }
        }

        if(latch.getCount() > 0) {
            fail("latch should have gone down to 0 but was " + latch.getCount());
        }
    }
}