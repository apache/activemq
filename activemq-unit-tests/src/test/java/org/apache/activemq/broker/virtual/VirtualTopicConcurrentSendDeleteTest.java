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
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class VirtualTopicConcurrentSendDeleteTest {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualTopicConcurrentSendDeleteTest.class);

    BrokerService brokerService;
    ConnectionFactory connectionFactory;

    @Before
    public void createBroker() throws Exception {
        createBroker(true);
    }

    public void createBroker(boolean delete) throws Exception  {
        brokerService = new BrokerService();
        //brokerService.setPersistent(false);
        brokerService.setDeleteAllMessagesOnStartup(delete);
        brokerService.setAdvisorySupport(false);
        ((KahaDBPersistenceAdapter)brokerService.getPersistenceAdapter()).setConcurrentStoreAndDispatchQueues(false);
        brokerService.start();

        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        activeMQConnectionFactory.setWatchTopicAdvisories(false);
        activeMQConnectionFactory.setAlwaysSyncSend(false);
        ActiveMQPrefetchPolicy zeroPrefetch = new ActiveMQPrefetchPolicy();
        zeroPrefetch.setAll(0);
        activeMQConnectionFactory.setPrefetchPolicy(zeroPrefetch);
        connectionFactory = activeMQConnectionFactory;
    }

    @After
    public void stopBroker() throws Exception  {
        brokerService.stop();
    }

    @Test
    public void testConsumerQueueDeleteOk() throws Exception {

        final int numConnections = 1;
        final int numDestinations = 10;
        final int numMessages = 4000;

        ExecutorService executorService = Executors.newFixedThreadPool(numConnections * 2);

        brokerService.getRegionBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQTopic("VirtualTopic.TEST"), false);

        // precreate dests to accentuate read access
        for (int i=0; i<numDestinations; i++ ) {
            brokerService.getRegionBroker().addDestination(
                    brokerService.getAdminConnectionContext(),
                    new ActiveMQQueue("Consumer." + i + ".VirtualTopic.TEST"),
                    false);
        }

        final CountDownLatch doneOne = new CountDownLatch(numConnections);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

                try {
                    int messagestoSend = 0;

                    Connection connection1 = connectionFactory.createConnection();
                    connection1.start();

                    Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(null);

                    do {
                        producer.send(new ActiveMQTopic("VirtualTopic.TEST"), new ActiveMQMessage());
                        messagestoSend++;

                        if (messagestoSend == 1000) {
                            doneOne.countDown();
                        }
                    } while (messagestoSend < numMessages);

                    connection1.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        for (int i = 0; i < numConnections; i++) {
            executorService.execute(runnable);
        }

        // delete all of the consumer queues
        final String prefix = "org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=";

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        doneOne.await(30, TimeUnit.SECONDS);

                        // delete in reverse to clash with send in forward direction
                        for (int i=numDestinations-1; i>=0; i--) {
                            final ActiveMQQueue toDelete = new ActiveMQQueue("Consumer." + i + ".VirtualTopic.TEST");

                            ObjectName queueViewMBeanName = new ObjectName(prefix + toDelete.getQueueName());
                            QueueViewMBean proxy = (QueueViewMBean)
                                    brokerService.getManagementContext().newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
                            LOG.info("Q len: " + toDelete.getQueueName() + ", " + proxy.getQueueSize());
                            brokerService.getAdminView().removeQueue(toDelete.getPhysicalName());

                            TimeUnit.MILLISECONDS.sleep(100);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            });


        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);

        LOG.info("Enqueues: " + ((RegionBroker)brokerService.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());
        final int numQueues = ((RegionBroker)brokerService.getRegionBroker()).getQueueRegion().getDestinationMap().size();
        LOG.info("Destinations: " + numQueues );

        assertEquals("no queues left", 0, numQueues);

        // the bug
        assertEquals("no queues, just one topic, in kahadb", 1, brokerService.getPersistenceAdapter().getDestinations().size());
    }
}
