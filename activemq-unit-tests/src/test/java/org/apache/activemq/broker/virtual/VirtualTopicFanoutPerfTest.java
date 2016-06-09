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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualTopicFanoutPerfTest {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualTopicFanoutPerfTest.class);

    int numConsumers = 100;
    int total = 500;
    BrokerService brokerService;
    ConnectionFactory connectionFactory;

    @Before
    public void createBroker() throws Exception  {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.start();

        for (DestinationInterceptor destinationInterceptor  : brokerService.getDestinationInterceptors()) {
                for (VirtualDestination virtualDestination : ((VirtualDestinationInterceptor) destinationInterceptor).getVirtualDestinations()) {
                    if (virtualDestination instanceof VirtualTopic) {
                        ((VirtualTopic) virtualDestination).setConcurrentSend(true);
                        ((VirtualTopic) virtualDestination).setTransactedSend(true);
                }
            }
        }
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");
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
    @Ignore("comparison test - concurrentSend=true virtual topic, use transaction")
    public void testFanoutDuration() throws Exception {

        Connection connection1 = connectionFactory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        for (int i=0; i<numConsumers; i++) {
            session.createConsumer(new ActiveMQQueue("Consumer." + i + ".VirtualTopic.TEST"));
        }

        // create topic producer
        Connection connection2 = connectionFactory.createConnection();
        connection2.start();
        Session producerSession = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(new ActiveMQTopic("VirtualTopic.TEST"));

        long start = System.currentTimeMillis();
        LOG.info("Starting producer: " + start);
        for (int i = 0; i < total; i++) {
            producer.send(producerSession.createTextMessage("message: " + i));
        }
        LOG.info("Done producer, duration: " + (System.currentTimeMillis() - start) );

        try {
            connection1.close();
        } catch (Exception ex) {}
        try {
            connection2.close();
        } catch (Exception ex) {}
    }
}
