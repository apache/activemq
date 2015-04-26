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

import java.net.URI;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ5421Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ5421Test.class);

    private static final int DEST_COUNT = 1000;
    private final Destination[] destination = new Destination[DEST_COUNT];
    private final MessageProducer[] producer = new MessageProducer[DEST_COUNT];
    private BrokerService brokerService;
    private String connectionUri;

    protected ConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory conFactory = new ActiveMQConnectionFactory(connectionUri);
        conFactory.setWatchTopicAdvisories(false);
        return conFactory;
    }

    protected AbortSlowAckConsumerStrategy createSlowConsumerStrategy() {
        AbortSlowAckConsumerStrategy strategy = new AbortSlowAckConsumerStrategy();
        strategy.setCheckPeriod(2000);
        strategy.setMaxTimeSinceLastAck(5000);
        strategy.setIgnoreIdleConsumers(false);

        return strategy;
    }

    @Before
    public void setUp() throws Exception {
        brokerService = BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false&useJmx=true"));
        PolicyEntry policy = new PolicyEntry();

        policy.setSlowConsumerStrategy(createSlowConsumerStrategy());
        policy.setQueuePrefetch(10);
        policy.setTopicPrefetch(10);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        brokerService.setUseJmx(false);
        brokerService.setDestinationPolicy(pMap);
        brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.start();

        connectionUri = brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString();
    }

    @Test
    public void testManyTempDestinations() throws Exception {
        Connection connection = createConnectionFactory().createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        for (int i = 0; i < DEST_COUNT; i++) {
            destination[i] = session.createTemporaryQueue();
            LOG.debug("Created temp queue: [}", i);
        }

        for (int i = 0; i < DEST_COUNT; i++) {
            producer[i] = session.createProducer(destination[i]);
            LOG.debug("Created producer: {}", i);
            TextMessage msg = session.createTextMessage(" testMessage " + i);
            producer[i].send(msg);
            LOG.debug("message sent: {}", i);
            MessageConsumer consumer = session.createConsumer(destination[i]);
            Message message = consumer.receive(1000);
            Assert.assertTrue(message.equals(msg));
        }

        for (int i = 0; i < DEST_COUNT; i++) {
            producer[i].close();
        }

        connection.close();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }
}
