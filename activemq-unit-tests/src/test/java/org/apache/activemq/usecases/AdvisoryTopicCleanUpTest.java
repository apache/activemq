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

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class AdvisoryTopicCleanUpTest {

    private static final Logger LOG = LoggerFactory.getLogger(AdvisoryTopicCleanUpTest.class);

    private BrokerService broker;
    private String connectionUri;

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionUri + "?jms.redeliveryPolicy.maximumRedeliveries=2");
    }

    @Before
    public void setUp() throws Exception {
        createBroker();
    }

    @After
    public void tearDown() throws Exception {
        destroyBroker();
    }

    private void createBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setUseJmx(true);
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();

        PolicyEntry policy = new PolicyEntry();
        policy.setAdvisoryForFastProducers(true);
        policy.setAdvisoryForConsumed(true);
        policy.setAdvisoryForDelivery(true);
        policy.setAdvisoryForDiscardingMessages(true);
        policy.setAdvisoryForSlowConsumers(true);
        policy.setAdvisoryWhenFull(true);
        policy.setProducerFlowControl(false);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);

        broker.start();
    }

    protected Connection createConnection() throws Exception {
        Connection con = createConnectionFactory().createConnection();
        con.start();
        return con;
    }

    private void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    @Test
    public void testAdvisoryTopic() throws Exception {
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQDestination queue = (ActiveMQDestination) session.createQueue("AdvisoryTopicCleanUpTestQueue");
        MessageProducer prod = session.createProducer(queue);
        Message message = session.createMessage();
        prod.send(message);
        message = session.createMessage();
        prod.send(message);
        message = session.createMessage();
        prod.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, 1000);
        connection.close();
        connection = createConnection();

        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        message = consumer.receive(60 * 1000);
        message.acknowledge();
        connection.close();
        connection = null;

        for (int i = 0; i < 2; i++) {
            connection = createConnection();
            session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
            consumer = session.createConsumer(queue);
            message = consumer.receive(60 * 1000);
            session.rollback();
            connection.close();
            connection = null;
        }

        Thread.sleep(2 * 1000);

        connection = createConnection();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(queue);
        message = consumer.receive(1000);
        if (message != null)
            message.acknowledge();
        connection.close();
        connection = null;

        TimeUnit.SECONDS.sleep(1);

        ActiveMQDestination dests[] = broker.getRegionBroker().getDestinations();

        for (ActiveMQDestination destination: dests) {
            String name = destination.getPhysicalName();
            if (name.contains(queue.getPhysicalName())) {
                LOG.info("Destination on Broker before removing the Queue: " + name);
            }
        }

        dests = broker.getRegionBroker().getDestinations();
        if (dests == null) {
            fail("Should have Destination for: " + queue.getPhysicalName());
        }

        broker.getAdminView().removeQueue(queue.getPhysicalName());

        dests = broker.getRegionBroker().getDestinations();
        if (dests != null)
        {
            for (ActiveMQDestination destination: dests) {
                String name = destination.getPhysicalName();
                LOG.info("Destination on broker after removing the Queue: " + name);
                assertFalse("Advisory topic should not exist. " + name,
                            name.startsWith("ActiveMQ.Advisory") && name.contains(queue.getPhysicalName()));
            }
        }
    }
}
