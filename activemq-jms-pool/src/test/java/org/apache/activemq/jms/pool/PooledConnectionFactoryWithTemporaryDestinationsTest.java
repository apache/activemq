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
package org.apache.activemq.jms.pool;

import static org.junit.Assert.assertEquals;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.RegionBroker;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledConnectionFactoryWithTemporaryDestinationsTest extends JmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PooledConnectionFactoryWithTemporaryDestinationsTest.class);

    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        brokerService = new BrokerService();
        brokerService.setUseJmx(false);
        brokerService.setPersistent(false);
        brokerService.setSchedulerSupport(false);
        brokerService.setAdvisorySupport(false);
        TransportConnector connector = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        factory = new ActiveMQConnectionFactory("mock:" + connector.getConnectUri() + "?closeAsync=false");
        pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);
    }

    @Test(timeout = 60000)
    public void testTemporaryQueueWithMultipleConnectionUsers() throws Exception {
        Connection pooledConnection = null;
        Connection pooledConnection2 = null;
        Session session = null;
        Session session2 = null;
        Queue tempQueue = null;
        Queue normalQueue = null;

        pooledConnection = pooledFactory.createConnection();
        session = pooledConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        tempQueue = session.createTemporaryQueue();
        LOG.info("Created queue named: " + tempQueue.getQueueName());

        assertEquals(1, countBrokerTemporaryQueues());

        pooledConnection2 = pooledFactory.createConnection();
        session2 = pooledConnection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        normalQueue = session2.createQueue("queue:FOO.TEST");
        LOG.info("Created queue named: " + normalQueue.getQueueName());

        // didn't create a temp queue on pooledConnection2 so we should still have a temp queue
        pooledConnection2.close();
        assertEquals(1, countBrokerTemporaryQueues());

        // after closing pooledConnection, where we created the temp queue, there should
        // be no temp queues left
        pooledConnection.close();
        assertEquals(0, countBrokerTemporaryQueues());
    }

    @Test(timeout = 60000)
    public void testTemporaryQueueLeakAfterConnectionClose() throws Exception {
        Connection pooledConnection = null;
        Session session = null;
        Queue tempQueue = null;
        for (int i = 0; i < 2; i++) {
            pooledConnection = pooledFactory.createConnection();
            session = pooledConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            tempQueue = session.createTemporaryQueue();
            LOG.info("Created queue named: " + tempQueue.getQueueName());
            pooledConnection.close();
        }

        assertEquals(0, countBrokerTemporaryQueues());
    }

    @Test(timeout = 60000)
    public void testTemporaryTopicLeakAfterConnectionClose() throws Exception {
        Connection pooledConnection = null;
        Session session = null;
        Topic tempTopic = null;
        for (int i = 0; i < 2; i++) {
            pooledConnection = pooledFactory.createConnection();
            session = pooledConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            tempTopic = session.createTemporaryTopic();
            LOG.info("Created topic named: " + tempTopic.getTopicName());
            pooledConnection.close();
        }

        assertEquals(0, countBrokerTemporaryTopics());
    }

    @Test(timeout = 60000)
    public void testTemporaryQueueLeakAfterConnectionCloseWithConsumer() throws Exception {
        Connection pooledConnection = null;
        Session session = null;
        Queue tempQueue = null;
        for (int i = 0; i < 2; i++) {
            pooledConnection = pooledFactory.createConnection();
            session = pooledConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            tempQueue = session.createTemporaryQueue();
            MessageConsumer consumer = session.createConsumer(tempQueue);
            consumer.receiveNoWait();
            LOG.info("Created queue named: " + tempQueue.getQueueName());
            pooledConnection.close();
        }

        assertEquals(0, countBrokerTemporaryQueues());
    }

    private int countBrokerTemporaryQueues() throws Exception {
        return ((RegionBroker) brokerService.getRegionBroker()).getTempQueueRegion().getDestinationMap().size();
    }

    private int countBrokerTemporaryTopics() throws Exception {
        return ((RegionBroker) brokerService.getRegionBroker()).getTempTopicRegion().getDestinationMap().size();
    }
}
