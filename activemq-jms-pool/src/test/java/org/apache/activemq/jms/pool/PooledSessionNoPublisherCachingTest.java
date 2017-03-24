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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PooledSessionNoPublisherCachingTest extends JmsPoolTestSupport {

    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;
    private String connectionUri;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        TransportConnector connector = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        connectionUri = connector.getPublishableConnectString();
        factory = new ActiveMQConnectionFactory(connectionUri);
        pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);
        pooledFactory.setMaxConnections(1);
        pooledFactory.setBlockIfSessionPoolIsFull(false);
        pooledFactory.setUseAnonymousProducers(false);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            pooledFactory.stop();
        } catch (Exception ex) {
            // ignored
        }

        super.tearDown();
    }
        
    @Test(timeout = 60000)
    public void testMessageProducersAreUnique() throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        PooledProducer producer1 = (PooledProducer) session.createProducer(queue1);
        PooledProducer producer2 = (PooledProducer) session.createProducer(queue2);

        assertNotSame(producer1.getMessageProducer(), producer2.getMessageProducer());
    }

    @Test(timeout = 60000)
    public void testThrowsWhenDestinationGiven() throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        PooledProducer producer = (PooledProducer) session.createProducer(queue1);

        try {
            producer.send(queue2, session.createTextMessage());
            fail("Should only be able to send to queue 1");
        } catch (Exception ex) {
        }

        try {
            producer.send(null, session.createTextMessage());
            fail("Should only be able to send to queue 1");
        } catch (Exception ex) {
        }
    }

    @Test(timeout = 60000)
    public void testCreateTopicPublisher() throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic1 = session.createTopic("Topic-1");
        Topic topic2 = session.createTopic("Topic-2");

        PooledTopicPublisher publisher1 = (PooledTopicPublisher) session.createPublisher(topic1);
        PooledTopicPublisher publisher2 = (PooledTopicPublisher) session.createPublisher(topic2);

        assertNotSame(publisher1.getMessageProducer(), publisher2.getMessageProducer());
    }

    @Test(timeout = 60000)
    public void testQueueSender() throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        PooledQueueSender sender1 = (PooledQueueSender) session.createSender(queue1);
        PooledQueueSender sender2 = (PooledQueueSender) session.createSender(queue2);

        assertNotSame(sender1.getMessageProducer(), sender2.getMessageProducer());
    }
}
