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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.Destination;
import javax.jms.MessageProducer;
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

public class PooledSessionTest extends JmsPoolTestSupport {

    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;
    private String connectionUri;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
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
    public void testPooledSessionStats() throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();

        assertEquals(0, connection.getNumActiveSessions());
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(1, connection.getNumActiveSessions());
        session.close();
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(1, connection.getNumtIdleSessions());
        assertEquals(1, connection.getNumSessions());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testMessageProducersAreAllTheSame() throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        PooledProducer producer1 = (PooledProducer) session.createProducer(queue1);
        PooledProducer producer2 = (PooledProducer) session.createProducer(queue2);

        assertSame(producer1.getMessageProducer(), producer2.getMessageProducer());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testThrowsWhenDifferentDestinationGiven() throws Exception {
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

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateTopicPublisher() throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic1 = session.createTopic("Topic-1");
        Topic topic2 = session.createTopic("Topic-2");

        PooledTopicPublisher publisher1 = (PooledTopicPublisher) session.createPublisher(topic1);
        PooledTopicPublisher publisher2 = (PooledTopicPublisher) session.createPublisher(topic2);

        assertSame(publisher1.getMessageProducer(), publisher2.getMessageProducer());
        connection.close();
    }

    @Test(timeout = 60000)
    public void testQueueSender() throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        PooledQueueSender sender1 = (PooledQueueSender) session.createSender(queue1);
        PooledQueueSender sender2 = (PooledQueueSender) session.createSender(queue2);

        assertSame(sender1.getMessageProducer(), sender2.getMessageProducer());
        connection.close();
    }

    @Test(timeout = 60000)
    public void testRepeatedCreateSessionProducerResultsInSame() throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();

        assertTrue(pooledFactory.isUseAnonymousProducers());

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic("test-topic");
        PooledProducer producer = (PooledProducer) session.createProducer(destination);
        MessageProducer original = producer.getMessageProducer();
        assertNotNull(original);
        session.close();

        assertEquals(1, brokerService.getAdminView().getDynamicDestinationProducers().length);

        for (int i = 0; i < 20; ++i) {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = (PooledProducer) session.createProducer(destination);
            assertSame(original, producer.getMessageProducer());
            session.close();
        }

        assertEquals(1, brokerService.getAdminView().getDynamicDestinationProducers().length);

        connection.close();
        pooledFactory.clear();
    }
}
