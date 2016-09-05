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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PooledConnectionFailoverTest extends JmsPoolTestSupport {

    protected ActiveMQConnectionFactory directConnFact;
    protected PooledConnectionFactory pooledConnFact;

    @Override
    @Before
    public void setUp() throws java.lang.Exception {
        super.setUp();

        String connectionURI = createBroker();

        // Create the ActiveMQConnectionFactory and the PooledConnectionFactory.
        directConnFact = new ActiveMQConnectionFactory(connectionURI);
        pooledConnFact = new PooledConnectionFactory();
        pooledConnFact.setConnectionFactory(directConnFact);
        pooledConnFact.setMaxConnections(1);
        pooledConnFact.setReconnectOnException(true);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            pooledConnFact.stop();
        } catch (Exception ex) {
            // ignored
        }

        super.tearDown();
    }

    @Test
    public void testConnectionFailures() throws Exception {

        final CountDownLatch failed = new CountDownLatch(1);

        Connection connection = pooledConnFact.createConnection();
        LOG.info("Fetched new connection from the pool: {}", connection);
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("Pooled Connection failed");
                failed.countDown();
            }
        });

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getTestName());
        MessageProducer producer = session.createProducer(queue);

        brokerService.stop();

        assertTrue(failed.await(15, TimeUnit.SECONDS));

        createBroker();

        try {
            producer.send(session.createMessage());
            fail("Should be disconnected");
        } catch (JMSException ex) {
            LOG.info("Producer failed as expected: {}", ex.getMessage());
        }

        Connection connection2 = pooledConnFact.createConnection();
        assertNotSame(connection, connection2);
        LOG.info("Fetched new connection from the pool: {}", connection2);
        session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

        connection2.close();

        pooledConnFact.stop();
    }

    private String createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setBrokerName("PooledConnectionSessionCleanupTestBroker");
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setPersistent(false);
        brokerService.setSchedulerSupport(false);
        brokerService.setAdvisorySupport(false);
        TransportConnector connector = brokerService.addConnector("tcp://0.0.0.0:61626");
        brokerService.start();
        brokerService.waitUntilStarted();

        return "failover:(" + connector.getPublishableConnectString() + ")?maxReconnectAttempts=5";
    }
}
