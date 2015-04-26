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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PooledConnectionSessionCleanupTest extends JmsPoolTestSupport {

    protected ActiveMQConnectionFactory directConnFact;
    protected Connection directConn1;
    protected Connection directConn2;

    protected PooledConnectionFactory pooledConnFact;
    protected Connection pooledConn1;
    protected Connection pooledConn2;

    private final ActiveMQQueue queue = new ActiveMQQueue("ContendedQueue");
    private final int MESSAGE_COUNT = 50;

    /**
     * Prepare to run a test case: create, configure, and start the embedded
     * broker, as well as creating the client connections to the broker.
     */
    @Override
    @Before
    public void setUp() throws java.lang.Exception {
        super.setUp();

        brokerService = new BrokerService();
        brokerService.setBrokerName("PooledConnectionSessionCleanupTestBroker");
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setPersistent(false);
        brokerService.setSchedulerSupport(false);
        brokerService.setAdvisorySupport(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        // Create the ActiveMQConnectionFactory and the PooledConnectionFactory.
        // Set a long idle timeout on the pooled connections to better show the
        // problem of holding onto created resources on close.
        directConnFact = new ActiveMQConnectionFactory(brokerService.getVmConnectorURI());
        pooledConnFact = new PooledConnectionFactory();
        pooledConnFact.setConnectionFactory(directConnFact);
        pooledConnFact.setIdleTimeout((int)TimeUnit.MINUTES.toMillis(60));
        pooledConnFact.setMaxConnections(1);

        // Prepare the connections
        directConn1 = directConnFact.createConnection();
        directConn1.start();
        directConn2 = directConnFact.createConnection();
        directConn2.start();

        // The pooled Connections should have the same underlying connection
        pooledConn1 = pooledConnFact.createConnection();
        pooledConn1.start();
        pooledConn2 = pooledConnFact.createConnection();
        pooledConn2.start();
    }

    @Override
    @After
    public void tearDown() throws java.lang.Exception {
        try {
            if (pooledConn1 != null) {
                pooledConn1.close();
            }
        } catch (JMSException jms_exc) {
        }
        try {
            if (pooledConn2 != null) {
                pooledConn2.close();
            }
        } catch (JMSException jms_exc) {
        }
        try {
            if (directConn1 != null) {
                directConn1.close();
            }
        } catch (JMSException jms_exc) {
        }
        try {
            if (directConn2 != null) {
                directConn2.close();
            }
        } catch (JMSException jms_exc) {
        }

        super.tearDown();
    }

    private void produceMessages() throws Exception {

        Session session = directConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < MESSAGE_COUNT; ++i) {
            producer.send(session.createTextMessage("Test Message: " + i));
        }
        producer.close();
    }

    @Test(timeout = 60000)
    public void testLingeringPooledSessionsHoldingPrefetchedMessages() throws Exception {

        produceMessages();

        Session pooledSession1 = pooledConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        pooledSession1.createConsumer(queue);

        final QueueViewMBean view = getProxyToQueue(queue.getPhysicalName());

        assertTrue("Should have all sent messages in flight:", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return view.getInFlightCount() == MESSAGE_COUNT;
            }
        }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(25)));

        // While all the message are in flight we should get anything on this consumer.
        Session session = directConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        assertNull(consumer.receive(1000));

        pooledConn1.close();

        assertTrue("Should have only one consumer now:", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return view.getSubscriptions().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(25)));

        // Now we'd expect that the message stuck in the prefetch of the pooled session's
        // consumer would be rerouted to the non-pooled session's consumer.
        assertNotNull(consumer.receive(10000));
    }

    @Test(timeout = 60000)
    public void testNonPooledConnectionCloseNotHoldingPrefetchedMessages() throws Exception {

        produceMessages();

        Session directSession = directConn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        directSession.createConsumer(queue);

        final QueueViewMBean view = getProxyToQueue(queue.getPhysicalName());

        assertTrue("Should have all sent messages in flight:", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return view.getInFlightCount() == MESSAGE_COUNT;
            }
        }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(25)));

        // While all the message are in flight we should get anything on this consumer.
        Session session = directConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        assertNull(consumer.receive(1000));

        directConn2.close();

        assertTrue("Should have only one consumer now:", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return view.getSubscriptions().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(25)));

        // Now we'd expect that the message stuck in the prefetch of the first session's
        // consumer would be rerouted to the alternate session's consumer.
        assertNotNull(consumer.receive(10000));
    }
}
