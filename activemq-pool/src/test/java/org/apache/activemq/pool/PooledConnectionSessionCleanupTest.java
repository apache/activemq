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

package org.apache.activemq.pool;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledConnectionSessionCleanupTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(PooledConnectionSessionCleanupTest.class);

    protected BrokerService service;

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
    @Before
    public void prepTest() throws java.lang.Exception {
        service = new BrokerService();
        service.setBrokerName("PooledConnectionSessionCleanupTestBroker");
        service.setUseJmx(true);
        service.setPersistent(false);
        service.setSchedulerSupport(false);
        service.start();
        service.waitUntilStarted();

        // Create the ActiveMQConnectionFactory and the PooledConnectionFactory.
        // Set a long idle timeout on the pooled connections to better show the
        // problem of holding onto created resources on close.
        directConnFact = new ActiveMQConnectionFactory(service.getVmConnectorURI());
        pooledConnFact = new PooledConnectionFactory(directConnFact);
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

    @After
    public void cleanupTest() throws java.lang.Exception {
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
        try {
            if (service != null) {
                service.stop();
                service.waitUntilStopped();
                service = null;
            }
        } catch (JMSException jms_exc) {
        }
    }

    private void produceMessages() throws Exception {

        Session session = directConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < MESSAGE_COUNT; ++i) {
            producer.send(session.createTextMessage("Test Message: " + i));
        }
        producer.close();
    }

    private QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq"
                + ":destinationType=Queue,destinationName=" + name
                + ",type=Broker,brokerName=" + service.getBrokerName());
        QueueViewMBean proxy = (QueueViewMBean) service.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    @Test
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
        }));

        // While all the message are in flight we should get anything on this consumer.
        Session session = directConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        assertNull(consumer.receive(2000));

        pooledConn1.close();

        assertTrue("Should have only one consumer now:", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return view.getSubscriptions().length == 1;
            }
        }));

        // Now we'd expect that the message stuck in the prefetch of the pooled session's
        // consumer would be rerouted to the non-pooled session's consumer.
        assertNotNull(consumer.receive(10000));
    }

    @Test
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
        }));

        // While all the message are in flight we should get anything on this consumer.
        Session session = directConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        assertNull(consumer.receive(2000));

        directConn2.close();

        assertTrue("Should have only one consumer now:", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return view.getSubscriptions().length == 1;
            }
        }));

        // Now we'd expect that the message stuck in the prefetch of the first session's
        // consumer would be rerouted to the alternate session's consumer.
        assertNotNull(consumer.receive(10000));
    }
}
