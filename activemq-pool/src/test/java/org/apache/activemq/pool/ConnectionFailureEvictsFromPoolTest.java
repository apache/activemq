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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.EnhancedConnection;
import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.jms.pool.PooledConnection;
import org.apache.activemq.test.TestSupport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.mock.MockTransport;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionFailureEvictsFromPoolTest extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionFailureEvictsFromPoolTest.class);
    private BrokerService broker;
    TransportConnector connector;

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("connectionFailureEvictsFromPoolTest");
        broker.setUseJmx(false);
        broker.setPersistent(false);
        connector = broker.addConnector("tcp://localhost:0");
        broker.start();
    }

    public void testEnhancedConnection() throws Exception {
        final XaPooledConnectionFactory pooledFactory =
                new XaPooledConnectionFactory(new ActiveMQXAConnectionFactory("mock:" + connector.getConnectUri() + "?closeAsync=false"));
        try {
            try (final PooledConnection connection = (PooledConnection) pooledFactory.createConnection()) {
                final EnhancedConnection enhancedConnection = (EnhancedConnection) connection.getConnection();
                final DestinationSource destinationSource = enhancedConnection.getDestinationSource();
                assertNotNull(destinationSource);
            }
        } finally {
            pooledFactory.stop();
        }
    }

    public void testEvictionXA() throws Exception {
        final XaPooledConnectionFactory pooledFactory =
                new XaPooledConnectionFactory(new ActiveMQXAConnectionFactory("mock:(" + connector.getConnectUri() + "?closeAsync=false)?jms.xaAckMode=1"));
        try {
            doTestEviction(pooledFactory);
        } finally {
            pooledFactory.stop();
        }
    }

    public void testEviction() throws Exception {
        final PooledConnectionFactory pooledFactory =
                new PooledConnectionFactory(new ActiveMQConnectionFactory("mock:" + connector.getConnectUri() + "?closeAsync=false"));
        try {
            doTestEviction(pooledFactory);
        } finally {
            pooledFactory.stop();
        }
    }

    public void doTestEviction(ConnectionFactory pooledFactory) throws Exception {
        final CountDownLatch gotExceptionEvent = new CountDownLatch(1);
        try (final PooledConnection connection = (PooledConnection) pooledFactory.createConnection()) {
            final ActiveMQConnection amqC = (ActiveMQConnection) connection.getConnection();
            // Intercept exception propagation at the MockTransport level where it fires
            // synchronously. ActiveMQConnection.addTransportListener() callbacks fire via
            // executeAsync(), which silently drops the task if the pool's ExceptionListener
            // closes the connection and shuts down the executor first (race condition that
            // affects the XA path).
            final MockTransport mockTransport = (MockTransport) amqC.getTransportChannel().narrow(MockTransport.class);
            final TransportListener originalListener = mockTransport.getTransportListener();
            mockTransport.setTransportListener(new TransportListener() {
                public void onCommand(Object command) {
                    originalListener.onCommand(command);
                }
                public void onException(IOException error) {
                    // fires synchronously when MockTransport.onException() is called
                    gotExceptionEvent.countDown();
                    originalListener.onException(error);
                }
                public void transportInterupted() {
                    originalListener.transportInterupted();
                }
                public void transportResumed() {
                    originalListener.transportResumed();
                }
            });

            sendMessage(connection);
            LOG.info("sent one message worked fine");
            createConnectionFailure(connection);
            try {
                sendMessage(connection);
                TestCase.fail("Expected Error");
            } catch (JMSException e) {
            }
            TestCase.assertTrue("exception event propagated ok", gotExceptionEvent.await(5, TimeUnit.SECONDS));
        }
        // After the failure, a new connection from the pool should work.
        // The pool eviction is async (ExceptionListener fires via executeAsync),
        // so retry until the pool returns a working connection.
        LOG.info("expect new connection after failure");
        assertTrue("pool should provide working connection after eviction",
            Wait.waitFor(() -> {
                try (final Connection connection2 = pooledFactory.createConnection()) {
                    sendMessage(connection2);
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }, 5000, 100));
    }

    private void createConnectionFailure(Connection connection) throws Exception {
        ActiveMQConnection c = (ActiveMQConnection) ((PooledConnection)connection).getConnection();
        MockTransport t = (MockTransport)c.getTransportChannel().narrow(MockTransport.class);
        t.onException(new IOException("forcing exception for " + getName() + " to force pool eviction"));
        LOG.info("arranged for failure, chucked exception");
    }

    private void sendMessage(Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue("FOO"));
        producer.send(session.createTextMessage("Test"));
        session.close();
    }

    protected void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }
}
