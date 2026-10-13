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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import jakarta.jms.Connection;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.transport.mock.MockTransport;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises the JMS 2.0 pooled JMSContext surface under transport and broker
 * failure: operations must fail with mapped runtime exceptions, and the pool's
 * reconnectOnException support (default true) must evict the dead connection so
 * the factory hands out working contexts again.
 */
public class PooledJMSContextBrokerFailureTest extends JmsPoolTestSupport {

    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;
    private int brokerPort;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        var connector = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        brokerService.waitUntilStarted();

        brokerPort = connector.getConnectUri().getPort();

        // MockTransport in the chain lets tests inject transport failures deterministically
        factory = new ActiveMQConnectionFactory("mock:" + connector.getConnectUri() + "?closeAsync=false");
        pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);
        pooledFactory.setMaxConnections(1);
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
    public void testSendFailsAfterTransportFailure() throws Exception {
        var context = pooledFactory.createContext();
        try {
            var queue = context.createQueue("test.context.failure.send");
            context.createProducer().send(queue, "before failure");

            injectTransportFailure();

            // Failure propagation is async. Transitional failures may surface as the
            // generic JMSRuntimeException (ConnectionFailedException); once the pool's
            // ExceptionListener has closed the connection the settled state is the
            // mapped IllegalStateRuntimeException.
            assertTrue("Send should settle into IllegalStateRuntimeException after transport failure",
                Wait.waitFor(() -> {
                    try {
                        context.createProducer().send(queue, "should fail");
                        return false;
                    } catch (IllegalStateRuntimeException settled) {
                        return true;
                    } catch (JMSRuntimeException transitional) {
                        return false;
                    }
                }, 5000, 10));
        } finally {
            closeQuietly(context);
        }
    }

    @Test(timeout = 60000)
    public void testReceiveFailsAfterTransportFailure() throws Exception {
        var context = pooledFactory.createContext();
        try {
            var queue = context.createQueue("test.context.failure.receive");
            context.createProducer().send(queue, "delivered");

            var consumer = context.createConsumer(queue);
            assertEquals("delivered", consumer.receiveBody(String.class, 5000));

            injectTransportFailure();

            // Until the async teardown closes the consumer, receive can simply return
            // null (no broker interaction with an empty prefetch); afterwards it must
            // throw the mapped IllegalStateRuntimeException.
            assertTrue("Receive should settle into IllegalStateRuntimeException after transport failure",
                Wait.waitFor(() -> {
                    try {
                        consumer.receive(50);
                        return false;
                    } catch (IllegalStateRuntimeException settled) {
                        return true;
                    } catch (JMSRuntimeException transitional) {
                        return false;
                    }
                }, 5000, 10));
        } finally {
            closeQuietly(context);
        }
    }

    @Test(timeout = 60000)
    public void testPoolEvictsFailedConnectionAndRecovers() throws Exception {
        var before = probeUnderlyingConnection();

        var context = pooledFactory.createContext();
        var queue = context.createQueue("test.context.failure.recovery");
        context.createProducer().send(queue, "before failure");

        injectTransportFailure();

        assertTrue("Context should start failing after transport failure",
            Wait.waitFor(() -> {
                try {
                    context.createProducer().send(queue, "should fail");
                    return false;
                } catch (JMSRuntimeException expected) {
                    return true;
                }
            }, 5000, 10));

        closeQuietly(context);

        // reconnectOnException (default true) must evict the dead connection and
        // let the factory produce a fully working context again
        assertTrue("Pool should provide a working context after eviction",
            Wait.waitFor(() -> {
                try (var fresh = pooledFactory.createContext()) {
                    var freshQueue = fresh.createQueue("test.context.failure.recovery.fresh");
                    fresh.createProducer().send(freshQueue, "recovered");
                    try (var consumer = fresh.createConsumer(freshQueue)) {
                        return "recovered".equals(consumer.receiveBody(String.class, 2000));
                    }
                } catch (Exception e) {
                    return false;
                }
            }, 10000, 10));

        var after = probeUnderlyingConnection();
        assertNotSame("Failed connection should have been evicted from the pool", before, after);
    }

    @Test(timeout = 60000)
    public void testContextCloseAfterTransportFailureReleasesPool() throws Exception {
        var context = pooledFactory.createContext();
        var queue = context.createQueue("test.context.failure.close");
        context.createProducer().send(queue, "before failure");

        injectTransportFailure();

        assertTrue("Context should start failing after transport failure",
            Wait.waitFor(() -> {
                try {
                    context.createProducer().send(queue, "should fail");
                    return false;
                } catch (JMSRuntimeException expected) {
                    return true;
                }
            }, 5000, 10));

        // close may or may not throw depending on how far the async teardown got;
        // either way the pool references must be released
        closeQuietly(context);

        assertTrue("A fresh context must work after closing the failed one",
            Wait.waitFor(() -> {
                try (var fresh = pooledFactory.createContext()) {
                    fresh.createProducer().send(fresh.createQueue("test.context.failure.close.fresh"), "ok");
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }, 10000, 10));
    }

    @Test(timeout = 60000)
    public void testBrokerRestartNewContextRecovers() throws Exception {
        var context = pooledFactory.createContext();
        var queue = context.createQueue("test.context.failure.restart");
        context.createProducer().send(queue, "before restart");

        brokerService.stop();
        brokerService.waitUntilStopped();

        assertTrue("Context should fail after broker stop",
            Wait.waitFor(() -> {
                try {
                    context.createProducer().send(queue, "should fail");
                    return false;
                } catch (JMSRuntimeException expected) {
                    return true;
                }
            }, 10000, 10));

        closeQuietly(context);

        // restart on the same port so the pooled factory's URI stays valid; reassign
        // the inherited field so JmsPoolTestSupport.tearDown stops the new instance
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        brokerService.addConnector("tcp://localhost:" + brokerPort);
        brokerService.start();
        brokerService.waitUntilStarted();

        assertTrue("A fresh context must recover after broker restart",
            Wait.waitFor(() -> {
                try (var fresh = pooledFactory.createContext()) {
                    var freshQueue = fresh.createQueue("test.context.failure.restart.fresh");
                    fresh.createProducer().send(freshQueue, "recovered");
                    try (var consumer = fresh.createConsumer(freshQueue)) {
                        return "recovered".equals(consumer.receiveBody(String.class, 2000));
                    }
                } catch (Exception e) {
                    return false;
                }
            }, 15000, 10));
    }

    /**
     * Fires an IOException into the MockTransport of the pool's current underlying
     * connection, simulating a transport drop.
     */
    private void injectTransportFailure() throws Exception {
        var amqConnection = (ActiveMQConnection) probeUnderlyingConnection();
        var transport = (MockTransport) amqConnection.getTransportChannel().narrow(MockTransport.class);
        transport.onException(new IOException("simulated transport failure"));
    }

    /**
     * Borrows a pooled connection facade (sharing the same keyed ConnectionPool the
     * contexts use, maxConnections=1) to observe the current underlying connection.
     */
    private Connection probeUnderlyingConnection() throws Exception {
        var probe = (PooledConnection) pooledFactory.createConnection();
        try {
            return probe.getConnection();
        } finally {
            probe.close();
        }
    }

    private void closeQuietly(JMSContext context) {
        try {
            context.close();
        } catch (JMSRuntimeException ignored) {
            // closing a context whose transport already failed may legitimately throw
        }
    }
}
