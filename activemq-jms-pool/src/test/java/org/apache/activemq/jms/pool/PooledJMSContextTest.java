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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.Connection;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PooledJMSContextTest extends JmsPoolTestSupport {

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
        var connector = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        connectionUri = connector.getPublishableConnectString();
        factory = new ActiveMQConnectionFactory(connectionUri);
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
    public void testCreateContext() throws Exception {
        var context = pooledFactory.createContext();
        assertNotNull(context);
        context.close();
    }

    @Test(timeout = 60000)
    public void testCreateContextWithSessionMode() throws Exception {
        var context = pooledFactory.createContext(Session.CLIENT_ACKNOWLEDGE);
        assertNotNull(context);
        assertEquals(Session.CLIENT_ACKNOWLEDGE, context.getSessionMode());
        assertFalse(context.getTransacted());
        context.close();
    }

    @Test(timeout = 60000)
    public void testCreateContextTransacted() throws Exception {
        var context = pooledFactory.createContext(Session.SESSION_TRANSACTED);
        assertNotNull(context);
        assertEquals(Session.SESSION_TRANSACTED, context.getSessionMode());
        assertTrue(context.getTransacted());
        context.close();
    }

    @Test(timeout = 60000)
    public void testCreateContextWithCredentials() throws Exception {
        var context = pooledFactory.createContext(null, null);
        assertNotNull(context);
        context.close();
    }

    @Test(timeout = 60000)
    public void testCreateContextWithCredentialsAndSessionMode() throws Exception {
        var context = pooledFactory.createContext(null, null, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(context);
        context.close();
    }

    @Test(timeout = 60000)
    public void testAutoStart() throws Exception {
        try (var context = pooledFactory.createContext()) {
            assertTrue(context.getAutoStart());
            context.setAutoStart(false);
            assertFalse(context.getAutoStart());
        }
    }

    @Test(timeout = 60000)
    public void testStartStop() throws Exception {
        try (var context = pooledFactory.createContext()) {
            context.start();
            context.stop();
        }
    }

    @Test(timeout = 60000)
    public void testGetMetaData() throws Exception {
        try (var context = pooledFactory.createContext()) {
            assertNotNull(context.getMetaData());
        }
    }

    @Test(timeout = 60000)
    public void testCreateMessageTypes() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var msg = context.createMessage();
            assertNotNull(msg);

            var textMsg = context.createTextMessage();
            assertNotNull(textMsg);

            var textMsgWithBody = context.createTextMessage("hello");
            assertNotNull(textMsgWithBody);
            assertEquals("hello", textMsgWithBody.getText());

            var bytesMsg = context.createBytesMessage();
            assertNotNull(bytesMsg);

            var mapMsg = context.createMapMessage();
            assertNotNull(mapMsg);

            var objMsg = context.createObjectMessage();
            assertNotNull(objMsg);

            var streamMsg = context.createStreamMessage();
            assertNotNull(streamMsg);
        }
    }

    @Test(timeout = 60000)
    public void testCreateQueue() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.context.queue");
            assertNotNull(queue);
            assertEquals("test.context.queue", queue.getQueueName());
        }
    }

    @Test(timeout = 60000)
    public void testCreateTopic() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var topic = context.createTopic("test.context.topic");
            assertNotNull(topic);
            assertEquals("test.context.topic", topic.getTopicName());
        }
    }

    @Test(timeout = 60000)
    public void testCreateTemporaryQueue() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var tempQueue = context.createTemporaryQueue();
            assertNotNull(tempQueue);
        }
    }

    @Test(timeout = 60000)
    public void testCreateTemporaryTopic() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var tempTopic = context.createTemporaryTopic();
            assertNotNull(tempTopic);
        }
    }

    @Test(timeout = 60000)
    public void testCreateProducer() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var producer = context.createProducer();
            assertNotNull(producer);
        }
    }

    @Test(timeout = 60000)
    public void testCreateConsumer() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.context.consumer");
            var consumer = context.createConsumer(queue);
            assertNotNull(consumer);
            consumer.close();
        }
    }

    @Test(timeout = 60000)
    public void testCreateConsumerWithSelector() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.context.consumer.sel");
            var consumer = context.createConsumer(queue, "color = 'red'");
            assertNotNull(consumer);
            consumer.close();
        }
    }

    @Test(timeout = 60000)
    public void testCreateConsumerWithSelectorAndNoLocal() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var topic = context.createTopic("test.context.consumer.nolocal");
            var consumer = context.createConsumer(topic, null, true);
            assertNotNull(consumer);
            consumer.close();
        }
    }

    @Test(timeout = 60000)
    public void testSendAndReceive() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.context.sendrecv");
            context.createProducer().send(queue, "message body");

            try (var consumer = context.createConsumer(queue)) {
                var body = consumer.receiveBody(String.class, 5000);
                assertEquals("message body", body);
            }
        }
    }

    @Test(timeout = 60000)
    public void testTransactedCommit() throws Exception {
        try (var context = pooledFactory.createContext(Session.SESSION_TRANSACTED)) {
            var queue = context.createQueue("test.context.txcommit");
            context.createProducer().send(queue, "tx msg");
            context.commit();

            try (var consumer = context.createConsumer(queue)) {
                var body = consumer.receiveBody(String.class, 5000);
                assertEquals("tx msg", body);
            }
        }
    }

    @Test(timeout = 60000)
    public void testTransactedRollback() throws Exception {
        try (var txContext = pooledFactory.createContext(Session.SESSION_TRANSACTED)) {
            var queue = txContext.createQueue("test.context.txrollback");
            txContext.createProducer().send(queue, "will be rolled back");
            txContext.rollback();
        }

        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.context.txrollback");
            try (var consumer = context.createConsumer(queue)) {
                var msg = consumer.receiveNoWait();
                assertTrue("Message should have been rolled back", msg == null);
            }
        }
    }

    @Test(timeout = 60000)
    public void testChildContext() throws Exception {
        var parent = pooledFactory.createContext();
        var child = parent.createContext(Session.AUTO_ACKNOWLEDGE);
        assertNotNull(child);

        var queue = child.createQueue("test.context.child");
        child.createProducer().send(queue, "from child");

        try (var consumer = child.createConsumer(queue)) {
            var body = consumer.receiveBody(String.class, 5000);
            assertEquals("from child", body);
        }

        child.close();
        parent.close();
    }

    @Test(timeout = 60000)
    public void testDoubleCloseIsHarmless() throws Exception {
        var context = pooledFactory.createContext();
        context.close();
        context.close();
    }

    @Test(timeout = 60000, expected = IllegalStateRuntimeException.class)
    public void testUseAfterCloseThrows() throws Exception {
        var context = pooledFactory.createContext();
        context.close();
        context.createQueue("should.fail");
    }

    @Test(timeout = 60000)
    public void testCreateDurableConsumer() throws Exception {
        try (var context = pooledFactory.createContext()) {
            context.setClientID("durable-test-client");
            var topic = context.createTopic("test.context.durable");
            var consumer = context.createDurableConsumer(topic, "durSub");
            assertNotNull(consumer);
            consumer.close();
            context.unsubscribe("durSub");
        }
    }

    @Test(timeout = 60000)
    public void testCreateBrowser() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.context.browser");
            context.createProducer().send(queue, "browse me");

            var browser = context.createBrowser(queue);
            assertNotNull(browser);
            browser.close();
        }
    }

    @Test(timeout = 60000)
    public void testCreateBrowserWithSelector() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.context.browser.sel");
            var browser = context.createBrowser(queue, "type = 'A'");
            assertNotNull(browser);
            browser.close();
        }
    }

    @Test(timeout = 60000)
    public void testExceptionListener() throws Exception {
        try (var context = pooledFactory.createContext()) {
            context.setExceptionListener(e -> LOG.warn("Exception: {}", e.getMessage()));
            assertNotNull(context.getExceptionListener());
        }
    }

    @Test(timeout = 60000)
    public void testSessionExhaustionThroughContexts() throws Exception {
        pooledFactory.setMaximumActiveSessionPerConnection(1);
        pooledFactory.setBlockIfSessionPoolIsFull(false);

        var context1 = pooledFactory.createContext();
        var context2 = pooledFactory.createContext();
        try {
            var queue = context1.createQueue("test.context.exhaustion");
            context1.createProducer().send(queue, "one");

            // context1 holds the only pooled session, so context2 cannot get one
            try {
                context2.createProducer();
                fail("Expected session pool exhaustion to surface as IllegalStateRuntimeException");
            } catch (IllegalStateRuntimeException expected) {
            }

            // closing context1 returns its session to the pool and unblocks context2
            context1.close();
            context2.createProducer().send(queue, "two");

            try (var consumer = context2.createConsumer(queue)) {
                assertEquals("one", consumer.receiveBody(String.class, 5000));
                assertEquals("two", consumer.receiveBody(String.class, 5000));
            }
        } finally {
            context1.close();
            context2.close();
        }
    }

    @Test(timeout = 60000)
    public void testExpiredConnectionEvictsFromPoolWithContexts() throws Exception {
        pooledFactory.setExpiryTimeout(10);

        var before = probeUnderlyingConnection();

        var context = pooledFactory.createContext();
        var queue = context.createQueue("test.context.expiry");
        context.createProducer().send(queue, "before expiry");

        // let the underlying connection expire while the context still holds it
        TimeUnit.MILLISECONDS.sleep(500);
        context.close();

        // the next context must trigger eviction and get a fresh underlying connection
        try (var context2 = pooledFactory.createContext()) {
            context2.createProducer().send(context2.createQueue("test.context.expiry"), "after expiry");
            var after = probeUnderlyingConnection();
            assertNotSame("expired connection should have been evicted from the pool", before, after);
        }
    }

    /**
     * Borrows a pooled connection facade from the factory (sharing the same
     * keyed ConnectionPool the contexts use) to observe the identity of the
     * current underlying provider connection.
     */
    private Connection probeUnderlyingConnection() throws Exception {
        var probe = (PooledConnection) pooledFactory.createConnection();
        try {
            return probe.getConnection();
        } finally {
            probe.close();
        }
    }

    @Test(timeout = 60000)
    public void testAcknowledgeAcksConsumedMessages() throws Exception {
        try (var context = pooledFactory.createContext(Session.CLIENT_ACKNOWLEDGE)) {
            var queue = context.createQueue("test.context.acknowledge");
            context.createProducer().send(queue, "ack me");

            try (var consumer = context.createConsumer(queue)) {
                var received = consumer.receive(5000);
                assertNotNull(received);
                context.acknowledge();
            }
        }

        // the acknowledged message must not be redelivered to a new consumer
        try (var context = pooledFactory.createContext(Session.CLIENT_ACKNOWLEDGE)) {
            var queue = context.createQueue("test.context.acknowledge");
            try (var consumer = context.createConsumer(queue)) {
                assertNull("Acknowledged message must not be redelivered", consumer.receive(1000));
            }
        }
    }

    @Test(timeout = 60000)
    public void testAcknowledgeWithoutReceiveIsNoOp() throws Exception {
        try (var context = pooledFactory.createContext(Session.CLIENT_ACKNOWLEDGE)) {
            context.acknowledge();
        }
    }

    @Test(timeout = 60000)
    public void testCommitOnNonTransactedThrowsIllegalStateRuntimeException() throws Exception {
        try (var context = pooledFactory.createContext()) {
            try {
                context.commit();
                fail("Expected IllegalStateRuntimeException");
            } catch (IllegalStateRuntimeException expected) {
            }
        }
    }

    @Test(timeout = 60000)
    public void testCreateContextAfterFactoryStopThrows() throws Exception {
        var stopped = new PooledConnectionFactory();
        stopped.setConnectionFactory(factory);
        stopped.stop();
        try {
            stopped.createContext();
            fail("Expected IllegalStateRuntimeException");
        } catch (IllegalStateRuntimeException expected) {
        }
    }

    @Test(timeout = 60000)
    public void testStartAfterStop() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.context.startafterstop");
            context.stop();
            context.start();

            context.createProducer().send(queue, "after restart");
            try (var consumer = context.createConsumer(queue)) {
                assertEquals("after restart", consumer.receiveBody(String.class, 5000));
            }
        }
    }

    @Test(timeout = 60000)
    public void testReceiveBodyWrongTypeThrowsMessageFormatRuntimeException() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var queue = context.createQueue("test.context.receivebody.wrongtype");
            context.createProducer().send(queue, "not a long");
            try (var consumer = context.createConsumer(queue)) {
                try {
                    consumer.receiveBody(Long.class, 5000);
                    fail("Expected MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException expected) {
                }
            }
        }
    }

    @Test(timeout = 60000)
    public void testCreateSharedConsumerUnsupportedByClient() throws Exception {
        try (var context = pooledFactory.createContext()) {
            var topic = context.createTopic("test.context.shared");
            try {
                context.createSharedConsumer(topic, "sharedSub");
                fail("Expected UnsupportedOperationException from ActiveMQ Classic client");
            } catch (UnsupportedOperationException expected) {
            }
        }
    }

    @Test(timeout = 60000)
    public void testConcurrentChildCloseDoesNotReleaseParentConnection() throws Exception {
        try (var parent = pooledFactory.createContext()) {
            for (int i = 0; i < 25; i++) {
                final var child = parent.createContext(Session.AUTO_ACKNOWLEDGE);
                final var barrier = new CyclicBarrier(2);
                Runnable closer = () -> {
                    try {
                        barrier.await(5, TimeUnit.SECONDS);
                    } catch (Exception ignored) {
                    }
                    child.close();
                };
                var t1 = new Thread(closer);
                var t2 = new Thread(closer);
                t1.start();
                t2.start();
                t1.join(10000);
                t2.join(10000);

                assertNotNull("Parent context must stay usable after concurrent child close (iteration " + i + ")",
                    parent.createTextMessage());
            }
        }
    }

    @Test(timeout = 60000)
    public void testCreateProducerWithoutAnonymousProducers() throws Exception {
        var nonAnon = new PooledConnectionFactory();
        nonAnon.setConnectionFactory(factory);
        nonAnon.setMaxConnections(1);
        nonAnon.setUseAnonymousProducers(false);
        try {
            try (var context = nonAnon.createContext()) {
                var queue = context.createQueue("test.context.nonanon");
                for (int i = 0; i < 5; i++) {
                    context.createProducer().send(queue, "msg" + i);
                }
                try (var consumer = context.createConsumer(queue)) {
                    for (int i = 0; i < 5; i++) {
                        assertNotNull(consumer.receive(5000));
                    }
                }
            }
        } finally {
            nonAnon.stop();
        }
    }

    @Test(timeout = 60000)
    public void testCreateProducerDoesNotStartConnection() throws Exception {
        var pool = new ConnectionPool(factory.createConnection());
        pool.incrementReferenceCount();
        var connection = new PooledConnection(pool);

        var context = new PooledJMSContext(connection, Session.AUTO_ACKNOWLEDGE);
        try {
            context.createProducer();
            assertFalse("Creating a producer must not auto-start the connection",
                ((ActiveMQConnection) connection.getConnection()).isStarted());

            var queue = context.createQueue("test.context.autostart");
            context.createConsumer(queue).close();
            assertTrue("Creating a consumer must auto-start the connection",
                ((ActiveMQConnection) connection.getConnection()).isStarted());
        } finally {
            context.close();
        }
    }

    @Test(timeout = 60000)
    public void testConnectionReleasedWhenSessionCloseFails() throws Exception {
        var pool = new ConnectionPool(factory.createConnection());
        pool.incrementReferenceCount();
        var connection = new SessionCloseFailsConnection(pool);

        var context = new PooledJMSContext(connection, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(context.createTextMessage()); // force lazy session creation

        try {
            context.close();
            fail("Expected JMSRuntimeException from failing session close");
        } catch (JMSRuntimeException expected) {
        }

        assertTrue("Connection must be released even when session close fails",
            connection.connectionClosed.get());
    }

    private static class SessionCloseFailsConnection extends PooledConnection {

        final AtomicBoolean connectionClosed = new AtomicBoolean();

        SessionCloseFailsConnection(ConnectionPool pool) {
            super(pool);
        }

        @Override
        public Session createSession(boolean transacted, int ackMode) throws JMSException {
            final var real = super.createSession(transacted, ackMode);
            return (Session) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] { Session.class },
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        throw new JMSException("Simulated session close failure");
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        }

        @Override
        public void close() throws JMSException {
            connectionClosed.set(true);
            super.close();
        }
    }
}
