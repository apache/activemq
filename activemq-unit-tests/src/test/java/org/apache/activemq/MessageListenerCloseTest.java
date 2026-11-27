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
package org.apache.activemq;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Closing or stopping from inside a MessageListener. JMS says the call must
 * fail with IllegalStateException, which ActiveMQ only does under strict
 * compliance. Without it the call goes through, as it always has.
 */
public class MessageListenerCloseTest {

    private interface ListenerCall {
        void run(Connection connection, Session session) throws Exception;
    }

    private BrokerService broker;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("vm://localhost");
        broker.start();
        broker.waitUntilStarted();
        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testConnectionCloseFromListenerIsAllowed() throws Exception {
        assertNull(callFromListener(false, (connection, session) -> connection.close()));
    }

    @Test(timeout = 60000)
    public void testConnectionStopFromListenerIsAllowed() throws Exception {
        assertNull(callFromListener(false, (connection, session) -> connection.stop()));
    }

    @Test(timeout = 60000)
    public void testSessionCloseFromListenerIsAllowed() throws Exception {
        assertNull(callFromListener(false, (connection, session) -> session.close()));
    }

    @Test(timeout = 60000)
    public void testStrictConnectionCloseFromListenerIsRefused() throws Exception {
        assertTrue(callFromListener(true, (connection, session) -> connection.close())
            instanceof IllegalStateException);
    }

    @Test(timeout = 60000)
    public void testStrictConnectionStopFromListenerIsRefused() throws Exception {
        assertTrue(callFromListener(true, (connection, session) -> connection.stop())
            instanceof IllegalStateException);
    }

    @Test(timeout = 60000)
    public void testStrictSessionCloseFromListenerIsRefused() throws Exception {
        assertTrue(callFromListener(true, (connection, session) -> session.close())
            instanceof IllegalStateException);
    }

    // runs the call inside onMessage and returns what it threw, or null
    private Throwable callFromListener(final boolean strictCompliance, final ListenerCall call) throws Exception {
        final var factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(strictCompliance);

        final var thrown = new AtomicReference<Throwable>();
        final var called = new CountDownLatch(1);

        final var connection = factory.createConnection();
        try {
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var queue = session.createQueue("test.listener.close");
            session.createConsumer(queue).setMessageListener(message -> {
                try {
                    call.run(connection, session);
                } catch (Throwable t) {
                    thrown.set(t);
                } finally {
                    called.countDown();
                }
            });
            connection.start();

            try (var sender = factory.createConnection()) {
                final var sendSession = sender.createSession(false, Session.AUTO_ACKNOWLEDGE);
                sendSession.createProducer(sendSession.createQueue("test.listener.close"))
                    .send(sendSession.createMessage());
            }

            assertTrue("the listener should have returned from the call", called.await(30, TimeUnit.SECONDS));
        } finally {
            connection.close();
        }
        return thrown.get();
    }
}
