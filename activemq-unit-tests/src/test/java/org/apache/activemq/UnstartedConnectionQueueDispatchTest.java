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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A queue consumer whose connection has never been started must not attract
 * push dispatch. Historically the broker round-robins messages into such a
 * consumer's prefetch, where they sit in the client's held dispatch channel
 * and starve the consumers that are actually running (seen as the TCK
 * queueReceiveTests hang).
 */
public class UnstartedConnectionQueueDispatchTest {

    private static final int MESSAGE_COUNT = 10;

    private BrokerService broker;
    private String connectionUri;
    private Connection startedConnection;
    private Connection unstartedConnection;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(false);
        broker.addConnector("vm://localhost");
        broker.start();
        broker.waitUntilStarted();
        connectionUri = "vm://localhost";
    }

    @After
    public void tearDown() throws Exception {
        if (startedConnection != null) {
            try { startedConnection.close(); } catch (Exception ignored) {}
        }
        if (unstartedConnection != null) {
            try { unstartedConnection.close(); } catch (Exception ignored) {}
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private ActiveMQConnectionFactory createFactory() {
        var factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setDeferPrefetchUntilStarted(true);
        return factory;
    }

    @Test(timeout = 60000)
    public void testStartedConsumerReceivesAllMessagesDespiteUnstartedCompetitor() throws Exception {
        var factory = createFactory();

        // competing consumer on a connection that is never started
        unstartedConnection = factory.createConnection();
        var unstartedSession = unstartedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var queue = unstartedSession.createQueue("test.unstarted.dispatch");
        var neverStarted = unstartedSession.createConsumer(queue);
        assertNotNull(neverStarted);

        // active consumer on a started connection
        startedConnection = factory.createConnection();
        startedConnection.start();
        var session = startedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var active = session.createConsumer(queue);

        var producer = session.createProducer(queue);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            producer.send(session.createTextMessage("message-" + i));
        }

        // every message must reach the running consumer; none may park in the
        // never-started consumer's prefetch
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            var received = active.receive(5000);
            assertNotNull("Message " + i + " was dispatched to the never-started consumer", received);
        }
    }

    @Test(timeout = 60000)
    public void testDeferredConsumerReceivesMessageReleasedByClosedConsumer() throws Exception {
        // TCK core/queueConnection connNotStartedQueueTest shape: a started
        // receiver prefetches two messages and consumes one; closing it returns
        // the other to the queue; a receiver on a never-started connection must
        // see nothing until start, then receive the released message.
        var factory = createFactory();

        startedConnection = factory.createConnection();
        startedConnection.start();
        var session = startedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var queue = session.createQueue("test.unstarted.released");
        var first = session.createConsumer(queue);
        var producer = session.createProducer(queue);
        producer.send(session.createTextMessage("one"));
        producer.send(session.createTextMessage("two"));
        assertNotNull(first.receive(5000));
        first.close();

        unstartedConnection = factory.createConnection();
        var unstartedSession = unstartedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var second = unstartedSession.createConsumer(queue);
        assertNull("No delivery before the connection is started", second.receive(1000));

        unstartedConnection.start();
        assertNotNull("Released message must be delivered once the connection starts", second.receive(5000));
    }

    @Test(timeout = 60000)
    public void testDeferredConsumerReceivesAfterConnectionStart() throws Exception {
        var factory = createFactory();

        unstartedConnection = factory.createConnection();
        var session = unstartedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var queue = session.createQueue("test.unstarted.recovery");
        var consumer = session.createConsumer(queue);

        // the async pattern: listener registered before the connection starts
        final var delivered = new CountDownLatch(MESSAGE_COUNT);
        consumer.setMessageListener(message -> delivered.countDown());

        startedConnection = factory.createConnection();
        startedConnection.start();
        var producerSession = startedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        var producer = producerSession.createProducer(queue);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            producer.send(producerSession.createTextMessage("message-" + i));
        }

        // nothing may be delivered while the connection is not started
        assertTrue("Messages must not be delivered before start", delivered.getCount() == MESSAGE_COUNT);

        // starting the connection restores the prefetch credit and delivery flows
        unstartedConnection.start();
        assertTrue("Messages should be delivered after connection start",
            delivered.await(10, TimeUnit.SECONDS));
    }
}
