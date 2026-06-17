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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TextMessage;
import java.util.Arrays;
import java.util.Collection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Response;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.vm.VMTransport;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version
 */
@RunWith(Parameterized.class)
public class JmsTempDestinationTest {

    @Parameters(name="allowTempDestinationStealing={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {false},
                {true},
        });
    }

    private static final Logger LOG = LoggerFactory.getLogger(JmsTempDestinationTest.class);
    private Connection connection;
    private ActiveMQConnectionFactory factory;
    protected List<Connection> connections = Collections.synchronizedList(new ArrayList<>());
    private BrokerService brokerService;
    private final boolean allowTempDestinationStealing;

    public JmsTempDestinationTest(boolean allowTempDestinationStealing) {
        this.allowTempDestinationStealing = allowTempDestinationStealing;
    }

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);

        PolicyEntry tempQueueEntry = new PolicyEntry();
        tempQueueEntry.setTempQueue(true);
        tempQueueEntry.setAllowTempDestinationStealing(allowTempDestinationStealing);
        PolicyEntry tempTopicEntry = new PolicyEntry();
        tempTopicEntry.setTempTopic(true);
        tempTopicEntry.setAllowTempDestinationStealing(allowTempDestinationStealing);

        PolicyMap pMap = new PolicyMap();
        pMap.setPolicyEntries(List.of(tempQueueEntry, tempTopicEntry));

        brokerService.setDestinationPolicy(pMap);
        brokerService.start();
        brokerService.waitUntilStarted();

        factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setAlwaysSyncSend(true);
        connection = factory.createConnection();
        connections.add(connection);
    }

    @After
    public void tearDown() throws Exception {
        for (Iterator<Connection> iter = connections.iterator(); iter.hasNext();) {
            Connection conn = iter.next();
            try {
                conn.close();
            } catch (Throwable e) {
            }
            iter.remove();
        }
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    /**
     * Make sure Temp destination can only be consumed by local connection
     *
     * @throws JMSException
     */
    @Test
    public void testTempDestOnlyConsumedByLocalConn() throws JMSException {
        connection.start();

        Session tempSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = tempSession.createTemporaryQueue();
        MessageProducer producer = tempSession.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = tempSession.createTextMessage("First");
        producer.send(message);

        // temp destination should not be consume when using another connection
        Connection otherConnection = factory.createConnection();
        connections.add(otherConnection);
        Session otherSession = otherConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue otherQueue = otherSession.createTemporaryQueue();
        MessageConsumer consumer = otherSession.createConsumer(otherQueue);
        Message msg = consumer.receive(2000);
        assertNull(msg);

        // should throw InvalidDestinationException when consuming a temp
        // destination from another connection.
        // Note that this check is done in the client side
        try {
            consumer = otherSession.createConsumer(queue);
            fail("Send should fail since temp destination should be used from another connection");
        } catch (InvalidDestinationException e) {
            assertTrue("failed to throw an exception", true);
        }

        // should be able to consume temp destination from the same connection
        consumer = tempSession.createConsumer(queue);
        msg = consumer.receive(2000);
        assertNotNull(msg);
    }

    // Test broker checks and enforces allowTempDestinationStealing flag
    @Test
    public void testAllowTempDestStealingQueue() throws Exception {
        testAllowTempDestStealing(false, allowTempDestinationStealing);
    }

    // Test broker checks and enforces allowTempDestinationStealing flag
    @Test
    public void testAllowTempDestStealingTopic() throws Exception {
        testAllowTempDestStealing(true, allowTempDestinationStealing);
    }

    // Test broker checks and enforces allowTempDestinationStealing flag
    private void testAllowTempDestStealing(boolean topic, boolean tempDestStealing) throws Exception {
        connection.start();

        // create a temporary queue on the first connection
        Session tempSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination tempDest = topic ? tempSession.createTemporaryTopic() :
                tempSession.createTemporaryQueue();

        // Create another connection/session
        ActiveMQConnection otherConnection = (ActiveMQConnection) factory.createConnection();
        connections.add(otherConnection);
        ActiveMQSession otherSession = (ActiveMQSession) otherConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Send a direct ConsumerInfo to bypass the client check that would normally block this
        // This will try and subscribe the second connection to the first connections
        // temporary dest
        ConsumerInfo info = new ConsumerInfo(otherSession.getNextConsumerId());
        info.setClientId(otherSession.connection.getClientID());
        info.setDestination(ActiveMQMessageTransformation.transformDestination(tempDest));
        Object result = otherConnection.getTransport().request(info, 1000);

        // The broker should allow because allowTempDestinationStealing = true
        if (tempDestStealing) {
            assertTrue(result instanceof Response);
            assertFalse(((Response) result).isException());
        } else {
            // The broker should throw an error because allowTempDestinationStealing = false
            assertTrue(result instanceof ExceptionResponse);
            assertTrue(((Response) result).isException());
            assertTrue(((ExceptionResponse) result).getException() instanceof InvalidDestinationException);
            assertTrue(((ExceptionResponse) result).getException().getMessage()
                    .contains("created by another connection is not permitted"));
        }
    }


    /**
     * Make sure that a temp queue does not drop message if there is an active
     * consumers.
     *
     * @throws JMSException
     */
    @Test
    public void testTempQueueHoldsMessagesWithConsumers() throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = session.createTextMessage("Hello");
        producer.send(message);

        Message message2 = consumer.receive(1000);
        assertNotNull(message2);
        assertTrue("Expected message to be a TextMessage", message2 instanceof TextMessage);
        assertEquals("Expected message to be a '" + message.getText() + "'",
                ((TextMessage) message2).getText(), message.getText());
    }

    /**
     * Make sure that a temp queue does not drop message if there are no active
     * consumers.
     *
     * @throws JMSException
     */
    @Test
    public void testTempQueueHoldsMessagesWithoutConsumers() throws JMSException {

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = session.createTextMessage("Hello");
        producer.send(message);

        connection.start();
        MessageConsumer consumer = session.createConsumer(queue);
        Message message2 = consumer.receive(1000);
        assertNotNull(message2);
        assertTrue("Expected message to be a TextMessage", message2 instanceof TextMessage);
        assertEquals("Expected message to be a '" + message.getText() + "'",
                ((TextMessage) message2).getText(), message.getText());

    }

    /**
     * Test temp queue works under load
     *
     * @throws JMSException
     */
    @Test
    public void testTmpQueueWorksUnderLoad() throws JMSException {
        int count = 500;
        int dataSize = 1024;

        ArrayList<BytesMessage> list = new ArrayList<>(count);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        byte[] data = new byte[dataSize];
        for (int i = 0; i < count; i++) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(data);
            message.setIntProperty("c", i);
            producer.send(message);
            list.add(message);
        }

        connection.start();
        MessageConsumer consumer = session.createConsumer(queue);
        for (int i = 0; i < count; i++) {
            Message message2 = consumer.receive(2000);
            assertNotNull(message2);
            assertEquals(i, message2.getIntProperty("c"));
            assertEquals(message2, list.get(i));
        }
    }

    /**
     * Make sure you cannot publish to a temp destination that does not exist
     * anymore.
     *
     * @throws JMSException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testPublishFailsForClosedConnection() throws Exception {
        // This test is slow and we only need to run this test with the default
        Assume.assumeFalse(allowTempDestinationStealing);

        Connection tempConnection = factory.createConnection();
        connections.add(tempConnection);
        Session tempSession = tempConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final TemporaryQueue queue = tempSession.createTemporaryQueue();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        final ActiveMQConnection activeMQConnection = (ActiveMQConnection) connection;
        assertTrue("creation advisory received in time with async dispatch",
                Wait.waitFor(() -> activeMQConnection.activeTempDestinations.contains(queue)));

        // This message delivery should work since the temp connection is still
        // open.
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = session.createTextMessage("First");
        producer.send(message);

        // Closing the connection should destroy the temp queue that was
        // created.
        tempConnection.close();
        Thread.sleep(5000); // Wait a little bit to let the delete take effect.

        // This message delivery NOT should work since the temp connection is
        // now closed.
        try {
            message = session.createTextMessage("Hello");
            producer.send(message);
            fail("Send should fail since temp destination should not exist anymore.");
        } catch (JMSException e) {
        }
    }

    /**
     * Make sure you cannot publish to a temp destination that does not exist
     * anymore.
     *
     * @throws JMSException
     * @throws InterruptedException
     */
    @Test
    public void testPublishFailsForDestroyedTempDestination() throws Exception {
        // This test is slow and we only need to run this test with the default
        Assume.assumeFalse(allowTempDestinationStealing);

        Connection tempConnection = factory.createConnection();
        connections.add(tempConnection);
        Session tempSession = tempConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final TemporaryQueue queue = tempSession.createTemporaryQueue();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        final ActiveMQConnection activeMQConnection = (ActiveMQConnection) connection;
        assertTrue("creation advisory received in time with async dispatch",
                Wait.waitFor((Wait.Condition) () -> activeMQConnection.activeTempDestinations.contains(queue)));

        // This message delivery should work since the temp connection is still
        // open.
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        TextMessage message = session.createTextMessage("First");
        producer.send(message);

        // deleting the Queue will cause sends to fail
        queue.delete();
        Thread.sleep(5000); // Wait a little bit to let the delete take effect.

        // This message delivery NOT should work since the temp connection is
        // now closed.
        try {
            message = session.createTextMessage("Hello");
            producer.send(message);
            fail("Send should fail since temp destination should not exist anymore.");
        } catch (JMSException e) {
            assertTrue("failed to throw an exception", true);
        }
    }

    /**
     * Test you can't delete a Destination with Active Subscribers
     *
     * @throws JMSException
     */
    @Test
    public void testDeleteDestinationWithSubscribersFails() throws JMSException {
        Connection connection = factory.createConnection();
        connections.add(connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = session.createTemporaryQueue();

        connection.start();

        session.createConsumer(queue);

        // This message delivery should NOT work since the temp connection is
        // now closed.
        try {
            queue.delete();
            fail("Should fail as Subscribers are active");
        } catch (JMSException e) {
            assertTrue("failed to throw an exception", true);
        }
    }

    @Test
    public void testSlowConsumerDoesNotBlockFastTempUsers() throws Exception {
        // This test is slow and we only need to run this test with the default
        Assume.assumeFalse(allowTempDestinationStealing);

        ActiveMQConnectionFactory advisoryConnFactory = new ActiveMQConnectionFactory("vm://localhost?asyncQueueDepth=20");
        Connection connection = advisoryConnFactory.createConnection();
        connections.add(connection);
        connection.start();

        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean ok = new AtomicBoolean(true);
        final AtomicBoolean first = new AtomicBoolean(true);
        VMTransport t = ((ActiveMQConnection)connection).getTransport().narrow(VMTransport.class);
        t.setTransportListener(new TransportListener() {
            @Override
            public void onCommand(Object command) {
                // block first dispatch for a while so broker backs up, but other connection should be able to proceed
                if (first.compareAndSet(true, false)) {
                    try {
                        ok.set(done.await(35, TimeUnit.SECONDS));
                        LOG.info("Done waiting: " + ok.get());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void onException(IOException error) {
            }

            @Override
            public void transportInterupted() {
            }

            @Override
            public void transportResumed() {
            }
        });

        connection = factory.createConnection();
        connections.add(connection);
        ((ActiveMQConnection)connection).setWatchTopicAdvisories(false);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        for (int i=0; i<2500; i++) {
            TemporaryQueue queue = session.createTemporaryQueue();
            MessageConsumer consumer = session.createConsumer(queue);
            consumer.close();
            queue.delete();
        }
        LOG.info("Done with work: " + ok.get());
        done.countDown();
        assertTrue("ok", ok.get());
    }
}
