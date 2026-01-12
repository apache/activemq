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

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import static org.junit.Assert.assertEquals;

/**
 * AMQ-9829 Track prefetched messages for duplicate suppression during failover
 *
 * Test for clearMessagesInProgress() behavior in transacted sessions. Bug revealed with FailoverDurableSubTransactionTest
 *
 * This test verifies that when clearMessagesInProgress() is called (during failover),
 * BOTH delivered messages AND prefetched (unconsumed) messages are tracked in
 * previouslyDeliveredMessages for duplicate suppression.
 */
@Category(ParallelTest.class)
public class ActiveMQMessageConsumerClearMessagesTest {

    private BrokerService brokerService;
    private String brokerURI;

    @Rule
    public TestName name = new TestName();

    @Before
    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        brokerURI = "vm://localhost?create=false";
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    /**
     * Helper to create a TestableConsumer for the given session and destination.
     */
    private TestableConsumer createTestableConsumer(final ActiveMQSession session, final Destination destination) throws Exception {
        final ConsumerId consumerId = session.getNextConsumerId();

        return new TestableConsumer(
            session,
            consumerId,
            ActiveMQMessageTransformation.transformDestination(destination),
            null,  // name (not durable)
            null,  // selector
            1000,  // prefetch
            -1,    // maximumPendingMessageCount
            false, // noLocal
            false, // browser
            true,  // dispatchAsync
            null   // messageListener
        );
    }

    /**
     * Test that clearMessagesInProgress() captures both delivered AND unconsumed messages
     * into previouslyDeliveredMessages for transacted sessions.
     *
     * Scenario:
     * 1. Create transacted consumer
     * 2. Receive 3 messages (they go to deliveredMessages since session is transacted)
     * 3. Manually add 5 messages to unconsumedMessages (simulating prefetched but not yet dispatched)
     * 4. Trigger clearMessagesInProgress() (simulating transport interrupt/failover)
     * 5. WITHOUT FIX: Only 3 messages tracked in previouslyDeliveredMessages
     * 6. WITH FIX: All 8 messages (3 delivered + 5 prefetched) tracked in previouslyDeliveredMessages
     */
    @Test
    public void testClearMessagesInProgressCapturesPrefetchedMessages() throws Exception {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);

        final Connection connection = factory.createConnection();
        connection.start();

        // Create TRANSACTED session - this is critical for the bug
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        final Destination destination = session.createQueue(name.getMethodName());

        // Create testable consumer that exposes protected fields
        final TestableConsumer consumer = createTestableConsumer((ActiveMQSession) session, destination);

        // Produce 3 messages that will be received and go to deliveredMessages
        final MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < 3; i++) {
            producer.send(session.createTextMessage("Delivered message " + i));
        }
        session.commit();

        // Receive the 3 messages (they stay in deliveredMessages because session is transacted)
        for (int i = 0; i < 3; i++) {
            consumer.receive(1000);
        }

        // Now manually add 5 messages to unconsumedMessages to simulate prefetched messages
        // that haven't been dispatched to the MessageListener yet
        for (int i = 0; i < 5; i++) {
            final ActiveMQTextMessage msg = new ActiveMQTextMessage();
            msg.setMessageId(new MessageId("TEST:1:1:1:" + (100 + i)));
            msg.setText("Prefetched message " + i);

            final MessageDispatch dispatch = new MessageDispatch();
            dispatch.setConsumerId(consumer.getConsumerId());
            dispatch.setMessage(msg);

            // Add to unconsumedMessages buffer (simulating prefetch)
            consumer.addToUnconsumedMessages(dispatch);
        }

        // Verify setup: should have 3 delivered, 5 unconsumed, 0 previously delivered
        assertEquals("Should have 3 delivered messages", 3, consumer.getDeliveredMessagesSize());
        assertEquals("Should have 5 unconsumed (prefetched) messages", 5, consumer.getUnconsumedMessagesSize());
        assertEquals("Should have 0 previously delivered before clearMessagesInProgress",
                     0, consumer.getPreviouslyDeliveredMessagesSize());

        // Now simulate transport interrupt / failover
        consumer.triggerTransportInterrupt();

        // The key assertion: without the fix for AMQ-9829, it would contain all delivered but the prefetched
        final int previouslyDeliveredCount = consumer.getPreviouslyDeliveredMessagesSize();

        assertEquals("previouslyDeliveredMessages should contain BOTH delivered (3) and prefetched (5) messages",
                     8, previouslyDeliveredCount);

        // Verify unconsumed buffer was cleared
        assertEquals("unconsumedMessages should be cleared after clearMessagesInProgress",
                     0, consumer.getUnconsumedMessagesSize());

        connection.close();
    }

    /**
     * Test that clearMessagesInProgress() works correctly for NON-transacted sessions.
     * In non-transacted sessions, previouslyDeliveredMessages should not be populated.
     */
    @Test
    public void testClearMessagesInProgressNonTransacted() throws Exception {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);

        final Connection connection = factory.createConnection();
        connection.start();

        // Create NON-transacted session
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Destination destination = session.createQueue(name.getMethodName());

        final TestableConsumer consumer = createTestableConsumer((ActiveMQSession) session, destination);

        // Simulate 5 prefetched messages (no broker messages needed for this test)
        for (int i = 0; i < 5; i++) {
            final ActiveMQTextMessage msg = new ActiveMQTextMessage();
            msg.setMessageId(new MessageId("TEST:1:1:1:" + (200 + i)));
            msg.setText("Prefetched message " + i);

            final MessageDispatch dispatch = new MessageDispatch();
            dispatch.setConsumerId(consumer.getConsumerId());
            dispatch.setMessage(msg);

            consumer.addToUnconsumedMessages(dispatch);
        }

        assertEquals("Should have 5 unconsumed messages", 5, consumer.getUnconsumedMessagesSize());

        // Trigger clearMessagesInProgress
        consumer.triggerTransportInterrupt();

        // For non-transacted sessions, previouslyDeliveredMessages should remain 0
        // (no duplicate suppression needed for non-transacted)
        assertEquals("previouslyDeliveredMessages should be 0 for non-transacted session",
                     0, consumer.getPreviouslyDeliveredMessagesSize());

        connection.close();
    }

    /**
     * Test edge case: clearMessagesInProgress() with only delivered messages (no prefetch).
     */
    @Test
    public void testClearMessagesInProgressOnlyDeliveredMessages() throws Exception {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);

        final Connection connection = factory.createConnection();
        connection.start();

        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        final Destination destination = session.createQueue(name.getMethodName());

        final TestableConsumer consumer = createTestableConsumer((ActiveMQSession) session, destination);

        // Produce and receive 3 messages - they go to deliveredMessages
        final MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < 3; i++) {
            producer.send(session.createTextMessage("Delivered message " + i));
        }
        session.commit();

        // Receive all 3 messages (they stay in deliveredMessages)
        for (int i = 0; i < 3; i++) {
            consumer.receive(1000);
        }

        // Don't add any to unconsumedMessages - testing only delivered messages case
        assertEquals("Should have 3 delivered messages", 3, consumer.getDeliveredMessagesSize());
        assertEquals("Should have 0 unconsumed messages", 0, consumer.getUnconsumedMessagesSize());

        // Trigger clearMessagesInProgress
        consumer.triggerTransportInterrupt();

        // Should have captured the 3 delivered messages
        assertEquals("Should have captured 3 delivered messages",
                     3, consumer.getPreviouslyDeliveredMessagesSize());

        connection.close();
    }

    /**
     * Test edge case: clearMessagesInProgress() with only prefetched messages (none delivered yet).
     * This is the most direct test for the bug!
     */
    @Test
    public void testClearMessagesInProgressOnlyPrefetchedMessages() throws Exception {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);

        final Connection connection = factory.createConnection();
        connection.start();

        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        final Destination destination = session.createQueue(name.getMethodName());

        final TestableConsumer consumer = createTestableConsumer((ActiveMQSession) session, destination);

        // Simulate only prefetched messages (none delivered/dispatched yet)
        // Don't produce any messages to the broker - we're only testing the prefetch scenario
        for (int i = 0; i < 7; i++) {
            final ActiveMQTextMessage msg = new ActiveMQTextMessage();
            msg.setMessageId(new MessageId("TEST:1:1:1:" + (300 + i)));
            msg.setText("Prefetched message " + i);

            final MessageDispatch dispatch = new MessageDispatch();
            dispatch.setConsumerId(consumer.getConsumerId());
            dispatch.setMessage(msg);

            consumer.addToUnconsumedMessages(dispatch);
        }

        assertEquals("Should have 0 delivered messages", 0, consumer.getDeliveredMessagesSize());
        assertEquals("Should have 7 unconsumed messages", 7, consumer.getUnconsumedMessagesSize());

        // Trigger clearMessagesInProgress
        consumer.triggerTransportInterrupt();

        // The key assertion, without the fix for AMQ-9829 it would be 0
        assertEquals("Should have captured all 7 prefetched messages",
                     7, consumer.getPreviouslyDeliveredMessagesSize());

        connection.close();
    }

    /**
     * Test subclass that exposes protected fields for testing clearMessagesInProgress().
     * This avoids adding too many test methods in ActiveMQMessageCOnsumer
     */
    static class TestableConsumer extends ActiveMQMessageConsumer {

        TestableConsumer(final ActiveMQSession session, final ConsumerId consumerId,
                        final ActiveMQDestination destination, final String name,
                        final String selector, final int prefetch, final int maximumPendingMessageCount,
                        final boolean noLocal, final boolean browser, final boolean dispatchAsync,
                        final MessageListener messageListener) throws Exception {
            super(session, consumerId, destination, name, selector, prefetch,
                  maximumPendingMessageCount, noLocal, browser, dispatchAsync, messageListener);
        }

        public int getUnconsumedMessagesSize() {
            synchronized(unconsumedMessages.getMutex()) {
                return unconsumedMessages.size();
            }
        }

        public int getDeliveredMessagesSize() {
            synchronized(deliveredMessages) {
                return deliveredMessages.size();
            }
        }

        public void addToUnconsumedMessages(final MessageDispatch dispatch) {
            synchronized(unconsumedMessages.getMutex()) {
                unconsumedMessages.enqueue(dispatch);
            }
        }

        public void triggerTransportInterrupt() {
            inProgressClearRequired();
            clearMessagesInProgress();
        }

        @Override
        public int getPreviouslyDeliveredMessagesSize() {
            return super.getPreviouslyDeliveredMessagesSize();
        }
    }
}
