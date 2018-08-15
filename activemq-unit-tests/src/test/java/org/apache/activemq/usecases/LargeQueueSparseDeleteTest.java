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

package org.apache.activemq.usecases;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test creates a fixed size queue and moves the last message in the
 * queue to another queue. The test is used to very the performance of
 * {@link org.apache.activemq.broker.region.Queue#moveMatchingMessagesTo(org.apache.activemq.broker.ConnectionContext, String, org.apache.activemq.command.ActiveMQDestination)}.
 */
public class LargeQueueSparseDeleteTest extends EmbeddedBrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(LargeQueueSparseDeleteTest.class);

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setUp() throws Exception {
        super.useTopic = false;
        super.setUp();
    }

    /**
     * The test queue is filled with QUEUE_SIZE test messages, each with a
     * numeric id property beginning at 0. Once the queue is filled, the last
     * message (id = QUEUE_SIZE-1) is moved to another queue. The test succeeds
     * if the move completes within TEST_TIMEOUT milliseconds.
     *
     * @throws Exception
     */
    public void testMoveMessages() throws Exception {
        final int QUEUE_SIZE = 30000;
        final String MOVE_TO_DESTINATION_NAME = getDestinationString()
                + ".dest";
        final long TEST_TIMEOUT = 20000;

        // Populate a test queue with uniquely-identifiable messages.
        Connection conn = createConnection();
        try {
            conn.start();
            Session session = conn.createSession(true,
                    Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(destination);
            for (int i = 0; i < QUEUE_SIZE; i++) {
                Message message = session.createMessage();
                message.setIntProperty("id", i);
                producer.send(message);
            }
            session.commit();
        } finally {
            conn.close();
        }

        // Access the implementation of the test queue and move the last message
        // to another queue. Verify that the move occurred within the limits of
        // the test.
        Queue queue = (Queue) broker.getRegionBroker().getDestinationMap().get(
                destination);

        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker.getBroker());
        context.getMessageEvaluationContext().setDestination(destination);

        long startTimeMillis = System.currentTimeMillis();
        Assert.assertEquals(1, queue
                .moveMatchingMessagesTo(context, "id=" + (QUEUE_SIZE - 1),
                        createDestination(MOVE_TO_DESTINATION_NAME)));

        long durationMillis = System.currentTimeMillis() - startTimeMillis;

        LOG.info("It took " + durationMillis
                + "ms to move the last message from a queue a " + QUEUE_SIZE
                + " messages.");

        Assert.assertTrue("Moving the message took too long: " + durationMillis
                + "ms", durationMillis < TEST_TIMEOUT);
    }

    public void testCopyMessages() throws Exception {
        final int QUEUE_SIZE = 30000;
        final String MOVE_TO_DESTINATION_NAME = getDestinationString()
                + ".dest";
        final long TEST_TIMEOUT = 10000;

        // Populate a test queue with uniquely-identifiable messages.
        Connection conn = createConnection();
        try {
            conn.start();
            Session session = conn.createSession(true,
                    Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(destination);
            for (int i = 0; i < QUEUE_SIZE; i++) {
                Message message = session.createMessage();
                message.setIntProperty("id", i);
                producer.send(message);
            }
            session.commit();
        } finally {
            conn.close();
        }

        // Access the implementation of the test queue and move the last message
        // to another queue. Verify that the move occurred within the limits of
        // the test.
        Queue queue = (Queue) broker.getRegionBroker().getDestinationMap().get(
                destination);

        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker.getBroker());
        context.getMessageEvaluationContext().setDestination(destination);

        long startTimeMillis = System.currentTimeMillis();
        Assert.assertEquals(1,
            queue.copyMatchingMessagesTo(context, "id=" + (QUEUE_SIZE - 1), createDestination(MOVE_TO_DESTINATION_NAME)));

        long durationMillis = System.currentTimeMillis() - startTimeMillis;

        LOG.info("It took " + durationMillis
                + "ms to copy the last message from a queue a " + QUEUE_SIZE
                + " messages.");

        Assert.assertTrue("Copying the message took too long: " + durationMillis
                + "ms", durationMillis < TEST_TIMEOUT);
    }

    public void testRemoveMessages() throws Exception {
        final int QUEUE_SIZE = 30000;
        final long TEST_TIMEOUT = 20000;

        // Populate a test queue with uniquely-identifiable messages.
        Connection conn = createConnection();
        try {
            conn.start();
            Session session = conn.createSession(true,
                    Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(destination);
            for (int i = 0; i < QUEUE_SIZE; i++) {
                Message message = session.createMessage();
                message.setIntProperty("id", i);
                producer.send(message);
            }
            session.commit();
        } finally {
            conn.close();
        }

        // Access the implementation of the test queue and move the last message
        // to another queue. Verify that the move occurred within the limits of
        // the test.
        Queue queue = (Queue) broker.getRegionBroker().getDestinationMap().get(
                destination);

        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker.getBroker());
        context.getMessageEvaluationContext().setDestination(destination);

        long startTimeMillis = System.currentTimeMillis();
        Assert.assertEquals(1,
            queue.removeMatchingMessages("id=" + (QUEUE_SIZE - 1)));

        long durationMillis = System.currentTimeMillis() - startTimeMillis;

        LOG.info("It took " + durationMillis
                + "ms to remove the last message from a queue a " + QUEUE_SIZE
                + " messages.");

        Assert.assertTrue("Removing the message took too long: " + durationMillis
                + "ms", durationMillis < TEST_TIMEOUT);
    }
}
