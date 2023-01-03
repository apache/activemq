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
package org.apache.activemq.broker.scheduler;

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.DestinationInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Ensure that when a queue gets created for the first time, by writing a message to the queue which has the
 * {@link ScheduledMessage#AMQ_SCHEDULED_DELAY} header, that the Queue Advisory message gets published to the
 * {@link AdvisorySupport#QUEUE_ADVISORY_TOPIC} topic.
 *
 * See https://issues.apache.org/jira/browse/AMQ-9187
 */
public class JobSchedulerWithAdvisoryMessageTest extends JobSchedulerTestSupport {

    final AtomicLong uniqueQueueId = new AtomicLong(System.currentTimeMillis());

    private Connection connection;
    private Session session;

    /**
     * The queues that got created according to the Queue Advisory Topic. See {@link AdvisorySupport#QUEUE_ADVISORY_TOPIC}
     */
    private List<String> queuesCreated;

    @Before
    public void setupQueueCreationObserver() throws Exception {
        assertTrue(broker.isAdvisorySupport()); // ensure Advisory Support is turned on

        connection = createConnection();
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        queuesCreated = new ArrayList<>();

        // register to listen to a Queue Advisory
        MessageConsumer consumer = session.createConsumer(AdvisorySupport.QUEUE_ADVISORY_TOPIC);
        consumer.setMessageListener((Message msg)->{
            ActiveMQMessage activeMessage = (ActiveMQMessage) msg;
            Object command = activeMessage.getDataStructure();
            if (command instanceof DestinationInfo) {
                DestinationInfo destinationInfo = (DestinationInfo) command;
                String physicalName = destinationInfo.getDestination().getPhysicalName();
                if (destinationInfo.isAddOperation()) {
                    queuesCreated.add(physicalName);
                }
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        session.close();
        connection.close();
        super.tearDown(); // clean up the broker
    }

    @Test
    public void sendDelayedMessage_usingNormalProducer() throws Exception {
        // send delayed message using a named (i.e. not anonymous) jms producer
        final String queueName = getNewQueueName();
        final Queue destination = session.createQueue(queueName);
        delay(200);
        assertFalse(queuesCreated.contains(queueName)); // we do not expect the queue to be created yet.

        MessageProducer producer = session.createProducer(destination);
        // The act of creating the jms producer actually creates the empty queue inside the broker
        // - so the queue already exists before we even send the first message to it. The Advisory message will get
        // sent immediately because a new queue was just created.
        delay(200);
        assertTrue(queuesCreated.contains(queueName));

        // send delayed message
        producer.send( createDelayedMessage() );

        // obviously this is still true as the queue was created before we even sent the delayed message
        assertTrue(queuesCreated.contains(queueName));
    }

    /**
     * See https://issues.apache.org/jira/browse/AMQ-9187
     */
    @Test
    public void sendDelayedMessage_usingAnonymousProducer() throws Exception {
        final String queueName = getNewQueueName();
        Queue destination = session.createQueue(queueName);
        delay(200);
        assertFalse(queuesCreated.contains(queueName)); // we do not expect the queue to be created yet.

        // an "Anonymous Producer" isn't bound to a single queue. It can be used for sending messages to any queue.
        MessageProducer anonymousProducer = session.createProducer(null);
        // creation of an anonymous producer does *not* cause any advisory message to be sent. This is expected.
        delay(200);
        assertFalse(queuesCreated.contains(queueName));

        // send delayed message. The queue will get created on-the-fly as we write the first message to it.
        // - but the queue doesn't get created immediately because the delayed message is first stored in
        //   the JobSchedulerStore. After the delay timeout is reached, then the message gets moved into the real
        //   queue. This is when the queue is actually created.
        anonymousProducer.send(destination, createDelayedMessage() );
        delay(500);  // the message was delayed for only 5ms so 500ms should be long enough

        // The Advisory message should be sent because the queue was created
        assertTrue(queuesCreated.contains(queueName));
    }

    private Message createDelayedMessage() throws JMSException {
        TextMessage message = session.createTextMessage("delayed message");
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 5); // use short delay for unit test
        return message;
    }

    private String getNewQueueName() {
        return "queue-" + uniqueQueueId.getAndIncrement();
    }

    public static void delay(long delayMs) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException ex) {
        }
    }
}
