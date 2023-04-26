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
package org.apache.activemq.broker.replica;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.replica.ReplicaSupport;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class ReplicationQueueOperationsTest extends ReplicaPluginTestSupport {
    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();
        secondBrokerConnection.start();
    }

    @Override
    protected void tearDown() throws Exception {
        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }
        super.tearDown();
    }

    @Test
    public void testSendMessageToReplicationQueues() throws Exception {
        JmsTemplate firstBrokerJmsTemplate = new JmsTemplate();
        firstBrokerJmsTemplate.setConnectionFactory(firstBrokerConnectionFactory);
        assertFunctionThrows(() -> firstBrokerJmsTemplate.convertAndSend(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME, getName()),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        assertFunctionThrows(() -> firstBrokerJmsTemplate.convertAndSend(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME, getName()),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        assertFunctionThrows(() -> firstBrokerJmsTemplate.convertAndSend(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME, getName()),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);

        JmsTemplate secondBrokerJmsTemplate = new JmsTemplate();
        secondBrokerJmsTemplate.setConnectionFactory(secondBrokerConnectionFactory);
        assertFunctionThrows(() -> secondBrokerJmsTemplate.convertAndSend(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME, getName()),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        assertFunctionThrows(() -> secondBrokerJmsTemplate.convertAndSend(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME, getName()),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        assertFunctionThrows(() -> secondBrokerJmsTemplate.convertAndSend(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME, getName()),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
    }

    @Test
    public void testConsumeMessageFromReplicationQueues() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer secondBrokerProducer = secondBrokerSession.createProducer(destination);
        final int NUM_OF_MESSAGE_SEND = 50;

        for (int i = 0; i < NUM_OF_MESSAGE_SEND; i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName());
            firstBrokerProducer.send(message);
            secondBrokerProducer.send(message);
        }

        JmsTemplate firstBrokerJmsTemplate = new JmsTemplate();
        firstBrokerJmsTemplate.setConnectionFactory(firstBrokerConnectionFactory);
        assertFunctionThrows(() -> firstBrokerJmsTemplate.receive(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        assertFunctionThrows(() -> firstBrokerJmsTemplate.receive(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        assertFunctionThrows(() -> firstBrokerJmsTemplate.receive(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);

        JmsTemplate secondBrokerJmsTemplate = new JmsTemplate();
        secondBrokerJmsTemplate.setConnectionFactory(secondBrokerConnectionFactory);
        assertFunctionThrows(() -> secondBrokerJmsTemplate.receive(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        assertFunctionThrows(() -> secondBrokerJmsTemplate.receive(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        assertFunctionThrows(() -> secondBrokerJmsTemplate.receive(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME),
            "JMSException: Not authorized to access destination: queue://" + ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testPurgeReplicationQueues() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer secondBrokerProducer = secondBrokerSession.createProducer(destination);
        final int NUM_OF_MESSAGE_SEND = 50;

        for (int i = 0; i < NUM_OF_MESSAGE_SEND; i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName());
            firstBrokerProducer.send(message);
            secondBrokerProducer.send(message);
        }

        QueueViewMBean firstBrokerMainQueue = getQueueView(firstBroker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        QueueViewMBean firstBrokerIntermediateQueue = getQueueView(firstBroker, ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);

        waitForQueueHasMessage(firstBrokerMainQueue);
        firstBrokerMainQueue.purge();
        Thread.sleep(LONG_TIMEOUT);
        assertEquals(0, firstBrokerMainQueue.getInFlightCount());


        waitForQueueHasMessage(firstBrokerIntermediateQueue);
        firstBrokerIntermediateQueue.purge();
        Thread.sleep(LONG_TIMEOUT);
        assertEquals(0, firstBrokerIntermediateQueue.getInFlightCount());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    private void assertFunctionThrows(Function testFunction, String expectedMessage) {
        try {
            testFunction.apply();
            fail("Should have thrown exception on " + testFunction);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    private void waitForQueueHasMessage(QueueViewMBean queue) throws Exception {
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return queue.getEnqueueCount() > 0;
            }
        });
    }

    @FunctionalInterface
    public interface Function {
        void apply() throws Exception;
    }
}
