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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.replica.ReplicaPlugin;
import org.apache.activemq.replica.ReplicaPolicy;
import org.apache.activemq.replica.ReplicaRole;
import org.apache.activemq.replica.ReplicaRoleManagementBroker;
import org.apache.activemq.replica.ReplicaStatistics;
import org.apache.activemq.replica.ReplicaSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.net.URI;

import java.text.MessageFormat;
import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class ReplicaAcknowledgeReplicationEventTest extends ReplicaPluginTestSupport {
    static final int MAX_BATCH_LENGTH = 500;

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaAcknowledgeReplicationEventTest.class);

    protected Connection firstBrokerConnection;

    ActiveMQConnectionFactory mockConnectionFactorySpy;

    ActiveMQConnection mockConnectionSpy;

    ReplicaPolicy mockReplicaPolicy;

    ActiveMQSession mockReplicaSession;

    @Before
    public void setUp() throws Exception {
        firstBroker = createFirstBroker();
        firstBroker.start();
        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        firstBrokerXAConnectionFactory = new ActiveMQXAConnectionFactory(firstBindAddress);

        mockReplicaPolicy = spy(ReplicaPolicy.class);
        mockConnectionFactorySpy = spy(new ActiveMQConnectionFactory(firstReplicaBindAddress));
        mockConnectionSpy = spy((ActiveMQConnection) mockConnectionFactorySpy.createConnection());
        doReturn(mockConnectionFactorySpy).when(mockReplicaPolicy).getOtherBrokerConnectionFactory();
        doReturn(mockConnectionSpy).when(mockConnectionFactorySpy).createConnection();

        if (secondBroker == null) {
            secondBroker = createSecondBroker();
        }

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();
    }

    @After
    protected void tearDown() throws Exception {
        firstBrokerConnection.close();
        mockConnectionSpy.close();
        mockReplicaSession.close();
        super.tearDown();
    }

    @Test
    public void testReplicaBrokerDoNotAckOnReplicaEvent() throws Exception {
        mockReplicaSession = spy((ActiveMQSession) mockConnectionSpy.createSession(false, ActiveMQSession.CLIENT_ACKNOWLEDGE));
        doReturn(mockReplicaSession).when(mockConnectionSpy).createSession(eq(false), eq(ActiveMQSession.CLIENT_ACKNOWLEDGE));
        doNothing().when(mockReplicaSession).acknowledge();

        startSecondBroker();
        destination = createDestination();
        Thread.sleep(SHORT_TIMEOUT);

        waitUntilReplicationQueueHasConsumer(firstBroker);

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName() + " No. 0");
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);
        QueueViewMBean firstBrokerMainQueueView = getQueueView(firstBroker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
        assertEquals(firstBrokerMainQueueView.getDequeueCount(), 0);
        assertTrue(firstBrokerMainQueueView.getEnqueueCount() >= 1);

        secondBroker.stop();
        secondBroker.waitUntilStopped();

        message  = new ActiveMQTextMessage();
        message.setText(getName() + " No. 1");
        firstBrokerProducer.send(message);

        secondBroker = super.createSecondBroker();
        secondBroker.start();
        Thread.sleep(LONG_TIMEOUT * 2);

        waitUntilReplicationQueueHasConsumer(firstBroker);

        waitForCondition(() -> {
            try {
                QueueViewMBean firstBrokerQueueView = getQueueView(firstBroker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
                assertEquals(firstBrokerQueueView.getDequeueCount(), 3);
                assertTrue(firstBrokerQueueView.getEnqueueCount() >= 2);

                QueueViewMBean secondBrokerSequenceQueueView = getQueueView(secondBroker, ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
                assertEquals(secondBrokerSequenceQueueView.browseMessages().size(), 1);
            } catch (Exception|Error urlException) {
                LOG.error("Caught error during wait: " + urlException.getMessage());
                throw new RuntimeException(urlException);
            }
        });

    }

    @Test
    public void testReplicaSendCorrectAck() throws Exception {
        mockConnectionSpy.start();
        LinkedList<ActiveMQMessage> messagesToAck = new LinkedList<>();
        ActiveMQQueue replicationSourceQueue = mockConnectionSpy.getDestinationSource().getQueues().stream()
            .peek(q -> System.out.println("Queue: " + q.getPhysicalName()))
            .filter(d -> ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME.equals(d.getPhysicalName()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                MessageFormat.format("There is no replication queue on the source broker {0}", mockConnectionSpy.getBrokerName())
            ));

        mockReplicaSession = (ActiveMQSession) mockConnectionSpy.createSession(false, ActiveMQSession.CLIENT_ACKNOWLEDGE);
        ActiveMQMessageConsumer mainQueueConsumer = (ActiveMQMessageConsumer) mockReplicaSession.createConsumer(replicationSourceQueue);

        mainQueueConsumer.setMessageListener(message -> {
            ActiveMQMessage msg = (ActiveMQMessage) message;
            messagesToAck.add(msg);
        });

        destination = createDestination();
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        for (int i = 0; i < MAX_BATCH_LENGTH * 4 + 10; i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName() + " No. " + i);
            firstBrokerProducer.send(message);
        }

        Thread.sleep(LONG_TIMEOUT * 2);

        MessageAck ack = new MessageAck(messagesToAck.getLast(), MessageAck.STANDARD_ACK_TYPE, messagesToAck.size());
        ack.setFirstMessageId(messagesToAck.getFirst().getMessageId());
        ack.setConsumerId(mainQueueConsumer.getConsumerId());
        mockReplicaSession.syncSendPacket(ack);
        Thread.sleep(LONG_TIMEOUT);

        waitForCondition(() -> {
            try {
                QueueViewMBean firstBrokerMainQueueView = getQueueView(firstBroker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
                assertEquals(firstBrokerMainQueueView.getDequeueCount(), messagesToAck.size());
                assertEquals(firstBrokerMainQueueView.getEnqueueCount(), messagesToAck.size());
            } catch (Exception|Error urlException) {
                LOG.error("Caught error during wait: " + urlException.getMessage());
                throw new RuntimeException(urlException);
            }
        });
    }

    @Test
    public void testReplicaSendOutOfOrderAck() throws Exception {
        mockConnectionSpy.start();
        ActiveMQQueue replicationSourceQueue = mockConnectionSpy.getDestinationSource().getQueues().stream()
            .peek(q -> System.out.println("Queue: " + q.getPhysicalName()))
            .filter(d -> ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME.equals(d.getPhysicalName()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                MessageFormat.format("There is no replication queue on the source broker {0}", mockConnectionSpy.getBrokerName())
            ));

        mockReplicaSession = (ActiveMQSession) mockConnectionSpy.createSession(false, ActiveMQSession.CLIENT_ACKNOWLEDGE);
        ActiveMQMessageConsumer mainQueueConsumer = (ActiveMQMessageConsumer) mockReplicaSession.createConsumer(replicationSourceQueue);

        mainQueueConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    ActiveMQMessage msg = (ActiveMQMessage) message;
                    MessageAck ack = new MessageAck(msg, MessageAck.STANDARD_ACK_TYPE, 1);
                    MessageId outOfOrderMessageId = msg.getMessageId().copy();
                    outOfOrderMessageId.setProducerSequenceId(msg.getMessageId().getProducerSequenceId() + 100);
                    ack.setFirstMessageId(outOfOrderMessageId);
                    ack.setLastMessageId(outOfOrderMessageId);
                    ack.setConsumerId(mainQueueConsumer.getConsumerId());
                    mockReplicaSession.syncSendPacket(ack);
                    fail("should have thrown IllegalStateException!");
                } catch (JMSException e) {
                    assertTrue(e.getMessage().contains("Could not find messages for ack"));
                    assertTrue(e.getCause() instanceof IllegalStateException);
                }
            }
        });

        destination = createDestination();
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName() + " No. 0");
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT * 2);

        waitForCondition(() -> {
            try {
                QueueViewMBean firstBrokerMainQueueView = getQueueView(firstBroker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
                assertEquals(firstBrokerMainQueueView.getDequeueCount(), 0);
                assertTrue(firstBrokerMainQueueView.getEnqueueCount() >= 1);
            } catch (Exception|Error urlException) {
                LOG.error("Caught error during wait: " + urlException.getMessage());
                throw new RuntimeException(urlException);
            }
        });
    }

    @Override
    protected BrokerService createSecondBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(false);
        answer.getManagementContext().setCreateConnector(false);
        answer.addConnector(secondBindAddress);
        answer.setDataDirectory(SECOND_KAHADB_DIRECTORY);
        answer.setBrokerName("secondBroker");
        mockReplicaPolicy.setTransportConnectorUri(URI.create(secondReplicaBindAddress));

        ReplicaPlugin replicaPlugin = new ReplicaPlugin() {
            @Override
            public Broker installPlugin(final Broker broker) {
                return new ReplicaRoleManagementBroker(broker, mockReplicaPolicy, ReplicaRole.replica, new ReplicaStatistics());
            }
        };
        replicaPlugin.setRole(ReplicaRole.replica);
        replicaPlugin.setTransportConnectorUri(secondReplicaBindAddress);
        replicaPlugin.setOtherBrokerUri(firstReplicaBindAddress);
        replicaPlugin.setControlWebConsoleAccess(false);
        replicaPlugin.setHeartBeatPeriod(0);

        answer.setPlugins(new BrokerPlugin[]{replicaPlugin});
        answer.setSchedulerSupport(true);
        return answer;
    }
}
