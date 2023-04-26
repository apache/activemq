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
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.replica.ReplicaBroker;
import org.apache.activemq.replica.ReplicaEvent;
import org.apache.activemq.replica.ReplicaEventSerializer;
import org.apache.activemq.replica.ReplicaEventType;
import org.apache.activemq.replica.ReplicaPlugin;
import org.apache.activemq.replica.ReplicaPolicy;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.replica.ReplicaRole;
import org.apache.activemq.replica.ReplicaSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ReplicationEventHandlingTest extends ReplicaPluginTestSupport {

    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final ActiveMQQueue sequenceQueue = new ActiveMQQueue(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
    private Broker nextBrokerSpy;
    private ReplicaReplicationQueueSupplier testQueueProvider;
    private ActiveMQQueue mockMainQueue;
    private TransportConnector replicationConnector;
    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    ActiveMQConnectionFactory mockConnectionFactorySpy;

    ActiveMQConnection mockConnectionSpy;

    ReplicaPolicy mockReplicaPolicy;

    @Before
    public void setUp() throws Exception {
        firstBroker = new BrokerService();
        firstBroker.setUseJmx(true);
        firstBroker.setPersistent(false);
        firstBroker.getManagementContext().setCreateConnector(false);
        firstBroker.addConnector(firstBindAddress);
        firstBroker.setDataDirectory(FIRST_KAHADB_DIRECTORY);
        firstBroker.setBrokerName("firstBroker");
        replicationConnector = firstBroker.addConnector(firstReplicaBindAddress);
        replicationConnector.setName("replication");
        firstBroker.start();
        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        firstBrokerXAConnectionFactory = new ActiveMQXAConnectionFactory(firstBindAddress);

        mockReplicaPolicy = spy(ReplicaPolicy.class);
        mockConnectionFactorySpy = spy(new ActiveMQConnectionFactory(firstReplicaBindAddress));
        mockConnectionSpy = spy((ActiveMQConnection) mockConnectionFactorySpy.createConnection());
        doReturn(mockConnectionFactorySpy).when(mockReplicaPolicy).getOtherBrokerConnectionFactory();

        mockMainQueue = new ActiveMQQueue(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);


        doReturn(getMainReplicationQueue()).when(mockConnectionSpy).getDestinationSource();
        doReturn(mockConnectionSpy).when(mockConnectionFactorySpy).createConnection();

        if (secondBroker == null) {
            secondBroker = createSecondBroker();
        }

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();
    }

    @After
    protected void tearDown() throws Exception {
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }

        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }

        super.tearDown();
    }

    @Test
    public void testReplicaBrokerHasOutOfOrderReplicationEvent() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(mockMainQueue);

        startSecondBroker();
        destination = createDestination();
        Thread.sleep(SHORT_TIMEOUT);

        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);
        secondBrokerXAConnectionFactory = new ActiveMQXAConnectionFactory(secondBindAddress);

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        ActiveMQMessage replicaEventMessage = new ActiveMQMessage();
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.DESTINATION_UPSERT)
            .setEventData(eventSerializer.serializeReplicationData(destination));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "0");

        firstBrokerProducer.send(mockMainQueue, replicaEventMessage);
        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean secondBrokerSequenceQueueView = getQueueView(secondBroker, ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
        assertEquals(secondBrokerSequenceQueueView.browseMessages().size(), 1);

        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);

        replicaEventMessage = spy(new ActiveMQMessage());

        event = new ReplicaEvent()
            .setEventType(ReplicaEventType.MESSAGE_SEND)
            .setEventData(eventSerializer.serializeMessageData(message));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "100");

        firstBrokerProducer.send(mockMainQueue, replicaEventMessage);
        Thread.sleep(LONG_TIMEOUT);

        verify(nextBrokerSpy, times(1)).send(any(), any());
        verify(replicaEventMessage, never()).acknowledge();
    }

    @Test
    public void testReplicaBrokerHasDuplicateReplicationEvent() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(mockMainQueue);

        startSecondBroker();
        destination = createDestination();
        Thread.sleep(SHORT_TIMEOUT);

        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);
        secondBrokerXAConnectionFactory = new ActiveMQXAConnectionFactory(secondBindAddress);

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        ActiveMQMessage replicaEventMessage = new ActiveMQMessage();
        ReplicaEvent event = new ReplicaEvent()
            .setEventType(ReplicaEventType.DESTINATION_UPSERT)
            .setEventData(eventSerializer.serializeReplicationData(destination));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "20");

        firstBrokerProducer.send(mockMainQueue, replicaEventMessage);
        Thread.sleep(LONG_TIMEOUT);

        QueueViewMBean secondBrokerSequenceQueueView = getQueueView(secondBroker, ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
        assertEquals(secondBrokerSequenceQueueView.browseMessages().size(), 1);
        TextMessage sequenceQueueMessage = (TextMessage) secondBrokerSequenceQueueView.browseMessages().get(0);
        String[] textMessageSequence = sequenceQueueMessage.getText().split("#");
        assertEquals(Integer.parseInt(textMessageSequence[0]), 20);

        MessageId messageId = new MessageId("1:1");

        ActiveMQMessage message = new ActiveMQMessage();
        message.setMessageId(messageId);
        message.setDestination(destination);

        replicaEventMessage = new ActiveMQMessage();

        event = new ReplicaEvent()
            .setEventType(ReplicaEventType.MESSAGE_SEND)
            .setEventData(eventSerializer.serializeMessageData(message));
        replicaEventMessage.setContent(event.getEventData());
        replicaEventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        replicaEventMessage.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, "10");

        System.out.println("sending first MESSAGE_SEND...");
        firstBrokerProducer.send(mockMainQueue, replicaEventMessage);
        Thread.sleep(LONG_TIMEOUT);

        ArgumentCaptor<ActiveMQMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ActiveMQMessage.class);
        verify(nextBrokerSpy, times(2)).send(any(), messageArgumentCaptor.capture());
        messageArgumentCaptor.getAllValues().stream()
            .forEach(msg -> assertEquals(msg.getDestination(), sequenceQueue));
    }

    private DestinationSource getMainReplicationQueue() throws Exception {
        DestinationSource destination = new DestinationSource(mockConnectionSpy);
        DestinationInfo destinationInfo = new DestinationInfo();
        destinationInfo.setDestination(mockMainQueue);
        ActiveMQMessage activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setDataStructure(destinationInfo);
        destination.onMessage(activeMQMessage);

        return destination;
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

        ReplicaPlugin replicaPlugin = new ReplicaPlugin() {
            @Override
            public Broker installPlugin(final Broker broker) {
                nextBrokerSpy = spy(broker);
                testQueueProvider = new ReplicaReplicationQueueSupplier(broker);
                return new ReplicaBroker(nextBrokerSpy, testQueueProvider, mockReplicaPolicy);
            }
        };
        replicaPlugin.setRole(ReplicaRole.replica);
        replicaPlugin.setOtherBrokerUri(firstReplicaBindAddress);

        answer.setPlugins(new BrokerPlugin[]{replicaPlugin});
        answer.setSchedulerSupport(true);
        return answer;
    }
}
