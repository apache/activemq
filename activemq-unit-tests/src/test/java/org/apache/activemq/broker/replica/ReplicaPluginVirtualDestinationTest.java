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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.CompositeQueue;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.Arrays;
import java.util.Collections;

public class ReplicaPluginVirtualDestinationTest extends ReplicaPluginTestSupport {

    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    private static final String CLIENT_ID_ONE = "one";

    private static final String VIRTUAL_QUEUE = "VIRT.QUEUE";
    private static final String PHYSICAL_QUEUE = "VQueue";
    private static final String PHYSICAL_TOPIC = "VTopic";

    private static final String VIRTUAL_QUEUE_FIRST_BROKER = "VIRT.QUEUE1";
    private static final String PHYSICAL_QUEUE_FIRST_BROKER = "VQueueFirst";

    private static final String VIRTUAL_QUEUE_SECOND_BROKER = "VIRT.QUEUE2";
    private static final String PHYSICAL_QUEUE_SECOND_BROKER = "VQueueSecond";

    private void setupCompositeDestinationsBothBrokers(BrokerService firstBroker, BrokerService secondBroker) {
        CompositeQueue virtualDestination = new CompositeQueue();
        virtualDestination.setName(VIRTUAL_QUEUE);
        virtualDestination.setForwardOnly(true);
        virtualDestination.setForwardTo(Arrays.asList(new ActiveMQQueue(PHYSICAL_QUEUE), new ActiveMQTopic(PHYSICAL_TOPIC)));

        VirtualDestinationInterceptor virtualDestinationInterceptor = new VirtualDestinationInterceptor();
        virtualDestinationInterceptor.setVirtualDestinations(Collections.singletonList(virtualDestination).toArray(VirtualDestination[]::new));
        DestinationInterceptor[] interceptors = Collections.singletonList(virtualDestinationInterceptor).toArray(DestinationInterceptor[]::new);

        firstBroker.setDestinationInterceptors(interceptors);
        secondBroker.setDestinationInterceptors(interceptors);
    }

    private void setupCompositeDestinationsOneBrokerOnly(BrokerService broker, String virtualQueue, String physicalQueue) {
        CompositeQueue virtualDestination = new CompositeQueue();
        virtualDestination.setName(virtualQueue);
        virtualDestination.setForwardOnly(true);
        virtualDestination.setForwardTo(Collections.singletonList(new ActiveMQQueue(physicalQueue)));

        VirtualDestinationInterceptor virtualDestinationInterceptor = new VirtualDestinationInterceptor();
        virtualDestinationInterceptor.setVirtualDestinations(Collections.singletonList(virtualDestination).toArray(VirtualDestination[]::new));
        DestinationInterceptor[] interceptors = broker.getDestinationInterceptors();
        interceptors = Arrays.copyOf(interceptors, interceptors.length + 1);
        interceptors[interceptors.length - 1] = virtualDestinationInterceptor;

        broker.setDestinationInterceptors(interceptors);
    }

    @Override
    protected void setUp() throws Exception {

        if (firstBroker == null) {
            firstBroker = createFirstBroker();
        }
        if (secondBroker == null) {
            secondBroker = createSecondBroker();
        }

        setupCompositeDestinationsBothBrokers(firstBroker, secondBroker);
        setupCompositeDestinationsOneBrokerOnly(firstBroker, VIRTUAL_QUEUE_FIRST_BROKER, PHYSICAL_QUEUE_FIRST_BROKER);
        setupCompositeDestinationsOneBrokerOnly(secondBroker, VIRTUAL_QUEUE_SECOND_BROKER, PHYSICAL_QUEUE_SECOND_BROKER);

        startFirstBroker();
        startSecondBroker();

        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.setClientID(CLIENT_ID_ONE);
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.setClientID(CLIENT_ID_ONE);
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

    public void testVirtualDestinationConfigurationBothBrokers() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        ActiveMQQueue virtualQueue = new ActiveMQQueue(VIRTUAL_QUEUE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(virtualQueue);

        ActiveMQTopic physicalTopic = new ActiveMQTopic(PHYSICAL_TOPIC);
        firstBrokerSession.createDurableSubscriber(physicalTopic, CLIENT_ID_ONE);

        ActiveMQQueue physicalQueue = new ActiveMQQueue(PHYSICAL_QUEUE);
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumerQueue = secondBrokerSession.createConsumer(physicalQueue);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessageFromQueue = secondBrokerConsumerQueue.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessageFromQueue);
        assertTrue(receivedMessageFromQueue instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessageFromQueue).getText());

        MessageConsumer secondBrokerConsumerTopic = secondBrokerSession.createDurableSubscriber(physicalTopic, CLIENT_ID_ONE);
        Message receivedMessageFromTopic = secondBrokerConsumerTopic.receive();
        assertNotNull(receivedMessageFromTopic);
        assertTrue(receivedMessageFromTopic instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessageFromTopic).getText());

        QueueViewMBean secondBrokerVirtualQueueViewMBean = getQueueView(secondBroker, virtualQueue.getPhysicalName());
        assertEquals(secondBrokerVirtualQueueViewMBean.getEnqueueCount(), 0);

        QueueViewMBean secondBrokerPhysicalQueueViewMBean = getQueueView(secondBroker, physicalQueue.getPhysicalName());
        assertEquals(secondBrokerPhysicalQueueViewMBean.getEnqueueCount(), 1);

        TopicViewMBean secondBrokerPhysicalTopicViewMBean = getTopicView(secondBroker, physicalTopic.getPhysicalName());
        assertEquals(secondBrokerPhysicalTopicViewMBean.getEnqueueCount(), 1);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testVirtualDestinationConfigurationFirstBrokerOnly() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        ActiveMQQueue virtualQueue = new ActiveMQQueue(VIRTUAL_QUEUE_FIRST_BROKER);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(virtualQueue);

        ActiveMQQueue physicalQueue = new ActiveMQQueue(PHYSICAL_QUEUE_FIRST_BROKER);
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(physicalQueue);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        // despite the fact that we don't have a virtual configuration on the replica broker
        // the state should be the same as on the source broker
        QueueViewMBean secondBrokerVirtualQueueViewMBean = getQueueView(secondBroker, virtualQueue.getPhysicalName());
        assertEquals(secondBrokerVirtualQueueViewMBean.getEnqueueCount(), 0);

        QueueViewMBean secondBrokerPhysicalQueueViewMBean = getQueueView(secondBroker, physicalQueue.getPhysicalName());
        assertEquals(secondBrokerPhysicalQueueViewMBean.getEnqueueCount(), 1);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testVirtualDestinationConfigurationSecondBrokerOnly() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        // on the first broker the destination is physical! but on the second it is virtual as per config
        ActiveMQQueue virtualQueue = new ActiveMQQueue(VIRTUAL_QUEUE_SECOND_BROKER);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(virtualQueue);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(virtualQueue);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        // on the replica side it should be treated like physical despite the virtual configuration
        QueueViewMBean secondBrokerVirtualQueueViewMBean = getQueueView(secondBroker, virtualQueue.getPhysicalName());
        assertEquals(secondBrokerVirtualQueueViewMBean.getEnqueueCount(), 1);

        // that is why virtual queue on the replica shouldn't forward to the physical destination as per configuration
        ActiveMQQueue physicalQueue = new ActiveMQQueue(PHYSICAL_QUEUE_SECOND_BROKER);
        String objectNameStr = secondBroker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName=" + physicalQueue.getPhysicalName();
        ObjectName objectName = new ObjectName(objectNameStr);
        MBeanServer mbeanServer = secondBroker.getManagementContext().getMBeanServer();
        assertFalse(mbeanServer.isRegistered(objectName));

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

}
