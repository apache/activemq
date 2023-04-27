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
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.replica.ReplicaPlugin;
import org.apache.activemq.replica.ReplicaRole;
import org.junit.Test;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import java.net.URI;
import java.util.Arrays;

public class ReplicaNetworkConnectorsOnTwoPairsOfReplicationBrokersTest extends ReplicaNetworkConnectorTest {
    protected String pair2FirstReplicaBindAddress = "tcp://localhost:61620";
    protected String pair2SecondReplicaBindAddress = "tcp://localhost:61621";
    @Override
    protected void setUp() throws Exception {
        if (firstBroker == null) {
            firstBroker = createFirstBroker();
        }
        if (secondBroker == null) {
            secondBroker = createSecondBroker();
        }

        startFirstBroker();
        startSecondBroker();

        firstBroker2 = createBrokerFromBrokerFactory(new URI("broker:(" + firstBroker2URI + ")/firstBroker2?persistent=false"), FIRSTBROKER2_KAHADB_DIRECTORY);
        secondBroker2 = createBrokerFromBrokerFactory(new URI("broker:(" + secondBroker2URI + ")/secondBroker2?persistent=false"), SECONDBROKER2_KAHADB_DIRECTORY);
        ReplicaPlugin firstBroker2ReplicaPlugin = new ReplicaPlugin();
        firstBroker2ReplicaPlugin.setRole(ReplicaRole.source);
        firstBroker2ReplicaPlugin.setTransportConnectorUri(pair2FirstReplicaBindAddress);
        firstBroker2ReplicaPlugin.setOtherBrokerUri(pair2SecondReplicaBindAddress);
        firstBroker2ReplicaPlugin.setControlWebConsoleAccess(false);
        firstBroker2.setPlugins(new BrokerPlugin[]{firstBroker2ReplicaPlugin});

        ReplicaPlugin secondBroker2ReplicaPlugin = new ReplicaPlugin();
        secondBroker2ReplicaPlugin.setRole(ReplicaRole.replica);
        secondBroker2ReplicaPlugin.setTransportConnectorUri(pair2SecondReplicaBindAddress);
        secondBroker2ReplicaPlugin.setOtherBrokerUri(pair2FirstReplicaBindAddress);
        secondBroker2ReplicaPlugin.setControlWebConsoleAccess(false);
        secondBroker2.setPlugins(new BrokerPlugin[]{secondBroker2ReplicaPlugin});

        firstBroker2.start();
        secondBroker2.start();
        firstBroker2.waitUntilStarted();
        secondBroker2.waitUntilStarted();

        primarySideNetworkConnector = startNetworkConnector(firstBroker, firstBroker2);
        replicaSideNetworkConnector = startNetworkConnector(secondBroker, secondBroker2);

        firstBroker2Connection = new ActiveMQConnectionFactory(firstBroker2URI).createConnection();
        secondBroker2Connection = new ActiveMQConnectionFactory(secondBroker2URI).createConnection();
        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        firstBroker2Connection.start();
        secondBroker2Connection.start();
        firstBrokerConnection.start();
        secondBrokerConnection.start();
        firstBrokerMBean = setBrokerMBean(firstBroker);
        firstBroker2MBean = setBrokerMBean(firstBroker2);
        secondBrokerMBean = setBrokerMBean(secondBroker);
        secondBroker2MBean = setBrokerMBean(secondBroker2);

        destination = createDestination();
    }

    @Test
    public void testMessageConsumedByReplicaSideNetworkConnectorBroker() throws Exception {
        Session firstBrokerProducerSession = firstBrokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = firstBrokerProducerSession.createProducer(destination);
        TextMessage message = firstBrokerProducerSession.createTextMessage(getName());
        producer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        assertEquals(Arrays.stream(firstBrokerMBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 1);

        assertEquals(Arrays.stream(firstBroker2MBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 0);

        assertEquals(Arrays.stream(secondBrokerMBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 1);

        assertEquals(Arrays.stream(secondBroker2MBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 0);

        Session firstBroker2Session = firstBroker2Connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = firstBroker2Session.createConsumer(destination);

        TextMessage receivedMessage = (TextMessage) consumer.receive(LONG_TIMEOUT);
        assertEquals(getName(), receivedMessage.getText());
        Thread.sleep(LONG_TIMEOUT);
        receivedMessage.acknowledge();
        Thread.sleep(LONG_TIMEOUT);

        assertEquals(Arrays.stream(firstBrokerMBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 1);

        assertEquals(Arrays.stream(firstBroker2MBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 1);

        assertEquals(Arrays.stream(secondBrokerMBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 1);

        assertEquals(Arrays.stream(secondBroker2MBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 1);

        QueueViewMBean firstBrokerDestinationQueue = getQueueView(firstBroker, destination.getPhysicalName());
        assertEquals(1, firstBrokerDestinationQueue.getDequeueCount());
        QueueViewMBean first2BrokerDestinationQueue = getQueueView(firstBroker2, destination.getPhysicalName());
        assertEquals(1, first2BrokerDestinationQueue.getDequeueCount());
        QueueViewMBean secondBrokerDestinationQueue = getQueueView(secondBroker, destination.getPhysicalName());
        assertEquals(1, secondBrokerDestinationQueue.getDequeueCount());
        QueueViewMBean secondBroker2DestinationQueue = getQueueView(secondBroker2, destination.getPhysicalName());
        assertEquals(1, secondBroker2DestinationQueue.getDequeueCount());

        firstBrokerProducerSession.close();
        firstBroker2Session.close();
    }
}
