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
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class ReplicaNetworkConnectorTest extends ReplicaPluginTestSupport {

    protected Connection firstBroker2Connection;
    protected Connection secondBroker2Connection;
    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;
    protected BrokerService firstBroker2;
    protected BrokerService secondBroker2;
    protected NetworkConnector primarySideNetworkConnector;
    protected NetworkConnector replicaSideNetworkConnector;
    protected BrokerViewMBean firstBrokerMBean;
    protected BrokerViewMBean secondBrokerMBean;
    protected BrokerViewMBean firstBroker2MBean;
    protected BrokerViewMBean secondBroker2MBean;
    protected static final String FIRSTBROKER2_KAHADB_DIRECTORY = "target/activemq-data/firstBroker2/";
    protected static final String SECONDBROKER2_KAHADB_DIRECTORY = "target/activemq-data/secondBroker2/";
    protected String firstBroker2URI = "vm://firstBroker2";
    protected String secondBroker2URI = "vm://secondBroker2";
    protected String secondReplicaBindAddress = "tcp://localhost:61611";


    @Override
    protected void setUp() throws Exception {
        cleanKahaDB(FIRST_KAHADB_DIRECTORY);
        cleanKahaDB(SECOND_KAHADB_DIRECTORY);
        super.setUp();
        firstBroker2 = createBrokerFromBrokerFactory(new URI("broker:(" + firstBroker2URI + ")/firstBroker2?persistent=false"), FIRSTBROKER2_KAHADB_DIRECTORY);
        secondBroker2 = createBrokerFromBrokerFactory(new URI("broker:(" + secondBroker2URI + ")/secondBroker2?persistent=false"), SECONDBROKER2_KAHADB_DIRECTORY);

        firstBroker2.start();
        secondBroker2.start();
        firstBroker2.waitUntilStarted();
        secondBroker2.waitUntilStarted();

        primarySideNetworkConnector = startNetworkConnector(firstBroker, firstBroker2);
        replicaSideNetworkConnector = startNetworkConnector(secondBroker, secondBroker2);

        firstBroker2Connection = new ActiveMQConnectionFactory(firstBroker2URI).createConnection();
        secondBroker2Connection = new ActiveMQConnectionFactory(secondBroker2URI).createConnection();
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
    }

    @Override
    protected void tearDown() throws Exception {
        if (firstBroker2Connection != null) {
            firstBroker2Connection.close();
            firstBroker2Connection = null;
        }
        if (secondBroker2Connection != null) {
            secondBroker2Connection.close();
            secondBroker2Connection = null;
        }

        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }

        primarySideNetworkConnector.stop();
        replicaSideNetworkConnector.stop();

        if (firstBroker2 != null) {
            try {
                firstBroker2.stop();
            } catch (Exception e) {
            }
        }
        if (secondBroker2 != null) {
            try {
                secondBroker2.stop();
            } catch (Exception e) {
            }
        }

        super.tearDown();
    }

    @Test
    public void testNetworkConnectorConsumeMessageInPrimarySide() throws Exception {
        Session firstBroker2ProducerSession = firstBroker2Connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = firstBroker2ProducerSession.createProducer(destination);

        TextMessage message = firstBroker2ProducerSession.createTextMessage(getName());
        producer.send(message);

        assertEquals(Arrays.stream(firstBrokerMBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 0);

        assertEquals(Arrays.stream(firstBroker2MBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 1);

        assertEquals(Arrays.stream(secondBrokerMBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 0);

        assertEquals(Arrays.stream(secondBroker2MBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 0);

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = firstBrokerSession.createConsumer(destination);

        TextMessage receivedMessage = (TextMessage) consumer.receive(LONG_TIMEOUT);

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
            .count(), 0);

        assertEquals(Arrays.stream(secondBroker2MBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 0);

        assertEquals(getName(), receivedMessage.getText());
        QueueViewMBean firstBrokerDestinationQueue = getQueueView(firstBroker, destination.getPhysicalName());
        assertEquals(1, firstBrokerDestinationQueue.getDequeueCount());
        QueueViewMBean firstBroker2DestinationQueue = getQueueView(firstBroker2, destination.getPhysicalName());
        assertEquals(1, firstBroker2DestinationQueue.getDequeueCount());


        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);
        assertNull(secondBrokerConsumer.receive(LONG_TIMEOUT));

        firstBroker2ProducerSession.close();
        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    @Test
    public void testNetworkConnectorConsumeMessageInFirstBroker2() throws Exception {
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
        QueueViewMBean firstBroker2DestinationQueue = getQueueView(firstBroker2, destination.getPhysicalName());
        assertEquals(1, firstBroker2DestinationQueue.getDequeueCount());
        QueueViewMBean secondBrokerDestinationQueue = getQueueView(secondBroker, destination.getPhysicalName());
        assertEquals(1, secondBrokerDestinationQueue.getDequeueCount());

        firstBroker2Session.close();
        firstBrokerProducerSession.close();
    }

    protected BrokerViewMBean setBrokerMBean(BrokerService broker) throws Exception {
        MBeanServer mBeanServer = broker.getManagementContext().getMBeanServer();
        ObjectName brokerViewMBeanName = assertRegisteredObjectName(mBeanServer, broker.getBrokerObjectName().toString());
        return MBeanServerInvocationHandler.newProxyInstance(mBeanServer, brokerViewMBeanName, BrokerViewMBean.class, true);
    }

    protected NetworkConnector startNetworkConnector(BrokerService broker1, BrokerService broker2) throws Exception {
        NetworkConnector nc = bridgeBrokers(broker1, broker2);
        nc.start();
        waitForNetworkBridgesFormation(List.of(broker1, broker2));
        return nc;
    }

    protected BrokerService createBrokerFromBrokerFactory(URI brokerUri, String KahaDBDir) throws Exception {
        cleanKahaDB(KahaDBDir);
        BrokerService broker = BrokerFactory.createBroker(brokerUri);
        broker.setDataDirectory(KahaDBDir);
        broker.getManagementContext().setCreateConnector(false);
        return broker;
    }

    private NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker) throws Exception {
        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        if (!transportConnectors.isEmpty()) {
            URI remoteURI = transportConnectors.get(0).getConnectUri();
            String uri = "static:(" + remoteURI + ")";
            NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
            connector.setName("to-" + remoteBroker.getBrokerName());
            connector.setDynamicOnly(false);
            connector.setConduitSubscriptions(true);
            localBroker.addNetworkConnector(connector);

            connector.setDuplex(true);
            return connector;
        } else {
            throw new Exception("Remote broker has no registered connectors.");
        }
    }

    private void waitForNetworkBridgesFormation(List<BrokerService> brokerServices) throws Exception {
        for (BrokerService broker: brokerServices) {
            waitForNetworkConnectorStarts(broker);
        }
    }

    private void waitForNetworkConnectorStarts(BrokerService broker) throws Exception {
        if (!broker.getNetworkConnectors().isEmpty()) {
            Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    int activeCount = 0;
                    for (NetworkBridge bridge : broker.getNetworkConnectors().get(0).activeBridges()) {
                        if (bridge.getRemoteBrokerName() != null) {
                            System.out.println("found bridge[" + bridge + "] to " + bridge.getRemoteBrokerName() + " on broker :" + broker.getBrokerName());
                            activeCount++;
                        }
                    }
                    return activeCount >= 1;
                }
            }, Wait.MAX_WAIT_MILLIS*2);
        } else {
            System.out.println("broker: " + broker.getBrokerName() + " doesn't have nc");
        }
    }

}
