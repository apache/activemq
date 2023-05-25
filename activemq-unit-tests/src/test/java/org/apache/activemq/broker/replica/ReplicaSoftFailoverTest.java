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
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.replica.ReplicaPlugin;
import org.apache.activemq.replica.ReplicaRole;
import org.apache.activemq.replica.ReplicaSupport;
import org.apache.activemq.replica.jmx.ReplicationViewMBean;
import org.apache.activemq.util.Wait;
import org.junit.Ignore;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class ReplicaSoftFailoverTest extends ReplicaPluginTestSupport {
    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;
    private ReplicationViewMBean firstBrokerReplicationView;
    private ReplicationViewMBean secondBrokerReplicationView;
    protected static String SECOND_REPLICA_BINDING_ADDRESS = "tcp://localhost:61611";
    private static int MESSAGES_TO_SEND = 500;
    private static int MAX_RETRY = 10;

    @Override
    protected void setUp() throws Exception {
        firstBroker = setUpFirstBroker();
        secondBroker = setUpSecondBroker();

        ReplicaPlugin firstBrokerPlugin = new ReplicaPlugin();
        firstBrokerPlugin.setRole(ReplicaRole.source);
        firstBrokerPlugin.setTransportConnectorUri(firstReplicaBindAddress);
        firstBrokerPlugin.setOtherBrokerUri(SECOND_REPLICA_BINDING_ADDRESS);
        firstBrokerPlugin.setControlWebConsoleAccess(false);
        firstBrokerPlugin.setHeartBeatPeriod(0);
        firstBroker.setPlugins(new BrokerPlugin[]{firstBrokerPlugin});

        ReplicaPlugin secondBrokerPlugin = new ReplicaPlugin();
        secondBrokerPlugin.setRole(ReplicaRole.replica);
        secondBrokerPlugin.setTransportConnectorUri(SECOND_REPLICA_BINDING_ADDRESS);
        secondBrokerPlugin.setOtherBrokerUri(firstReplicaBindAddress);
        secondBrokerPlugin.setControlWebConsoleAccess(false);
        secondBrokerPlugin.setHeartBeatPeriod(0);
        secondBroker.setPlugins(new BrokerPlugin[]{secondBrokerPlugin});

        firstBroker.start();
        secondBroker.start();
        firstBroker.waitUntilStarted();
        secondBroker.waitUntilStarted();

        firstBrokerReplicationView = getReplicationViewMBean(firstBroker);
        secondBrokerReplicationView = getReplicationViewMBean(secondBroker);
        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);

        destination = createDestination();

        waitUntilReplicationQueueHasConsumer(firstBroker);
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
    public void testSoftFailover() throws Exception {
        firstBrokerReplicationView.setReplicationRole(ReplicaRole.replica.name(), false);
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                firstBrokerReplicationView = getReplicationViewMBean(firstBroker);
                return firstBrokerReplicationView.getReplicationRole().equals(ReplicaRole.replica.name());
            }
        }, Wait.MAX_WAIT_MILLIS*2);

        Thread.sleep(SHORT_TIMEOUT);
        assertFalse(secondBroker.isStopped());
        waitUntilReplicationQueueHasConsumer(secondBroker);

        secondBrokerReplicationView = getReplicationViewMBean(secondBroker);
        assertEquals(ReplicaRole.replica, ReplicaRole.valueOf(firstBrokerReplicationView.getReplicationRole()));
//        TODO: fix this
//        assertEquals(ReplicaRole.source, ReplicaRole.valueOf(secondBrokerReplicationView.getReplicationRole()));

        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);
        MessageProducer secondBrokerProducer = secondBrokerSession.createProducer(destination);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        secondBrokerProducer.send(message);
        receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        receivedMessage.acknowledge();

        firstBrokerSession.close();
        secondBrokerSession.close();
        firstBrokerConnection.stop();
        secondBrokerConnection.stop();
    }

    @Test
    public void testPutMessagesBeforeFailover() throws Exception {
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        int retryCounter = 1;
        QueueViewMBean firstBrokerIntermediateQueueView = getQueueView(firstBroker, ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        while (firstBrokerIntermediateQueueView.getInFlightCount() <= 1) {
            sendMessages(firstBrokerProducer, MESSAGES_TO_SEND * retryCounter);
            retryCounter++;
            if (retryCounter == MAX_RETRY) {
                fail(String.format("MAX RETRY [%d] times reached! Failed to put load onto source broker!", MAX_RETRY));
            }
        }

        firstBrokerReplicationView.setReplicationRole(ReplicaRole.replica.name(), false);
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                firstBrokerReplicationView = getReplicationViewMBean(firstBroker);
                return firstBrokerReplicationView.getReplicationRole().equals(ReplicaRole.replica.name());
            }
        }, Wait.MAX_WAIT_MILLIS*2);

        Thread.sleep(SHORT_TIMEOUT);
        assertFalse(secondBroker.isStopped());

        secondBrokerReplicationView = getReplicationViewMBean(secondBroker);
        assertEquals(ReplicaRole.replica, ReplicaRole.valueOf(firstBrokerReplicationView.getReplicationRole()));
        waitUntilReplicationQueueHasConsumer(secondBroker);
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();
        ActiveMQDestination destination2 = createDestination(getDestinationString() + "No.2");


        firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        firstBrokerProducer = firstBrokerSession.createProducer(destination2);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination2);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination2);
        MessageProducer secondBrokerProducer = secondBrokerSession.createProducer(destination2);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        secondBrokerProducer.send(message);
        receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        receivedMessage.acknowledge();

        firstBrokerSession.close();
        secondBrokerSession.close();
        firstBrokerConnection.stop();
        secondBrokerConnection.stop();
    }

    @Ignore
    @Test
    public void doubleFailover() throws Exception {
        firstBrokerReplicationView.setReplicationRole(ReplicaRole.replica.name(), false);
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                firstBrokerReplicationView = getReplicationViewMBean(firstBroker);
                return firstBrokerReplicationView.getReplicationRole().equals(ReplicaRole.replica.name());
            }
        }, Wait.MAX_WAIT_MILLIS*2);

        Thread.sleep(SHORT_TIMEOUT);
        assertFalse(secondBroker.isStopped());
        waitUntilReplicationQueueHasConsumer(secondBroker);

        secondBrokerReplicationView = getReplicationViewMBean(secondBroker);
        assertEquals(ReplicaRole.replica, ReplicaRole.valueOf(firstBrokerReplicationView.getReplicationRole()));

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer secondBrokerProducer = secondBrokerSession.createProducer(destination);
        int retryCounter = 1;
        QueueViewMBean secondBrokerIntermediateQueueView = getQueueView(secondBroker, ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
        while (secondBrokerIntermediateQueueView.getInFlightCount() <= 1) {
            sendMessages(secondBrokerProducer, MESSAGES_TO_SEND * retryCounter);
            retryCounter++;
            if (retryCounter == MAX_RETRY) {
                fail(String.format("MAX RETRY [%d] times reached! Failed to put load onto source broker!", MAX_RETRY));
            }
        }

        secondBrokerReplicationView = getReplicationViewMBean(secondBroker);
        secondBrokerReplicationView.setReplicationRole(ReplicaRole.replica.name(), false);
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                secondBrokerReplicationView = getReplicationViewMBean(secondBroker);
                return secondBrokerReplicationView.getReplicationRole().equals(ReplicaRole.replica.name());
            }
        }, Wait.MAX_WAIT_MILLIS*2);

        Thread.sleep(SHORT_TIMEOUT);
        assertFalse(firstBroker.isStopped());
        secondBrokerReplicationView = getReplicationView(secondBroker);
        assertEquals(ReplicaRole.replica, ReplicaRole.valueOf(secondBrokerReplicationView.getReplicationRole()));
        waitUntilReplicationQueueHasConsumer(firstBroker);

        // firstBroker now is primary
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();
        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();
        ActiveMQDestination destination2 = createDestination(getDestinationString() + "No.2");

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination2);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createConsumer(destination2);

        secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination2);
        secondBrokerProducer = secondBrokerSession.createProducer(destination2);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        secondBrokerProducer.send(message);

        Message receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerProducer.send(message);
        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        receivedMessage.acknowledge();

        firstBrokerSession.close();
        secondBrokerSession.close();
        firstBrokerConnection.stop();
        secondBrokerConnection.stop();

    }

    private void sendMessages(MessageProducer producer, int messagesToSend) throws Exception {
        for (int i = 0; i < messagesToSend; i++) {
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName() + " No. " + i);
            producer.send(message);
        }
    }

    private void waitUntilReplicationQueueHasConsumer(BrokerService broker) throws Exception {
        assertTrue("Replication Main Queue has Consumer",
            Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    try {
                        QueueViewMBean brokerMainQueueView = getQueueView(broker, ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
                        return brokerMainQueueView.getConsumerCount() > 0;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                }
            }, Wait.MAX_WAIT_MILLIS*2));
    }

    private BrokerService setUpSecondBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(false);
        answer.getManagementContext().setCreateConnector(false);
        answer.addConnector(secondBindAddress);
        answer.setDataDirectory(SECOND_KAHADB_DIRECTORY);
        answer.setBrokerName("secondBroker");
        return answer;
    }

    private BrokerService setUpFirstBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(false);
        answer.getManagementContext().setCreateConnector(false);
        answer.addConnector(firstBindAddress);
        answer.setDataDirectory(FIRST_KAHADB_DIRECTORY);
        answer.setBrokerName("firstBroker");
        return answer;
    }

    private ReplicationViewMBean getReplicationViewMBean(BrokerService broker) throws Exception {
        MBeanServer mbeanServer = broker.getManagementContext().getMBeanServer();
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",service=Plugins,instanceName=ReplicationPlugin";
        ObjectName replicaViewMBeanName = assertRegisteredObjectName(mbeanServer, objectNameStr);
        return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, replicaViewMBeanName, ReplicationViewMBean.class, true);
    }

    public ObjectName assertRegisteredObjectName(MBeanServer mbeanServer, String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            System.out.println("Bean Registered: " + objectName);
        } else {
            System.err.println("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

}
