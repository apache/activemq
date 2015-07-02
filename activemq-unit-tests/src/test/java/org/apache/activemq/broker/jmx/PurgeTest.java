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
package org.apache.activemq.broker.jmx;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import junit.framework.Test;
import junit.textui.TestRunner;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specific test of Queue.purge() functionality
 */
public class PurgeTest extends EmbeddedBrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeTest.class);

    protected MBeanServer mbeanServer;
    protected String domain = "org.apache.activemq";
    protected String clientID = "foo";

    protected Connection connection;
    protected boolean transacted;
    protected int authMode = Session.AUTO_ACKNOWLEDGE;
    protected int messageCount = 10;
    public PersistenceAdapter persistenceAdapter;

    public static void main(String[] args) {
        TestRunner.run(PurgeTest.class);
    }

    public static Test suite() {
        return suite(PurgeTest.class);
    }

    public void testPurge() throws Exception {
        // Send some messages
        connection = connectionFactory.createConnection();
        connection.setClientID(clientID);
        connection.start();
        Session session = connection.createSession(transacted, authMode);
        destination = createDestination();
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < messageCount; i++) {
            Message message = session.createTextMessage("Message: " + i);
            producer.send(message);
        }

        // Now get the QueueViewMBean and purge
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName="+getDestinationString();
        ObjectName queueViewMBeanName = assertRegisteredObjectName(objectNameStr);
        QueueViewMBean proxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        long count = proxy.getQueueSize();
        assertEquals("Queue size", count, messageCount);

        assertEquals("Browse size", messageCount, proxy.browseMessages().size());

        proxy.purge();
        count = proxy.getQueueSize();
        assertEquals("Queue size", count, 0);
        assertEquals("Browse size", proxy.browseMessages().size(), 0);

        // Queues have a special case once there are more than a thousand
        // dead messages, make sure we hit that.
        messageCount += 1000;
        for (int i = 0; i < messageCount; i++) {
            Message message = session.createTextMessage("Message: " + i);
            producer.send(message);
        }

        count = proxy.getQueueSize();
        assertEquals("Queue size", count, messageCount);

        proxy.purge();
        count = proxy.getQueueSize();
        assertEquals("Queue size", count, 0);
        assertEquals("Browse size", proxy.browseMessages().size(), 0);

        producer.close();
    }

    public void initCombosForTestDelete() {
        addCombinationValues("persistenceAdapter", new Object[] {new MemoryPersistenceAdapter(), new KahaDBPersistenceAdapter()});
    }

    public void testDeleteSameProducer() throws Exception {
        connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination();

        MessageProducer producer = session.createProducer(destination);
        Message message = session.createTextMessage("Test Message");
        producer.send(message);

        MessageConsumer consumer = session.createConsumer(destination);

        Message received = consumer.receive(1000);
        assertEquals(message, received);

        ObjectName brokerViewMBeanName = assertRegisteredObjectName(domain + ":type=Broker,brokerName=localhost");
        BrokerViewMBean brokerProxy = (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, brokerViewMBeanName, BrokerViewMBean.class, true);

        brokerProxy.removeQueue(getDestinationString());
        producer.send(message);

        received = consumer.receive(1000);

        assertNotNull("Message not received", received);
        assertEquals(message, received);
    }

    public void testDelete() throws Exception {
        // Send some messages
        connection = connectionFactory.createConnection();
        connection.setClientID(clientID);
        connection.start();
        Session session = connection.createSession(transacted, authMode);
        destination = createDestination();
        sendMessages(session, messageCount);

        // Now get the QueueViewMBean and purge

        ObjectName queueViewMBeanName = assertRegisteredObjectName(domain + ":type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + getDestinationString());
        QueueViewMBean queueProxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        ObjectName brokerViewMBeanName = assertRegisteredObjectName(domain + ":type=Broker,brokerName=localhost");
        BrokerViewMBean brokerProxy = (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, brokerViewMBeanName, BrokerViewMBean.class, true);

        long count = queueProxy.getQueueSize();
        assertEquals("Queue size", count, messageCount);

        brokerProxy.removeQueue(getDestinationString());

        sendMessages(session, messageCount);

        queueViewMBeanName = assertRegisteredObjectName(domain + ":type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + getDestinationString());
        queueProxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        count = queueProxy.getQueueSize();
        assertEquals("Queue size", count, messageCount);

        queueProxy.purge();

        // Queue have a special case once there are more than a thousand
        // dead messages, make sure we hit that.
        messageCount += 1000;
        sendMessages(session, messageCount);

        count = queueProxy.getQueueSize();
        assertEquals("Queue size", count, messageCount);

        brokerProxy.removeQueue(getDestinationString());

        sendMessages(session, messageCount);

        queueViewMBeanName = assertRegisteredObjectName(domain + ":type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + getDestinationString());
        queueProxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        count = queueProxy.getQueueSize();
        assertEquals("Queue size", count, messageCount);
    }

    private void sendMessages(Session session, int count) throws Exception {
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < messageCount; i++) {
            Message message = session.createTextMessage("Message: " + i);
            producer.send(message);
        }
    }

    protected ObjectName assertRegisteredObjectName(String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            echo("Bean Registered: " + objectName);
        } else {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:0";
        useTopic = false;
        super.setUp();
        mbeanServer = broker.getManagementContext().getMBeanServer();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setEnableStatistics(true);
        answer.addConnector(bindAddress);
        answer.setPersistenceAdapter(persistenceAdapter);
        answer.deleteAllMessages();
        return answer;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
    }

    protected void echo(String text) {
        LOG.info(text);
    }

    /**
     * Returns the name of the destination used in this test case
     */
    protected String getDestinationString() {
        return getClass().getName() + "." + getName(true);
    }
}
