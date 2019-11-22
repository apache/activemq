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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import static org.junit.Assert.*;

// https://issues.apache.org/jira/browse/AMQ-7302
public class JmxOpPageInOnMemoryLimitTest {

    BrokerService broker;
    protected MBeanServer mbeanServer;
    protected String domain = "org.apache.activemq";

    protected Connection connection;
    protected int messageCount = 4000;
    ActiveMQQueue destination = new ActiveMQQueue("QUEUE_TO_FILL_PAST_MEM_LIMIT");
    String lastMessageId = "";

    @Test(timeout = 60*1000)
    public void testNoHangOnPageInForJmxOps() throws Exception {

        // Now get the QueueViewMBean and ...
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName="+destination.getQueueName();
        ObjectName queueViewMBeanName = assertRegisteredObjectName(objectNameStr);
        final QueueViewMBean proxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        assertFalse("limit reached, cache disabled", proxy.isCacheEnabled());

        proxy.removeMessage(lastMessageId);

        proxy.copyMessageTo(lastMessageId, "someOtherQ");

        proxy.moveMatchingMessagesTo("JMSMessageID = '" + lastMessageId +  "'","someOtherQ");


        // flick dlq flag to allow retry work
        proxy.setDLQ(true);
        proxy.retryMessages();

        try {
            proxy.retryMessage(lastMessageId);
        } catch (JMSException expected) {
            assertTrue("Could not find", expected.getMessage().contains("find"));
        }

        long count = proxy.getQueueSize();
        boolean cursorFull = proxy.getCursorPercentUsage() >= 70;
        assertTrue("Cursor full", cursorFull);

        assertEquals("Queue size", messageCount, count);
    }

    private String produceMessages() throws Exception {
        connection = createConnectionFactory().createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String trackLastMessageId = "";
        MessageProducer producer = session.createProducer(destination);
        final byte[] payload = new byte[1024];
        for (int i = 0; i < messageCount; i++) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload);
            producer.send(message);
            trackLastMessageId = message.getJMSMessageID();
        }
        producer.close();
        connection.close();
        return trackLastMessageId;
    }


    protected ObjectName assertRegisteredObjectName(String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (!mbeanServer.isRegistered(objectName)) {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

    @Before
    public void setUp() throws Exception {
        createBroker();
        mbeanServer = broker.getManagementContext().getMBeanServer();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        broker.stop();
    }

    protected BrokerService createBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setEnableStatistics(true);
        broker.addConnector("tcp://localhost:0");
        ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).setConcurrentStoreAndDispatchQueues(false);

        broker.deleteAllMessages();

        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setMemoryLimit(1024*1024);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        lastMessageId = produceMessages();
        return broker;
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
    }

}
