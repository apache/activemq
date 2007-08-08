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

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import junit.textui.TestRunner;

/**
 * A specific test of Queue.purge() functionality
 *
 * @version $Revision$
 */
public class PurgeTest extends EmbeddedBrokerTestSupport {

    protected MBeanServer mbeanServer;
    protected String domain = "org.apache.activemq";
    protected String clientID = "foo";

    protected Connection connection;
    protected boolean transacted;
    protected int authMode = Session.AUTO_ACKNOWLEDGE;
    protected int messageCount = 10;

    public static void main(String[] args) {
        TestRunner.run(PurgeTest.class);
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
        ObjectName queueViewMBeanName = assertRegisteredObjectName(domain + ":Type=Queue,Destination=" + getDestinationString() + ",BrokerName=localhost");
        QueueViewMBean proxy = (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        long count = proxy.getQueueSize();
        assertEquals("Queue size", count, messageCount);

        proxy.purge();
        count = proxy.getQueueSize();
        assertEquals("Queue size", count, 0);

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
    }

    protected ObjectName assertRegisteredObjectName(String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            echo("Bean Registered: " + objectName);
        }
        else {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:61616";
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
        answer.setPersistent(false);
        answer.addConnector(bindAddress);   
        return answer;
    }

    protected void echo(String text) {
        LOG.info(text);
    }
}
