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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MBeanOperationTimeoutTest {
    private static final Logger LOG = LoggerFactory.getLogger(MBeanOperationTimeoutTest.class);

    private ActiveMQConnectionFactory connectionFactory;
    private BrokerService broker;
    private String connectionUri;
    private static final String destinationName = "MBeanOperationTimeoutTestQ";
    private static final String moveToDestinationName = "MBeanOperationTimeoutTestQ.Moved";

    protected MBeanServer mbeanServer;
    protected String domain = "org.apache.activemq";

    protected int messageCount = 50000;

    @Test(expected = TimeoutException.class)
    public void testLongOperationTimesOut() throws Exception {

        sendMessages(messageCount);
        LOG.info("Produced " + messageCount + " messages to the broker.");

        // Now get the QueueViewMBean and purge
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName="+destinationName;

        ObjectName queueViewMBeanName = assertRegisteredObjectName(objectNameStr);
        QueueViewMBean proxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        long count = proxy.getQueueSize();
        assertEquals("Queue size", count, messageCount);

        LOG.info("Attempting to move one message, TimeoutException expected");
        proxy.moveMatchingMessagesTo(null, moveToDestinationName);
    }

    private void sendMessages(int count) throws Exception {
        Connection connection = connectionFactory.createConnection();
        try {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue(destinationName);
            MessageProducer producer = session.createProducer(destination);
            for (int i = 0; i < messageCount; i++) {
                Message message = session.createMessage();
                message.setIntProperty("id", i);
                producer.send(message);
            }
            session.commit();
        } finally {
            connection.close();
        }
    }

    protected ObjectName assertRegisteredObjectName(String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            LOG.info("Bean Registered: " + objectName);
        } else {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
        mbeanServer = broker.getManagementContext().getMBeanServer();
    }

    @After
    public void tearDown() throws Exception {
        Thread.sleep(500);
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setMbeanInvocationTimeout(TimeUnit.SECONDS.toMillis(1));
        answer.setUseJmx(true);
        answer.addConnector("vm://localhost");
        answer.setDeleteAllMessagesOnStartup(true);
        return answer;
    }
}
