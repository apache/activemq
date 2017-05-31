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
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConcurrentMoveTest extends EmbeddedBrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentMoveTest.class);

    protected MBeanServer mbeanServer;
    protected String domain = "org.apache.activemq";

    protected Connection connection;
    protected boolean transacted;
    protected int authMode = Session.AUTO_ACKNOWLEDGE;
    protected int messageCount = 2000;


    public void testConcurrentMove() throws Exception {

        // Send some messages
        connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(transacted, authMode);
        destination = createDestination();
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < messageCount; i++) {
            Message message = session.createTextMessage("Message: " + i);
            producer.send(message);
        }

        long usageBeforMove = broker.getPersistenceAdapter().size();
        LOG.info("Store usage:"  + usageBeforMove);

        // Now get the QueueViewMBean and purge
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName="+getDestinationString();
        ObjectName queueViewMBeanName = assertRegisteredObjectName(objectNameStr);
        final QueueViewMBean proxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        final ActiveMQQueue to = new ActiveMQQueue("TO");
        ((RegionBroker)broker.getRegionBroker()).addDestination(broker.getAdminConnectionContext(), to, false);

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i=0; i<50; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        proxy.moveMatchingMessagesTo(null, to.getPhysicalName());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);

        long count = proxy.getQueueSize();
        assertEquals("Queue size", count, 0);
        assertEquals("Browse size", proxy.browseMessages().size(), 0);

        objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName="+to.getQueueName();
        queueViewMBeanName = assertRegisteredObjectName(objectNameStr);
        QueueViewMBean toProxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        count = toProxy.getQueueSize();
        assertEquals("Queue size", count, messageCount);

        long usageAfterMove = broker.getPersistenceAdapter().size();
        LOG.info("Store usage, before: " + usageBeforMove + ", after:"  + usageAfterMove);
        LOG.info("Store size increase:" + FileUtils.byteCountToDisplaySize(usageAfterMove - usageBeforMove));

        assertTrue("Usage not more than doubled", usageAfterMove < (usageBeforMove * 3));

        producer.close();
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
        ((KahaDBPersistenceAdapter)answer.getPersistenceAdapter()).setConcurrentStoreAndDispatchQueues(false);
        answer.deleteAllMessages();
        return answer;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
    }

    /**
     * Returns the name of the destination used in this test case
     */
    protected String getDestinationString() {
        return getClass().getName() + "." + getName(true);
    }
}
