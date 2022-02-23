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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests to ensure when the temp usage limit is updated on the broker the queues also have their
 * temp usage limits automatically updated.
 */
public class AMQ7085Test
{
    private BrokerService brokerService;
    private String testQueueName = "testAMQ7085Queue";
    private ActiveMQQueue queue = new ActiveMQQueue(testQueueName);

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        String connectionUri = brokerService.addConnector("tcp://localhost:0").getPublishableConnectString();
        brokerService.start();
        brokerService.waitUntilStarted();

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUri);
        final Connection conn = connectionFactory.createConnection();
        try {
            conn.start();
            final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination queue = session.createQueue(testQueueName);
            final Message toSend = session.createMessage();
            toSend.setStringProperty("foo", "bar");
            final MessageProducer producer = session.createProducer(queue);
            producer.send(queue, toSend);
        } finally {
            conn.close();
        }
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test
    public void testQueueTempUsageWhenSetExplicitly() throws Exception {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + queue.getQueueName());
        QueueViewMBean queueViewMBean = (QueueViewMBean) brokerService.getManagementContext().newProxyInstance(
                queueViewMBeanName, QueueViewMBean.class, true);

        // Check that by default the queue's temp limit is the same as the broker's.
        BrokerView brokerView = brokerService.getAdminView();
        long brokerTempLimit = brokerView.getTempLimit();
        assertEquals(brokerTempLimit, queueViewMBean.getTempUsageLimit());

        // Change the queue's temp limit independently of the broker's setting and check the broker's limit does not
        // change.
        long queueTempLimit = brokerTempLimit + 111;
        queueViewMBean.setTempUsageLimit(queueTempLimit);
        assertEquals(queueViewMBean.getTempUsageLimit(), queueTempLimit);
        assertEquals(brokerView.getTempLimit(), brokerTempLimit);

        // Now increase the broker's temp limit.  Since the queue's limit was explicitly changed it should remain
        // unchanged.
        long newBrokerTempLimit = brokerTempLimit + 555;
        brokerView.setTempLimit(newBrokerTempLimit);
        assertEquals(brokerView.getTempLimit(), newBrokerTempLimit);
        assertEquals(queueViewMBean.getTempUsageLimit(), queueTempLimit);

        //Verify that TempUsage is cleaned up on Queue stop
        brokerService.getDestination(queue).stop();
        assertFalse(brokerService.getDestination(queue).getTempUsage().isStarted());
    }

    @Test
    public void testQueueTempUsageWhenBrokerTempUsageUpdated() throws Exception {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + queue.getQueueName());
        QueueViewMBean queueViewMBean = (QueueViewMBean) brokerService.getManagementContext().newProxyInstance(
                queueViewMBeanName, QueueViewMBean.class, true);

        // Check that by default the queue's temp limit is the same as the broker's.
        BrokerView brokerView = brokerService.getAdminView();
        long brokerTempLimit = brokerView.getTempLimit();
        assertEquals(brokerTempLimit, queueViewMBean.getTempUsageLimit());

        // Increase the broker's temp limit and check the queue's limit is updated to the same value.
        long newBrokerTempLimit = brokerTempLimit + 555;
        brokerView.setTempLimit(newBrokerTempLimit);
        assertEquals(brokerView.getTempLimit(), newBrokerTempLimit);
        assertEquals(queueViewMBean.getTempUsageLimit(), newBrokerTempLimit);

        //Verify that TempUsage is cleaned up on Queue stop
        brokerService.getDestination(queue).stop();
        assertFalse(brokerService.getDestination(queue).getTempUsage().isStarted());
    }
}
