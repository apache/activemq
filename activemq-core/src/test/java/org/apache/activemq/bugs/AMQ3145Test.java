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

import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AMQ3145Test {
    private static final Log LOG = LogFactory.getLog(AMQ3145Test.class);
    private final String MESSAGE_TEXT = new String(new byte[1024]);
    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    Queue queue;
    MessageConsumer consumer;

    @Before
    public void createBroker() throws Exception {
        createBroker(true);
    }

    public void createBroker(boolean deleteAll) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(deleteAll);
        broker.setDataDirectory("target/AMQ3145Test");
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        factory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri().toString());
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
        session.close();
        connection.stop();
        connection.close();
        broker.stop();
    }

    @Test
    public void testCacheDisableReEnable() throws Exception {
        createProducerAndSendMessages(1);
        QueueViewMBean proxy = getProxyToQueueViewMBean();
        assertTrue("cache is enabled", proxy.isCacheEnabled());
        tearDown();
        createBroker(false);
        proxy = getProxyToQueueViewMBean();
        assertEquals("one pending message", 1, proxy.getQueueSize());
        assertTrue("cache is disabled when there is a pending message", !proxy.isCacheEnabled());

        createConsumer(1);
        createProducerAndSendMessages(1);
        assertTrue("cache is enabled again on next send when there are no messages", proxy.isCacheEnabled());
    }

    private void applyBrokerSpoolingPolicy() {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setProducerFlowControl(false);
        PendingQueueMessageStoragePolicy pendingQueuePolicy = new FilePendingQueueMessageStoragePolicy();
        defaultEntry.setPendingQueuePolicy(pendingQueuePolicy);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
    }

    private QueueViewMBean getProxyToQueueViewMBean()
            throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq"
                + ":Type=Queue,Destination=" + queue.getQueueName()
                + ",BrokerName=localhost");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName,
                        QueueViewMBean.class, true);
        return proxy;
    }

    private void createProducerAndSendMessages(int numToSend) throws Exception {
        queue = session.createQueue("test1");
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < numToSend; i++) {
            TextMessage message = session.createTextMessage(MESSAGE_TEXT + i);
            if (i  != 0 && i % 50000 == 0) {
                LOG.info("sent: " + i);
            }
            producer.send(message);
        }
        producer.close();
    }

    private void createConsumer(int numToConsume) throws Exception {
        consumer = session.createConsumer(queue);
        // wait for buffer fill out
        for (int i = 0; i < numToConsume; ++i) {
            Message message = consumer.receive(2000);
            message.acknowledge();
        }
        consumer.close();
    }
}