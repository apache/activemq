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
package org.apache.activemq.broker.region;

import java.io.File;

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

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QueuePurgeTest extends TestCase {
    private static final Log LOG = LogFactory.getLog(QueuePurgeTest.class);
    private final String MESSAGE_TEXT = new String(new byte[1024]);
    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    Queue queue;
    MessageConsumer consumer;

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setDataDirectory("target/activemq-data");
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(new File("target/activemq-data/kahadb/QueuePurgeTest"));
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        factory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri().toString());
        connection = factory.createConnection();
        connection.start();
    }

    protected void tearDown() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
        session.close();
        connection.stop();
        connection.close();
        broker.stop();
    }

    public void testPurgeQueueWithActiveConsumer() throws Exception {
        createProducerAndSendMessages(10000);
        QueueViewMBean proxy = getProxyToQueueViewMBean();
        createConsumer();
        proxy.purge();
        assertEquals("Queue size is not zero, it's " + proxy.getQueueSize(), 0,
                proxy.getQueueSize());
    }
    
    
    public void testPurgeLargeQueue() throws Exception {       
        applyBrokerSpoolingPolicy();
        createProducerAndSendMessages(90000);
        QueueViewMBean proxy = getProxyToQueueViewMBean();
        LOG.info("purging..");
        proxy.purge();
        assertEquals("Queue size is not zero, it's " + proxy.getQueueSize(), 0,
                proxy.getQueueSize());
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

    
    public void testPurgeLargeQueueWithConsumer() throws Exception {       
        applyBrokerSpoolingPolicy();
        createProducerAndSendMessages(90000);
        QueueViewMBean proxy = getProxyToQueueViewMBean();
        createConsumer();
        long start = System.currentTimeMillis();
        LOG.info("purging..");
        proxy.purge();
        LOG.info("purge done: " + (System.currentTimeMillis() - start) + "ms");
        assertEquals("Queue size is not zero, it's " + proxy.getQueueSize(), 0,
                proxy.getQueueSize());
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
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
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

    private void createConsumer() throws Exception {
        consumer = session.createConsumer(queue);
        // wait for buffer fill out
        Thread.sleep(5 * 1000);
        for (int i = 0; i < 500; ++i) {
            Message message = consumer.receive();
            message.acknowledge();
        }
    }
}
