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

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.usage.SystemUsage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ2801Test
{
    private static final Logger LOG = LoggerFactory.getLogger(AMQ2801Test.class);

    private static final String TOPICNAME = "InvalidPendingQueueTest";
    private static final String SELECTOR1 = "JMS_ID" + " = '" + "TEST" + "'";
    private static final String SELECTOR2 = "JMS_ID" + " = '" + "TEST2" + "'";
    private static final String SUBSCRIPTION1 = "InvalidPendingQueueTest_1";
    private static final String SUBSCRIPTION2 = "InvalidPendingQueueTest_2";
    private static final int MSG_COUNT = 2500;
    private Session session1;
    private Connection conn1;
    private Topic topic1;
    private MessageConsumer consumer1;
    private Session session2;
    private Connection conn2;
    private Topic topic2;
    private MessageConsumer consumer2;
    private BrokerService broker;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("tcp://localhost:0").setName("Default");
        applyMemoryLimitPolicy(broker);
        broker.start();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    private void applyMemoryLimitPolicy(BrokerService broker) {
        final SystemUsage memoryManager = new SystemUsage();
        memoryManager.getMemoryUsage().setLimit(5818230784L);
        memoryManager.getStoreUsage().setLimit(6442450944L);
        memoryManager.getTempUsage().setLimit(3221225472L);
        broker.setSystemUsage(memoryManager);

        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
        final PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setProducerFlowControl(false);
        entry.setMemoryLimit(504857608);
        entry.setPendingQueuePolicy(new FilePendingQueueMessageStoragePolicy());
        policyEntries.add(entry);

        final PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(policyEntries);
        broker.setDestinationPolicy(policyMap);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }

        conn1.close();
        conn2.close();
    }

    private void produceMessages() throws Exception {
        TopicConnection connection = createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(TOPICNAME);
        TopicPublisher producer = session.createPublisher(topic);
        connection.start();
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        long tStamp = System.currentTimeMillis();
        BytesMessage message = session2.createBytesMessage();
        for (int i = 1; i <= MSG_COUNT; i++)
        {
            message.setStringProperty("JMS_ID", "TEST");
            message.setIntProperty("Type", i);
            producer.publish(message);
            if (i%100 == 0) {
                LOG.info("sent: " + i + " @ " + ((System.currentTimeMillis() - tStamp) / 100)  + "m/ms");
                tStamp = System.currentTimeMillis() ;
            }
        }
    }

    private void activeateSubscribers() throws Exception {
        // First consumer
        conn1 = createConnection();
        conn1.setClientID(SUBSCRIPTION1);
        session1 = conn1.createSession(true, Session.SESSION_TRANSACTED);
        topic1 = session1.createTopic(TOPICNAME);
        consumer1 = session1.createDurableSubscriber(topic1, SUBSCRIPTION1, SELECTOR1, false);
        conn1.start();

        // Second consumer that just exists
        conn2 = createConnection();
        conn2.setClientID(SUBSCRIPTION2);
        session2 = conn2.createSession(true, Session.SESSION_TRANSACTED);
        topic2 = session2.createTopic(TOPICNAME);
        consumer2 = session2.createDurableSubscriber(topic2, SUBSCRIPTION2, SELECTOR2, false);
        conn2.start();
    }

    @Test
    public void testInvalidPendingQueue() throws Exception {

        activeateSubscribers();

        assertNotNull(consumer1);
        assertNotNull(consumer2);

        produceMessages();
        LOG.debug("Sent messages to a single subscriber");
        Thread.sleep(2000);

        LOG.debug("Closing durable subscriber connections");
        conn1.close();
        conn2.close();
        LOG.debug("Closed durable subscriber connections");

        Thread.sleep(2000);
        LOG.debug("Re-starting durable subscriber connections");

        activeateSubscribers();
        LOG.debug("Started up durable subscriber connections - now view activemq console to see pending queue size on the other subscriber");

        ObjectName[] subs = broker.getAdminView().getDurableTopicSubscribers();

        for (int i = 0; i < subs.length; i++) {
            ObjectName subName = subs[i];
            DurableSubscriptionViewMBean sub = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(subName, DurableSubscriptionViewMBean.class, true);

            LOG.info(sub.getSubscriptionName() + ": pending = " + sub.getPendingQueueSize());
            if(sub.getSubscriptionName().equals(SUBSCRIPTION1)) {
                assertEquals("Incorrect number of pending messages", MSG_COUNT, sub.getPendingQueueSize());
            } else {
                assertEquals("Incorrect number of pending messages", 0, sub.getPendingQueueSize());
            }
        }
    }

    private TopicConnection createConnection() throws Exception
    {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setBrokerURL(connectionUri);
        TopicConnection conn = connectionFactory.createTopicConnection();
        return conn;
    }

}
