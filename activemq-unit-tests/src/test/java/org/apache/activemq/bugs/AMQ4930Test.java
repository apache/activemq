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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4930Test extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ4930Test.class);
    final int messageCount = 150;
    final int messageSize = 1024*1024;
    final int maxBrowsePageSize = 50;
    final ActiveMQQueue bigQueue = new ActiveMQQueue("BIG");
    BrokerService broker;
    ActiveMQConnectionFactory factory;

    protected void configureBroker() throws Exception {
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setAdvisorySupport(false);
        broker.getSystemUsage().getMemoryUsage().setLimit(1*1024*1024);

        PolicyMap pMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        // disable expriy processing as this will call browse in parallel
        policy.setExpireMessagesPeriod(0);
        policy.setMaxPageSize(maxBrowsePageSize);
        policy.setMaxBrowsePageSize(maxBrowsePageSize);
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);
    }

    public void testBrowsePendingNonPersistent() throws Exception {
        doTestBrowsePending(DeliveryMode.NON_PERSISTENT);
    }

    public void testBrowsePendingPersistent() throws Exception {
        doTestBrowsePending(DeliveryMode.PERSISTENT);
    }

    public void testWithStatsDisabled() throws Exception {
        ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().setEnabled(false);
        doTestBrowsePending(DeliveryMode.PERSISTENT);
    }

    public void doTestBrowsePending(int deliveryMode) throws Exception {

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(bigQueue);
        producer.setDeliveryMode(deliveryMode);
        BytesMessage bytesMessage = session.createBytesMessage();
        bytesMessage.writeBytes(new byte[messageSize]);

        for (int i = 0; i < messageCount; i++) {
            producer.send(bigQueue, bytesMessage);
        }

        final QueueViewMBean queueViewMBean = (QueueViewMBean)
                broker.getManagementContext().newProxyInstance(broker.getAdminView().getQueues()[0], QueueViewMBean.class, false);

        LOG.info(queueViewMBean.getName() + " Size: " + queueViewMBean.getEnqueueCount());

        connection.close();

        assertFalse("Cache disabled on q", queueViewMBean.isCacheEnabled());

        // ensure repeated browse does now blow mem

        final Queue underTest = (Queue) ((RegionBroker)broker.getRegionBroker()).getQueueRegion().getDestinationMap().get(bigQueue);

        // do twice to attempt to pull in 2*maxBrowsePageSize which uses up the system memory limit
        Message[] browsed = underTest.browse();
        LOG.info("Browsed: " + browsed.length);
        assertEquals("maxBrowsePageSize", maxBrowsePageSize, browsed.length);
        browsed = underTest.browse();
        LOG.info("Browsed: " + browsed.length);
        assertEquals("maxBrowsePageSize", maxBrowsePageSize, browsed.length);
        Runtime.getRuntime().gc();
        long free = Runtime.getRuntime().freeMemory()/1024;
        LOG.info("free at start of check: " + free);
        // check for memory growth
        for (int i=0; i<10; i++) {
            LOG.info("free: " + Runtime.getRuntime().freeMemory()/1024);
            browsed = underTest.browse();
            LOG.info("Browsed: " + browsed.length);
            assertEquals("maxBrowsePageSize", maxBrowsePageSize, browsed.length);
            Runtime.getRuntime().gc();
            Runtime.getRuntime().gc();
            assertTrue("No growth: " + Runtime.getRuntime().freeMemory()/1024 + " >= " + (free - (free * 0.2)), Runtime.getRuntime().freeMemory()/1024 >= (free - (free * 0.2)));
        }
    }


    protected void setUp() throws Exception {
        super.setUp();
        broker = new BrokerService();
        broker.setBrokerName("thisOne");
        configureBroker();
        broker.start();
        factory = new ActiveMQConnectionFactory("vm://thisOne?jms.alwaysSyncSend=true");
        factory.setWatchTopicAdvisories(false);

    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }

}