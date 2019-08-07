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

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AMQ7270Test extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ7270Test.class);
    final int messageCount = 150;
    final int messageSize = 1024*1024;
    final int maxPageSize = 50;
    final ActiveMQQueue activeMQQueue = new ActiveMQQueue("BIG");
    BrokerService broker;
    ActiveMQConnectionFactory factory;

    protected void configureBroker() throws Exception {
        broker.setPersistent(false);
        broker.setAdvisorySupport(false);

        PolicyMap pMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        // disable expriy processing as this will call browse in parallel
        policy.setExpireMessagesPeriod(0);
        policy.setMaxPageSize(maxPageSize);
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);
    }

    public void testConcurrentCopyMatchingPageSizeOk() throws Exception {

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(activeMQQueue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        BytesMessage bytesMessage = session.createBytesMessage();

        for (int i = 0; i < messageCount; i++) {
            bytesMessage.setIntProperty("id", i);
            producer.send(activeMQQueue, bytesMessage);
        }

        final QueueViewMBean queueViewMBean = (QueueViewMBean)
                broker.getManagementContext().newProxyInstance(broker.getAdminView().getQueues()[0], QueueViewMBean.class, false);

        LOG.info(queueViewMBean.getName() + " Size: " + queueViewMBean.getEnqueueCount());

        connection.close();


        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i=0; i<20; i++) {

            executor.submit(new Runnable() {
                @Override
                public void run() {

                    try {
                        // only match the last to require pageIn
                        queueViewMBean.copyMatchingMessagesTo("id=" + (messageCount - 1), "To");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executor.shutdown();
        assertTrue("all work done", executor.awaitTermination(30, TimeUnit.SECONDS));

        final Queue underTest = (Queue) ((RegionBroker)broker.getRegionBroker()).getQueueRegion().getDestinationMap().get(activeMQQueue);
        assertEquals("page Size as expected " + underTest, maxPageSize, underTest.getMaxPageSize());
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