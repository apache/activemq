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
package org.apache.activemq.broker.virtual;


import junit.framework.Assert;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.CompositeTopic;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.ByteSequence;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Test to ensure the CompositeTopic Memory Usage returns to zero after messages forwarded to underlying queues
 */

public class CompositeTopicMemoryUsageTest {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeTopicMemoryUsageTest.class);
    public int messageSize = 5*1024;
    public int messageCount = 1000;
    ActiveMQTopic target = new ActiveMQTopic("target");
    BrokerService brokerService;
    ActiveMQConnectionFactory connectionFactory;

    @Test
    public void testMemoryUsage() throws Exception {
        startBroker(4, true);

        messageSize = 20*1024;

        produceMessages(20, target);

        long memoryUsage = getMemoryUsageForTopic(target.getPhysicalName());
        Assert.assertEquals("MemoryUsage should be zero",0l, memoryUsage);

        brokerService.stop();
        brokerService.waitUntilStopped();

    }

    private long getMemoryUsageForTopic(String topicName) throws Exception {
        ObjectName[] topics = brokerService.getAdminView().getTopics();

        for (ObjectName objectName: topics) {

            if (objectName.getCanonicalName().contains(topicName)) {
                TopicViewMBean topicViewMBean = (TopicViewMBean)
                        brokerService.getManagementContext().newProxyInstance(objectName, TopicViewMBean.class, false);
                return topicViewMBean.getMemoryUsageByteCount();
            }
        }
        throw new Exception("NO TOPIC FOUND");
    }




    protected void produceMessages(int messageCount, ActiveMQDestination destination) throws Exception {
        final ByteSequence payLoad = new ByteSequence(new byte[messageSize]);
        Connection connection = connectionFactory.createConnection();
        MessageProducer messageProducer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createProducer(destination);
        messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();
        message.setContent(payLoad);
        for(int i =0; i< messageCount; i++){
            messageProducer.send(message);
        }
        connection.close();
    }

    private void startBroker(int fanoutCount, boolean concurrentSend) throws Exception {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setUseVirtualTopics(true);
        brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.setAdvisorySupport(false);
        PolicyMap destPolicyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(0);
        defaultEntry.setOptimizedDispatch(true);
        defaultEntry.setCursorMemoryHighWaterMark(110);
        destPolicyMap.setDefaultEntry(defaultEntry);
        brokerService.setDestinationPolicy(destPolicyMap);

        CompositeTopic route = new CompositeTopic();
        route.setName("target");
        route.setForwardOnly(false);
        route.setConcurrentSend(concurrentSend);
        Collection<ActiveMQQueue> routes = new ArrayList<ActiveMQQueue>();
        for (int i=0; i<fanoutCount; i++) {
            routes.add(new ActiveMQQueue("route." + i));
        }
        route.setForwardTo(routes);
        VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        interceptor.setVirtualDestinations(new VirtualDestination[]{route});
        brokerService.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        brokerService.start();

        connectionFactory = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getPublishableConnectString());
        connectionFactory.setWatchTopicAdvisories(false);

    }
}