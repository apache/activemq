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

import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.region.policy.FilePendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.StorePendingDurableSubscriberMessageStoragePolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class AMQ4656Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ4656Test.class);
    private static BrokerService brokerService;
    private static String BROKER_ADDRESS = "tcp://localhost:0";

    private String connectionUri;

    @Parameterized.Parameter
    public PendingDurableSubscriberMessageStoragePolicy pendingDurableSubPolicy;

    @Parameterized.Parameters(name="{0}")
    public static Iterable<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{{new FilePendingDurableSubscriberMessageStoragePolicy()},{new StorePendingDurableSubscriberMessageStoragePolicy()}});
    }

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setPendingDurableSubscriberPolicy(pendingDurableSubPolicy);
        policyMap.setDefaultEntry(defaultEntry);
        brokerService.setDestinationPolicy(policyMap);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.setDeleteAllMessagesOnStartup(true);
        connectionUri = brokerService.addConnector(BROKER_ADDRESS).getPublishableConnectString();
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test
    public void testDurableConsumerEnqueueCountWithZeroPrefetch() throws Exception {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUri);

        Connection connection = connectionFactory.createConnection();
        connection.setClientID(getClass().getName());
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic("DurableTopic");

        MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "EnqueueSub");

        BrokerView view = brokerService.getAdminView();
        view.getDurableTopicSubscribers();

        ObjectName subName = view.getDurableTopicSubscribers()[0];

        DurableSubscriptionViewMBean sub = (DurableSubscriptionViewMBean)
            brokerService.getManagementContext().newProxyInstance(subName, DurableSubscriptionViewMBean.class, true);

        assertEquals(0, sub.getEnqueueCounter());
        assertEquals(0, sub.getDequeueCounter());
        assertEquals(0, sub.getPendingQueueSize());
        assertEquals(0, sub.getDispatchedCounter());
        assertEquals(0, sub.getDispatchedQueueSize());

        consumer.close();

        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < 20; i++) {
            producer.send(session.createMessage());
        }
        producer.close();

        consumer = session.createDurableSubscriber((Topic) destination, "EnqueueSub");

        Thread.sleep(1000);

        assertEquals(20, sub.getEnqueueCounter());
        assertEquals(0, sub.getDequeueCounter());
        assertEquals(0, sub.getPendingQueueSize());
        assertEquals(20, sub.getDispatchedCounter());
        assertEquals(20, sub.getDispatchedQueueSize());

        LOG.info("Pending Queue Size with no receives: {}", sub.getPendingQueueSize());

        assertNotNull(consumer.receive(1000));
        assertNotNull(consumer.receive(1000));

        consumer.close();

        Thread.sleep(2000);

        LOG.info("Pending Queue Size with two receives: {}", sub.getPendingQueueSize());

        assertEquals(20, sub.getEnqueueCounter());
        assertEquals(2, sub.getDequeueCounter());
        assertEquals(18, sub.getPendingQueueSize());
        assertEquals(20, sub.getDispatchedCounter());
        assertEquals(0, sub.getDispatchedQueueSize());

        session.close();
        connection.close();
    }
}