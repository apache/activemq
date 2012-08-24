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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ3992Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ3992Test.class);
    private static BrokerService brokerService;
    private static String BROKER_ADDRESS = "tcp://localhost:0";

    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
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
        connectionFactory.getPrefetchPolicy().setAll(0);

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

        LOG.info("Enqueue counter for sub before pull requests: " + sub.getEnqueueCounter());

        // Trigger some pull Timeouts.
        consumer.receive(500);
        consumer.receive(500);
        consumer.receive(500);
        consumer.receive(500);
        consumer.receive(500);

        // Let them all timeout.
        Thread.sleep(600);

        LOG.info("Enqueue counter for sub after pull requests: " + sub.getEnqueueCounter());
        assertEquals(0, sub.getEnqueueCounter());

        consumer.close();
        session.close();
        connection.close();
    }
}
