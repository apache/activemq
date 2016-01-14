/*
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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AMQ6117Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ6117Test.class);

    private BrokerService broker;

    @Test
    public void testViewIsStale() throws Exception {

        final int MSG_COUNT = 10;

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("Test-Queue");
        Queue dlq = session.createQueue("ActiveMQ.DLQ");

        MessageProducer producer = session.createProducer(queue);

        // Ensure there is a DLQ in existence to start.
        session.createProducer(dlq);

        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createMessage(), DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 1000);
        }

        final QueueViewMBean queueView = getProxyToQueue(dlq.getQueueName());

        assertTrue("Message should be DLQ'd", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getQueueSize() == MSG_COUNT;
            }
        }));

        LOG.info("DLQ has captured all expired messages");

        Deque<String> browsed = new LinkedList<String>();
        CompositeData[] elements = queueView.browse();
        assertEquals(MSG_COUNT, elements.length);

        for (CompositeData element : elements) {
            String messageID = (String) element.get("JMSMessageID");
            LOG.debug("MessageID: {}", messageID);
            browsed.add(messageID);
        }

        String removedMsgId = browsed.removeFirst();
        assertTrue(queueView.removeMessage(removedMsgId));
        assertEquals(MSG_COUNT - 1, queueView.getQueueSize());
        elements = queueView.browse();
        assertEquals(MSG_COUNT - 1, elements.length);

        for (CompositeData element : elements) {
            String messageID = (String) element.get("JMSMessageID");
            LOG.debug("MessageID: {}", messageID);
            assertFalse(messageID.equals(removedMsgId));
        }
    }

    @Before
    public void setup() throws Exception {

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();

        PolicyEntry pe = new PolicyEntry();
        pe.setExpireMessagesPeriod(1500);
        pe.setQueue(">");
        entries.add(pe);

        policyMap.setPolicyEntries(entries);

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}
