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
import static org.junit.Assert.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(ParallelTest.class)
public class AMQ3145Test {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ3145Test.class);
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
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
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
        try {
            if (consumer != null) {
                consumer.close();
            }
            session.close();
            connection.stop();
            connection.close();
        } catch (Exception e) {
            //swallow any error so broker can still be stopped
        }
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

    private QueueViewMBean getProxyToQueueViewMBean()
            throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq"
                + ":destinationType=Queue,destinationName=" + queue.getQueueName()
                + ",type=Broker,brokerName=localhost");
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
