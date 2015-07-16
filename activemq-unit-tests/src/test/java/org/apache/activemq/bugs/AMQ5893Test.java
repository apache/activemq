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
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ5893Test {

    private static Logger LOG = LoggerFactory.getLogger(AMQ5893Test.class);

    private final int MSG_COUNT = 20;

    private BrokerService brokerService;
    private Connection connection;
    private String brokerURI;
    private CountDownLatch done;

    @Rule public TestName name = new TestName();

    @Before
    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        brokerService.waitUntilStarted();

        brokerURI = "failover:" + brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString();
    }

    @After
    public void stopBroker() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ex) {}
        }

        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test(timeout = 60000)
    public void tesIndividualAcksWithClosedConsumerAndAuditAsync() throws Exception {
        produceSomeMessages(MSG_COUNT);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        assertEquals(MSG_COUNT, queueView.getQueueSize());

        connection = createConnection();
        Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();

        // Consume all messages with no ACK
        done = new CountDownLatch(MSG_COUNT);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                LOG.debug("Received message: {}", message);
                done.countDown();
            }
        });
        done.await(15, TimeUnit.SECONDS);
        consumer.close();
        assertEquals(MSG_COUNT, queueView.getQueueSize());

        // Consumer the same batch again.
        consumer = session.createConsumer(queue);
        done = new CountDownLatch(MSG_COUNT);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                LOG.debug("Received message: {}", message);
                done.countDown();
            }
        });
        done.await(15, TimeUnit.SECONDS);
        consumer.close();
        assertEquals(MSG_COUNT, queueView.getQueueSize());
    }

    @Test(timeout = 60000)
    public void tesIndividualAcksWithClosedConsumerAndAuditSync() throws Exception {
        produceSomeMessages(MSG_COUNT);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        assertEquals(MSG_COUNT, queueView.getQueueSize());

        connection = createConnection();
        Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();

        // Consume all messages with no ACK
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = consumer.receive(1000);
            assertNotNull(message);
            LOG.debug("Received message: {}", message);
        }
        consumer.close();
        assertEquals(MSG_COUNT, queueView.getQueueSize());

        // Consumer the same batch again.
        consumer = session.createConsumer(queue);
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = consumer.receive(1000);
            assertNotNull(message);
            LOG.debug("Received message: {}", message);
        }
        consumer.close();
        assertEquals(MSG_COUNT, queueView.getQueueSize());
    }

    private QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    private void produceSomeMessages(int count) throws Exception {
        Connection connection = createConnection();
        Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < count; ++i) {
            Message message = session.createMessage();
            producer.send(message);
        }

        connection.close();
    }

    private Connection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);
        Connection connection = factory.createConnection();
        return connection;
    }

    private String getDestinationName() {
        return name.getMethodName() + "-Queue";
    }
}
