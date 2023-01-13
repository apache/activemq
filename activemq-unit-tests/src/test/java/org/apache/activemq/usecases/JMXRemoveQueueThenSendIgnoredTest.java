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
package org.apache.activemq.usecases;

import static org.junit.Assert.assertEquals;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMXRemoveQueueThenSendIgnoredTest {

    private static final Logger LOG = LoggerFactory.getLogger(JMXRemoveQueueThenSendIgnoredTest.class);
    private static final String domain = "org.apache.activemq";

    private BrokerService brokerService;
    private MessageProducer producer;
    private QueueSession session;
    private QueueConnection connection;
    private Queue queue;
    private int count = 1;

    @Before
    public void setUp() throws Exception  {
        brokerService = new BrokerService();
        brokerService.setBrokerName("dev");
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        final String brokerUri = brokerService.getTransportConnectors().get(0).getPublishableConnectString();

        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokerUri);
        connection = activeMQConnectionFactory.createQueueConnection();
        session = connection.createQueueSession(true, Session.AUTO_ACKNOWLEDGE/*SESSION_TRANSACTED*/);
        queue = session.createQueue("myqueue");
        producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        connection.start();
    }

    @Test
    public void testRemoveQueueAndProduceAfterNewConsumerAdded() throws Exception {
        MessageConsumer firstConsumer = registerConsumer();
        produceMessage();
        Message message = firstConsumer.receive(5000);
        LOG.info("Received message " + message);

        assertEquals(1, numberOfMessages());
        firstConsumer.close();
        session.commit();
        Thread.sleep(1000);

        removeQueue();
        Thread.sleep(1000);

        MessageConsumer secondConsumer = registerConsumer();
        produceMessage();
        message = secondConsumer.receive(5000);
        LOG.debug("Received message " + message);

        assertEquals(1, numberOfMessages());
        secondConsumer.close();
    }

    @Test
    public void testRemoveQueueAndProduceBeforeNewConsumerAdded() throws Exception {
        MessageConsumer firstConsumer = registerConsumer();
        produceMessage();
        Message message = firstConsumer.receive(5000);
        LOG.info("Received message " + message);

        assertEquals(1, numberOfMessages());
        firstConsumer.close();
        session.commit();
        Thread.sleep(1000);

        removeQueue();
        Thread.sleep(1000);

        produceMessage();
        MessageConsumer secondConsumer = registerConsumer();
        message = secondConsumer.receive(5000);
        LOG.debug("Received message " + message);

        assertEquals(1, numberOfMessages());
        secondConsumer.close();
    }

    private MessageConsumer registerConsumer() throws JMSException {
        MessageConsumer consumer = session.createConsumer(queue);
        return consumer;
    }

    private int numberOfMessages() throws Exception {
        ObjectName queueViewMBeanName = new ObjectName(
            domain + ":destinationType=Queue,destinationName=myqueue,type=Broker,brokerName=dev");
        QueueViewMBean queue = (QueueViewMBean)
                brokerService.getManagementContext().newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        long size = queue.getQueueSize();
        return (int)size;
    }

    private void removeQueue() throws Exception {
        LOG.debug("Removing Destination: myqueue");
        brokerService.getAdminView().removeQueue("myqueue");
    }

    private void produceMessage() throws JMSException {
        TextMessage textMessage = session.createTextMessage();
        textMessage.setText("Sending message: " + count++);
        LOG.debug("Sending message: " + textMessage);
        producer.send(textMessage);
        session.commit();
    }

    @After
    public void tearDown() throws Exception {
        try {
            connection.close();
        } catch (Exception e) {
            //swallow any error so broker can still be stopped
        }
        brokerService.stop();
    }
}
