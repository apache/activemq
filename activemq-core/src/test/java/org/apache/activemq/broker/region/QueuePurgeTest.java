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
package org.apache.activemq.broker.region;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;

public class QueuePurgeTest extends TestCase {
    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    Queue queue;
    MessageConsumer consumer;

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setPersistent(false);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        factory = new ActiveMQConnectionFactory("vm://localhost");
        connection = factory.createConnection();
        connection.start();
    }

    protected void tearDown() throws Exception {
        consumer.close();
        session.close();
        connection.stop();
        connection.close();
        broker.stop();
    }

    public void testPurgeQueueWithActiveConsumer() throws Exception {
        createProducerAndSendMessages();
        QueueViewMBean proxy = getProxyToQueueViewMBean();
        createConsumer();
        proxy.purge();
        assertEquals("Queue size is not zero, it's " + proxy.getQueueSize(), 0,
                proxy.getQueueSize());
    }

    private QueueViewMBean getProxyToQueueViewMBean()
            throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq"
                + ":Type=Queue,Destination=" + queue.getQueueName()
                + ",BrokerName=localhost");
        QueueViewMBean proxy = (QueueViewMBean) MBeanServerInvocationHandler
                .newProxyInstance(broker.getManagementContext()
                        .getMBeanServer(), queueViewMBeanName,
                        QueueViewMBean.class, true);
        return proxy;
    }

    private void createProducerAndSendMessages() throws Exception {
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        queue = session.createQueue("test1");
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < 10000; i++) {
            TextMessage message = session.createTextMessage("message " + i);
            producer.send(message);
        }
        producer.close();
    }

    private void createConsumer() throws Exception {
        consumer = session.createConsumer(queue);
        // wait for buffer fill out
        Thread.sleep(5 * 1000);
        for (int i = 0; i < 100; ++i) {
            Message message = consumer.receive();
            message.acknowledge();
        }
    }
}
