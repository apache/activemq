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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ3445Test {

    private ConnectionFactory connectionFactory;
    private BrokerService broker;
    private String connectionUri;

    private final String queueName = "Consumer.MyApp.VirtualTopic.FOO";
    private final String topicName = "VirtualTopic.FOO";

    @Before
    public void startBroker() throws Exception {
        createBroker(true);
    }

    private void createBroker(boolean deleteMessages) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(deleteMessages);
        broker.setPersistenceAdapter(new JDBCPersistenceAdapter());
        broker.setAdvisorySupport(false);
        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();
        broker.waitUntilStarted();
        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
    }

    private void restartBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }

        createBroker(false);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testJDBCRetiansDestinationAfterRestart() throws Exception {

        broker.getAdminView().addQueue(queueName);
        broker.getAdminView().addTopic(topicName);

        assertTrue(findDestination(queueName, false));
        assertTrue(findDestination(topicName, true));

        QueueViewMBean queue = getProxyToQueueViewMBean();
        assertEquals(0, queue.getQueueSize());

        restartBroker();

        assertTrue(findDestination(queueName, false));
        queue = getProxyToQueueViewMBean();
        assertEquals(0, queue.getQueueSize());

        sendMessage();
        restartBroker();
        assertTrue(findDestination(queueName, false));

        queue = getProxyToQueueViewMBean();
        assertEquals(1, queue.getQueueSize());
        sendMessage();
        assertEquals(2, queue.getQueueSize());

        restartBroker();
        assertTrue(findDestination(queueName, false));
        queue = getProxyToQueueViewMBean();
        assertEquals(2, queue.getQueueSize());
    }

    private void sendMessage() throws Exception {
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session.createTopic(topicName));
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(session.createTextMessage("Testing"));
        producer.close();
        connection.close();
    }

    private QueueViewMBean getProxyToQueueViewMBean() throws Exception {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq"
                + ":destinationType=Queue,destinationName=" + queueName
                + ",type=Broker,brokerName=localhost");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    private boolean findDestination(String name, boolean topic) throws Exception {

        ObjectName[] destinations;

        if (topic) {
            destinations = broker.getAdminView().getTopics();
        } else {
            destinations = broker.getAdminView().getQueues();
        }

        for (ObjectName destination : destinations) {
            if (destination.toString().contains(name)) {
                return true;
            }
        }

        return false;
    }
}
