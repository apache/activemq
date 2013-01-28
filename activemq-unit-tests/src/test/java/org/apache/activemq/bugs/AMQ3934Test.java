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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AMQ3934Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ3934Test.class);
    private static BrokerService brokerService;
    private static String TEST_QUEUE = "testQueue";
    private static ActiveMQQueue queue = new ActiveMQQueue(TEST_QUEUE);
    private static String BROKER_ADDRESS = "tcp://localhost:0";

    private ActiveMQConnectionFactory connectionFactory;
    private String connectionUri;
    private String messageID;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        connectionUri = brokerService.addConnector(BROKER_ADDRESS).getPublishableConnectString();
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
        sendMessage();
    }

    public void sendMessage() throws Exception {
        final Connection conn = connectionFactory.createConnection();
        try {
            conn.start();
            final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination queue = session.createQueue(TEST_QUEUE);
            final Message toSend = session.createMessage();
            final MessageProducer producer = session.createProducer(queue);
            producer.send(queue, toSend);
        } finally {
            conn.close();
        }
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test
    public void getMessage() throws Exception {
        final QueueViewMBean queueView = getProxyToQueueViewMBean();
        final CompositeData messages[] = queueView.browse();
        messageID = (String) messages[0].get("JMSMessageID");
        assertNotNull(messageID);
        assertNotNull(queueView.getMessage(messageID));
        LOG.debug("Attempting to remove message ID: " + messageID);
        queueView.removeMessage(messageID);
        assertNull(queueView.getMessage(messageID));
    }

    private QueueViewMBean getProxyToQueueViewMBean() throws MalformedObjectNameException, NullPointerException,
            JMSException {
        final ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + queue.getQueueName());
        final QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext().newProxyInstance(
                queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}
