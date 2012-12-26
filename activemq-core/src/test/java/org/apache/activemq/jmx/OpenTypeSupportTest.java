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
package org.apache.activemq.jmx;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.CompositeDataConstants;
import org.apache.activemq.broker.jmx.OpenTypeSupport;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;

public class OpenTypeSupportTest {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTypeSupportTest.class);

    private static BrokerService brokerService;
    private static String TESTQUEUE = "testQueue";
    private static ActiveMQConnectionFactory connectionFactory;
    private static String BYTESMESSAGE_TEXT = "This is a short text";
    private static String BROKER_ADDRESS = "tcp://localhost:0";
    private static ActiveMQQueue queue = new ActiveMQQueue(TESTQUEUE);

    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        connectionUri = brokerService.addConnector(BROKER_ADDRESS).getPublishableConnectString();
        brokerService.start();
        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
        sendMessage();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    private static void sendMessage() throws JMSException {
        Connection conn = connectionFactory.createConnection();
        try {
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(TESTQUEUE);
            BytesMessage toSend = session.createBytesMessage();
            toSend.writeBytes(BYTESMESSAGE_TEXT.getBytes());
            MessageProducer producer = session.createProducer(queue);
            producer.send(queue, toSend);
        } finally {
            conn.close();
        }
    }

    @Test
    public void bytesMessagePreview() throws Exception {
        QueueViewMBean queue = getProxyToQueueViewMBean();
        assertEquals(extractText(queue.browse()[0]), extractText(queue.browse()[0]));
    }

    @Test
    public void testBrowseByteMessageFails() throws Exception {
        ActiveMQBytesMessage bm = new ActiveMQBytesMessage();
        bm.writeBytes("123456".getBytes());
        Object result = OpenTypeSupport.convert(bm);
        LOG.info("result : " + result);
    }

    private String extractText(CompositeData message) {
        Byte content[] = (Byte[]) message.get(CompositeDataConstants.BODY_PREVIEW);
        byte out[] = new byte[content.length];
        for (int i = 0; i < content.length; i++) {
            out[i] = content[i];
        }
        return new String(out);
    }

    private QueueViewMBean getProxyToQueueViewMBean() throws MalformedObjectNameException, JMSException {
        final ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + queue.getQueueName());
        QueueViewMBean proxy = (QueueViewMBean)
            brokerService.getManagementContext().newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}
