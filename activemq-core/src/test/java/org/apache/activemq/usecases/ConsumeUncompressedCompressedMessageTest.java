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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeUncompressedCompressedMessageTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumeUncompressedCompressedMessageTest.class);

    private BrokerService broker;
    private URI tcpUri;

    ActiveMQConnectionFactory factory;
    ActiveMQConnection connection;
    Session session;
    Queue queue;

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();

        factory = new ActiveMQConnectionFactory(tcpUri);
        factory.setUseCompression(true);

        connection = (ActiveMQConnection) factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue("CompressionTestQueue");
    }

    @After
    public void tearDown() throws Exception {

        if(connection != null) {
            connection.close();
        }

        broker.stop();
        broker.waitUntilStopped();
    }

    protected BrokerService createBroker() throws Exception {
        return createBroker(true);
    }

    protected BrokerService createBroker(boolean delete) throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(false);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.setSchedulerSupport(false);
        answer.setUseJmx(true);
        TransportConnector connector = answer.addConnector("tcp://localhost:0");
        tcpUri = connector.getConnectUri();
        return answer;
    }

    @Test
    public void testBrowseAndReceiveCompressedMessages() throws Exception {

        assertTrue(connection.isUseCompression());

        createProducerAndSendMessages(1);

        QueueViewMBean queueView = getProxyToQueueViewMBean();

        assertNotNull(queueView);

        CompositeData[] compdatalist = queueView.browse();
        if (compdatalist.length == 0) {
            fail("There is no message in the queue:");
        }

        CompositeData cdata = compdatalist[0];

        assertComplexData(0, cdata, "Text", "Test Text Message: " + 0);

        assertMessageAreCorrect(1);
    }

    @Test
    public void testReceiveAndResendWithCompressionOff() throws Exception {

        assertTrue(connection.isUseCompression());

        createProducerAndSendMessages(1);

        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage message = (TextMessage) consumer.receive(5000);

        assertTrue(((ActiveMQMessage) message).isCompressed());

        LOG.debug("Received Message with Text = " + message.getText());

        connection.setUseCompression(false);

        MessageProducer producer = session.createProducer(queue);
        producer.send(message);
        producer.close();

        message = (TextMessage) consumer.receive(5000);

        LOG.debug("Received Message with Text = " + message.getText());
    }

    protected void assertComplexData(int messageIndex, CompositeData cdata, String name, Object expected) {
        Object value = cdata.get(name);
        assertEquals("Message " + messageIndex + " CData field: " + name, expected, value);
    }

    private void createProducerAndSendMessages(int numToSend) throws Exception {
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < numToSend; i++) {
            TextMessage message = session.createTextMessage("Test Text Message: " + i);
            if (i  != 0 && i % 10000 == 0) {
                LOG.info("sent: " + i);
            }
            producer.send(message);
        }
        producer.close();
    }

    private QueueViewMBean getProxyToQueueViewMBean()
            throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq"
                + ":destinationType=Queue,destinationName=" + queue.getQueueName()
                + ",type=Broker,brokerName=localhost");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class,
                        true);
        return proxy;
    }

    private void assertMessageAreCorrect(int numToReceive) throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        try{

            for (int i = 0; i < numToReceive; ++i) {
                TextMessage message = (TextMessage) consumer.receive(5000);
                assertNotNull(message);
                assertEquals("Test Text Message: " + i, message.getText());
            }

        } finally {
            consumer.close();
        }
    }
}
