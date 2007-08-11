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
package org.apache.activemq.network.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class QueueBridgeTest extends TestCase implements MessageListener {

    protected static final int MESSAGE_COUNT = 10;
    private static final Log LOG = LogFactory.getLog(QueueBridgeTest.class);

    protected AbstractApplicationContext context;
    protected QueueConnection localConnection;
    protected QueueConnection remoteConnection;
    protected QueueRequestor requestor;
    protected QueueSession requestServerSession;
    protected MessageConsumer requestServerConsumer;
    protected MessageProducer requestServerProducer;

    protected void setUp() throws Exception {
        super.setUp();
        context = createApplicationContext();

        createConnections();

        requestServerSession = localConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue theQueue = requestServerSession.createQueue(getClass().getName());
        requestServerConsumer = requestServerSession.createConsumer(theQueue);
        requestServerConsumer.setMessageListener(this);
        requestServerProducer = requestServerSession.createProducer(null);

        QueueSession session = remoteConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        requestor = new QueueRequestor(session, theQueue);
    }

    protected void createConnections() throws JMSException {
        ActiveMQConnectionFactory fac = (ActiveMQConnectionFactory)context.getBean("localFactory");
        localConnection = fac.createQueueConnection();
        localConnection.start();

        fac = (ActiveMQConnectionFactory)context.getBean("remoteFactory");
        remoteConnection = fac.createQueueConnection();
        remoteConnection.start();
    }

    protected AbstractApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("org/apache/activemq/network/jms/queue-config.xml");
    }

    protected void tearDown() throws Exception {
        localConnection.close();
        super.tearDown();
    }

    public void testQueueRequestorOverBridge() throws JMSException {
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            TextMessage msg = requestServerSession.createTextMessage("test msg: " + i);
            TextMessage result = (TextMessage)requestor.request(msg);
            assertNotNull(result);
            LOG.info(result.getText());
        }
    }

    public void onMessage(Message msg) {
        try {
            TextMessage textMsg = (TextMessage)msg;
            String payload = "REPLY: " + textMsg.getText();
            Destination replyTo;
            replyTo = msg.getJMSReplyTo();
            textMsg.clearBody();
            textMsg.setText(payload);
            requestServerProducer.send(replyTo, textMsg);
        } catch (JMSException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
