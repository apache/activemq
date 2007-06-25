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
package org.apache.activemq.broker.virtual;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.spring.ConsumerBean;
import org.apache.activemq.xbean.XBeanBrokerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.JMSException;

import java.net.URI;

/**
 * 
 * @version $Revision$
 */
public class CompositeQueueTest extends EmbeddedBrokerTestSupport {

    private static final Log log = LogFactory.getLog(CompositeQueueTest.class);
    
    private Connection connection;

    protected int total = 10;


    public void testVirtualTopicCreation() throws Exception {
        if (connection == null) {
            connection = createConnection();
        }
        connection.start();

        ConsumerBean messageList1 = new ConsumerBean();
        ConsumerBean messageList2 = new ConsumerBean();
        messageList1.setVerbose(true);
        messageList2.setVerbose(true);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        Destination producerDestination = getProducerDestination();
        Destination destination1 = getConsumer1Dsetination();
        Destination destination2 = getConsumer2Dsetination();
        
        log.info("Sending to: " + producerDestination);
        log.info("Consuming from: " + destination1 + " and " + destination2);
        
        MessageConsumer c1 = session.createConsumer(destination1);
        MessageConsumer c2 = session.createConsumer(destination2);

        c1.setMessageListener(messageList1);
        c2.setMessageListener(messageList2);

        // create topic producer
        MessageProducer producer = session.createProducer(producerDestination);
        assertNotNull(producer);

        for (int i = 0; i < total; i++) {
            producer.send(createMessage(session, i));
        }

        assertMessagesArrived(messageList1, messageList2);
    }

    protected void assertMessagesArrived(ConsumerBean messageList1, ConsumerBean messageList2) {
        messageList1.assertMessagesArrived(total);
        messageList2.assertMessagesArrived(total);
    }

    protected TextMessage createMessage(Session session, int i) throws JMSException {
        TextMessage textMessage = session.createTextMessage("message: " + i);
        if (i % 2 != 0) {
            textMessage.setStringProperty("odd", "yes");
        }
        textMessage.setIntProperty("i", i);
        return textMessage;
    }

    protected Destination getConsumer1Dsetination() {
        return new ActiveMQQueue("FOO");
    }

    protected Destination getConsumer2Dsetination() {
        return new ActiveMQTopic("BAR");
    }

    protected Destination getProducerDestination() {
        return new ActiveMQQueue("MY.QUEUE");
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        XBeanBrokerFactory factory = new XBeanBrokerFactory();
        BrokerService answer = factory.createBroker(new URI(getBrokerConfigUri()));
        return answer;
    }

    protected String getBrokerConfigUri() {
        return "org/apache/activemq/broker/virtual/composite-queue.xml";
    }
}
