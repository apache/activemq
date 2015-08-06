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
package org.apache.activemq.broker.virtual;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.plugin.SubQueueSelectorCacheBroker;
import org.apache.activemq.spring.ConsumerBean;
import org.apache.activemq.xbean.XBeanBrokerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test case for  https://issues.apache.org/jira/browse/AMQ-3004
 */

public class VirtualTopicDisconnectSelectorTest extends EmbeddedBrokerTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualTopicDisconnectSelectorTest.class);
    protected Connection connection;

    public void testVirtualTopicSelectorDisconnect() throws Exception {
        testVirtualTopicDisconnect("odd = 'no'", 3000, 1500);
    }

    public void testVirtualTopicNoSelectorDisconnect() throws Exception {
        testVirtualTopicDisconnect(null, 3000, 3000);
    }

    public void testVirtualTopicDisconnect(String messageSelector, int total , int expected) throws Exception {
        if (connection == null) {
            connection = createConnection();
        }
        connection.start();

        final ConsumerBean messageList = new ConsumerBean();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        Destination producerDestination = getProducerDestination();
        Destination destination = getConsumerDsetination();

        LOG.info("Sending to: " + producerDestination);
        LOG.info("Consuming from: " + destination );

        MessageConsumer consumer = createConsumer(session, destination, messageSelector);

        MessageListener listener = new MessageListener(){
            public void onMessage(Message message){
                messageList.onMessage(message);
                try {
                    message.acknowledge();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        };

        consumer.setMessageListener(listener);


        // create topic producer
        MessageProducer producer = session.createProducer(producerDestination);
        assertNotNull(producer);

        int disconnectCount = total/3;
        int reconnectCount = (total * 2)/3;

        for (int i = 0; i < total; i++) {
            producer.send(createMessage(session, i));

            if (i==disconnectCount){
               consumer.close();
            }
            if (i==reconnectCount){
                consumer = createConsumer(session, destination, messageSelector);
                consumer.setMessageListener(listener);
            }
        }

        assertMessagesArrived(messageList, expected ,10000);
    }
            
    protected ActiveMQQueue getConsumerDsetination() {
        return new ActiveMQQueue("Consumer.VirtualTopic.TEST");
    }


    protected Destination getProducerDestination() {
        return new ActiveMQTopic("VirtualTopic.TEST");
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected MessageConsumer createConsumer(Session session, Destination destination, String messageSelector) throws JMSException {
        if (messageSelector != null) {
            return session.createConsumer(destination, messageSelector);
        } else {
            return session.createConsumer(destination);
        }
    }

    protected TextMessage createMessage(Session session, int i) throws JMSException {
        TextMessage textMessage = session.createTextMessage("message: " + i);
        if (i % 2 != 0) {
            textMessage.setStringProperty("odd", "yes");
        } else {
            textMessage.setStringProperty("odd", "no");
        }
        textMessage.setIntProperty("i", i);
        return textMessage;
    }



    protected void assertMessagesArrived(ConsumerBean messageList, int expected, long timeout) {
        messageList.assertMessagesArrived(expected,timeout);

        messageList.flushMessages();

        
        LOG.info("validate no other messages on queues");
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                
            Destination destination1 = getConsumerDsetination();

            MessageConsumer c1 = session.createConsumer(destination1, null);
            c1.setMessageListener(messageList);

            
            LOG.info("send one simple message that should go to both consumers");
            MessageProducer producer = session.createProducer(getProducerDestination());
            assertNotNull(producer);
            
            producer.send(session.createTextMessage("Last Message"));
            
            messageList.assertMessagesArrived(1);

        } catch (JMSException e) {
            e.printStackTrace();
            fail("unexpeced ex while waiting for last messages: " + e);
        }
    }


    protected String getBrokerConfigUri() {
        return "org/apache/activemq/broker/virtual/disconnected-selector.xml";
    }

    protected BrokerService createBroker() throws Exception {
        XBeanBrokerFactory factory = new XBeanBrokerFactory();
        BrokerService answer = factory.createBroker(new URI(getBrokerConfigUri()));
        return answer;
    }


    protected void startBroker() throws Exception {
        super.startBroker();
        // start with a clean slate
        SubQueueSelectorCacheBroker selectorCacheBroker  = (SubQueueSelectorCacheBroker) broker.getBroker().getAdaptor(SubQueueSelectorCacheBroker.class);
        selectorCacheBroker.deleteAllSelectorsForDestination(getConsumerDsetination().getQualifiedName());
    }
}
