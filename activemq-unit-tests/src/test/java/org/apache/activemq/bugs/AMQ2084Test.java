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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ2084Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ2084Test.class);
    BrokerService broker;
    CountDownLatch qreceived;
    String connectionUri;

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();
        broker.start();

        qreceived = new CountDownLatch(1);
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    public void listenQueue(final String queueName, final String selectors) {
        try {
            Properties props = new Properties();
            props.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
            props.put("java.naming.provider.url", connectionUri);
            props.put("queue.queueName", queueName);

            javax.naming.Context ctx = new InitialContext(props);
            QueueConnectionFactory factory = (QueueConnectionFactory) ctx.lookup("ConnectionFactory");
            QueueConnection conn = factory.createQueueConnection();
            final Queue queue = (Queue) ctx.lookup("queueName");
            QueueSession session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueReceiver receiver = session.createReceiver(queue, selectors);
            System.out.println("Message Selector: " + receiver.getMessageSelector());
            receiver.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    try {
                        if (message instanceof TextMessage) {
                            TextMessage txtMsg = (TextMessage) message;
                            String msg = txtMsg.getText();
                            LOG.info("Queue Message Received: " + queueName + " - " + msg);
                            qreceived.countDown();

                        }
                        message.acknowledge();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            });
            conn.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void listenTopic(final String topicName, final String selectors) {
        try {
            Properties props = new Properties();
            props.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
            props.put("java.naming.provider.url", connectionUri);
            props.put("topic.topicName", topicName);

            javax.naming.Context ctx = new InitialContext(props);
            TopicConnectionFactory factory = (TopicConnectionFactory) ctx.lookup("ConnectionFactory");
            TopicConnection conn = factory.createTopicConnection();
            final Topic topic = (Topic) ctx.lookup("topicName");
            TopicSession session = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber receiver = session.createSubscriber(topic, selectors, false);

            receiver.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    try {
                        if (message instanceof TextMessage) {
                            TextMessage txtMsg = (TextMessage) message;
                            String msg = txtMsg.getText();
                            LOG.info("Topic Message Received: " + topicName + " - " + msg);
                        }
                        message.acknowledge();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            conn.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void publish(String topicName, String message) {
        try {
            Properties props = new Properties();
            props.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
            props.put("java.naming.provider.url", connectionUri);
            props.put("topic.topicName", topicName);
            javax.naming.Context ctx = new InitialContext(props);
            TopicConnectionFactory factory = (TopicConnectionFactory) ctx.lookup("ConnectionFactory");
            TopicConnection conn = factory.createTopicConnection();
            Topic topic = (Topic) ctx.lookup("topicName");
            TopicSession session = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicPublisher publisher = session.createPublisher(topic);
            if (message != null) {
                Message msg = session.createTextMessage(message);
                publisher.send(msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void tryXpathSelectorMatch() throws Exception {
        String xPath = "XPATH '//books//book[@lang=''en'']'";
        listenQueue("Consumer.Sample.VirtualTopic.TestXpath", xPath);
        publish("VirtualTopic.TestXpath", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><books><book lang=\"en\">ABC</book></books>");
        assertTrue("topic received: ", qreceived.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void tryXpathSelectorNoMatch() throws Exception {
        String xPath = "XPATH '//books//book[@lang=''es'']'";
        listenQueue("Consumer.Sample.VirtualTopic.TestXpath", xPath);
        publish("VirtualTopic.TestXpath", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><books><book lang=\"en\">ABC</book></books>");
        assertFalse("topic did not receive unmatched", qreceived.await(5, TimeUnit.SECONDS));
    }

}
