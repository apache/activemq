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
package org.apache.activemq.test.retroactive;

import java.net.URI;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.MessageIdList;

/**
 * @version $Revision: 1.1 $
 */
public class RetroactiveConsumerTestWithSimpleMessageListTest extends EmbeddedBrokerTestSupport {
    protected int messageCount = 20;
    protected Connection connection;
    protected Session session;

    public void testSendThenConsume() throws Exception {

        // lets some messages
        connection = createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = createProducer();
        for (int i = 0; i < messageCount; i++) {
            TextMessage message = session.createTextMessage("Message: " + i + " sent at: " + new Date());
            sendMessage(producer, message);
        }
        producer.close();
        session.close();
        connection.close();

        connection = createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = createConsumer();
        MessageIdList listener = new MessageIdList();
        consumer.setMessageListener(listener);
        listener.waitForMessagesToArrive(messageCount);
        listener.assertMessagesReceived(messageCount);

    }

    protected void setUp() throws Exception {
        useTopic = true;
        bindAddress = "vm://localhost";
        super.setUp();
    }

    protected void tearDown() throws Exception {
        if (session != null) {
            session.close();
            session = null;
        }
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory answer = new ActiveMQConnectionFactory(bindAddress);
        answer.setUseRetroactiveConsumer(true);
        return answer;
    }

    protected BrokerService createBroker() throws Exception {
        String uri = getBrokerXml();
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

    protected void startBroker() throws Exception {
        // broker already started by XBean
    }

    protected String getBrokerXml() {
        return "org/apache/activemq/test/retroactive/activemq-fixed-buffer.xml";
    }

    protected MessageProducer createProducer() throws JMSException {
        return session.createProducer(destination);
    }

    protected void sendMessage(MessageProducer producer, TextMessage message) throws JMSException {
        producer.send(message);
    }

    protected MessageConsumer createConsumer() throws JMSException {
        return session.createConsumer(destination);
    }
}
