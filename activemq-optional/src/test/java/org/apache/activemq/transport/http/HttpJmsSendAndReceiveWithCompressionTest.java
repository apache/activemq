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
package org.apache.activemq.transport.http;

import java.util.List;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.JmsTopicSendReceiveWithTwoConnectionsTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the Wire Level Http GZip compression.
 */
public class HttpJmsSendAndReceiveWithCompressionTest extends JmsTopicSendReceiveWithTwoConnectionsTest {
    private static final Logger logger = LoggerFactory.getLogger(HttpJmsSendAndReceiveWithCompressionTest.class);

    protected BrokerService broker;

    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
            broker.start();
        }
        super.setUp();
        WaitForJettyListener.waitForJettySocketToAccept(getBrokerURL());
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(getBrokerURL());
        return connectionFactory;
    }

    protected String getBrokerURL() {
        return "http://localhost:8161?useCompression=true";
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(false);
        answer.addConnector(getBrokerURL());
        return answer;
    }

    protected void consumeMessage(Message message, List<Message> messageList) {
        super.consumeMessage(message, messageList);
        if (message instanceof TextMessage) {
            TextMessage textMessage = TextMessage.class.cast(message);
            try {
                logger.debug("Received text message with text: {}", textMessage.getText());
            } catch( javax.jms.JMSException jmsE) {
                logger.debug("Received an exception while trying to retrieve the text message", jmsE);
                throw new RuntimeException(jmsE);
            }
        } else {
            logger.debug("Received a non text message: {}", message);
        }
    }
}
