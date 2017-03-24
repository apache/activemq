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
package org.apache.activemq.test;

import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsTopicSendReceiveWithEmbeddedBrokerAndUserIDTest extends JmsTopicSendReceiveWithTwoConnectionsAndEmbeddedBrokerTest {
    private static final Logger LOG = LoggerFactory.getLogger(JmsTopicSendReceiveWithEmbeddedBrokerAndUserIDTest.class);

    protected String userName = "James";

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory answer = super.createConnectionFactory();
        answer.setUserName(userName);
        return answer;
    }

    @Override
    protected TransportConnector configureBroker(BrokerService answer) throws Exception {
        answer.setPopulateJMSXUserID(true);
        return super.configureBroker(answer);
    }

    @Override
    protected void assertMessagesReceivedAreValid(List<Message> receivedMessages) throws JMSException {
        super.assertMessagesReceivedAreValid(receivedMessages);

        // lets assert that the user ID is set
        for (Message message : receivedMessages) {
            String userID = message.getStringProperty("JMSXUserID");
            LOG.info("Received message with userID: " + userID);
            assertEquals("JMSXUserID header", userName, userID);
        }
    }

    protected void assertMessagesAreReceived2() throws JMSException {
        waitForMessagesToBeDelivered();
        assertMessagesReceivedAreValid2(messages);
    }

    protected void assertMessagesReceivedAreValid2(List<Message> receivedMessages) throws JMSException {
        super.assertMessagesReceivedAreValid(receivedMessages);

        // lets assert that the user ID is set
        for (Message message : receivedMessages) {
            String userID = (String) message.getObjectProperty("JMSXUserID");
            LOG.info("Received message with userID: " + userID);
            assertEquals("JMSXUserID header", userName, userID);
        }
    }

    public void testSpoofedJMSXUserIdIsIgnored() throws Exception {
        for (int i = 0; i < data.length; i++) {
            Message message = createMessage(i);
            configureMessage(message);
            message.setStringProperty("JMSXUserID", "spoofedId");
            if (verbose) {
                LOG.info("About to send a message: " + message + " with text: " + data[i]);
            }
            sendMessage(i, message);
        }
        assertMessagesAreReceived();
        LOG.info("" + data.length + " messages(s) received, closing down connections");
    }

    public void testSpoofedJMSXUserIdIsIgnoredAsObjectProperty() throws Exception {
        for (int i = 0; i < data.length; i++) {
            Message message = createMessage(i);
            configureMessage(message);
            message.setStringProperty("JMSXUserID", "spoofedId");
            if (verbose) {
                LOG.info("About to send a message: " + message + " with text: " + data[i]);
            }
            sendMessage(i, message);
        }
        assertMessagesAreReceived2();
        LOG.info("" + data.length + " messages(s) received, closing down connections");
    }
}
