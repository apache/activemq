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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

public class HttpMaxFrameSizeTest {

    protected BrokerService brokerService;

    @Before
    public void setup() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.deleteAllMessages();
        brokerService.addConnector("http://localhost:8888?wireFormat.maxFrameSize=4000");
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void teardown() throws Exception {
        brokerService.stop();
    }

    @Test
    public void sendOversizedMessageTest() throws Exception {
        try {
            send(5000);
        } catch (JMSException jmsException) {
            Assert.assertTrue(jmsException.getMessage().contains("405 Method Not Allowed"));
        }
    }

    @Test
    public void sendGoodMessageTest() throws Exception {
        // no exception expected there
        send(10);
    }

    private void send(int size) throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("http://localhost:8888");
        try(Connection connection = connectionFactory.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(new ActiveMQQueue("test"));
            String payload = "*".repeat(size);
            TextMessage textMessage = session.createTextMessage(payload);
            producer.send(textMessage);
        }
    }

}
