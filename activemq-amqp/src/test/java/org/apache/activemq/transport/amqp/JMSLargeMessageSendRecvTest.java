/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class JMSLargeMessageSendRecvTest extends AmqpClientTestSupport {

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"amqp", false},
            {"amqp+ws", false},
            {"amqp+ssl", true},
            {"amqp+wss", true}
        });
    }

    public JMSLargeMessageSendRecvTest(String connectorScheme, boolean secure) {
        super(connectorScheme, secure);
    }

    @Rule
    public TestName testName = new TestName();

    protected static final Logger LOG = LoggerFactory.getLogger(JMSLargeMessageSendRecvTest.class);

    private String createLargeString(int sizeInBytes) {
        byte[] base = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < sizeInBytes; i++) {
            builder.append(base[i % base.length]);
        }

        LOG.debug("Created string with size : " + builder.toString().getBytes().length + " bytes");
        return builder.toString();
    }

    @Test(timeout = 60 * 1000)
    public void testSendSmallerMessages() throws JMSException {
        for (int i = 512; i <= (8 * 1024); i += 512) {
            doTestSendLargeMessage(i);
        }
    }

    @Test(timeout = 60 * 1000)
    public void testSendFixedSizedMessages() throws JMSException {
        doTestSendLargeMessage(65536);
        doTestSendLargeMessage(65536 * 2);
        doTestSendLargeMessage(65536 * 4);
    }

    @Test(timeout = 60 * 1000)
    public void testSendHugeMessage() throws JMSException {
        doTestSendLargeMessage(1024 * 1024 * 10);
    }

    public void doTestSendLargeMessage(int expectedSize) throws JMSException{
        LOG.info("doTestSendLargeMessage called with expectedSize " + expectedSize);
        String payload = createLargeString(expectedSize);
        assertEquals(expectedSize, payload.getBytes().length);

        Connection connection = JMSClientContext.INSTANCE.createConnection(getBrokerAmqpConnectionURI());
        long startTime = System.currentTimeMillis();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(testName.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage();
        message.setText(payload);
        producer.send(message);
        long endTime = System.currentTimeMillis();
        LOG.info("Returned from send after {} ms", endTime - startTime);

        startTime = System.currentTimeMillis();
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();
        LOG.info("Calling receive");
        Message receivedMessage = consumer.receive();
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        TextMessage receivedTextMessage = (TextMessage) receivedMessage;
        assertNotNull(receivedMessage);
        endTime = System.currentTimeMillis();
        LOG.info("Returned from receive after {} ms", endTime - startTime);
        String receivedText = receivedTextMessage.getText();
        assertEquals(expectedSize, receivedText.getBytes().length);
        assertEquals(payload, receivedText);
        connection.close();
    }
}
