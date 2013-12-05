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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import static org.junit.Assert.*;

public class AMQ4887Test {
    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ4887Test.class);
    private static final Integer ITERATIONS = 10;
    @Rule
    public TestName name = new TestName();

    @Test
    public void testSetPropertyBeforeCopy() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.toString());
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        byte[] messageContent = "bytes message".getBytes();
        BytesMessage bytesMessage = session.createBytesMessage();
        bytesMessage.writeBytes(messageContent);

        for (int i=0; i < ITERATIONS; i++) {
            long sendTime = System.currentTimeMillis();
            bytesMessage.setLongProperty("sendTime", sendTime);
            producer.send(bytesMessage);

            LOG.debug("Receiving message " + i);
            Message receivedMessage =  consumer.receive(5000);
            assertNotNull("On message " + i, receivedMessage);
            assertTrue("On message " + i, receivedMessage instanceof BytesMessage);

            BytesMessage receivedBytesMessage = (BytesMessage) receivedMessage;
            byte[] receivedBytes = new byte[(int) receivedBytesMessage.getBodyLength()];
            receivedBytesMessage.readBytes(receivedBytes);
            LOG.debug("Message " + i + " content [" + new String(receivedBytes) + "]");
            assertEquals("On message " + i, messageContent.length, receivedBytes.length);
            assertArrayEquals("On message " + i, messageContent, receivedBytes);

            long receivedSendTime = receivedBytesMessage.getLongProperty("sendTime");
            assertEquals("On message " + i, receivedSendTime, sendTime);

            Thread.sleep(10);
        }
    }

}
