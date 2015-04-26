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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSMessageGroupsTest extends JMSClientTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JMSMessageGroupsTest.class);

    private static final int ITERATIONS = 10;
    private static final int MESSAGE_COUNT = 10;
    private static final int MESSAGE_SIZE = 200 * 1024;
    private static final int RECEIVE_TIMEOUT = 3000;
    private static final String JMSX_GROUP_ID = "JmsGroupsTest";

    @Test(timeout = 60000)
    public void testGroupSeqIsNeverLost() throws Exception {
        AtomicInteger sequenceCounter = new AtomicInteger();

        for (int i = 0; i < ITERATIONS; ++i) {
            connection = createConnection();
            {
                sendMessagesToBroker(MESSAGE_COUNT, sequenceCounter);
                readMessagesOnBroker(MESSAGE_COUNT);
            }
            connection.close();
        }
    }

    protected void readMessagesOnBroker(int count) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageConsumer consumer = session.createConsumer(queue);

        for (int i = 0; i < MESSAGE_COUNT; ++i) {
            Message message = consumer.receive(RECEIVE_TIMEOUT);
            assertNotNull(message);
            LOG.debug("Read message #{}: type = {}", i, message.getClass().getSimpleName());
            String gid = message.getStringProperty("JMSXGroupID");
            String seq = message.getStringProperty("JMSXGroupSeq");
            LOG.debug("Message assigned JMSXGroupID := {}", gid);
            LOG.debug("Message assigned JMSXGroupSeq := {}", seq);
        }

        consumer.close();
    }

    protected void sendMessagesToBroker(int count, AtomicInteger sequence) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);

        byte[] buffer = new byte[MESSAGE_SIZE];
        for (count = 0; count < MESSAGE_SIZE; count++) {
            String s = String.valueOf(count % 10);
            Character c = s.charAt(0);
            int value = c.charValue();
            buffer[count] = (byte) value;
        }

        LOG.debug("Sending {} messages to destination: {}", MESSAGE_COUNT, queue);
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            BytesMessage message = session.createBytesMessage();
            message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            message.setStringProperty("JMSXGroupID", JMSX_GROUP_ID);
            message.setIntProperty("JMSXGroupSeq", sequence.incrementAndGet());
            message.writeBytes(buffer);
            producer.send(message);
        }

        producer.close();
    }
}
