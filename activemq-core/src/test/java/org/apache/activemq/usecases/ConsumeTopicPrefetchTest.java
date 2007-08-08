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
package org.apache.activemq.usecases;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ConsumeTopicPrefetchTest extends ProducerConsumerTestSupport {

    protected int prefetchSize = 100;
    protected String[] messageTexts;
    protected long consumerTimeout = 10000L;

    public void testSendPrefetchSize() throws JMSException {
        testWithMessageCount(prefetchSize);
    }

    public void testSendDoublePrefetchSize() throws JMSException {
        testWithMessageCount(prefetchSize * 2);
    }

    public void testSendPrefetchSizePlusOne() throws JMSException {
        testWithMessageCount(prefetchSize + 1);
    }

    protected void testWithMessageCount(int messageCount) throws JMSException {
        makeMessages(messageCount);

        log.info("About to send and receive: " + messageCount + " on destination: " + destination
                + " of type: " + destination.getClass().getName());

        for (int i = 0; i < messageCount; i++) {
            Message message = session.createTextMessage(messageTexts[i]);
            producer.send(message);
        }

        // lets consume them in two fetch batches
        for (int i = 0; i < messageCount; i++) {
            consumeMessge(i);
        }
    }

    protected Connection createConnection() throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) super.createConnection();
        connection.getPrefetchPolicy().setQueuePrefetch(prefetchSize);
        connection.getPrefetchPolicy().setTopicPrefetch(prefetchSize);
        return connection;
    }

    protected void consumeMessge(int i) throws JMSException {
        Message message = consumer.receive(consumerTimeout);
        assertTrue("Should have received a message by now for message: " + i, message != null);
        assertTrue("Should be a TextMessage: " + message, message instanceof TextMessage);
        TextMessage textMessage = (TextMessage) message;
        assertEquals("Message content", messageTexts[i], textMessage.getText());
    }


    protected void makeMessages(int messageCount) {
        messageTexts = new String[messageCount];
        for (int i = 0; i < messageCount; i++) {
            messageTexts[i] = "Message for test: + " + getName() + " = " + i;
        }
    }

}
