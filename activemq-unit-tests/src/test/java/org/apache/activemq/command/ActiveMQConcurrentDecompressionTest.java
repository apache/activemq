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
package org.apache.activemq.command;

import static org.junit.Assert.assertNull;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.Assert;
import org.junit.Test;

/**
 * AMQ-6142
 *
 * This tests that all messages will be properly decompressed when there
 * are several consumers
 *
 */
public class ActiveMQConcurrentDecompressionTest {
    private volatile AssertionError assertionError;

    @Test
    public void bytesMessageCorruption() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("embedded");
        brokerService.setPersistent(false);
        brokerService.start();

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://embedded");
        connectionFactory.setUseCompression(true);

        Connection connection = connectionFactory.createConnection();
        connection.start();

        for (int i = 0; i < 10; i++) {
            Session mySession = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            mySession.createConsumer(mySession.createTopic("foo.bar"))
                    .setMessageListener(new MessageListener() {

                        @Override
                        public void onMessage(Message message) {
                            try {
                                Assert.assertEquals(1l, ((ActiveMQBytesMessage) message).getBodyLength());
                                Assert.assertEquals("a".getBytes()[0],
                                        ((ActiveMQBytesMessage) message).readByte());
                            } catch (JMSException | Error e) {
                                assertionError = new AssertionError(
                                        "Exception in thread", e);
                            }
                        }
                    });
        }

        Session producerSession = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        MessageProducer messageProducer = producerSession
                .createProducer(producerSession.createTopic("foo.bar"));

        for (int i = 0; i < 1000; i++) {
            BytesMessage bytesMessage = producerSession.createBytesMessage();
            bytesMessage.writeBytes("a".getBytes());
            messageProducer.send(bytesMessage);

            if (assertionError != null) {
                throw assertionError;
            }
        }

        assertNull(assertionError);
    }

}