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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ5136Test {

    BrokerService brokerService;
    @Before
    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.start();
    }

    @After
    public void stopBroker() throws Exception {
        brokerService.stop();
    }

    @Test
    public void memoryUsageOnCommit() throws Exception {
        sendMessagesAndAssertMemoryUsage(new TransactionHandler() {
            @Override
            public void finishTransaction(Session session) throws JMSException {
                session.commit();
            }
        });
    }

    @Test
    public void memoryUsageOnRollback() throws Exception {
        sendMessagesAndAssertMemoryUsage(new TransactionHandler() {
            @Override
            public void finishTransaction(Session session) throws JMSException {
                session.rollback();
            }
        });
    }

    private void sendMessagesAndAssertMemoryUsage(TransactionHandler transactionHandler) throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Topic destination = session.createTopic("ActiveMQBug");
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < 100; i++) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(generateBytes());
            producer.send(message);
            transactionHandler.finishTransaction(session);
        }
        connection.close();
        org.junit.Assert.assertEquals(0, BrokerRegistry.getInstance().findFirst().getSystemUsage().getMemoryUsage().getPercentUsage());
    }

    private byte[] generateBytes() {
        byte[] bytes = new byte[100000];
        for (int i = 0; i < 100000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }

    private static interface TransactionHandler {
        void finishTransaction(Session session) throws JMSException;
    }
}
