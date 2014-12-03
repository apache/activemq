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
package org.apache.activemq.transport.amqp.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.spring.SpringSslContext;
import org.apache.activemq.transport.amqp.AmqpTestSupport;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.junit.Test;

public class AMQ4753Test extends AmqpTestSupport {

    @Override
    protected boolean isUseTcpConnector() {
        return false;
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return true;
    }

    @Test(timeout = 120 * 1000)
    public void testAmqpNioPlusSslSendReceive() throws JMSException{
        Connection connection = createAMQPConnection(nioPlusSslPort, true);
        runSimpleSendReceiveTest(connection);
    }

    public void runSimpleSendReceiveTest(Connection connection) throws JMSException{
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueImpl queue = new QueueImpl("queue://txqueue");
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage();
        String messageText = "hello  sent at " + new java.util.Date().toString();
        message.setText(messageText);
        producer.send(message);

        // Get the message we just sent
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();
        Message receivedMessage = consumer.receive(5000);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        TextMessage textMessage = (TextMessage) receivedMessage;
        assertEquals(messageText, textMessage.getText());
        connection.close();
    }

    private Connection createAMQPConnection(int testPort, boolean useSSL) throws JMSException {
        LOG.debug("In createConnection using port {} ssl? {}", testPort, useSSL);
        final ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl("localhost", testPort, "admin", "password", null, useSSL);

        if (useSSL) {
            SpringSslContext sslContext = (SpringSslContext) brokerService.getSslContext();
            connectionFactory.setKeyStorePath(sslContext.getKeyStore());
            connectionFactory.setKeyStorePassword("password");
            connectionFactory.setTrustStorePath(sslContext.getTrustStore());
            connectionFactory.setTrustStorePassword("password");
        }

        final Connection connection = connectionFactory.createConnection();
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                exception.printStackTrace();
            }
        });
        connection.start();
        return connection;
    }
}
