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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Enumeration;

import javax.jms.*;

import org.apache.activemq.transport.amqp.joram.ActiveMQAdmin;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.junit.Test;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JMSClientTest extends AmqpTestSupport {

    @Test
    public void testTransactions() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        QueueImpl queue = new QueueImpl("queue://txqueue");

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = session.createProducer(queue);

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            p.send(message);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            while (enumeration.hasMoreElements()) {
                Message m = (Message) enumeration.nextElement();
                assertTrue(m instanceof TextMessage);
            }

            MessageConsumer consumer = session.createConsumer(queue);
            Message msg = consumer.receive(TestConfig.TIMEOUT);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
        }
        connection.close();

    }

    @Test
    public void testSelectors() throws Exception{
        ActiveMQAdmin.enableJMSFrameTracing();
        QueueImpl queue = new QueueImpl("queue://txqueue");

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = session.createProducer(queue);

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            p.send(message, DeliveryMode.PERSISTENT, 5, 0);

            message = session.createTextMessage();
            message.setText("hello + 9");
            p.send(message, DeliveryMode.PERSISTENT, 9, 0);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            int count = 0;
            while (enumeration.hasMoreElements()) {
                Message m = (Message) enumeration.nextElement();
                assertTrue(m instanceof TextMessage);
                count ++;
            }

            assertEquals(2, count);

            MessageConsumer consumer = session.createConsumer(queue, "JMSPriority > 8");
            Message msg = consumer.receive(TestConfig.TIMEOUT);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("hello + 9", ((TextMessage) msg).getText());
        }
        connection.close();
    }

    private Connection createConnection() throws JMSException {
        final ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", port, "admin", "password");
        final Connection connection = factory.createConnection();
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
