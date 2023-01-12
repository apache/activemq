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

import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AmqpAdvisoryTest extends AmqpTestSupport {
    protected Connection connection1;
    protected Connection connection2;

    @Override
    public void setUp() throws Exception {
        advisorySupport = true;
        super.setUp();
    }

    @Test()
    public void testConnectionAdvisory() throws Exception {
        connection1 = createAmqpConnection();
        connection1.start();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination advisoryTopic = session1.createTopic("ActiveMQ.Advisory.Consumer.Queue.workshop.queueA");
        MessageConsumer advisoryTopicConsumer = session1.createConsumer(advisoryTopic);


        connection2 = createAmqpConnection();
        connection2.start();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session2.createQueue("workshop.queueA");
        session2.createConsumer(queue);

        Message connectMessage = advisoryTopicConsumer.receive(100);
        assertNotNull(connectMessage);
        assertEquals("ConsumerInfo", connectMessage.getStringProperty("ActiveMqDataStructureType"));

        connection2.close();

        Message removeMessage = advisoryTopicConsumer.receive(100);
        assertNotNull(removeMessage);
        assertEquals("RemoveInfo", removeMessage.getStringProperty("ActiveMqDataStructureType"));
        connection1.close();
    }

    public Connection createAmqpConnection() throws JMSException {
        final JmsConnectionFactory factory = new JmsConnectionFactory(amqpURI);
        final Connection connection = factory.createConnection();
        connection.setExceptionListener(Throwable::printStackTrace);
        connection.start();
        return connection;
    }
}
