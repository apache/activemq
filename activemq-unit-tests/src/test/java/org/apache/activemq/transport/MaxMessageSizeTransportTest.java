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
package org.apache.activemq.transport;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This tests that {@link TransportConnector#setMaximumMessageSize(int)} is
 * enforced.
 *
 */
public class MaxMessageSizeTransportTest {
    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;
    Queue queue;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();

        MemoryPersistenceAdapter persistenceAdapter = new MemoryPersistenceAdapter();
        broker.setPersistenceAdapter(persistenceAdapter);
        TransportConnector connector = broker.addConnector("tcp://localhost:0");

        // Set maximum size at 30 kbytes
        connector.setMaximumMessageSize(30720);
        broker.start();
        factory = new ActiveMQConnectionFactory(broker.getTransportConnectors()
                .get(0).getConnectUri().toString());
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue("queue.test");
        producer = session.createProducer(queue);
    }

    @After
    public void tearDown() throws Exception {
        session.close();
        connection.stop();
        connection.close();
        broker.stop();
    }

    /**
     * Test successful message publish
     */
    @Test
    public void testMaxDestinationDefaultPolicySuccess() throws Exception {
        BytesMessage m = session.createBytesMessage();
        m.writeBytes(new byte[20000]);
        producer.send(m);
    }

    /**
     * Test that a message over the limit throws a JMSException
     */
    @Test(expected = javax.jms.JMSException.class)
    public void testMessageSendTooLarge() throws Exception {
        BytesMessage m = session.createBytesMessage();
        m.writeBytes(new byte[35000]);
        producer.send(m);
    }

}
