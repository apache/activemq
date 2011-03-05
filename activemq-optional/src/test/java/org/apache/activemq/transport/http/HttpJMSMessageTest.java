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

package org.apache.activemq.transport.http;

import java.net.URISyntaxException;

import javax.jms.ConnectionFactory;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JMSMessageTest;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;

public class HttpJMSMessageTest extends JMSMessageTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        return suite(HttpJMSMessageTest.class);
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws URISyntaxException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(getBrokerURL());
        return factory;
    }

    protected String getBrokerURL() {
        return "http://localhost:8161";
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(false);
        answer.setUseJmx(false);
        answer.setManagementContext(null);
        answer.addConnector(getBrokerURL());
        return answer;
    }

    public void testEmptyMapMessage() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // send the message.
        {
            MapMessage message = session.createMapMessage();
            producer.send(message);
        }

        // get the message.
        {
            MapMessage message = (MapMessage)consumer.receive(1000);
            assertNotNull(message);
            assertFalse(message.getMapNames().hasMoreElements());
        }
        assertNull(consumer.receiveNoWait());
    }
}
