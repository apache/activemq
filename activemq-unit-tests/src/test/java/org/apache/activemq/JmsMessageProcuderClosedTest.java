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
package org.apache.activemq;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test behavior of JMS MessageProducer API implementation when closed.
 */
public class JmsMessageProcuderClosedTest {

    private Connection connection;
    private MessageProducer producer;
    private Message message;
    private Destination destination;
    private BrokerService brokerService;

    protected BrokerService createBroker() throws Exception {
        BrokerService brokerService = new BrokerService();

        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);

        return brokerService;
    }

    protected MessageProducer createClosedProducer() throws Exception {
        MessageProducer producer = createProducer();
        producer.close();
        return producer;
    }

    protected MessageProducer createProducer() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        message = session.createMessage();
        destination = session.createTopic("test");
        return session.createProducer(destination);
    }

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();

        producer = createClosedProducer();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ex) {}

        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test(timeout=30000)
    public void testClose() throws Exception {
        producer.close();
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSetDisableMessageIDFails() throws Exception {
        producer.setDisableMessageID(true);
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testGetDisableMessageIDFails() throws Exception {
        producer.getDisableMessageID();
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSetDisableMessageTimestampFails() throws Exception {
        producer.setDisableMessageTimestamp(false);
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testGetDisableMessageTimestampFails() throws Exception {
        producer.getDisableMessageTimestamp();
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSetDeliveryModeFails() throws Exception {
        producer.setDeliveryMode(1);
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testGetDeliveryModeFails() throws Exception {
        producer.getDeliveryMode();
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSetPriorityFails() throws Exception {
        producer.setPriority(1);
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testGetPriorityFails() throws Exception {
        producer.getPriority();
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSetTimeToLiveFails() throws Exception {
        producer.setTimeToLive(1);
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testGetTimeToLiveFails() throws Exception {
        producer.getTimeToLive();
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testGetDestinationFails() throws Exception {
        producer.getDestination();
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSendFails() throws Exception {
        producer.send(message);
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSendWithDestinationFails() throws Exception {
        producer.send(destination, message);
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSendWithModePriorityTTLFails() throws Exception {
        producer.send(message, 1, 3, 111);
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSendWithDestinationModePriorityTTLFails() throws Exception {
        producer.send(destination, message, 1, 3, 111);
    }
}
