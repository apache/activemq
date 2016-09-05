/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueZeroPrefetchLazyDispatchPriorityTest {

    private static final Logger LOG = LoggerFactory.getLogger(QueueZeroPrefetchLazyDispatchPriorityTest.class);

    private final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    private final int ITERATIONS = 6;

    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test(timeout=120000)
    public void testPriorityMessages() throws Exception {

        for (int i = 0; i < ITERATIONS; i++) {

            // send 4 message priority MEDIUM
            produceMessages(4, 4, "TestQ");

            // send 1 message priority HIGH
            produceMessages(1, 5, "TestQ");

            LOG.info("On iteration {}", i);

            Thread.sleep(500);

            // consume messages
            ArrayList<Message> consumeList = consumeMessages("TestQ");
            LOG.info("Consumed list " + consumeList.size());

            // compare lists
            assertEquals("message 1 should be priority high", 5, consumeList.get(0).getJMSPriority());
            assertEquals("message 2 should be priority medium", 4, consumeList.get(1).getJMSPriority());
            assertEquals("message 3 should be priority medium", 4, consumeList.get(2).getJMSPriority());
            assertEquals("message 4 should be priority medium", 4, consumeList.get(3).getJMSPriority());
            assertEquals("message 5 should be priority medium", 4, consumeList.get(4).getJMSPriority());
        }
    }

    @Test(timeout=120000)
    public void testPriorityMessagesMoreThanPageSize() throws Exception {

        final int numToSend = 450;
        for (int i = 0; i < ITERATIONS; i++) {
            produceMessages(numToSend - 1, 4, "TestQ");

            // ensure we get expiry processing
            Thread.sleep(700);

            // send 1 message priority HIGH
            produceMessages(1, 5, "TestQ");

            Thread.sleep(500);

            LOG.info("On iteration {}", i);

            // consume messages
            ArrayList<Message> consumeList = consumeMessages("TestQ");
            LOG.info("Consumed list {}", consumeList.size());

            // compare lists
            assertFalse("Consumed list should not be empty", consumeList.isEmpty());
            assertEquals("message 1 should be priority high", 5, consumeList.get(0).getJMSPriority());
            for (int j = 1; j < (numToSend - 1); j++) {
                assertEquals("message " + j + " should be priority medium", 4, consumeList.get(j).getJMSPriority());
            }
        }
    }

    @Test(timeout=120000)
    public void testLongLivedPriorityConsumer() throws Exception {

        final int numToSend = 150;

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        Connection connection = connectionFactory.createConnection();

        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(new ActiveMQQueue("TestQ"));
            connection.start();

            for (int i = 0; i < ITERATIONS; i++) {
                produceMessages(numToSend - 1, 4, "TestQ");

                // send 1 message priority HIGH
                produceMessages(1, 5, "TestQ");

                Message message = consumer.receive(4000);

                assertEquals("message should be priority high", 5, message.getJMSPriority());
            }
        } finally {
            connection.close();
        }

        ArrayList<Message> consumeList = consumeMessages("TestQ");
        LOG.info("Consumed list {}", consumeList.size());

        for (Message message : consumeList) {
            assertEquals("should be priority medium", 4, message.getJMSPriority());
        }
    }

    @Test(timeout=120000)
    public void testPriorityMessagesWithJmsBrowser() throws Exception {

        final int numToSend = 250;

        for (int i = 0; i < ITERATIONS; i++) {
            produceMessages(numToSend - 1, 4, "TestQ");

            ArrayList<Message> browsed = browseMessages("TestQ");

            LOG.info("Browsed: {}", browsed.size());

            // send 1 message priority HIGH
            produceMessages(1, 5, "TestQ");

            Thread.sleep(500);

            LOG.info("On iteration {}", i);

            Message message = consumeOneMessage("TestQ");
            assertNotNull(message);
            assertEquals(5, message.getJMSPriority());

            // consume messages
            ArrayList<Message> consumeList = consumeMessages("TestQ");
            LOG.info("Consumed list {}", consumeList.size());

            // compare lists
            // assertEquals("Iteration: " + i
            // +", message 1 should be priority high", 5,
            // consumeList.get(0).getJMSPriority());
            for (int j = 1; j < (numToSend - 1); j++) {
                assertEquals("Iteration: " + i + ", message " + j + " should be priority medium", 4, consumeList.get(j).getJMSPriority());
            }
        }
    }

    @Test(timeout=120000)
    public void testJmsBrowserGetsPagedIn() throws Exception {
        final int numToSend = 10;

        for (int i = 0; i < ITERATIONS; i++) {
            produceMessages(numToSend, 4, "TestQ");

            ArrayList<Message> browsed = browseMessages("TestQ");

            LOG.info("Browsed: {}", browsed.size());

            assertEquals(0, browsed.size());

            Message message = consumeOneMessage("TestQ", Session.CLIENT_ACKNOWLEDGE);
            assertNotNull(message);

            browsed = browseMessages("TestQ");

            LOG.info("Browsed: {}", browsed.size());

            assertEquals("see only the paged in for pull", 1, browsed.size());

            // consume messages
            ArrayList<Message> consumeList = consumeMessages("TestQ");
            LOG.info("Consumed list " + consumeList.size());
            assertEquals(numToSend, consumeList.size());
        }
    }

    private void produceMessages(int numberOfMessages, int priority, String queueName) throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        connectionFactory.setConnectionIDPrefix("pri-" + priority);
        Connection connection = connectionFactory.createConnection();

        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(new ActiveMQQueue(queueName));
            connection.start();

            for (int i = 0; i < numberOfMessages; i++) {
                BytesMessage m = session.createBytesMessage();
                m.writeBytes(PAYLOAD);
                m.setJMSPriority(priority);
                producer.send(m, Message.DEFAULT_DELIVERY_MODE, m.getJMSPriority(), Message.DEFAULT_TIME_TO_LIVE);
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private ArrayList<Message> consumeMessages(String queueName) throws Exception {
        ArrayList<Message> returnedMessages = new ArrayList<Message>();

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        Connection connection = connectionFactory.createConnection();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(new ActiveMQQueue(queueName));
            connection.start();
            boolean finished = false;

            while (!finished) {
                Message message = consumer.receive(returnedMessages.isEmpty() ? 5000 : 1000);
                if (message == null) {
                    finished = true;
                }

                if (message != null) {
                    returnedMessages.add(message);
                }
            }

            consumer.close();
            return returnedMessages;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private Message consumeOneMessage(String queueName) throws Exception {
        return consumeOneMessage(queueName, Session.AUTO_ACKNOWLEDGE);
    }

    private Message consumeOneMessage(String queueName, int ackMode) throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        Connection connection = connectionFactory.createConnection();
        try {
            Session session = connection.createSession(false, ackMode);
            MessageConsumer consumer = session.createConsumer(new ActiveMQQueue(queueName));
            connection.start();

            return consumer.receive(5000);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private ArrayList<Message> browseMessages(String queueName) throws Exception {
        ArrayList<Message> returnedMessages = new ArrayList<Message>();

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        Connection connection = connectionFactory.createConnection();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueBrowser consumer = session.createBrowser(new ActiveMQQueue(queueName));
            connection.start();

            Enumeration<?> enumeration = consumer.getEnumeration();
            while (enumeration.hasMoreElements()) {
                Message message = (Message) enumeration.nextElement();
                returnedMessages.add(message);
            }

            return returnedMessages;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);

        // add the policy entries
        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();

        pe.setPrioritizedMessages(true);

        pe.setExpireMessagesPeriod(500);

        pe.setMaxPageSize(100);
        pe.setMaxExpirePageSize(0);
        pe.setMaxBrowsePageSize(0);

        pe.setQueuePrefetch(0);
        pe.setLazyDispatch(true);

        pe.setOptimizedDispatch(true);

        pe.setUseCache(false);

        pe.setQueue(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);

        broker.addConnector("tcp://0.0.0.0:0");
        return broker;
    }
}