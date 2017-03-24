/**
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
package org.apache.activemq.bugs;

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
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// https://issues.apache.org/jira/browse/AMQ-6128
public class JMSQueueBrowserPriorityTest extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(JMSQueueBrowserPriorityTest.class);
    private static final String TEST_AMQ_BROKER_URI = "tcp://localhost:0";
    private BrokerService broker;
    public static final byte[] PAYLOAD = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};


    protected void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    protected void tearDown() throws Exception {

        if (broker != null) {
            broker.stop();
        }
    }


    /**
     * Send MEDIUM priority
     * Send HIGH priority
     * Send HIGH priority
     * <p/>
     * browse the list of messages
     * <p/>
     * consume the messages from the queue
     * <p/>
     * Compare browse and consumed messages - they should be the same
     *
     * @throws Exception
     */
    public void testBrowsePriorityMessages() throws Exception {


        for (int i = 0; i < 5; i++) {

            // MED
            produceMessages(3, 4, "TestQ");

            Thread.sleep(1000);

            // HI
            produceMessages(3, 9, "TestQ");

            // browse messages, will page in
            ArrayList<Integer> browseList = browseQueue("TestQ");

            // HI
            produceMessages(3, 9, "TestQ");

            // browse again to be sure new messages are picked up
            browseList = browseQueue("TestQ");

            // consume messages to verify order
            ArrayList<Integer> consumeList = consumeMessages("TestQ");

            if (!browseList.equals(consumeList)) {
                LOG.info("browseList size " + browseList.size());
                LOG.info("consumeList size " + consumeList.size());
                LOG.info("browseList is:" + browseList);
                LOG.info("consumeList is:" + consumeList);
            }

            // compare lists
            assertTrue("browseList and consumeList should be equal, iteration " + i, browseList.equals(consumeList));
        }
    }

    private void produceMessages(int numberOfMessages, int priority, String queueName) throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getDefaultSocketURIString());
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

    private ArrayList<Integer> browseQueue(String queueName) throws Exception {

        ArrayList<Integer> returnedMessages = new ArrayList<Integer>();

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getDefaultSocketURIString());
        Connection connection = connectionFactory.createConnection();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueBrowser browser = session.createBrowser(new ActiveMQQueue(queueName));
            connection.start();

            Enumeration<Message> browsedMessages = browser.getEnumeration();

            while (browsedMessages.hasMoreElements()) {
                returnedMessages.add(browsedMessages.nextElement().getJMSPriority());
            }

            return returnedMessages;

        } finally {

            if (connection != null) {
                connection.close();
            }
        }

    }


    private ArrayList<Integer> consumeMessages(String queueName) throws Exception {

        ArrayList<Integer> returnedMessages = new ArrayList<Integer>();

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getDefaultSocketURIString());
        connectionFactory.setMessagePrioritySupported(true);
        Connection connection = connectionFactory.createConnection();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(new ActiveMQQueue(queueName));
            connection.start();
            boolean finished = false;

            while (!finished) {

                Message message = consumer.receive(1000);
                if (message == null) {
                    finished = true;
                }

                if (message != null) {
                    returnedMessages.add(message.getJMSPriority());
                }

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

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();


        pe.setProducerFlowControl(true);
        pe.setUseCache(true);

        pe.setPrioritizedMessages(true);
        pe.setExpireMessagesPeriod(0);

        pe.setQueue(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);


        broker.addConnector(TEST_AMQ_BROKER_URI);
        broker.deleteAllMessages();
        return broker;
    }
}