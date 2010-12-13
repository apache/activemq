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
package org.apache.activemq.usecases;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.StorePendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertTrue;

public class JdbcDurableSubDupTest {

    private static final Log LOG = LogFactory.getLog(JdbcDurableSubDupTest.class);
    final int prefetchVal = 150;
    String urlOptions = "jms.watchTopicAdvisories=false";
    String url = null;
    String queueName = "topicTest?consumer.prefetchSize=" + prefetchVal;
    String xmlMessage = "<Example 01234567890123456789012345678901234567890123456789 MessageText>";

    String selector = "";
    String clntVersion = "87";
    String clntId = "timsClntId345" + clntVersion;
    String subscriptionName = "subscriptionName-y" + clntVersion;
    SimpleDateFormat dtf = new SimpleDateFormat("HH:mm:ss");

    final int TO_RECEIVE = 5000;
    BrokerService broker = null;
    Vector<Throwable> exceptions = new Vector();
    final int MAX_MESSAGES = 100000;
    int[] dupChecker = new int[MAX_MESSAGES];

    @Before
    public void startBroker() throws Exception {
        exceptions.clear();
        for (int i = 0; i < MAX_MESSAGES; i++) {
            dupChecker[i] = 0;
        }
        broker = new BrokerService();
        broker.setAdvisorySupport(false);
        broker.setPersistenceAdapter(new JDBCPersistenceAdapter());
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setMaxAuditDepth(2000);
        policyEntry.setMaxPageSize(150);
        policyEntry.setPrioritizedMessages(true);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);

        broker.addConnector("tcp://localhost:0");
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();
        broker.waitUntilStarted();
        url = broker.getTransportConnectors().get(0).getConnectUri().toString() + "?" + urlOptions;
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    public void testNoDupsOnSlowConsumerReconnect() throws Exception {
        JmsConsumerDup consumer = new JmsConsumerDup();
        consumer.done.set(true);
        consumer.run();

        consumer.done.set(false);

        LOG.info("serial production then consumption");
        JmsProvider provider = new JmsProvider();
        provider.run();

        consumer.run();

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());

        for (int i = 0; i < TO_RECEIVE; i++) {
            assertTrue("got message " + i, dupChecker[i] == 1);
        }
    }

    @Test
    public void testNoDupsOnSlowConsumerLargePriorityGapReconnect() throws Exception {
        JmsConsumerDup consumer = new JmsConsumerDup();
        consumer.done.set(true);
        consumer.run();

        consumer.done.set(false);
        JmsProvider provider = new JmsProvider();
        provider.priorityModulator = 2500;
        provider.run();

        consumer.run();

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
        for (int i = 0; i < TO_RECEIVE; i++) {
            assertTrue("got message " + i, dupChecker[i] == 1);
        }
        
    }

    class JmsConsumerDup implements MessageListener {
        long count = 0;

        AtomicBoolean done = new AtomicBoolean(false);

        public void run() {
            Connection connection = null;
            Session session;
            Topic topic;
            ActiveMQConnectionFactory factory;
            MessageConsumer consumer;

            factory = new ActiveMQConnectionFactory(url);

            try {
                connection = factory.createConnection("MyUsername", "MyPassword");
                connection.setClientID(clntId);
                connection.start();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                topic = session.createTopic(queueName);
                consumer = session.createDurableSubscriber(topic, subscriptionName, selector, false);
                consumer.setMessageListener(this);
                LOG.info("Waiting for messages...");

                while (!done.get()) {
                    TimeUnit.SECONDS.sleep(5);
                    if (count == TO_RECEIVE || !exceptions.isEmpty()) {
                        done.set(true);
                    }
                }
            } catch (Exception e) {
                LOG.error("caught", e);
                exceptions.add(e);
                throw new RuntimeException(e);
            } finally {
                if (connection != null) {
                    try {
                        LOG.info("consumer done (" + exceptions.isEmpty() + "), closing connection");
                        connection.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void onMessage(Message message) {
            ++count;

            try {
                Thread.sleep(0L);
            } catch (InterruptedException e) {
            }
            ;

            try {
                TextMessage m = (TextMessage) message;

                if (count%100 == 0) {
                    LOG.info("Rcvd Msg #-" + count + " " + m.getText()
                            + " Sent->" + dtf.format(new Date(m.getJMSTimestamp()))
                            + " Recv->" + dtf.format(new Date())
                            + " Expr->" + dtf.format(new Date(m.getJMSExpiration()))
                            + ", mid: " + m.getJMSMessageID()
                    );
                }
                int i = m.getIntProperty("SeqNo");

                //check for duplicate messages
                if (i < MAX_MESSAGES) {
                    if (dupChecker[i] == 1) {
                        LOG.error("Duplicate message received at count: " + count + ", id: " + m.getJMSMessageID());
                        exceptions.add(new RuntimeException("Got Duplicate at: " + m.getJMSMessageID()));

                    } else {
                        dupChecker[i] = 1;
                    }
                }
            } catch (JMSException e) {
                LOG.error("caught ", e);
                exceptions.add(e);
            }
        }
    }


    class JmsProvider implements Runnable {

        int priorityModulator = 10;

        public void run() {

            Connection connection;
            Session session;
            Topic topic;

            ActiveMQConnectionFactory factory;
            MessageProducer messageProducer;
            long timeToLive = 0l;

            TextMessage message = null;

            factory = new ActiveMQConnectionFactory(url);

            try {
                connection = factory.createConnection("MyUserName", "MyPassword");
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                topic = session.createTopic(queueName);
                messageProducer = session.createProducer(topic);
                messageProducer.setPriority(3);
                messageProducer.setTimeToLive(timeToLive);
                messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

                int msgSeqNo = 0;
                int NUM_MSGS = 1000;
                int NUM_GROUPS = TO_RECEIVE/NUM_MSGS;
                for (int n = 0; n < NUM_GROUPS; n++) {

                    message = session.createTextMessage();

                    for (int i = 0; i < NUM_MSGS; i++) {
                        int priority = 0;
                        if (priorityModulator <= 10) {
                            priority = msgSeqNo % priorityModulator;
                        } else {
                            priority = (msgSeqNo >= priorityModulator) ? 9 : 0;
                        }
                        message.setText(xmlMessage + msgSeqNo + "-" + priority);
                        message.setJMSPriority(priority);
                        message.setIntProperty("SeqNo", msgSeqNo);
                        if (i > 0 && i%100 == 0) {
                            LOG.info("Sending message: " + message.getText());
                        }
                        messageProducer.send(message, DeliveryMode.PERSISTENT, message.getJMSPriority(), timeToLive);
                        msgSeqNo++;
                    }
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        exceptions.add(e);
                    }
                }

            } catch (JMSException e) {
                LOG.error("caught ", e);
                e.printStackTrace();
                exceptions.add(e);

            }
        }

    }
}
