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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.DestinationView;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueBrowsingTest {

    private static final Logger LOG = LoggerFactory.getLogger(QueueBrowsingTest.class);

    private BrokerService broker;
    private URI connectUri;
    private ActiveMQConnectionFactory factory;
    private final int maxPageSize = 100;

    @Before
    public void startBroker() throws Exception {
        broker = createBroker();
        TransportConnector connector = broker.addConnector("tcp://0.0.0.0:0");
        broker.deleteAllMessages();
        broker.start();
        broker.waitUntilStarted();

        PolicyEntry policy = new PolicyEntry();
        policy.setMaxPageSize(maxPageSize);
        broker.setDestinationPolicy(new PolicyMap());
        broker.getDestinationPolicy().setDefaultEntry(policy);

        connectUri = connector.getConnectUri();
        factory = new ActiveMQConnectionFactory(connectUri);
        factory.setWatchTopicAdvisories(false);
        factory.getRedeliveryPolicy().setInitialRedeliveryDelay(0l);
        factory.getRedeliveryPolicy().setRedeliveryDelay(0l);
        factory.getRedeliveryPolicy().setMaximumRedeliveryDelay(0l);
    }

    public BrokerService createBroker() throws IOException {
        return new BrokerService();
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test
    public void testBrowsing() throws JMSException {

        int messageToSend = 370;

        ActiveMQQueue queue = new ActiveMQQueue("TEST");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);

        String data = "";
        for( int i=0; i < 1024*2; i++ ) {
            data += "x";
        }

        for( int i=0; i < messageToSend; i++ ) {
            producer.send(session.createTextMessage(data));
        }

        QueueBrowser browser = session.createBrowser(queue);
        Enumeration<?> enumeration = browser.getEnumeration();
        int received = 0;
        while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            received++;
            LOG.info("Browsed message " + received + ": " + m.getJMSMessageID());
        }

        browser.close();

        assertEquals(messageToSend, received);
    }

    @Test
    public void testBrowseConcurrent() throws Exception {
        final int messageToSend = 370;

        final ActiveMQQueue queue = new ActiveMQQueue("TEST");
        Connection connection = factory.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);

        String data = "";
        for( int i=0; i < 1024*2; i++ ) {
            data += "x";
        }

        for( int i=0; i < messageToSend; i++ ) {
            producer.send(session.createTextMessage(data));
        }

        Thread browserThread = new Thread() {
            @Override
            public void run() {
                try {
                    QueueBrowser browser = session.createBrowser(queue);
                    Enumeration<?> enumeration = browser.getEnumeration();
                    int received = 0;
                    while (enumeration.hasMoreElements()) {
                        Message m = (Message) enumeration.nextElement();
                        received++;
                        LOG.info("Browsed message " + received + ": " + m.getJMSMessageID());
                    }
                    assertEquals("Browsed all messages", messageToSend, received);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        browserThread.start();

        Thread consumerThread = new Thread() {
            @Override
            public void run() {
                try {
                    MessageConsumer consumer = session.createConsumer(queue);
                    int received = 0;
                    while (true) {
                        Message m = consumer.receive(1000);
                        if (m == null)
                            break;
                        received++;
                    }
                    assertEquals("Consumed all messages", messageToSend, received);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        consumerThread.start();

        browserThread.join();
        consumerThread.join();
    }

    @Test
    public void testMemoryLimit() throws Exception {
        broker.getSystemUsage().getMemoryUsage().setLimit((maxPageSize + 10) * 4 * 1024);

        int messageToSend = 370;

        ActiveMQQueue queue = new ActiveMQQueue("TEST");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);

        String data = "";
        for( int i=0; i < 1024*2; i++ ) {
            data += "x";
        }

        for( int i=0; i < messageToSend; i++ ) {
            producer.send(session.createTextMessage(data));
        }

        //Consume one message to free memory and allow the cursor to pageIn messages
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.receive(1000);

        QueueBrowser browser = session.createBrowser(queue);
        Enumeration<?> enumeration = browser.getEnumeration();
        int received = 0;
        while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            received++;
            LOG.info("Browsed message " + received + ": " + m.getJMSMessageID());
        }

        browser.close();
        assertTrue("got at least maxPageSize", received >= maxPageSize);
    }

    @Test // https://issues.apache.org/jira/browse/AMQ-9554
    public void testBrowseRedeliveryMaxRedelivered() throws Exception {
        browseRedelivery(0, true);
    }

    @Test // Ignore https://issues.apache.org/jira/browse/AMQ-9554
    public void testBrowseRedeliveryIgnored() throws Exception {
        browseRedelivery(1, false);
    }

    protected void browseRedelivery(int browseExpected, boolean dlqDlqExpected) throws Exception {
        IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
        individualDeadLetterStrategy.setQueuePrefix("");
        individualDeadLetterStrategy.setQueueSuffix(".dlq");
        individualDeadLetterStrategy.setUseQueueForQueueMessages(true);
        broker.getDestinationPolicy().getDefaultEntry().setDeadLetterStrategy(individualDeadLetterStrategy);
        broker.getDestinationPolicy().getDefaultEntry().setPersistJMSRedelivered(true);
        
        if(dlqDlqExpected) {
            factory.getRedeliveryPolicy().setQueueBrowserIgnore(false);
        }
 
        String messageId = null;

        String queueName = "browse.redeliverd.tx";
        String dlqQueueName = "browse.redeliverd.tx.dlq";
        String dlqDlqQueueName = "browse.redeliverd.tx.dlq.dlq";

        ActiveMQQueue queue = new ActiveMQQueue(queueName + "?consumer.prefetchSize=0");
        ActiveMQQueue queueDLQ = new ActiveMQQueue(dlqQueueName + "?consumer.prefetchSize=0");
        ActiveMQQueue queueDLQDLQ = new ActiveMQQueue(dlqDlqQueueName);

        broker.getAdminView().addQueue(queueName);
        broker.getAdminView().addQueue(dlqQueueName);

        DestinationView dlqQueueView = broker.getAdminView().getBroker().getQueueView(dlqQueueName);
        DestinationView queueView = broker.getAdminView().getBroker().getQueueView(queueName);

        verifyQueueStats(0l, 0l, 0l, dlqQueueView);
        verifyQueueStats(0l, 0l, 0l, queueView);

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(queue);

        Message sendMessage = session.createTextMessage("Hello world!");
        producer.send(sendMessage);
        messageId = sendMessage.getJMSMessageID();
        session.commit();
        producer.close();

        verifyQueueStats(0l, 0l, 0l, dlqQueueView);
        verifyQueueStats(1l, 0l, 1l, queueView);

        // Redeliver message to DLQ
        Message message = null;
        MessageConsumer consumer = session.createConsumer(queue);
        int rollbackCount = 0;
        do {
            message = consumer.receive(2000l);
            if(message != null) {
                session.rollback();
                rollbackCount++;
            }
        } while (message != null);

        assertEquals(Integer.valueOf(7), Integer.valueOf(rollbackCount));
        verifyQueueStats(1l, 0l, 1l, dlqQueueView);
        verifyQueueStats(1l, 1l, 0l, queueView);

        session.commit();
        consumer.close();

        // Increment redelivery counter on the message in the DLQ
        // Close the consumer to force broker to dispatch
        Message messageDLQ = null;
        MessageConsumer consumerDLQ = session.createConsumer(queueDLQ);
        int dlqRollbackCount = 0;
        int dlqRollbackCountLimit = 5;
        do {
            messageDLQ = consumerDLQ.receive(2000l);
            if(messageDLQ != null) {
                session.rollback();
                session.close();
                consumerDLQ.close();
                session = connection.createSession(true, Session.SESSION_TRANSACTED);
                consumerDLQ = session.createConsumer(queueDLQ);
                dlqRollbackCount++;
            }
        } while (messageDLQ != null && dlqRollbackCount < dlqRollbackCountLimit);
        session.commit();
        consumerDLQ.close();

        // Browse in tx mode works when we are at the edge of maxRedeliveries 
        // aka browse does not increment redeliverCounter as expected
        Queue brokerQueueDLQ = resolveQueue(broker, queueDLQ);

        for(int i=0; i<16; i++) {
            QueueBrowser browser = session.createBrowser(queueDLQ);
            Enumeration<?> enumeration = browser.getEnumeration();
            ActiveMQMessage activemqMessage = null;
            int received = 0;
            while (enumeration.hasMoreElements()) {
                activemqMessage = (ActiveMQMessage)enumeration.nextElement();
                received++;
            }
            browser.close();
            assertEquals(Integer.valueOf(1), Integer.valueOf(received));
            assertEquals(Integer.valueOf(6), Integer.valueOf(activemqMessage.getRedeliveryCounter()));

            // Confirm broker-side redeliveryCounter
            QueueMessageReference queueMessageReference = brokerQueueDLQ.getMessage(messageId);
            assertEquals(Integer.valueOf(6), Integer.valueOf(queueMessageReference.getRedeliveryCounter()));
        }

        session.close();
        connection.close();

        // Change redelivery max and the browser will fail
        factory.getRedeliveryPolicy().setMaximumRedeliveries(3);
        final Connection browseConnection = factory.createConnection();
        browseConnection.start();

        final AtomicInteger browseCounter = new AtomicInteger(0);
        final AtomicInteger jmsExceptionCounter = new AtomicInteger(0);

        final Session browseSession = browseConnection.createSession(true, Session.SESSION_TRANSACTED);

        Thread browseThread = new Thread() {
            public void run() {
                
                QueueBrowser browser = null;
                try {
                    browser = browseSession.createBrowser(queueDLQ);
                    Enumeration<?> enumeration = browser.getEnumeration();
                    if(Thread.currentThread().isInterrupted()) {
                        Thread.currentThread().interrupt();
                    }
                    while (enumeration.hasMoreElements()) {
                        Message message = (Message)enumeration.nextElement();
                        if(message != null) {
                            browseCounter.incrementAndGet();
                        }
                    }
                } catch (JMSException e) {
                    jmsExceptionCounter.incrementAndGet();
                } finally {
                    if(browser != null) { try { browser.close(); } catch (JMSException e) { jmsExceptionCounter.incrementAndGet(); } }
                    if(browseSession != null) { try { browseSession.close(); } catch (JMSException e) { jmsExceptionCounter.incrementAndGet(); } }
                    if(browseConnection != null) { try { browseConnection.close(); } catch (JMSException e) { jmsExceptionCounter.incrementAndGet(); } }
                }
            }
        };
        browseThread.start();
        Thread.sleep(2000l);
        browseThread.interrupt();

        assertEquals(Integer.valueOf(browseExpected), Integer.valueOf(browseCounter.get()));
        assertEquals(Integer.valueOf(0), Integer.valueOf(jmsExceptionCounter.get()));

        // ActiveMQConsumer sends a poison ack, messages gets moved to .dlq.dlq AND remains on the .dlq
        DestinationView dlqDlqQueueView = broker.getAdminView().getBroker().getQueueView(dlqDlqQueueName);
        verifyQueueStats(1l, 1l, 0l, queueView);
        verifyQueueStats(1l, 0l, 1l, dlqQueueView);

        if(dlqDlqExpected) {
            verifyQueueStats(1l, 0l, 1l, dlqDlqQueueView);
        } else {
            assertNull(dlqDlqQueueView);
        }
    }
    protected static void verifyQueueStats(long enqueueCount, long dequeueCount, long queueSize, DestinationView queueView) {
        assertEquals(Long.valueOf(enqueueCount), Long.valueOf(queueView.getEnqueueCount()));
        assertEquals(Long.valueOf(dequeueCount), Long.valueOf(queueView.getDequeueCount()));
        assertEquals(Long.valueOf(queueSize), Long.valueOf(queueView.getQueueSize()));
    }

    protected static Queue resolveQueue(BrokerService brokerService, ActiveMQQueue activemqQueue) throws Exception {
        Set<Destination> destinations = brokerService.getBroker().getDestinations(activemqQueue);
        if(destinations == null || destinations.isEmpty()) {
            return null;
        }

        if(destinations.size() > 1) {
            fail("Expected one-and-only one queue for: " + activemqQueue);
        }

        return (Queue)destinations.iterator().next();
    }
}
