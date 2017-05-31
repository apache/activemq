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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.Test;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsQueueBrowserTest extends JmsTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQXAConnectionFactoryTest.class);
    public boolean isUseCache = false;

    public static Test suite() throws Exception {
        return suite(JmsQueueBrowserTest.class);
    }

    /**
     * Tests the queue browser. Browses the messages then the consumer tries to receive them. The messages should still
     * be in the queue even when it was browsed.
     *
     * @throws Exception
     */
    public void testReceiveBrowseReceive() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destination);
        connection.start();

        Message[] outbound = new Message[]{session.createTextMessage("First Message"),
                                           session.createTextMessage("Second Message"),
                                           session.createTextMessage("Third Message")};

        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }

        producer.send(outbound[0]);
        producer.send(outbound[1]);
        producer.send(outbound[2]);

        // Get the first.
        assertEquals(outbound[0], consumer.receive(1000));
        consumer.close();

        QueueBrowser browser = session.createBrowser(destination);
        Enumeration<?> enumeration = browser.getEnumeration();

        // browse the second
        assertTrue("should have received the second message", enumeration.hasMoreElements());
        assertEquals(outbound[1], enumeration.nextElement());

        // browse the third.
        assertTrue("Should have received the third message", enumeration.hasMoreElements());
        assertEquals(outbound[2], enumeration.nextElement());

        // There should be no more.
        boolean tooMany = false;
        while (enumeration.hasMoreElements()) {
            LOG.info("Got extra message: " + ((TextMessage) enumeration.nextElement()).getText());
            tooMany = true;
        }
        assertFalse(tooMany);
        browser.close();

        // Re-open the consumer.
        consumer = session.createConsumer(destination);
        // Receive the second.
        assertEquals(outbound[1], consumer.receive(1000));
        // Receive the third.
        assertEquals(outbound[2], consumer.receive(1000));
        consumer.close();
    }

    public void initCombosForTestBatchSendBrowseReceive() {
        addCombinationValues("isUseCache", new Boolean[]{Boolean.TRUE, Boolean.FALSE});
    }

    public void testBatchSendBrowseReceive() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destination);
        connection.start();

        TextMessage[] outbound = new TextMessage[10];
        for (int i=0; i<10; i++) {
            outbound[i] = session.createTextMessage( i + " Message");
        };

        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }
        consumer.close();

        for (int i=0;i<outbound.length; i++) {
            producer.send(outbound[i]);
        }

        QueueBrowser browser = session.createBrowser(destination);
        Enumeration<?> enumeration = browser.getEnumeration();

        for (int i=0; i<outbound.length; i++) {
            assertTrue("should have a", enumeration.hasMoreElements());
            assertEquals(outbound[i], enumeration.nextElement());
        }
        browser.close();

        for (int i=0;i<outbound.length; i++) {
            producer.send(outbound[i]);
        }

        // verify second batch is visible to browse
        browser = session.createBrowser(destination);
        enumeration = browser.getEnumeration();
        for (int j=0; j<2;j++) {
            for (int i=0; i<outbound.length; i++) {
                assertTrue("should have a", enumeration.hasMoreElements());
                assertEquals("j=" + j + ", i=" + i, outbound[i].getText(), ((TextMessage) enumeration.nextElement()).getText());
            }
        }
        browser.close();

        consumer = session.createConsumer(destination);
        for (int i=0; i<outbound.length * 2; i++) {
            assertNotNull("Got message: " + i, consumer.receive(2000));
        }
        consumer.close();
    }

    public void initCombosForTestBatchSendJmxBrowseReceive() {
        addCombinationValues("isUseCache", new Boolean[]{Boolean.TRUE, Boolean.FALSE});
    }

    public void testBatchSendJmxBrowseReceive() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destination);
        connection.start();

        TextMessage[] outbound = new TextMessage[10];
        for (int i=0; i<10; i++) {
            outbound[i] = session.createTextMessage( i + " Message");
        };

        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }
        consumer.close();

        for (int i=0;i<outbound.length; i++) {
            producer.send(outbound[i]);
        }

        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");

        LOG.info("Create QueueView MBean...");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext().newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        long concount = proxy.getConsumerCount();
        LOG.info("Consumer Count :" + concount);
        long messcount = proxy.getQueueSize();
        LOG.info("current number of messages in the queue :" + messcount);

        // lets browse
        CompositeData[] compdatalist = proxy.browse();
        if (compdatalist.length == 0) {
            fail("There is no message in the queue:");
        }
        String[] messageIDs = new String[compdatalist.length];

        for (int i = 0; i < compdatalist.length; i++) {
            CompositeData cdata = compdatalist[i];

            if (i == 0) {
                LOG.info("Columns: " + cdata.getCompositeType().keySet());
            }
            messageIDs[i] = (String)cdata.get("JMSMessageID");
            LOG.info("message " + i + " : " + cdata.values());
        }

        TabularData table = proxy.browseAsTable();
        LOG.info("Found tabular data: " + table);
        assertTrue("Table should not be empty!", table.size() > 0);

        assertEquals("Queue size", outbound.length, proxy.getQueueSize());
        assertEquals("Queue size", outbound.length, compdatalist.length);
        assertEquals("Queue size", outbound.length, table.size());


        LOG.info("Send another 10");
        for (int i=0;i<outbound.length; i++) {
            producer.send(outbound[i]);
        }

        LOG.info("Browse again");

        messcount = proxy.getQueueSize();
        LOG.info("current number of messages in the queue :" + messcount);

        compdatalist = proxy.browse();
        if (compdatalist.length == 0) {
            fail("There is no message in the queue:");
        }
        messageIDs = new String[compdatalist.length];

        for (int i = 0; i < compdatalist.length; i++) {
            CompositeData cdata = compdatalist[i];

            if (i == 0) {
                LOG.info("Columns: " + cdata.getCompositeType().keySet());
            }
            messageIDs[i] = (String)cdata.get("JMSMessageID");
            LOG.info("message " + i + " : " + cdata.values());
        }

        table = proxy.browseAsTable();
        LOG.info("Found tabular data: " + table);
        assertTrue("Table should not be empty!", table.size() > 0);

        assertEquals("Queue size", outbound.length*2, proxy.getQueueSize());
        assertEquals("Queue size", outbound.length*2, compdatalist.length);
        assertEquals("Queue size", outbound.length * 2, table.size());

        consumer = session.createConsumer(destination);
        for (int i=0; i<outbound.length * 2; i++) {
            assertNotNull("Got message: " + i, consumer.receive(2000));
        }
        consumer.close();
    }

    public void testBrowseReceive() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");

        connection.start();

        // create consumer
        MessageConsumer consumer = session.createConsumer(destination);
        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }

        Message[] outbound = new Message[]{session.createTextMessage("First Message"),
                                           session.createTextMessage("Second Message"),
                                           session.createTextMessage("Third Message")};

        MessageProducer producer = session.createProducer(destination);
        producer.send(outbound[0]);

        // create browser first
        QueueBrowser browser = session.createBrowser(destination);
        Enumeration<?> enumeration = browser.getEnumeration();

        // browse the first message
        assertTrue("should have received the first message", enumeration.hasMoreElements());
        assertEquals(outbound[0], enumeration.nextElement());

        // Receive the first message.
        assertEquals(outbound[0], consumer.receive(1000));
        consumer.close();
        browser.close();
        producer.close();
    }

    public void testLargeNumberOfMessages() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        connection.start();

        MessageProducer producer = session.createProducer(destination);

        int numberOfMessages = 4096;

        for (int i = 0; i < numberOfMessages; i++) {
            producer.send(session.createTextMessage("Message: "  + i));
        }

        QueueBrowser browser = session.createBrowser(destination);
        Enumeration<?> enumeration = browser.getEnumeration();

        assertTrue(enumeration.hasMoreElements());

        int numberBrowsed = 0;

        while (enumeration.hasMoreElements()) {
            Message browsed = (Message) enumeration.nextElement();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Browsed Message [{}]", browsed.getJMSMessageID());
            }

            numberBrowsed++;
        }

        System.out.println("Number browsed:  " + numberBrowsed);
        assertEquals(numberOfMessages, numberBrowsed);
        browser.close();
        producer.close();
    }

    public void testQueueBrowserWith2Consumers() throws Exception {
        final int numMessages = 1000;
        connection.setAlwaysSyncSend(false);
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        ActiveMQQueue destinationPrefetch10 = new ActiveMQQueue("TEST?jms.prefetchSize=10");
        ActiveMQQueue destinationPrefetch1 = new ActiveMQQueue("TEST?jms.prefetchsize=1");
        connection.start();

        ActiveMQConnection connection2 = (ActiveMQConnection)factory.createConnection(userName, password);
        connection2.start();
        connections.add(connection2);
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destinationPrefetch10);

        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }

        for (int i=0; i<numMessages; i++) {
            TextMessage message = session.createTextMessage("Message: " + i);
            producer.send(message);
        }

        QueueBrowser browser = session2.createBrowser(destinationPrefetch1);
        @SuppressWarnings("unchecked")
        Enumeration<Message> browserView = browser.getEnumeration();

        List<Message> messages = new ArrayList<Message>();
        for (int i = 0; i < numMessages; i++) {
            Message m1 = consumer.receive(5000);
            assertNotNull("m1 is null for index: " + i, m1);
            messages.add(m1);
        }

        int i = 0;
        for (; i < numMessages && browserView.hasMoreElements(); i++) {
            Message m1 = messages.get(i);
            Message m2 = browserView.nextElement();
            assertNotNull("m2 is null for index: " + i, m2);
            assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
        }

        // currently browse max page size is ignored for a queue browser consumer
        // only guarantee is a page size - but a snapshot of pagedinpending is
        // used so it is most likely more
        assertTrue("got at least our expected minimum in the browser: ", i > BaseDestination.MAX_PAGE_SIZE);

        assertFalse("nothing left in the browser", browserView.hasMoreElements());
        assertNull("consumer finished", consumer.receiveNoWait());
    }

    public void testBrowseClose() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");

        connection.start();

        TextMessage[] outbound = new TextMessage[]{session.createTextMessage("First Message"),
                                           session.createTextMessage("Second Message"),
                                           session.createTextMessage("Third Message")};

        // create consumer
        MessageConsumer consumer = session.createConsumer(destination);
        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }

        MessageProducer producer = session.createProducer(destination);
        producer.send(outbound[0]);
        producer.send(outbound[1]);
        producer.send(outbound[2]);

        // create browser first
        QueueBrowser browser = session.createBrowser(destination);
        Enumeration<?> enumeration = browser.getEnumeration();

        // browse some messages
        assertEquals(outbound[0], enumeration.nextElement());
        assertEquals(outbound[1], enumeration.nextElement());
        //assertEquals(outbound[2], (Message) enumeration.nextElement());

        browser.close();

        // Receive the first message.
        TextMessage msg = (TextMessage)consumer.receive(1000);
        assertEquals("Expected " + outbound[0].getText() + " but received " + msg.getText(),  outbound[0], msg);
        msg = (TextMessage)consumer.receive(1000);
        assertEquals("Expected " + outbound[1].getText() + " but received " + msg.getText(), outbound[1], msg);
        msg = (TextMessage)consumer.receive(1000);
        assertEquals("Expected " + outbound[2].getText() + " but received " + msg.getText(), outbound[2], msg);

        consumer.close();
        producer.close();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService brokerService = super.createBroker();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setUseCache(isUseCache);
        policyEntry.setMaxBrowsePageSize(4096);
        policyMap.setDefaultEntry(policyEntry);
        brokerService.setDestinationPolicy(policyMap);
        return brokerService;
    }
}
