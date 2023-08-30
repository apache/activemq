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
package org.apache.activemq.jms2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

import javax.management.JMX;
import javax.management.MBeanServer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class ActiveMQJMS2TestBase {

    public static final String DEFAULT_JMX_DOMAIN_NAME = "org.apache.activemq";
    public static final String DEFAULT_JMX_BROKER_NAME = "localhost";

    public static final String DEFAULT_JMS_USER = "admin";
    public static final String DEFAULT_JMS_PASS = "admin";

    protected static ActiveMQConnectionFactory activemqConnectionFactory = null;

    @Rule public TestName testName = new TestName();

    // Control session
    protected Connection connection = null;
    protected Session session = null;
    protected MessageProducer messageProducer = null;

    protected String methodNameDestinationName = null;
    protected MBeanServer mbeanServer = null;

    @BeforeClass
    public static void setUpClass() {
        activemqConnectionFactory = new ActiveMQConnectionFactory("vm://localhost?marshal=false&broker.persistent=false");
        List<String> newTrustedPackages = new LinkedList<>();
        newTrustedPackages.addAll(activemqConnectionFactory.getTrustedPackages());
        newTrustedPackages.add(ActiveMQJMS2TestBase.class.getPackageName());
        activemqConnectionFactory.setTrustedPackages(newTrustedPackages);
    }

    @AfterClass
    public static void tearDownClass() {
        activemqConnectionFactory = null;
    }

    @Before
    public void setUp() throws Exception {
        connection = activemqConnectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        methodNameDestinationName = "AMQ.JMS2." + cleanParameterizedMethodName(testName.getMethodName().toUpperCase());
        messageProducer = session.createProducer(session.createQueue(methodNameDestinationName));
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @After
    public void tearDown() {
        if(messageProducer != null) {
            try { messageProducer.close(); } catch (Exception e) { } finally { messageProducer = null; }
        }

        if(session != null) {
            try { session.close(); } catch (Exception e) { } finally { session = null; }
        }

        if(connection != null) {
            try { connection.close(); } catch (Exception e) { } finally { connection = null; }
        }

        methodNameDestinationName = null;
    }

    protected DestinationViewMBean getDestinationViewMBean(String destinationType, ActiveMQDestination destination) throws Exception {
        switch(destinationType) {
        case "queue": return getQueueViewMBean(destination);
        case "topic": return getTopicViewMBean(destination);
        case "temp-queue": return getTempQueueViewMBean(destination);
        case "temp-topic": return getTempTopicViewMBean(destination);
        default: throw new IllegalStateException("Unsupported destinationType: " + destinationType);
        }
    }

    protected QueueViewMBean getQueueViewMBean(ActiveMQDestination destination) throws Exception {
        return JMX.newMBeanProxy(mbeanServer, BrokerMBeanSupport.createDestinationName(BrokerMBeanSupport.createBrokerObjectName(DEFAULT_JMX_DOMAIN_NAME, DEFAULT_JMX_BROKER_NAME).toString(), destination), QueueViewMBean.class);
    }

    protected TopicViewMBean getTopicViewMBean(ActiveMQDestination destination) throws Exception {
        return JMX.newMBeanProxy(mbeanServer, BrokerMBeanSupport.createDestinationName(BrokerMBeanSupport.createBrokerObjectName(DEFAULT_JMX_DOMAIN_NAME, DEFAULT_JMX_BROKER_NAME).toString(), destination), TopicViewMBean.class);
    }

    protected TopicViewMBean getTempQueueViewMBean(ActiveMQDestination destination) throws Exception {
        return JMX.newMBeanProxy(mbeanServer, BrokerMBeanSupport.createDestinationName(BrokerMBeanSupport.createBrokerObjectName(DEFAULT_JMX_DOMAIN_NAME, DEFAULT_JMX_BROKER_NAME).toString(), destination), TopicViewMBean.class);
    }

    protected TopicViewMBean getTempTopicViewMBean(ActiveMQDestination destination) throws Exception {
        return JMX.newMBeanProxy(mbeanServer, BrokerMBeanSupport.createDestinationName(BrokerMBeanSupport.createBrokerObjectName(DEFAULT_JMX_DOMAIN_NAME, DEFAULT_JMX_BROKER_NAME).toString(), destination), TopicViewMBean.class);
    }

    protected void verifySession(Session session, int acknowledgeMode) throws JMSException {
        try {
            assertNotNull(session);
            assertEquals(acknowledgeMode, session.getAcknowledgeMode());
            assertEquals(acknowledgeMode == Session.SESSION_TRANSACTED, session.getTransacted());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    protected static String cleanParameterizedMethodName(String methodName) {
        // clean up parameterized method string: TESTMESSAGETIMESTAMPTIMETOLIVE[DESTINATIONTYPE=QUEUE, MESSAGETYPE=BYTES]
        // returns: TESTMESSAGETIMESTAMPTIMETOLIVE.QUEUE.BYTES

        if(methodName == null || (!methodName.contains("[") && !methodName.contains("]"))) {
            return methodName;
        }

        String[] step1 = methodName.split("\\[", 2);
        String[] step2 = step1[1].split("\\]", 2);
        String[] step3 = step2[0].split(",", 16);

        return step1[0] + "." + step3[0].split("=", 2)[1] + "." + step3[1].split("=", 2)[1];
    }

    protected static void sendMessage(JMSContext jmsContext, Destination testDestination, String textBody) {
        assertNotNull(jmsContext);
        JMSProducer jmsProducer = jmsContext.createProducer();
        jmsProducer.send(testDestination, textBody);
    }

    protected static void browseMessage(JMSContext jmsContext, Destination testDestination, String expectedTextBody, boolean expectFound) throws JMSException {
        assertNotNull(jmsContext);
        assertTrue(Queue.class.isAssignableFrom(testDestination.getClass()));
        Queue testQueue = Queue.class.cast(testDestination);
        try(QueueBrowser queueBrowser = jmsContext.createBrowser(testQueue)) {
            Enumeration<?> messageEnumeration = queueBrowser.getEnumeration();
            assertNotNull(messageEnumeration);

            boolean found = false; 
            while(!found && messageEnumeration.hasMoreElements()) {
                jakarta.jms.Message message = (jakarta.jms.Message)messageEnumeration.nextElement();
                assertNotNull(message);
                assertTrue(TextMessage.class.isAssignableFrom(message.getClass()));
                assertEquals(expectedTextBody, TextMessage.class.cast(message).getText());
                found = true;
            }
            assertEquals(expectFound, found);
        }
    }

    protected static void recvMessage(JMSContext jmsContext, Destination testDestination, String expectedTextBody) throws JMSException {
        assertNotNull(jmsContext);
        try(JMSConsumer jmsConsumer = jmsContext.createConsumer(testDestination)) {
            jakarta.jms.Message message = jmsConsumer.receive(1000l);
            assertNotNull(message);
            assertTrue(TextMessage.class.isAssignableFrom(message.getClass()));
            assertEquals(expectedTextBody, TextMessage.class.cast(message).getText());
        }
    }

    protected static void recvMessageDurable(JMSContext jmsContext, Topic testTopic, String subscriptionName, String selector, boolean noLocal, String expectedTextBody) throws JMSException {
        assertNotNull(jmsContext);
        try(JMSConsumer jmsConsumer = jmsContext.createDurableConsumer(testTopic, subscriptionName, selector, noLocal)) {
            jakarta.jms.Message message = jmsConsumer.receive(1000l);
            assertNotNull(message);
            assertTrue(TextMessage.class.isAssignableFrom(message.getClass()));
            assertEquals(expectedTextBody, TextMessage.class.cast(message).getText());
        }
    }
}
