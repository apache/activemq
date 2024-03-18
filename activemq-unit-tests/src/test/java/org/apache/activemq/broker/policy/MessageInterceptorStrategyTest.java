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
package org.apache.activemq.broker.policy;

import java.io.File;
import java.time.Duration;
import java.util.Enumeration;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.HeaderMessageInterceptorStrategy;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.test.TestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * This unit test is to test that MessageInterceptorStrategy features
 *
 */
public class MessageInterceptorStrategyTest extends TestSupport {

    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;
    QueueBrowser queueBrowser;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();

        File testDataDir = new File("target/activemq-data/message-interceptor-strategy");
        broker.setDataDirectoryFile(testDataDir);
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(1024l * 1024 * 64);
        broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
        broker.addConnector("tcp://localhost:0");
        broker.start();
        factory = new ActiveMQConnectionFactory(broker.getTransportConnectors()
                .get(0).getConnectUri().toString());
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        if(producer != null) {
            producer.close();
        }
        session.close();
        connection.stop();
        connection.close();
        broker.stop();
    }

    /**
     * Test sending messages can be forced to Persistent
     */
    @Test
    public void testForceDeliveryModePersistent() throws Exception {
        applyHeaderMessageInterceptor(true, true, false, 0l, Long.MAX_VALUE);

        Queue queue = createQueue("mis.forceDeliveryMode.true");
        Message sendMessageP = session.createTextMessage("forceDeliveryMode=true");
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(queue, sendMessageP);

        Message sendMessageNP = session.createTextMessage("forceDeliveryMode=true");
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(queue, sendMessageNP);

        queueBrowser = session.createBrowser(queue);
        Enumeration<?> browseEnumeration = queueBrowser.getEnumeration();

        int count = 0;
        while(browseEnumeration.hasMoreElements()) {
            Message message = (Message)browseEnumeration.nextElement();
            assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
            count++;
        }
        assertEquals(Integer.valueOf(2), Integer.valueOf(count));
    }

    /**
     * Test sending messages can be forced to NonPersistent
     */
    @Test
    public void testForceDeliveryModeNonPersistent() throws Exception {
        applyHeaderMessageInterceptor(true, false, false, 0l, Long.MAX_VALUE);

        Queue queue = createQueue("mis.forceDeliveryMode.false");
        Message sendMessageP = session.createTextMessage("forceDeliveryMode=false");
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(queue, sendMessageP);

        Message sendMessageNP = session.createTextMessage("forceDeliveryMode=false");
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(queue, sendMessageNP);

        queueBrowser = session.createBrowser(queue);
        Enumeration<?> browseEnumeration = queueBrowser.getEnumeration();

        int count = 0;
        while(browseEnumeration.hasMoreElements()) {
            Message message = (Message)browseEnumeration.nextElement();
            assertEquals(DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());
            count++;
        }
        assertEquals(Integer.valueOf(2), Integer.valueOf(count));
    }

    /**
     * Test not overriding expiration
     */
    @Test
    public void testForceExpirationDisabled() throws Exception {
        applyHeaderMessageInterceptor(false, false, false, 100_000l, Long.MAX_VALUE);

        Queue queue = createQueue("mis.forceExpiration.zero");
        Message sendMessageP = session.createTextMessage("expiration=zero");
        producer.setTimeToLive(0l);
        producer.send(queue, sendMessageP);

        queueBrowser = session.createBrowser(queue);
        Enumeration<?> browseEnumeration = queueBrowser.getEnumeration();

        int count = 0;
        while(browseEnumeration.hasMoreElements()) {
            Message message = (Message)browseEnumeration.nextElement();
            assertEquals(Long.valueOf(0l), Long.valueOf(message.getJMSExpiration()));
            count++;
        }
        assertEquals(Integer.valueOf(1), Integer.valueOf(count));
    }

    /**
     * Test overriding zero (0) expiration
     */
    @Test
    public void testForceExpirationZeroOverride() throws Exception {
        long expiryTime = 100_000l;
        applyHeaderMessageInterceptor(false, false, true, expiryTime, Long.MAX_VALUE);

        long currentTime = System.currentTimeMillis();
        Queue queue = createQueue("mis.forceExpiration.100k");
        Message sendMessageP = session.createTextMessage("expiration=zero");
        producer.setTimeToLive(100_000l);
        producer.send(queue, sendMessageP);

        queueBrowser = session.createBrowser(queue);
        Enumeration<?> browseEnumeration = queueBrowser.getEnumeration();

        int count = 0;
        while(browseEnumeration.hasMoreElements()) {
            Message message = (Message)browseEnumeration.nextElement();
            assertTrue(Long.valueOf(message.getJMSExpiration()) > currentTime +  (expiryTime / 2));
            count++;
        }
        assertEquals(Integer.valueOf(1), Integer.valueOf(count));
    }

    /**
     * Test overriding zero (0) expiration
     */
    @Test
    public void testForceExpirationZeroOverrideDLQ() throws Exception {
        long expiryTime = 1l;
        applyHeaderMessageInterceptor(false, false, true, expiryTime, Long.MAX_VALUE);

        Queue queue = createQueue("mis.forceExpiration.zero-no-dlq-expiry");
        Message sendMessageP = session.createTextMessage("expiration=zero-no-dlq-expiry");
        producer.send(queue, sendMessageP);

        Thread.sleep(250l);

        queueBrowser = session.createBrowser(queue);
        Enumeration<?> browseEnumeration = queueBrowser.getEnumeration();

        int count = 0;
        while(browseEnumeration.hasMoreElements()) {
            count++;
        }
        assertEquals(Integer.valueOf(0), Integer.valueOf(count));

        QueueBrowser dlqQueueBrowser = session.createBrowser(createQueue("mis.forceExpiration.zero-no-dlq-expiry.dlq"));
        Enumeration<?> dlqBrowseEnumeration = dlqQueueBrowser.getEnumeration();

        int dlqCount = 0;
        while(dlqBrowseEnumeration.hasMoreElements()) {
            Message dlqMessage = (Message)dlqBrowseEnumeration.nextElement();
            assertEquals(sendMessageP.getJMSMessageID(), dlqMessage.getJMSMessageID());
            assertEquals("Expiration should be zero" + dlqMessage.getJMSExpiration() + "\n", dlqMessage.getJMSExpiration(), 0);
            dlqCount++;
        }
        assertEquals(Integer.valueOf(1), Integer.valueOf(dlqCount));
    }

    /**
     * Test overriding expiration ceiling
     */
    @Test
    public void testForceExpirationCeilingOverride() throws Exception {
        long zeroOverrideExpiryTime = 100_000l;
        long expirationCeiling = Duration.ofDays(1).toMillis();
        applyHeaderMessageInterceptor(false, false, true, zeroOverrideExpiryTime, expirationCeiling);

        long currentTime = System.currentTimeMillis();
        long expiryTime = Duration.ofDays(10).toMillis();
        Queue queue = createQueue("mis.forceExpiration.maxValue");
        Message sendMessageP = session.createTextMessage("expiration=ceiling");
        producer.setTimeToLive(expiryTime);
        producer.send(queue, sendMessageP);

        queueBrowser = session.createBrowser(queue);
        Enumeration<?> browseEnumeration = queueBrowser.getEnumeration();

        int count = 0;
        while(browseEnumeration.hasMoreElements()) {
            Message message = (Message)browseEnumeration.nextElement();
            assertTrue(Long.valueOf(message.getJMSExpiration()) <  (currentTime + Duration.ofDays(9).toMillis()));
            count++;
        }
        assertEquals(Integer.valueOf(1), Integer.valueOf(count));
    }

    private PolicyMap applyHeaderMessageInterceptor(boolean forceDeliveryMode, boolean persistent, boolean forceExpiration, long zeroExpirationOverride, long expirationCeiling) {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();

        HeaderMessageInterceptorStrategy headerMessageInterceptorStrategy = new HeaderMessageInterceptorStrategy();

        // Persistence related fields
        headerMessageInterceptorStrategy.setForceDeliveryMode(forceDeliveryMode);
        headerMessageInterceptorStrategy.setPersistent(persistent);

        // Expiration related fields
        headerMessageInterceptorStrategy.setForceExpiration(forceExpiration);
        headerMessageInterceptorStrategy.setZeroExpirationOverride(zeroExpirationOverride);
        headerMessageInterceptorStrategy.setExpirationCeiling(expirationCeiling);
        defaultEntry.setMessageInterceptorStrategy(headerMessageInterceptorStrategy);

        IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
        individualDeadLetterStrategy.setQueuePrefix("");
        individualDeadLetterStrategy.setQueueSuffix(".dlq");
        defaultEntry.setDeadLetterStrategy(individualDeadLetterStrategy);

        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
        return policyMap;
    }

    private Queue createQueue(String queueName) throws Exception {
        Queue queue = session.createQueue(queueName);
        producer = session.createProducer(queue);
        return queue;
    }

}