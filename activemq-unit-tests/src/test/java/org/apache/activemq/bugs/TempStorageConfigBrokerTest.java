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
package org.apache.activemq.bugs;

import static org.junit.Assert.*;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.plist.PListStoreImpl;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that when configuring small temp store limits the journal size must also
 * be smaller than the configured limit, but will still send a ResourceAllocationException
 * if its not when sendFailIfNoSpace is enabled.
 */
public class TempStorageConfigBrokerTest {

    public int deliveryMode = DeliveryMode.PERSISTENT;

    private static final Logger LOG = LoggerFactory.getLogger(TempStorageConfigBrokerTest.class);
    private static byte[] buf = new byte[4 * 1024];
    private BrokerService broker;
    private AtomicInteger messagesSent = new AtomicInteger(0);
    private AtomicInteger messagesConsumed = new AtomicInteger(0);

    private String brokerUri;
    private long messageReceiveTimeout = 10000L;
    private Destination destination = new ActiveMQTopic("FooTwo");

    @Test(timeout=360000)
    @Ignore("blocks in hudson, needs investigation")
    public void testFillTempAndConsumeWithBadTempStoreConfig() throws Exception {

        createBrokerWithInvalidTempStoreConfig();

        broker.getSystemUsage().setSendFailIfNoSpace(true);
        destination = new ActiveMQQueue("Foo");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUri);
        final ActiveMQConnection producerConnection = (ActiveMQConnection) factory.createConnection();
        // so we can easily catch the ResourceAllocationException on send
        producerConnection.setAlwaysSyncSend(true);
        producerConnection.start();

        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        try {
            while (true) {
                Message message = session.createTextMessage(new String(buf) + messagesSent.toString());
                producer.send(message);
                messagesSent.incrementAndGet();
                if (messagesSent.get() % 100 == 0) {
                    LOG.info("Sent Message " + messagesSent.get());
                    LOG.info("Temp Store Usage " + broker.getSystemUsage().getTempUsage().getUsage());
                }
            }
        } catch (ResourceAllocationException ex) {
            assertTrue("Should not be able to send 100 messages: ", messagesSent.get() < 100);
            LOG.info("Got resource exception : " + ex + ", after sent: " + messagesSent.get());
        }
    }

    @Test(timeout=360000)
    @Ignore("blocks in hudson, needs investigation")
    public void testFillTempAndConsumeWithGoodTempStoreConfig() throws Exception {

        createBrokerWithValidTempStoreConfig();

        broker.getSystemUsage().setSendFailIfNoSpace(true);
        destination = new ActiveMQQueue("Foo");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUri);
        final ActiveMQConnection producerConnection = (ActiveMQConnection) factory.createConnection();
        // so we can easily catch the ResourceAllocationException on send
        producerConnection.setAlwaysSyncSend(true);
        producerConnection.start();

        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        try {
            while (true) {
                Message message = session.createTextMessage(new String(buf) + messagesSent.toString());
                producer.send(message);
                messagesSent.incrementAndGet();
                if (messagesSent.get() % 100 == 0) {
                    LOG.info("Sent Message " + messagesSent.get());
                    LOG.info("Temp Store Usage " + broker.getSystemUsage().getTempUsage().getUsage());
                }
            }
        } catch (ResourceAllocationException ex) {
            assertTrue("Should be able to send at least 200 messages but was: " + messagesSent.get(),
                       messagesSent.get() > 200);
            LOG.info("Got resource exception : " + ex + ", after sent: " + messagesSent.get());
        }

        // consume all sent
        Connection consumerConnection = factory.createConnection();
        consumerConnection.start();

        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);

        while (consumer.receive(messageReceiveTimeout) != null) {
            messagesConsumed.incrementAndGet();
            if (messagesConsumed.get() % 1000 == 0) {
                LOG.info("received Message " + messagesConsumed.get());
                LOG.info("Temp Store Usage " + broker.getSystemUsage().getTempUsage().getUsage());
            }
        }

        assertEquals("Incorrect number of Messages Consumed: " + messagesConsumed.get(),
                     messagesConsumed.get(), messagesSent.get());
    }

    private void createBrokerWithValidTempStoreConfig() throws Exception {
        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistenceAdapter(new KahaDBPersistenceAdapter());

        broker.getSystemUsage().setSendFailIfNoSpace(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(1048576);
        broker.getSystemUsage().getTempUsage().setLimit(2*1048576);
        ((PListStoreImpl)broker.getSystemUsage().getTempUsage().getStore()).setJournalMaxFileLength(2 * 1048576);
        broker.getSystemUsage().getStoreUsage().setLimit(20*1048576);

        PolicyEntry defaultPolicy = new PolicyEntry();
        defaultPolicy.setProducerFlowControl(false);
        defaultPolicy.setMemoryLimit(10 * 1024);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultPolicy);

        broker.setDestinationPolicy(policyMap);
        broker.addConnector("tcp://localhost:0").setName("Default");
        broker.start();

        brokerUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    private void createBrokerWithInvalidTempStoreConfig() throws Exception {
        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistenceAdapter(new KahaDBPersistenceAdapter());

        broker.getSystemUsage().setSendFailIfNoSpace(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(1048576);
        broker.getSystemUsage().getTempUsage().setLimit(2*1048576);
        broker.getSystemUsage().getStoreUsage().setLimit(2*1048576);

        PolicyEntry defaultPolicy = new PolicyEntry();
        defaultPolicy.setProducerFlowControl(false);
        defaultPolicy.setMemoryLimit(10 * 1024);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultPolicy);

        broker.setDestinationPolicy(policyMap);
        broker.addConnector("tcp://localhost:0").setName("Default");
        broker.start();

        brokerUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

}
