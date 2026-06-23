/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.Topic;

import java.io.File;
import java.net.URI;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class ActiveMQMessageFormatExceptionSelectorTest {

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    private URI clientUri;
    private BrokerService brokerService;
    private final AtomicInteger dlqCount = new AtomicInteger();

    @Before
    public void setUp() throws Exception {
        dlqCount.set(0);
        startBroker();
    }

    @After
    public void tearDown() throws Exception {
        stopBroker();
    }

    // Test that queue browsers just skip corrupt messages as they are only going to be
    // DLQ'd and removed for normal queue consumers
    @Test(timeout = 30000)
    public void testUnmarshalQueueBrowseSubscription() throws Exception {
        try (ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory(
                clientUri).createConnection()) {
            connection.setClientID("client");
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue dest = session.createQueue("test.queue");
            MessageProducer producer = session.createProducer(dest);
            QueueBrowser browser =  session.createBrowser(dest, "stringProperty = 'a'");

            // Mix good and bad messages to ensure the good ones are still received
            for (int i = 0; i < 100; i++) {
                ActiveMQTextMessage message = (ActiveMQTextMessage) session.createTextMessage("test-message");
                message.setIntProperty("count", i);
                message.setStringProperty("stringProperty", "a");

                if (i % 5 == 0) {
                    message.beforeMarshall(null);
                    ByteSequenceData.writeIntBig(message.getMarshalledProperties(), 1024 * 1024);
                }

                producer.send(message);
            }

            Enumeration enumeration = browser.getEnumeration();
            int i = 0;
            while (enumeration.hasMoreElements()) {
                // skip expected bad message
                if (i % 5 == 0) {
                    i++;
                }
                javax.jms.Message message = (Message) enumeration.nextElement();
                assertNotNull(message);
                assertEquals(i, message.getIntProperty("count"));
                i++;
            }

            Destination destination = brokerService.getDestination((ActiveMQDestination) dest);
            // browsers should NOT remove/move messages
            assertEquals(100, destination.getDestinationStatistics().getMessages().getCount());
            assertTrue(destination.getMemoryUsage().getUsage() > 0);
            assertEquals(0, dlqCount.get());
            assertEquals(100, destination.getMessageStore().getMessageCount());
        }
    }

    @Test(timeout = 30000)
    public void testUnmarshalQueueSubscription() throws Exception {
        testUnmarshalFail("test.queue", true, false);
        // For queues, the message is already accepted onto the queue so
        // if there is an error when trying to match for a consumer, and it is
        // corrupted it makes sense to just remove with the first error and
        // send to the DLQ
        assertEquals(20, dlqCount.get());
    }

    @Test(timeout = 30000)
    public void testUnmarshalTopicSubscription() throws Exception {
        testUnmarshalFail("test.topic", false, false);
        // for topic subscription on error it just won't match the selector
        // and skips adding to the sub, but other subs may be able to receive
        // as it evaluates each sub independently (not all subs may use a selector)
        // Messages won't be stuck and block other subscriptions so no need to
        // do anything special and there is no DLQ because it was never even added
        // to the subscription
    }

    @Test(timeout = 30000)
    public void testUnmarshalDurableSubscription() throws Exception {
        testUnmarshalFail("test.topic", false, true);
        // for durable subscription on error it just won't match the selector
        // and skips adding to the sub, but other subs may be able to receive
        // as it evaluates each sub independently (not all subs may use a selector)
        // Messages won't be stuck and block other subscriptions so no need to
        // do anything special and there is no DLQ because it was never even added
        // to the subscription
    }

    // Test mixing subs with and without selectors
    @Test(timeout = 30000)
    public void testMultipleTopicSubs() throws Exception {
        testMultipleSubs(false);
    }

    @Test(timeout = 30000)
    public void testMultipleDurableSubs() throws Exception {
        testMultipleSubs(true);
    }

    private void testMultipleSubs(boolean durable) throws Exception {
        try (ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory(
                clientUri).createConnection()) {
            connection.setClientID("client");
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ActiveMQTopic dest =  (ActiveMQTopic) session.createTopic("topic");
            MessageProducer producer = session.createProducer(dest);
            MessageConsumer selectorConsumer = durable ?
                    session.createDurableSubscriber((Topic) dest, "sub", "stringProperty = 'a'", false) :
                    session.createConsumer(dest, "stringProperty = 'a'");
            MessageConsumer consumer = durable ?
                    session.createDurableSubscriber((Topic) dest, "sub2") : session.createConsumer(dest);

            // Mix good and bad messages to ensure the good ones are still received
            for (int i = 0; i < 10; i++) {
                ActiveMQTextMessage message = (ActiveMQTextMessage) session.createTextMessage("test-message");
                message.setIntProperty("count", i);
                message.setStringProperty("stringProperty", "a");
                message.beforeMarshall(null);
                // corrupt
                ByteSequenceData.writeIntBig(message.getMarshalledProperties(), 1024 * 1024);
                producer.send(message);
            }

            // no selector should get all 10
            for (int i = 0; i < 10; i++) {
                javax.jms.Message message = consumer.receive(100);
                assertNotNull(message);
            }

            // selector so should error and not get any
            assertNull(selectorConsumer.receive(100));

            // messages should be gone
            Destination destination = brokerService.getDestination(dest);
            assertTrue(Wait.waitFor(() -> destination.getDestinationStatistics().getMessages().getCount() == 0,
                    500, 10));
            assertTrue(Wait.waitFor(
                    () -> destination.getMemoryUsage().getUsage() == 0, 1000, 100));
        }
    }

    private void testUnmarshalFail(String destName, boolean queue, boolean durable) throws Exception {
        try (ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory(
                clientUri).createConnection()) {
            connection.setClientID("client");
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ActiveMQDestination dest = queue ? (ActiveMQDestination) session.createQueue(destName) :
                    (ActiveMQDestination) session.createTopic(destName);
            MessageProducer producer = session.createProducer(dest);
            MessageConsumer consumer = durable ?
                    session.createDurableSubscriber((Topic) dest, "sub", "stringProperty = 'a'", false) :
                    session.createConsumer(dest, "stringProperty = 'a'");

            // Mix good and bad messages to ensure the good ones are still received
            for (int i = 0; i < 100; i++) {
                ActiveMQTextMessage message = (ActiveMQTextMessage) session.createTextMessage("test-message");
                message.setIntProperty("count", i);
                message.setStringProperty("stringProperty", "a");

                if (i % 5 == 0) {
                    message.beforeMarshall(null);
                    ByteSequenceData.writeIntBig(message.getMarshalledProperties(), 1024 * 1024);
                }
                producer.send(message);
            }

            for (int i = 0; i < 100; i++) {
                // skip expected bad message
                if (i % 5 == 0) {
                    i++;
                }
                javax.jms.Message message = consumer.receive(100);
                assertNotNull(message);
                assertEquals(i, message.getIntProperty("count"));
            }

            Destination destination = brokerService.getDestination(dest);
            assertTrue(Wait.waitFor(() -> destination.getDestinationStatistics().getMessages().getCount() == 0,
                    500, 10));
            assertTrue(Wait.waitFor(
                    () -> destination.getMemoryUsage().getUsage() == 0, 1000, 100));
            assertEquals(0, destination.getMessageStore().getMessageCount());
        }
    }

    // test that the messages that are corrupt are still handled by the queue
    // on restart and load when consumers come online. This is really only a big concern for queues.
    // For topic/durables subs the messages won't be added as they won't match in the first place
    // if corrupt so they should not be there on restart. If they are for some reason
    // (maybe the broker crashed during sub matching and didn't ack) then an exception is logged and
    // they get skipped and not loaded (this is the exact same behavior as previously). Topic sub
    // behavior on restart could look to be improved in a future change but is an edge case
    // and logging the error allows an admin to address if it does happen for topics
    @Test
    public void testRestart() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(clientUri);
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.setClientID("client");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQDestination dest = (ActiveMQDestination) session.createQueue("Test'") ;
        MessageProducer producer = session.createProducer(dest);

        // send 10 good and 10 bad
        for (int i = 0; i < 20; i++) {
            ActiveMQTextMessage message = (ActiveMQTextMessage) session.createTextMessage("test-message");
            message.setStringProperty("stringProperty", "a");
            if (i % 2 == 0) {
                message.beforeMarshall(null);
                ByteSequenceData.writeIntBig(message.getMarshalledProperties(), 1024 * 1024);
            }
            producer.send(message);
        }
        connection.close();
        // All 20 messages should be persisted as no consumers yet
        Destination destination = brokerService.getDestination(dest);
        assertEquals(20, destination.getMessageStore().getMessageCount());
        // stop and restart broker to verify it loads all the messages
        brokerService.stop();
        brokerService.waitUntilStopped();
        startBroker();

        // bring the consumer online which should trigger the errors and DLQ
        factory = new ActiveMQConnectionFactory(clientUri);
        connection = (ActiveMQConnection) factory.createConnection();
        connection.setClientID("client");
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(dest, "stringProperty = 'a'");

        // we should get 10
        for (int i = 0; i < 10; i++) {
            javax.jms.Message message = consumer.receive(100);
            assertNotNull(message);
        }
        // should only get 10
        assertNull(consumer.receive(100));
        Destination regionDest = brokerService.getDestination(dest);
        // makes sure messages are gone
        assertTrue(Wait.waitFor(() -> regionDest.getDestinationStatistics().getMessages().getCount() == 0,
                500, 10));
        assertTrue(Wait.waitFor(
                () -> regionDest.getMemoryUsage().getUsage() == 0, 1000, 100));
        assertEquals(0, regionDest.getMessageStore().getMessageCount());
    }

    // Xpath selector will trigger the body to unmarshal, make sure that is handled as well
    // The queue should detect that error when trying to add to the consumer and DLQ
    @Test
    public void testXpath() throws Exception {
        try (ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory(
                clientUri).createConnection()) {
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ActiveMQDestination dest = (ActiveMQDestination) session.createQueue("test.queue");
            MessageProducer producer = session.createProducer(dest);
            MessageConsumer consumer = session.createConsumer(dest, "XPATH '//books//book[@lang=''en'']'");

            ActiveMQTextMessage message = (ActiveMQTextMessage) session.createTextMessage(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><books><book lang=\"en\">ABC</book></books>");
            message.storeContentAndClear();
            ByteSequenceData.writeIntBig(message.getContent(), 1024 * 1024);
            producer.send(message);

            assertNull(consumer.receive(100));

            Destination destination = brokerService.getDestination(dest);
            assertTrue(Wait.waitFor(() -> destination.getDestinationStatistics().getMessages().getCount() == 0,
                    500, 10));
            assertTrue(Wait.waitFor(
                    () -> destination.getMemoryUsage().getUsage() == 0, 1000, 100));
            assertEquals(0, destination.getMessageStore().getMessageCount());
        }
    }


    protected void startBroker() throws Exception {
        brokerService = new BrokerService();
        PolicyEntry policy = new PolicyEntry();
        policy.setSendAdvisoryIfNoConsumers(true);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        brokerService.setDestinationPolicy(pMap);
        brokerService.setPersistent(true);
        brokerService.setDataDirectoryFile(dataFileDir.getRoot());
        KahaDBStore store = new KahaDBStore();
        store.setDirectory(dataFileDir.getRoot());
        store.setJournalDiskSyncStrategy(Journal.JournalDiskSyncStrategy.NEVER.name());
        brokerService.setPersistenceAdapter(store);
        brokerService.setUseJmx(false);
        brokerService.setPlugins(new BrokerPlugin[]{new BrokerPluginSupport() {
            @Override
            public Broker installPlugin(Broker broker) {
                return new BrokerFilter(broker) {
                    @Override
                    public boolean sendToDeadLetterQueue(ConnectionContext context,
                            MessageReference messageReference, Subscription subscription,
                            Throwable poisonCause) {
                        dlqCount.getAndIncrement();
                        return super.sendToDeadLetterQueue(context, messageReference,
                                subscription, poisonCause);
                    }
                };
            }
        }});

        TransportConnector connector = brokerService.addConnector("nio://localhost:0");
        brokerService.start();
        brokerService.waitUntilStarted();

        clientUri = connector.getPublishableConnectURI();
    }

    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }
}
