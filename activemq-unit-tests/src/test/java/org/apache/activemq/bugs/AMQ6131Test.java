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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.MessageDatabase;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class is to show that a durable can lose messages after index deletion.
 */
public class AMQ6131Test {

    protected BrokerService broker;
    protected URI brokerConnectURI;

    @Before
    public void startBroker() throws Exception {
        org.apache.log4j.Logger.getLogger(MessageDatabase.class).setLevel(Level.TRACE);
        setUpBroker(true);
    }

    protected void setUpBroker(boolean clearDataDir) throws Exception {

        broker = new BrokerService();
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(clearDataDir);

        // set up a transport
        TransportConnector connector = broker.addConnector(new TransportConnector());
        connector.setUri(new URI("tcp://0.0.0.0:0"));
        connector.setName("tcp");

        broker.start();
        broker.waitUntilStarted();
        brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    protected BrokerService getBroker() {
        return this.broker;
    }

    public File getPersistentDir() throws IOException {
        return getBroker().getPersistenceAdapter().getDirectory();
    }

    @Test(timeout = 300000)
    public void testDurableWithOnePendingAfterRestartAndIndexRecovery() throws Exception {
        final File persistentDir = getPersistentDir();

        broker.getBroker().addDestination(broker.getAdminConnectionContext(), new ActiveMQTopic("durable.sub"), false);

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(this.brokerConnectURI);
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setClientID("myId");
        connection.start();
        final Session jmsSession = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);

        TopicSubscriber durable = jmsSession.createDurableSubscriber(new ActiveMQTopic("durable.sub"), "sub");
        final MessageProducer producer = jmsSession.createProducer(new ActiveMQTopic("durable.sub"));

        final int original = new ArrayList<File>(FileUtils.listFiles(persistentDir, new WildcardFileFilter("*.log"), TrueFileFilter.INSTANCE)).size();

        // 100k messages
        final byte[] data = new byte[100000];
        final Random random = new Random();
        random.nextBytes(data);

        // run test with enough messages to create a second journal file
        final AtomicInteger messageCount = new AtomicInteger();
        assertTrue("Should have added a journal file", Wait.waitFor(new Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                final ActiveMQBytesMessage message = new ActiveMQBytesMessage();
                message.setContent(new ByteSequence(data));

                for (int i = 0; i < 100; i++) {
                    producer.send(message);
                    messageCount.getAndIncrement();
                }

                return new ArrayList<File>(FileUtils.listFiles(persistentDir, new WildcardFileFilter("*.log"), TrueFileFilter.INSTANCE)).size() > original;
            }
        }));

        // Consume all but 1 message
        for (int i = 0; i < messageCount.get() - 1; i++) {
            durable.receive();
        }

        durable.close();

        // wait until a journal file has been GC'd after receiving messages
        assertTrue("Subscription should go inactive", Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        // force a GC of unneeded journal files
        getBroker().getPersistenceAdapter().checkpoint(true);

        // wait until a journal file has been GC'd after receiving messages
        assertFalse("Should not have garbage collected", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return new ArrayList<File>(FileUtils.listFiles(persistentDir, new WildcardFileFilter("*.log"), TrueFileFilter.INSTANCE)).size() == original;
            }
        }, 5000, 500));

        // stop the broker so we can blow away the index
        getBroker().stop();
        getBroker().waitUntilStopped();

        // delete the index so that the durables are gone from the index
        // The test passes if you take out this delete section
        for (File index : FileUtils.listFiles(persistentDir, new WildcardFileFilter("db.*"), TrueFileFilter.INSTANCE)) {
            FileUtils.deleteQuietly(index);
        }

        stopBroker();
        setUpBroker(false);

        assertEquals(1, broker.getAdminView().getInactiveDurableTopicSubscribers().length);
        assertEquals(0, broker.getAdminView().getDurableTopicSubscribers().length);

        ActiveMQConnectionFactory connectionFactory2 = new ActiveMQConnectionFactory(this.brokerConnectURI);
        ActiveMQConnection connection2 = (ActiveMQConnection) connectionFactory2.createConnection();
        connection2.setClientID("myId");
        connection2.start();
        final Session jmsSession2 = connection2.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);

        TopicSubscriber durable2 = jmsSession2.createDurableSubscriber(new ActiveMQTopic("durable.sub"), "sub");

        assertEquals(0, broker.getAdminView().getInactiveDurableTopicSubscribers().length);
        assertEquals(1, broker.getAdminView().getDurableTopicSubscribers().length);

        assertNotNull(durable2.receive(5000));
    }

    @Test(timeout = 300000)
    public void testDurableWithNoMessageAfterRestartAndIndexRecovery() throws Exception {
        final File persistentDir = getPersistentDir();

        broker.getBroker().addDestination(broker.getAdminConnectionContext(), new ActiveMQTopic("durable.sub"), false);

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(this.brokerConnectURI);
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setClientID("myId");
        connection.start();
        final Session jmsSession = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);

        TopicSubscriber durable = jmsSession.createDurableSubscriber(new ActiveMQTopic("durable.sub"), "sub");
        final MessageProducer producer = jmsSession.createProducer(new ActiveMQTopic("durable.sub"));

        final int original = new ArrayList<File>(FileUtils.listFiles(persistentDir, new WildcardFileFilter("*.log"), TrueFileFilter.INSTANCE)).size();

        // 100k messages
        final byte[] data = new byte[100000];
        final Random random = new Random();
        random.nextBytes(data);

        // run test with enough messages to create a second journal file
        final AtomicInteger messageCount = new AtomicInteger();
        assertTrue("Should have added a journal file", Wait.waitFor(new Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                final ActiveMQBytesMessage message = new ActiveMQBytesMessage();
                message.setContent(new ByteSequence(data));

                for (int i = 0; i < 100; i++) {
                    producer.send(message);
                    messageCount.getAndIncrement();
                }

                return new ArrayList<File>(FileUtils.listFiles(persistentDir, new WildcardFileFilter("*.log"), TrueFileFilter.INSTANCE)).size() > original;
            }
        }));

        // Consume all messages
        for (int i = 0; i < messageCount.get(); i++) {
            durable.receive();
        }

        durable.close();

        assertTrue("Subscription should go inactive", Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        // force a GC of unneeded journal files
        getBroker().getPersistenceAdapter().checkpoint(true);

        // wait until a journal file has been GC'd after receiving messages
        assertTrue("Should have garbage collected", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return new ArrayList<File>(FileUtils.listFiles(persistentDir, new WildcardFileFilter("*.log"), TrueFileFilter.INSTANCE)).size() == original;
            }
        }));

        // stop the broker so we can blow away the index
        getBroker().stop();
        getBroker().waitUntilStopped();

        // delete the index so that the durables are gone from the index
        // The test passes if you take out this delete section
        for (File index : FileUtils.listFiles(persistentDir, new WildcardFileFilter("db.*"), TrueFileFilter.INSTANCE)) {
            FileUtils.deleteQuietly(index);
        }

        stopBroker();
        setUpBroker(false);

        assertEquals(1, broker.getAdminView().getInactiveDurableTopicSubscribers().length);
        assertEquals(0, broker.getAdminView().getDurableTopicSubscribers().length);

        ActiveMQConnectionFactory connectionFactory2 = new ActiveMQConnectionFactory(this.brokerConnectURI);
        ActiveMQConnection connection2 = (ActiveMQConnection) connectionFactory2.createConnection();
        connection2.setClientID("myId");
        connection2.start();
        final Session jmsSession2 = connection2.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);

        TopicSubscriber durable2 = jmsSession2.createDurableSubscriber(new ActiveMQTopic("durable.sub"), "sub");

        assertEquals(0, broker.getAdminView().getInactiveDurableTopicSubscribers().length);
        assertEquals(1, broker.getAdminView().getDurableTopicSubscribers().length);

        assertNull(durable2.receive(500));
    }
}