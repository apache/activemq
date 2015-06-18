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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;

import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for spec compliance for durable subscriptions that change the noLocal flag.
 */
public class DurableSubscriptionWithNoLocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionWithNoLocalTest.class);

    private final int MSG_COUNT = 10;
    private final String KAHADB_DIRECTORY = "target/activemq-data/";

    @Rule public TestName name = new TestName();

    private BrokerService brokerService;
    private String connectionUri;
    private ActiveMQConnectionFactory factory;

    @Before
    public void setUp() throws Exception {
        createBroker(true);
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Ignore("Requires Broker be able to remove and recreate on noLocal change")
    @Test(timeout = 60000)
    public void testDurableSubWithNoLocalChange() throws Exception {
        TopicConnection connection = factory.createTopicConnection();

        connection.setClientID(getClientId());
        connection.start();

        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getDestinationName());
        TopicPublisher publisher = session.createPublisher(topic);

        LOG.debug("Create DurableSubscriber with noLocal = true");
        TopicSubscriber subscriber = session.createSubscriber(topic);
        TopicSubscriber durableSub = session.createDurableSubscriber(topic, getSubscriptionName(), null, true);

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.info("Attempting to receive messages from non-durable subscriber");
        for (int i = 0; i < MSG_COUNT; i++) {
            assertNotNull(subscriber.receive(500));
        }

        LOG.info("Attempting to receive messages from (noLocal=true) subscriber");
        assertNull(durableSub.receive(500));

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.debug("Close DurableSubscriber with noLocal=true");
        durableSub.close();

        LOG.debug("Create DurableSubscriber with noLocal=false");
        durableSub = session.createDurableSubscriber(topic, getSubscriptionName(), null, false);

        LOG.info("Attempting to receive messages from reconnected (noLocal=false) subscription");
        assertNull(durableSub.receive(500));

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.info("Attempting to receive messages from (noLocal=false) durable subscriber");
        for (int i = 0; i < MSG_COUNT; i++) {
            assertNotNull(durableSub.receive(500));
        }

        // Should be empty now
        assertNull(durableSub.receive(100));
    }

    @Ignore("Requires Broker be able to remove and recreate on noLocal change")
    @Test(timeout = 60000)
    public void testInvertedDurableSubWithNoLocalChange() throws Exception {
        TopicConnection connection = factory.createTopicConnection();

        connection.setClientID(getClientId());
        connection.start();

        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getDestinationName());
        TopicPublisher publisher = session.createPublisher(topic);

        LOG.debug("Create DurableSubscriber with noLocal = true");
        TopicSubscriber subscriber = session.createSubscriber(topic);
        TopicSubscriber durableSub = session.createDurableSubscriber(topic, getSubscriptionName(), null, false);

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.info("Attempting to receive messages from non-durable subscriber");
        for (int i = 0; i < MSG_COUNT; i++) {
            assertNotNull(subscriber.receive(500));
        }

        LOG.info("Attempting to receive messages from (noLocal=false) durable subscriber");
        for (int i = 0; i < MSG_COUNT; i++) {
            assertNotNull(durableSub.receive(500));
        }

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.debug("Close DurableSubscriber with noLocal=true");
        durableSub.close();

        LOG.debug("Create DurableSubscriber with noLocal=false");
        durableSub = session.createDurableSubscriber(topic, getSubscriptionName(), null, true);

        LOG.info("Attempting to receive messages from reconnected (noLocal=true) subscription");
        assertNull(durableSub.receive(500));

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.info("Attempting to receive messages from reconnected (noLocal=true) subscription");
        assertNull(durableSub.receive(500));

        // Should be empty now
        assertNull(durableSub.receive(100));
    }

    @Test(timeout = 60000)
    public void testDurableSubWithNoLocalChangeAfterRestart() throws Exception {
        TopicConnection connection = factory.createTopicConnection();

        connection.setClientID(getClientId());
        connection.start();

        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getDestinationName());
        TopicPublisher publisher = session.createPublisher(topic);

        LOG.debug("Create DurableSubscriber with noLocal = true");
        TopicSubscriber subscriber = session.createSubscriber(topic);
        TopicSubscriber durableSub = session.createDurableSubscriber(topic, getSubscriptionName(), null, true);

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.info("Attempting to receive messages from non-durable subscriber");
        for (int i = 0; i < MSG_COUNT; i++) {
            assertNotNull(subscriber.receive(500));
        }

        LOG.info("Attempting to receive messages from (noLocal=true) subscriber");
        assertNull(durableSub.receive(500));

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        tearDown();
        createBroker(false);

        connection = factory.createTopicConnection();
        connection.setClientID(getClientId());
        connection.start();

        session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(getDestinationName());
        publisher = session.createPublisher(topic);

        LOG.debug("Create DurableSubscriber with noLocal=false");
        durableSub = session.createDurableSubscriber(topic, getSubscriptionName(), null, false);

        LOG.info("Attempting to receive messages from reconnected (noLocal=false) subscription");
        assertNull(durableSub.receive(500));

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.info("Attempting to receive messages from (noLocal=false) durable subscriber");
        for (int i = 0; i < MSG_COUNT; i++) {
            assertNotNull(durableSub.receive(500));
        }

        // Should be empty now
        assertNull(durableSub.receive(100));
    }

    @Ignore("Requires Broker be able to remove and recreate on noLocal change")
    @Test(timeout = 60000)
    public void testInvertedDurableSubWithNoLocalChangeAfterRestart() throws Exception {
        TopicConnection connection = factory.createTopicConnection();

        connection.setClientID(getClientId());
        connection.start();

        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getDestinationName());
        TopicPublisher publisher = session.createPublisher(topic);

        LOG.debug("Create DurableSubscriber with noLocal = true");
        TopicSubscriber subscriber = session.createSubscriber(topic);
        TopicSubscriber durableSub = session.createDurableSubscriber(topic, getSubscriptionName(), null, false);

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.info("Attempting to receive messages from non-durable subscriber");
        for (int i = 0; i < MSG_COUNT; i++) {
            assertNotNull(subscriber.receive(500));
        }

        LOG.info("Attempting to receive messages from (noLocal=false) durable subscriber");
        for (int i = 0; i < MSG_COUNT; i++) {
            assertNotNull(durableSub.receive(500));
        }

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        tearDown();
        createBroker(false);

        connection = factory.createTopicConnection();
        connection.setClientID(getClientId());
        connection.start();

        session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(getDestinationName());
        publisher = session.createPublisher(topic);

        LOG.debug("Create DurableSubscriber with noLocal=true");
        durableSub = session.createDurableSubscriber(topic, getSubscriptionName(), null, true);

        LOG.info("Attempting to receive messages from (noLocal=true) subscriber");
        assertNull(durableSub.receive(500));

        LOG.debug("Sending " + MSG_COUNT + " messages to topic");
        for (int i = 0; i < MSG_COUNT; i++) {
            publisher.publish(session.createMessage());
        }

        LOG.info("Attempting to receive messages from (noLocal=true) subscriber");
        assertNull(durableSub.receive(500));

        // Should be empty now
        assertNull(durableSub.receive(100));
    }

    private void createBroker(boolean deleteAllMessages) throws Exception {
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File(KAHADB_DIRECTORY + "-" + name.getMethodName()));

        brokerService = new BrokerService();
        brokerService.setPersistent(true);
        brokerService.setPersistenceAdapter(kaha);
        brokerService.setStoreOpenWireVersion(CommandTypes.PROTOCOL_VERSION);
        brokerService.setUseJmx(false);
        brokerService.setDeleteAllMessagesOnStartup(deleteAllMessages);
        TransportConnector connector = brokerService.addConnector("tcp://0.0.0.0:0");

        brokerService.start();
        brokerService.waitUntilStarted();

        connectionUri = connector.getPublishableConnectString();
        factory = new ActiveMQConnectionFactory(connectionUri);
    }

    private String getDestinationName() {
        return name.getMethodName();
    }

    private String getClientId() {
        return name.getMethodName() + "-Client";
    }

    private String getSubscriptionName() {
        return name.getMethodName() + "-Subscription";
    }
}
