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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that the durable sub updates when the offline sub is reactivated with new values.
 */
public class DurableSubscriptionUpdatesTest {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionUpdatesTest.class);

    private final int MSG_COUNT = 5;

    private BrokerService brokerService;
    private URI connectionUri;

    private String clientId;
    private String subscriptionName;
    private String topicName;

    private TopicConnection connection;

    @Rule public TestName name = new TestName();

    protected TopicConnection createConnection() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setUseAsyncSend(true);

        TopicConnection connection = factory.createTopicConnection();
        connection.setClientID(clientId);
        connection.start();

        return connection;
    }

    @Before
    public void setUp() throws Exception {
        startBroker();
    }

    @After
    public void tearDown() throws Exception {
        try {
            connection.close();
        } catch (Exception e) {
        }

        stopBroker();
    }

    @Test(timeout = 60000)
    public void testSelectorChange() throws Exception {
        connection = createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(topicName);

        // Create a Durable Topic Subscription with noLocal set to true.
        TopicSubscriber durableSubscriber = session.createDurableSubscriber(topic, subscriptionName, "JMSPriority > 8", false);

        // Public first set, only the non durable sub should get these.
        publishToTopic(session, topic, 9);
        publishToTopic(session, topic, 8);

        // Standard subscriber should receive them
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = durableSubscriber.receive(2000);
            assertNotNull(message);
            assertEquals(9, message.getJMSPriority());
        }

        // Subscriber should not receive the others.
        {
            Message message = durableSubscriber.receive(500);
            assertNull(message);
        }

        // Public second set for testing durable sub changed.
        publishToTopic(session, topic, 9);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        // Durable now goes inactive.
        durableSubscriber.close();

        assertTrue("Should have no durables.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getDurableTopicSubscribers().length == 0;
            }
        }));
        assertTrue("Should have an inactive sub.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        LOG.debug("Testing that updated selector subscription does get any messages.");

        // Recreate a Durable Topic Subscription with noLocal set to false.
        durableSubscriber = session.createDurableSubscriber(topic, subscriptionName, "JMSPriority > 7", false);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        // Durable subscription should not receive them as the subscriptions should
        // have been removed and recreated to update the noLocal flag.
        {
            Message message = durableSubscriber.receive(500);
            assertNull(message);
        }

        // Public third set which should get queued for the durable sub with noLocal=false
        publishToTopic(session, topic, 8);

        // Durable subscriber should receive them
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = durableSubscriber.receive(5000);
            assertNotNull("Should get messages now", message);
            assertEquals(8, message.getJMSPriority());
        }
    }

    @Test(timeout = 60000)
    public void testResubscribeWithNewNoLocalValueNoBrokerRestart() throws Exception {
        connection = createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(topicName);

        // Create a Durable Topic Subscription with noLocal set to true.
        TopicSubscriber durableSubscriber = session.createDurableSubscriber(topic, subscriptionName, null, true);

        // Create a Durable Topic Subscription with noLocal set to true.
        TopicSubscriber nonDurableSubscriber = session.createSubscriber(topic);

        // Public first set, only the non durable sub should get these.
        publishToTopic(session, topic);

        LOG.debug("Testing that noLocal=true subscription doesn't get any messages.");

        // Standard subscriber should receive them
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = nonDurableSubscriber.receive(2000);
            assertNotNull(message);
        }

        // Durable noLocal=true subscription should not receive them
        {
            Message message = durableSubscriber.receive(500);
            assertNull(message);
        }

        // Public second set for testing durable sub changed.
        publishToTopic(session, topic);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        // Durable now goes inactive.
        durableSubscriber.close();

        assertTrue("Should have no durables.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getDurableTopicSubscribers().length == 0;
            }
        }));
        assertTrue("Should have an inactive sub.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        LOG.debug("Testing that updated noLocal=false subscription does get any messages.");

        // Recreate a Durable Topic Subscription with noLocal set to false.
        durableSubscriber = session.createDurableSubscriber(topic, subscriptionName, null, false);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        // Durable noLocal=false subscription should not receive them as the subscriptions should
        // have been removed and recreated to update the noLocal flag.
        {
            Message message = durableSubscriber.receive(500);
            assertNull(message);
        }

        // Public third set which should get queued for the durable sub with noLocal=false
        publishToTopic(session, topic);

        // Durable subscriber should receive them
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = durableSubscriber.receive(5000);
            assertNotNull("Should get local messages now", message);
        }
    }

    @Test(timeout = 60000)
    public void testDurableResubscribeWithNewNoLocalValueWithBrokerRestart() throws Exception {
        connection = createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(topicName);

        // Create a Durable Topic Subscription with noLocal set to true.
        TopicSubscriber durableSubscriber = session.createDurableSubscriber(topic, subscriptionName, null, true);

        // Create a Durable Topic Subscription with noLocal set to true.
        TopicSubscriber nonDurableSubscriber = session.createSubscriber(topic);

        // Public first set, only the non durable sub should get these.
        publishToTopic(session, topic);

        LOG.debug("Testing that noLocal=true subscription doesn't get any messages.");

        // Standard subscriber should receive them
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = nonDurableSubscriber.receive(2000);
            assertNotNull(message);
        }

        // Durable noLocal=true subscription should not receive them
        {
            Message message = durableSubscriber.receive(500);
            assertNull(message);
        }

        // Public second set for testing durable sub changed.
        publishToTopic(session, topic);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        // Durable now goes inactive.
        durableSubscriber.close();

        assertTrue("Should have no durables.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getDurableTopicSubscribers().length == 0;
            }
        }));
        assertTrue("Should have an inactive sub.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        LOG.debug("Testing that updated noLocal=false subscription does get any messages.");

        connection.close();

        restartBroker();

        connection = createConnection();

        session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        // The previous subscription should be restored as an offline subscription.
        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        // Recreate a Durable Topic Subscription with noLocal set to false.
        durableSubscriber = session.createDurableSubscriber(topic, subscriptionName, null, false);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        // Durable noLocal=false subscription should not receive them as the subscriptions should
        // have been removed and recreated to update the noLocal flag.
        {
            Message message = durableSubscriber.receive(500);
            assertNull(message);
        }

        // Public third set which should get queued for the durable sub with noLocal=false
        publishToTopic(session, topic);

        // Durable subscriber should receive them
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = durableSubscriber.receive(2000);
            assertNotNull("Should get local messages now", message);
        }
    }

    private void publishToTopic(TopicSession session, Topic destination) throws Exception {
        publishToTopic(session, destination, Message.DEFAULT_PRIORITY);
    }

    private void publishToTopic(TopicSession session, Topic destination, int priority) throws Exception {
        TopicPublisher publisher = session.createPublisher(destination);
        for (int i = 0; i < MSG_COUNT; ++i) {
            publisher.send(session.createMessage(), Message.DEFAULT_DELIVERY_MODE, priority, Message.DEFAULT_TIME_TO_LIVE);
        }

        publisher.close();
    }

    private void startBroker() throws Exception {
        createBroker(true);
    }

    private void restartBroker() throws Exception {
        stopBroker();
        createBroker(false);
    }

    private void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }

    private void createBroker(boolean deleteMessages) throws Exception {
        brokerService = new BrokerService();
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateMBeanServer(false);
        brokerService.setPersistent(true);
        brokerService.setDeleteAllMessagesOnStartup(deleteMessages);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        brokerService.setKeepDurableSubsActive(false);
        TransportConnector connector = brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.start();

        connectionUri = connector.getPublishableConnectURI();

        clientId = name.getMethodName() + "-ClientId";
        subscriptionName = name.getMethodName() + "-Subscription";
        topicName = name.getMethodName();
    }
}
