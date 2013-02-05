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

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jms.ConnectionConsumer;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ3961Test {

    private static BrokerService brokerService;
    private static String BROKER_ADDRESS = "tcp://localhost:0";

    private ActiveMQConnectionFactory connectionFactory;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.setDeleteAllMessagesOnStartup(true);
        connectionUri = brokerService.addConnector(BROKER_ADDRESS).getPublishableConnectString();
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    public class TestServerSessionPool implements ServerSessionPool {

        private final TopicConnection connection;

        public TestServerSessionPool(final TopicConnection connection) {
            this.connection = connection;
        }

        @Override
        public ServerSession getServerSession() throws JMSException {
            final TopicSession topicSession = connection.createTopicSession(true, Session.AUTO_ACKNOWLEDGE);
            return new TestServerSession(topicSession);
        }
    }

    public class TestServerSession implements ServerSession, MessageListener {

        private final TopicSession session;

        public TestServerSession(final TopicSession session) throws JMSException {
            this.session = session;
            session.setMessageListener(this);
        }

        @Override
        public Session getSession() throws JMSException {
            return session;
        }

        @Override
        public void start() throws JMSException {
            session.run();
        }

        @Override
        public void onMessage(final Message message) {
            synchronized (processedSessions) {
                processedSessions.add(this);
            }
        }
    }

    public static final int MESSAGE_COUNT = 16;
    private final List<TestServerSession> processedSessions = new LinkedList<TestServerSession>();
    private final List<TestServerSession> committedSessions = new LinkedList<TestServerSession>();

    @Test
    public void testPrefetchInDurableSubscription() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic("TestTopic");

        final TopicConnection initialSubConnection = connectionFactory.createTopicConnection();
        initialSubConnection.setClientID("TestClient");
        initialSubConnection.start();
        final TopicSession initialSubSession = initialSubConnection.createTopicSession(false, Session.CLIENT_ACKNOWLEDGE);
        final TopicSubscriber initialSubscriber = initialSubSession.createDurableSubscriber(topic, "TestSubscriber");

        initialSubscriber.close();
        initialSubSession.close();
        initialSubConnection.close();

        final TopicConnection publisherConnection = connectionFactory.createTopicConnection();
        publisherConnection.start();
        final TopicSession publisherSession = publisherConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        final TopicPublisher publisher = publisherSession.createPublisher(topic);
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            final Message msg = publisherSession.createTextMessage("Message #" + i);
            publisher.publish(msg);
        }
        publisher.close();
        publisherSession.close();
        publisherConnection.close();

        final TopicConnection connection = connectionFactory.createTopicConnection();
        connection.setClientID("TestClient");
        connection.start();
        final TestServerSessionPool pool = new TestServerSessionPool(connection);
        final ConnectionConsumer connectionConsumer = connection.createDurableConnectionConsumer(topic, "TestSubscriber", null, pool, 1);
        while (true) {
            int lastMsgCount = 0;
            int msgCount = 0;
            do {
                lastMsgCount = msgCount;
                Thread.sleep(200L);
                synchronized (processedSessions) {
                    msgCount = processedSessions.size();
                }
            } while (lastMsgCount < msgCount);

            if (lastMsgCount == 0) {
                break;
            }

            final LinkedList<TestServerSession> collected;
            synchronized (processedSessions) {
                collected = new LinkedList<TestServerSession>(processedSessions);
                processedSessions.clear();
            }

            final Iterator<TestServerSession> sessions = collected.iterator();
            while (sessions.hasNext()) {
                final TestServerSession session = sessions.next();
                committedSessions.add(session);
                session.getSession().commit();
                session.getSession().close();
            }
        }

        connectionConsumer.close();
        final TopicSession finalSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        finalSession.unsubscribe("TestSubscriber");
        finalSession.close();
        connection.close();
        assertEquals(MESSAGE_COUNT, committedSessions.size());
    }
}
