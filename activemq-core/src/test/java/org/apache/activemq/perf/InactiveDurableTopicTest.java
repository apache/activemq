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
package org.apache.activemq.perf;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadaptor.KahaPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$
 */
public class InactiveDurableTopicTest extends TestCase {
    private static final transient Log LOG = LogFactory.getLog(InactiveDurableTopicTest.class);

    private static final int MESSAGE_COUNT = 1000000;
    private static final String DEFAULT_PASSWORD = "";
    private static final String USERNAME = "testuser";
    private static final String CLIENTID = "mytestclient";
    private static final String TOPIC_NAME = "testevent";
    private static final String SUBID = "subscription1";
    private static final int DELIVERY_MODE = javax.jms.DeliveryMode.PERSISTENT;
    private static final int DELIVERY_PRIORITY = javax.jms.Message.DEFAULT_PRIORITY;
    private Connection connection;
    private MessageProducer publisher;
    private TopicSubscriber subscriber;
    private Topic topic;
    private Session session;
    private ActiveMQConnectionFactory connectionFactory;
    private BrokerService broker;

    protected void setUp() throws Exception {
        super.setUp();
        broker = new BrokerService();

        //broker.setPersistenceAdapter(new KahaPersistenceAdapter());
        /*
         * JournalPersistenceAdapterFactory factory = new
         * JournalPersistenceAdapterFactory();
         * factory.setDataDirectoryFile(broker.getDataDirectory());
         * factory.setTaskRunnerFactory(broker.getTaskRunnerFactory());
         * factory.setUseJournal(false); broker.setPersistenceFactory(factory);
         */
        broker.addConnector(ActiveMQConnectionFactory.DEFAULT_BROKER_URL);
        broker.start();
        connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_BROKER_URL);
        /*
         * Doesn't matter if you enable or disable these, so just leaving them
         * out for this test case connectionFactory.setAlwaysSessionAsync(true);
         * connectionFactory.setAsyncDispatch(true);
         */
        connectionFactory.setUseAsyncSend(true);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        broker.stop();
    }

    public void test1CreateSubscription() throws Exception {
        try {
            /*
             * Step 1 - Establish a connection with a client id and create a
             * durable subscription
             */
            connection = connectionFactory.createConnection(USERNAME, DEFAULT_PASSWORD);
            assertNotNull(connection);
            connection.setClientID(CLIENTID);
            connection.start();
            session = connection.createSession(false, javax.jms.Session.CLIENT_ACKNOWLEDGE);
            assertNotNull(session);
            topic = session.createTopic(TOPIC_NAME);
            assertNotNull(topic);
            subscriber = session.createDurableSubscriber(topic, SUBID, "", false);
            assertNotNull(subscriber);
            subscriber.close();
            session.close();
            connection.close();
        } catch (JMSException ex) {
            try {
                connection.close();
            } catch (Exception ignore) {
            }
            throw new AssertionFailedError("Create Subscription caught: " + ex);
        }
    }

    public void test2ProducerTestCase() {
        /*
         * Step 2 - Establish a connection without a client id and create a
         * producer and start pumping messages. We will get hung
         */
        try {
            connection = connectionFactory.createConnection(USERNAME, DEFAULT_PASSWORD);
            assertNotNull(connection);
            session = connection.createSession(false, javax.jms.Session.CLIENT_ACKNOWLEDGE);
            assertNotNull(session);
            topic = session.createTopic(TOPIC_NAME);
            assertNotNull(topic);
            publisher = session.createProducer(topic);
            assertNotNull(publisher);
            MapMessage msg = session.createMapMessage();
            assertNotNull(msg);
            msg.setString("key1", "value1");
            int loop;
            for (loop = 0; loop < MESSAGE_COUNT; loop++) {
                msg.setInt("key2", loop);
                publisher.send(msg, DELIVERY_MODE, DELIVERY_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                if (loop % 5000 == 0) {
                    LOG.info("Sent " + loop + " messages");
                }
            }
            Assert.assertEquals(loop, MESSAGE_COUNT);
            publisher.close();
            session.close();
            connection.stop();
            connection.stop();
        } catch (JMSException ex) {
            try {
                connection.close();
            } catch (Exception ignore) {
            }
            throw new AssertionFailedError("Create Subscription caught: " + ex);
        }
    }

    public void test3CreateSubscription() throws Exception {
        try {
            /*
             * Step 1 - Establish a connection with a client id and create a
             * durable subscription
             */
            connection = connectionFactory.createConnection(USERNAME, DEFAULT_PASSWORD);
            assertNotNull(connection);
            connection.setClientID(CLIENTID);
            connection.start();
            session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
            assertNotNull(session);
            topic = session.createTopic(TOPIC_NAME);
            assertNotNull(topic);
            subscriber = session.createDurableSubscriber(topic, SUBID, "", false);
            assertNotNull(subscriber);
            int loop;
            for (loop = 0; loop < MESSAGE_COUNT; loop++) {
                Message msg = subscriber.receive();
                if (loop % 500 == 0) {
                    LOG.debug("Received " + loop + " messages");
                }
            }
            this.assertEquals(loop, MESSAGE_COUNT);
            subscriber.close();
            session.close();
            connection.close();
        } catch (JMSException ex) {
            try {
                connection.close();
            } catch (Exception ignore) {
            }
            throw new AssertionFailedError("Create Subscription caught: " + ex);
        }
    }
}
