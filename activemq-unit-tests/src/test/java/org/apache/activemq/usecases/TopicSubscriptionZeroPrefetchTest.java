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
package org.apache.activemq.usecases;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicSubscriptionZeroPrefetchTest {

    private static final Logger LOG = LoggerFactory.getLogger(TopicSubscriptionZeroPrefetchTest.class);

    @Rule
    public TestName name = new TestName();

    private Connection connection;
    private Session session;
    private ActiveMQTopic destination;
    private MessageProducer producer;
    private MessageConsumer consumer;
    private BrokerService brokerService;

    public String getTopicName() {
        return name.getMethodName();
    }

    @Before
    public void setUp() throws Exception {

        brokerService = createBroker();

        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");

        activeMQConnectionFactory.setWatchTopicAdvisories(true);
        connection = activeMQConnectionFactory.createConnection();
        connection.setClientID("ClientID-1");
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = new ActiveMQTopic(getTopicName());
        producer = session.createProducer(destination);

        connection.start();
    }

    /*
     * test non durable topic subscription with prefetch set to zero
     */
    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZero() throws Exception {

        ActiveMQTopic consumerDestination = new ActiveMQTopic(getTopicName() + "?consumer.retroactive=true&consumer.prefetchSize=0");
        consumer = session.createConsumer(consumerDestination);

        // publish messages
        Message txtMessage = session.createTextMessage("M");
        producer.send(txtMessage);

        Message consumedMessage = consumer.receiveNoWait();

        Assert.assertNotNull("should have received a message the published message", consumedMessage);
    }

    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZeroClientAckLoopReceive() throws Exception {
        ActiveMQTopic consumerDestination = new ActiveMQTopic(getTopicName() + "?consumer.retroactive=true&consumer.prefetchSize=0");
        Session consumerClientAckSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = consumerClientAckSession.createConsumer(consumerDestination);

        final int count = 10;
        for (int i = 0; i < count; i++) {
            Message txtMessage = session.createTextMessage("M:" + i);
            producer.send(txtMessage);
        }

        for (int i = 0; i < count; i++) {
            Message consumedMessage = consumer.receive();
            Assert.assertNotNull("should have received message[" + i + "]", consumedMessage);
        }
    }

    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZeroClientAckLoopTimedReceive() throws Exception {
        ActiveMQTopic consumerDestination = new ActiveMQTopic(getTopicName() + "?consumer.retroactive=true&consumer.prefetchSize=0");
        Session consumerClientAckSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = consumerClientAckSession.createConsumer(consumerDestination);

        final int count = 10;
        for (int i = 0; i < count; i++) {
            Message txtMessage = session.createTextMessage("M:" + i);
            producer.send(txtMessage);
        }

        for (int i = 0; i < count; i++) {
            Message consumedMessage = consumer.receive(2000);
            Assert.assertNotNull("should have received message[" + i + "]", consumedMessage);
        }
    }

    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZeroClientAckLoopReceiveNoWait() throws Exception {
        ActiveMQTopic consumerDestination = new ActiveMQTopic(getTopicName() + "?consumer.retroactive=true&consumer.prefetchSize=0");
        Session consumerClientAckSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = consumerClientAckSession.createConsumer(consumerDestination);

        final int count = 10;
        for (int i = 0; i < count; i++) {
            Message txtMessage = session.createTextMessage("M:" + i);
            producer.send(txtMessage);
        }

        for (int i = 0; i < count; i++) {
            Message consumedMessage = consumer.receiveNoWait();
            Assert.assertNotNull("should have received message[" + i + "]", consumedMessage);
        }
    }

    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZeroConcurrentProduceConsumeAutoAck() throws Exception {
        doTestTopicConsumerPrefetchZeroConcurrentProduceConsume(Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZeroConcurrentProduceConsumeClientAck() throws Exception {
        doTestTopicConsumerPrefetchZeroConcurrentProduceConsume(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZeroConcurrentProduceConsumeDupsOk() throws Exception {
        doTestTopicConsumerPrefetchZeroConcurrentProduceConsume(Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZeroConcurrentProduceConsumeTransacted() throws Exception {
        doTestTopicConsumerPrefetchZeroConcurrentProduceConsume(Session.SESSION_TRANSACTED);
    }

    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZeroConcurrentProduceConsumeTransactedComitInBatches() throws Exception {
        doTestTopicConsumerPrefetchZeroConcurrentProduceConsume(Session.SESSION_TRANSACTED);
    }

    @Test(timeout = 60000)
    public void testTopicConsumerPrefetchZeroConcurrentProduceConsumeIndividual() throws Exception {
        doTestTopicConsumerPrefetchZeroConcurrentProduceConsume(ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
    }

    private void doTestTopicConsumerPrefetchZeroConcurrentProduceConsume(int ackMode) throws Exception {
        doTestTopicConsumerPrefetchZeroConcurrentProduceConsume(ackMode, false);
    }

    private void doTestTopicConsumerPrefetchZeroConcurrentProduceConsume(int ackMode, boolean commitBatch) throws Exception {

        ActiveMQTopic consumerDestination = new ActiveMQTopic(getTopicName() + "?consumer.retroactive=true&consumer.prefetchSize=0");
        Session consumerSession = connection.createSession(ackMode == Session.SESSION_TRANSACTED, ackMode);
        consumer = consumerSession.createConsumer(consumerDestination);

        final int MSG_COUNT = 2000;

        final AtomicBoolean error = new AtomicBoolean();
        final CountDownLatch done = new CountDownLatch(MSG_COUNT);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    for (int i = 0; i < MSG_COUNT; i++) {
                        Message consumedMessage = consumer.receive();
                        if (consumedMessage != null) {
                            done.countDown();
                            consumedMessage.acknowledge();
                            if (ackMode == Session.SESSION_TRANSACTED && commitBatch && ((i + 1) % 50) == 0) {
                                consumerSession.commit();
                            }
                        }
                    }
                } catch (Exception ex) {
                    LOG.error("Caught exception during receive: {}", ex);
                    error.set(true);
                } finally {
                    if (ackMode == Session.SESSION_TRANSACTED) {
                        try {
                            consumerSession.commit();
                        } catch (JMSException e) {
                            LOG.error("Caught exception on commit: {}", e);
                            error.set(true);
                        }
                    }
                }
            }
        });

        for (int i = 0; i < MSG_COUNT; i++) {
            Message txtMessage = session.createTextMessage("M:" + i);
            producer.send(txtMessage);
        }

        assertFalse("Should not have gotten any errors", error.get());
        assertTrue("Should have read all messages", done.await(10, TimeUnit.SECONDS));
    }

    /*
     * test durable topic subscription with prefetch zero
     */
    @Test(timeout = 60000)
    public void testDurableTopicConsumerPrefetchZero() throws Exception {

        ActiveMQTopic consumerDestination = new ActiveMQTopic(getTopicName() + "?consumer.prefetchSize=0");
        consumer = session.createDurableSubscriber(consumerDestination, "mysub1");

        // publish messages
        Message txtMessage = session.createTextMessage("M");
        producer.send(txtMessage);

        Message consumedMessage = consumer.receive(100);

        Assert.assertNotNull("should have received a message the published message", consumedMessage);
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
        producer.close();
        session.close();
        connection.close();
        brokerService.stop();
    }

    // helper method to create a broker with slow consumer advisory turned on
    private BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setUseJmx(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("vm://localhost");
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }
}
