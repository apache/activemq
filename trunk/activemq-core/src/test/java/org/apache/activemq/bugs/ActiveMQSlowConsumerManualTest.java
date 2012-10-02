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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.OldestMessageEvictionStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author James Furness
 *         https://issues.apache.org/jira/browse/AMQ-3607
 */
public class ActiveMQSlowConsumerManualTest {
    private static final int PORT = 12345;
    private static final ActiveMQTopic TOPIC = new ActiveMQTopic("TOPIC");
    private static final String URL = "nio://localhost:" + PORT + "?socket.tcpNoDelay=true";

    @Test(timeout = 60000)
    public void testDefaultSettings() throws Exception {
        runTest("testDefaultSettings", 30, -1, -1, false, false, false, false);
    }

    @Test(timeout = 60000)
    public void testDefaultSettingsWithOptimiseAcknowledge() throws Exception {
        runTest("testDefaultSettingsWithOptimiseAcknowledge", 30, -1, -1, false, false, true, false);
    }

    @Test(timeout = 60000)
    public void testBounded() throws Exception {
        runTest("testBounded", 30, 5, 25, false, false, false, false);
    }

    @Test(timeout = 60000)
    public void testBoundedWithOptimiseAcknowledge() throws Exception {
        runTest("testBoundedWithOptimiseAcknowledge", 30, 5, 25, false, false, true, false);
    }

    public void runTest(String name, int sendMessageCount, int prefetchLimit, int messageLimit, boolean evictOldestMessage, boolean disableFlowControl, boolean optimizeAcknowledge, boolean persistent) throws Exception {
        BrokerService broker = createBroker(persistent);
        broker.setDestinationPolicy(buildPolicy(TOPIC, prefetchLimit, messageLimit, evictOldestMessage, disableFlowControl));
        broker.start();

        // Slow consumer
        Session slowConsumerSession = buildSession("SlowConsumer", URL, optimizeAcknowledge);
        final CountDownLatch blockSlowConsumer = new CountDownLatch(1);
        final AtomicInteger slowConsumerReceiveCount = new AtomicInteger();
        final List<Integer> slowConsumerReceived = sendMessageCount <= 1000 ? new ArrayList<Integer>() : null;
        MessageConsumer slowConsumer = createSubscriber(slowConsumerSession,
                new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        try {
                            slowConsumerReceiveCount.incrementAndGet();
                            int count = Integer.parseInt(((TextMessage) message).getText());
                            if (slowConsumerReceived != null) slowConsumerReceived.add(count);
                            if (count % 10000 == 0) System.out.println("SlowConsumer: Receive " + count);
                            blockSlowConsumer.await();
                        } catch (Exception ignored) {
                        }
                    }
                }
        );

        // Fast consumer
        Session fastConsumerSession = buildSession("FastConsumer", URL, optimizeAcknowledge);
        final AtomicInteger fastConsumerReceiveCount = new AtomicInteger();
        final List<Integer> fastConsumerReceived = sendMessageCount <= 1000 ? new ArrayList<Integer>() : null;
        MessageConsumer fastConsumer = createSubscriber(fastConsumerSession,
                new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        try {
                            fastConsumerReceiveCount.incrementAndGet();
                            TimeUnit.MILLISECONDS.sleep(5);
                            int count = Integer.parseInt(((TextMessage) message).getText());
                            if (fastConsumerReceived != null) fastConsumerReceived.add(count);
                            if (count % 10000 == 0) System.out.println("FastConsumer: Receive " + count);
                        } catch (Exception ignored) {
                        }
                    }
                }
        );

        // Wait for consumers to connect
        Thread.sleep(500);

        // Publisher
        AtomicInteger sentCount = new AtomicInteger();
        List<Integer> sent = sendMessageCount <= 1000 ? new ArrayList<Integer>() : null;
        Session publisherSession = buildSession("Publisher", URL, optimizeAcknowledge);
        MessageProducer publisher = createPublisher(publisherSession);
        for (int i = 0; i < sendMessageCount; i++) {
            sentCount.incrementAndGet();
            if (sent != null) sent.add(i);
            if (i % 10000 == 0) System.out.println("Publisher: Send " + i);
            publisher.send(publisherSession.createTextMessage(Integer.toString(i)));
        }

        // Wait for messages to arrive
        Thread.sleep(500);

        System.out.println(name + ": Publisher Sent: " + sentCount + " " + sent);
        System.out.println(name + ": Whilst slow consumer blocked:");
        System.out.println("\t\t- SlowConsumer Received: " + slowConsumerReceiveCount + " " + slowConsumerReceived);
        System.out.println("\t\t- FastConsumer Received: " + fastConsumerReceiveCount + " " + fastConsumerReceived);

        // Unblock slow consumer
        blockSlowConsumer.countDown();

        // Wait for messages to arrive
        Thread.sleep(500);

        System.out.println(name + ": After slow consumer unblocked:");
        System.out.println("\t\t- SlowConsumer Received: " + slowConsumerReceiveCount + " " + slowConsumerReceived);
        System.out.println("\t\t- FastConsumer Received: " + fastConsumerReceiveCount + " " + fastConsumerReceived);
        System.out.println();

        publisher.close();
        publisherSession.close();
        slowConsumer.close();
        slowConsumerSession.close();
        fastConsumer.close();
        fastConsumerSession.close();
        broker.stop();

        Assert.assertEquals("Fast consumer missed messages whilst slow consumer was blocking", sent, fastConsumerReceived);
        // this is too timine dependent  as sometimes there is message eviction, would need to check the dlq
        //Assert.assertEquals("Slow consumer received incorrect message count", Math.min(sendMessageCount, prefetchLimit + (messageLimit > 0 ? messageLimit : Integer.MAX_VALUE)), slowConsumerReceived.size());
    }

    private static BrokerService createBroker(boolean persistent) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("TestBroker");
        broker.setPersistent(persistent);
        broker.addConnector(URL);
        return broker;
    }

    private static MessageConsumer createSubscriber(Session session, MessageListener messageListener) throws JMSException {
        MessageConsumer consumer = session.createConsumer(TOPIC);
        consumer.setMessageListener(messageListener);
        return consumer;
    }

    private static MessageProducer createPublisher(Session session) throws JMSException {
        MessageProducer producer = session.createProducer(TOPIC);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return producer;
    }

    private static Session buildSession(String clientId, String url, boolean optimizeAcknowledge) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);

        connectionFactory.setCopyMessageOnSend(false);
        connectionFactory.setDisableTimeStampsByDefault(true);
        connectionFactory.setOptimizeAcknowledge(optimizeAcknowledge);
        if (optimizeAcknowledge) {
            connectionFactory.setOptimizeAcknowledgeTimeOut(1);
        }

        Connection connection = connectionFactory.createConnection();
        connection.setClientID(clientId);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        connection.start();

        return session;
    }

    private static PolicyMap buildPolicy(ActiveMQTopic topic, int prefetchLimit, int messageLimit, boolean evictOldestMessage, boolean disableFlowControl) {
        PolicyMap policyMap = new PolicyMap();

        PolicyEntry policyEntry = new PolicyEntry();

        if (evictOldestMessage) {
            policyEntry.setMessageEvictionStrategy(new OldestMessageEvictionStrategy());
        }

        if (disableFlowControl) {
            policyEntry.setProducerFlowControl(false);
        }

        if (prefetchLimit > 0) {
            policyEntry.setTopicPrefetch(prefetchLimit);
        }

        if (messageLimit > 0) {
            ConstantPendingMessageLimitStrategy messageLimitStrategy = new ConstantPendingMessageLimitStrategy();
            messageLimitStrategy.setLimit(messageLimit);
            policyEntry.setPendingMessageLimitStrategy(messageLimitStrategy);
        }

        policyMap.put(topic, policyEntry);

        return policyMap;
    }
}