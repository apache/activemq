/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.bugs;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class AMQ4413Test {

    static final Logger LOG = LoggerFactory.getLogger(AMQ4413Test.class);

    final String brokerUrl = "tcp://localhost:0";
    private String connectionUri;
    final int numMsgsTriggeringReconnection = 2;
    final int numMsgs = 30;
    final int numTests = 75;
    final ExecutorService threadPool = Executors.newCachedThreadPool();

    @Test
    public void testDurableSubMessageLoss() throws Exception{
        // start embedded broker
        BrokerService brokerService = new BrokerService();
        connectionUri = brokerService.addConnector(brokerUrl).getPublishableConnectString();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setKeepDurableSubsActive(true);
        brokerService.setAdvisorySupport(false);
        brokerService.start();
        LOG.info("##### broker started");

        // repeat test 50 times
        try {
            for (int i = 0; i < numTests; ++i) {
                LOG.info("##### test " + i + " started");
                test();
            }

            LOG.info("##### tests are done");
        } catch (Exception e) {
            e.printStackTrace();
            LOG.info("##### tests failed!");
        } finally {
            threadPool.shutdown();
            brokerService.stop();
            LOG.info("##### broker stopped");
        }
    }

    private void test() throws Exception {

        final String topicName = "topic-" + UUID.randomUUID();
        final String clientId = "client-" + UUID.randomUUID();
        final String subName = "sub-" + UUID.randomUUID();

        // create (and only create) subscription first
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setWatchTopicAdvisories(false);
        Connection connection = factory.createConnection();
        connection.setClientID(clientId);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(topicName);
        TopicSubscriber durableSubscriptionCreator = session.createDurableSubscriber(topic, subName);

        connection.stop();
        durableSubscriptionCreator.close();
        session.close();
        connection.close();

        // publisher task
        Callable<Boolean> publisher = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Connection connection = null;

                try {
                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
                    factory.setWatchTopicAdvisories(false);
                    connection = factory.createConnection();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Topic topic = session.createTopic(topicName);

                    MessageProducer producer = session.createProducer(topic);
                    producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                    producer.setPriority(Message.DEFAULT_PRIORITY);
                    producer.setTimeToLive(Message.DEFAULT_TIME_TO_LIVE);

                    for (int seq = 1; seq <= numMsgs; ++seq) {
                        TextMessage msg = session.createTextMessage(String.valueOf(seq));
                        producer.send(msg);
                        LOG.info("pub sent msg: " + seq);
                        Thread.sleep(1L);
                    }

                    LOG.info("pub is done");
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                }
                return Boolean.TRUE;
            }
        };

        // subscriber task
        Callable<Boolean> durableSubscriber = new Callable<Boolean>() {
            ActiveMQConnectionFactory factory;
            Connection connection;
            Session session;
            Topic topic;
            TopicSubscriber consumer;

            @Override
            public Boolean call() throws Exception {
                factory = new ActiveMQConnectionFactory(connectionUri);
                factory.setWatchTopicAdvisories(false);

                try {
                    connect();

                    for (int seqExpected = 1; seqExpected <= numMsgs; ++seqExpected) {
                        TextMessage msg = (TextMessage) consumer.receive(3000L);
                        if (msg == null) {
                            LOG.info("expected: " + seqExpected + ", actual: timed out", msg);
                            return Boolean.FALSE;
                        }

                        int seq = Integer.parseInt(msg.getText());

                        LOG.info("sub received msg: " + seq);

                        if (seqExpected != seq) {
                            LOG.info("expected: " + seqExpected + ", actual: " + seq);
                            return Boolean.FALSE;
                        }

                        if (seq % numMsgsTriggeringReconnection == 0) {
                            close(false);
                            connect();

                            LOG.info("sub reconnected");
                        }
                    }

                    LOG.info("sub is done");
                } finally {
                    try {
                        close(true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                return Boolean.TRUE;
            }

            void connect() throws Exception {
                connection = factory.createConnection();
                connection.setClientID(clientId);
                connection.start();

                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                topic = session.createTopic(topicName);
                consumer = session.createDurableSubscriber(topic, subName);
            }

            void close(boolean unsubscribe) throws Exception {
                if (connection != null) {
                    connection.stop();
                }

                if (consumer != null) {
                    consumer.close();
                }

                if (session != null) {
                    if (unsubscribe) {
                        session.unsubscribe(subName);
                    }
                    session.close();
                }

                if (connection != null) {
                    connection.close();
                }
            }
        };

        ArrayList<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
        results.add(threadPool.submit(publisher));
        results.add(threadPool.submit(durableSubscriber));

        for (Future<Boolean> result : results) {
            assertTrue(result.get());
        }
    }
}
