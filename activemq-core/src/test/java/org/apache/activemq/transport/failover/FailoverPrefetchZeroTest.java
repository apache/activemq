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
package org.apache.activemq.transport.failover;

import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Test;


import static org.junit.Assert.assertTrue;

// see: https://issues.apache.org/activemq/browse/AMQ-2877
public class FailoverPrefetchZeroTest {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverPrefetchZeroTest.class);
    private static final String QUEUE_NAME = "FailoverPrefetchZero";
    private String url = "tcp://localhost:61616";
    final int prefetch = 0;
    BrokerService broker;

    public void startCleanBroker() throws Exception {
        startBroker(true);
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    public void startBroker(boolean deleteAllMessagesOnStartup) throws Exception {
        broker = createBroker(deleteAllMessagesOnStartup);
        broker.start();
    }

    public BrokerService createBroker(boolean deleteAllMessagesOnStartup) throws Exception {
        broker = new BrokerService();
        broker.addConnector(url);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessagesOnStartup);
        return broker;
    }

    @Test
    public void testPrefetchZeroConsumerThroughRestart() throws Exception {
        broker = createBroker(true);

        final CountDownLatch pullDone = new CountDownLatch(1);
        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public Response messagePull(ConnectionContext context, final MessagePull pull) throws Exception {
                        context.setDontSendReponse(true);
                        pullDone.countDown();
                        Executors.newSingleThreadExecutor().execute(new Runnable() {
                            public void run() {
                                LOG.info("Stopping broker on pull: " + pull);
                                try {
                                    broker.stop();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                        return null;
                    }
                }
        });
        broker.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        cf.setWatchTopicAdvisories(false);

        final ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();

        final Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = consumerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=" + prefetch);

        final MessageConsumer consumer = consumerSession.createConsumer(destination);
        produceMessage(consumerSession, destination, 1);

        final CountDownLatch receiveDone = new CountDownLatch(1);
        final Vector<Message> received = new Vector<Message>();
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                try {
                    LOG.info("receive one...");
                    Message msg = consumer.receive(30000);
                    if (msg != null) {
                        received.add(msg);
                    }
                    receiveDone.countDown();
                    LOG.info("done receive");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // will be stopped by the plugin
        assertTrue("pull completed on broker", pullDone.await(30, TimeUnit.SECONDS));
        broker.waitUntilStopped();
        broker = createBroker(false);
        broker.start();

        assertTrue("receive completed through failover", receiveDone.await(30, TimeUnit.SECONDS));

        assertTrue("we got our message:", !received.isEmpty());

        connection.close();
    }

    private void produceMessage(final Session producerSession, Queue destination, long count)
            throws JMSException {
        MessageProducer producer = producerSession.createProducer(destination);
        for (int i = 0; i < count; i++) {
            TextMessage message = producerSession.createTextMessage("Test message " + i);
            producer.send(message);
        }
        producer.close();
    }
}
