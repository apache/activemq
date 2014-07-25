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

import static org.junit.Assert.assertTrue;

import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQMessageTransformation;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Test;

// see https://issues.apache.org/activemq/browse/AMQ-2573
public class FailoverConsumerUnconsumedTest {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverConsumerUnconsumedTest.class);
    private static final String QUEUE_NAME = "FailoverWithUnconsumed";
    private static final String TRANSPORT_URI = "tcp://localhost:0";
    private String url;
    final int prefetch = 10;
    BrokerService broker;

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
        return createBroker(deleteAllMessagesOnStartup, TRANSPORT_URI);
    }

    public BrokerService createBroker(boolean deleteAllMessagesOnStartup, String bindAddress) throws Exception {
        broker = new BrokerService();
        broker.addConnector(bindAddress);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessagesOnStartup);

        this.url = broker.getTransportConnectors().get(0).getConnectUri().toString();

        return broker;
    }

    @Test
    public void testFailoverConsumerDups() throws Exception {
        doTestFailoverConsumerDups(true);
    }

    @Test
    public void testFailoverConsumerDupsNoAdvisoryWatch() throws Exception {
        doTestFailoverConsumerDups(false);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFailoverClientAckMissingRedelivery() throws Exception {

        final int maxConsumers = 2;
        broker = createBroker(true);

        broker.setPlugins(new BrokerPlugin[] {
                new BrokerPluginSupport() {
                    int consumerCount;

                    // broker is killed on x create consumer
                    @Override
                    public Subscription addConsumer(ConnectionContext context,
                            final ConsumerInfo info) throws Exception {
                         if (++consumerCount == maxConsumers) {
                             context.setDontSendReponse(true);
                             Executors.newSingleThreadExecutor().execute(new Runnable() {
                                 public void run() {
                                     LOG.info("Stopping broker on consumer: " + info.getConsumerId());
                                     try {
                                         broker.stop();
                                     } catch (Exception e) {
                                         e.printStackTrace();
                                     }
                                 }
                             });
                         }
                        return super.addConsumer(context, info);
                    }
                }
        });
        broker.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        cf.setWatchTopicAdvisories(false);

        final ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();

        final Session consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final Queue destination = consumerSession.createQueue(QUEUE_NAME + "?jms.consumer.prefetch=" + prefetch);

        final Vector<TestConsumer> testConsumers = new Vector<TestConsumer>();
        TestConsumer testConsumer = new TestConsumer(consumerSession, destination, connection);
        testConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    LOG.info("onMessage:" + message.getJMSMessageID());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        testConsumers.add(testConsumer);


        produceMessage(consumerSession, destination, maxConsumers * prefetch);

        assertTrue("add messages are delivered", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                int totalDelivered = 0;
                for (TestConsumer testConsumer : testConsumers) {
                    long delivered = testConsumer.deliveredSize();
                    LOG.info(testConsumer.getConsumerId() + " delivered: " + delivered);
                    totalDelivered += delivered;
                }
                return totalDelivered == maxConsumers * prefetch;
            }
        }));

        final CountDownLatch shutdownConsumerAdded = new CountDownLatch(1);

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                try {
                    LOG.info("add last consumer...");
                    TestConsumer testConsumer = new TestConsumer(consumerSession, destination, connection);
                    testConsumer.setMessageListener(new MessageListener() {
                                @Override
                                public void onMessage(Message message) {
                                    try {
                                        LOG.info("onMessage:" + message.getJMSMessageID());
                                    } catch (JMSException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                    testConsumers.add(testConsumer);
                    shutdownConsumerAdded.countDown();
                    LOG.info("done add last consumer");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // will be stopped by the plugin
        broker.waitUntilStopped();

        broker = createBroker(false, this.url);
        broker.start();

        assertTrue("consumer added through failover", shutdownConsumerAdded.await(30, TimeUnit.SECONDS));

        // each should again get prefetch messages - all unacked deliveries should be rolledback
        assertTrue("after restart all messages are re dispatched", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                int totalDelivered = 0;
                for (TestConsumer testConsumer : testConsumers) {
                    long delivered = testConsumer.deliveredSize();
                    LOG.info(testConsumer.getConsumerId() + " delivered: " + delivered);
                    totalDelivered += delivered;
                }
                return totalDelivered == maxConsumers * prefetch;
            }
        }));

        assertTrue("after restart each got prefetch amount", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                for (TestConsumer testConsumer : testConsumers) {
                    long delivered = testConsumer.deliveredSize();
                    LOG.info(testConsumer.getConsumerId() + " delivered: " + delivered);
                    if (delivered != prefetch) {
                        return false;
                    }
                }
                return true;
            }
        }));

        connection.close();
    }

    @SuppressWarnings("unchecked")
    public void doTestFailoverConsumerDups(final boolean watchTopicAdvisories) throws Exception {

        final int maxConsumers = 4;
        broker = createBroker(true);

        broker.setPlugins(new BrokerPlugin[] {
                new BrokerPluginSupport() {
                    int consumerCount;

                    // broker is killed on x create consumer
                    @Override
                    public Subscription addConsumer(ConnectionContext context,
                            final ConsumerInfo info) throws Exception {
                         if (++consumerCount == maxConsumers + (watchTopicAdvisories ? 1:0)) {
                             context.setDontSendReponse(true);
                             Executors.newSingleThreadExecutor().execute(new Runnable() {
                                 public void run() {
                                     LOG.info("Stopping broker on consumer: " + info.getConsumerId());
                                     try {
                                         broker.stop();
                                     } catch (Exception e) {
                                         e.printStackTrace();
                                     }
                                 }
                             });
                         }
                        return super.addConsumer(context, info);
                    }
                }
        });
        broker.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        cf.setWatchTopicAdvisories(watchTopicAdvisories);

        final ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();

        final Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = consumerSession.createQueue(QUEUE_NAME + "?jms.consumer.prefetch=" + prefetch);

        final Vector<TestConsumer> testConsumers = new Vector<TestConsumer>();
        for (int i=0; i<maxConsumers -1; i++) {
            testConsumers.add(new TestConsumer(consumerSession, destination, connection));
        }

        produceMessage(consumerSession, destination, maxConsumers * prefetch);

        assertTrue("add messages are dispatched", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                int totalUnconsumed = 0;
                for (TestConsumer testConsumer : testConsumers) {
                    long unconsumed = testConsumer.unconsumedSize();
                    LOG.info(testConsumer.getConsumerId() + " unconsumed: " + unconsumed);
                    totalUnconsumed += unconsumed;
                }
                return totalUnconsumed == (maxConsumers-1) * prefetch;
            }
        }));

        final CountDownLatch shutdownConsumerAdded = new CountDownLatch(1);

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                try {
                    LOG.info("add last consumer...");
                    testConsumers.add(new TestConsumer(consumerSession, destination, connection));
                    shutdownConsumerAdded.countDown();
                    LOG.info("done add last consumer");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // will be stopped by the plugin
        broker.waitUntilStopped();

        // verify interrupt
        assertTrue("add messages dispatched and unconsumed are cleaned up", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                int totalUnconsumed = 0;
                for (TestConsumer testConsumer : testConsumers) {
                    long unconsumed = testConsumer.unconsumedSize();
                    LOG.info(testConsumer.getConsumerId() + " unconsumed: " + unconsumed);
                    totalUnconsumed += unconsumed;
                }
                return totalUnconsumed == 0;
            }
        }));

        broker = createBroker(false, this.url);
        broker.start();

        assertTrue("consumer added through failover", shutdownConsumerAdded.await(30, TimeUnit.SECONDS));

        // each should again get prefetch messages - all unconsumed deliveries should be rolledback
        assertTrue("after start all messages are re dispatched", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                int totalUnconsumed = 0;
                for (TestConsumer testConsumer : testConsumers) {
                    long unconsumed = testConsumer.unconsumedSize();
                    LOG.info(testConsumer.getConsumerId() + " after restart: unconsumed: " + unconsumed);
                    totalUnconsumed += unconsumed;
                }
                return totalUnconsumed == (maxConsumers) * prefetch;
            }
        }));

        connection.close();
    }

    private void produceMessage(final Session producerSession, Queue destination, long count)
        throws JMSException {
        MessageProducer producer = producerSession.createProducer(destination);
        for (int i=0; i<count; i++) {
            TextMessage message = producerSession.createTextMessage("Test message " + i);
            producer.send(message);
        }
        producer.close();
    }

    // allow access to unconsumedMessages
    class TestConsumer extends ActiveMQMessageConsumer {

        TestConsumer(Session consumerSession, Destination destination, ActiveMQConnection connection) throws Exception {
            super((ActiveMQSession) consumerSession,
                new ConsumerId(new SessionId(connection.getConnectionInfo().getConnectionId(),1), nextGen()),
                ActiveMQMessageTransformation.transformDestination(destination), null, "",
                prefetch, -1, false, false, true, null);
        }

        public int unconsumedSize() {
            return unconsumedMessages.size();
        }

        public int deliveredSize() {
            return deliveredMessages.size();
        }
    }

    static long idGen = 100;
    private static long nextGen() {
        idGen -=5;
        return idGen;
    }
}
