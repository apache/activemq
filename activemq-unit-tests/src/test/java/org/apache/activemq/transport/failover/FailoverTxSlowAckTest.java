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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;

public class FailoverTxSlowAckTest {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverTxSlowAckTest.class);
    private static final String QUEUE_IN = "IN";
    private static final String QUEUE_OUT = "OUT";

    private static final String MESSAGE_TEXT = "Test message ";
    private static final String TRANSPORT_URI = "tcp://localhost:0";
    private String url;
    final int prefetch = 1;
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
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setOptimizedDispatch(true);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);

        return broker;
    }

    @Test
    public void testFailoverDuringAckRollsback() throws Exception {
        broker = createBroker(true);

        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        broker.setPlugins(new BrokerPlugin[] {
                new BrokerPluginSupport() {
                    int sendCount = 0;
                    @Override
                    public void send(ProducerBrokerExchange producerExchange, org.apache.activemq.command.Message messageSend) throws Exception {
                        super.send(producerExchange, messageSend);
                        sendCount++;
                        if (sendCount > 1) {
                            // need new thread b/c we have the service write lock
                            executorService.execute(new Runnable() {
                                @Override
                                public void run() {
                                    LOG.info("Stopping broker before commit...");
                                    try {
                                        broker.stop();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }
                    }}});

        broker.start();
        url = broker.getTransportConnectors().get(0).getConnectUri().toString();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        cf.setWatchTopicAdvisories(false);
        cf.setDispatchAsync(false);

        final ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();

        final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue in = producerSession.createQueue(QUEUE_IN + "?consumer.prefetchSize=" + prefetch);

        final Session consumerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        final Queue out = consumerSession.createQueue(QUEUE_OUT);
        final MessageProducer consumerProducer = consumerSession.createProducer(out);


        final CountDownLatch commitDoneLatch = new CountDownLatch(1);
        final CountDownLatch messagesReceived = new CountDownLatch(1);
        final CountDownLatch brokerDisconnectedLatch = new CountDownLatch(1);
        final AtomicInteger receivedCount = new AtomicInteger();

        final AtomicBoolean gotDisconnect = new AtomicBoolean();
        final AtomicBoolean gotReconnected = new AtomicBoolean();

        final MessageConsumer testConsumer = consumerSession.createConsumer(in);
        testConsumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                LOG.info("consume one and commit");

                assertNotNull("got message", message);
                receivedCount.incrementAndGet();
                messagesReceived.countDown();


                try {
                    // ensure message expires broker side so it won't get redelivered
                    TimeUnit.SECONDS.sleep(1);

                    consumerProducer.send(message);

                    // hack to block the transaction completion
                    // ensure session does not get to send commit message before failover reconnect
                    // if the commit message is in progress during failover we get rollback via the state
                    // tracker
                    ((ActiveMQSession) consumerSession).getTransactionContext().addSynchronization(new Synchronization() {

                        @Override
                        public void beforeEnd() throws Exception {

                            LOG.info("waiting for failover reconnect");

                            gotDisconnect.set(Wait.waitFor(new Wait.Condition() {
                                @Override
                                public boolean isSatisified() throws Exception {
                                    return !((ActiveMQSession) consumerSession).getConnection().getTransport().isConnected();
                                }
                            }));

                            //connect down to trigger reconnect
                            brokerDisconnectedLatch.countDown();

                            LOG.info("got disconnect");
                            gotReconnected.set(Wait.waitFor(new Wait.Condition() {
                                @Override
                                public boolean isSatisified() throws Exception {
                                    return ((ActiveMQSession) consumerSession).getConnection().getTransport().isConnected();
                                }
                            }));

                            LOG.info("got failover reconnect");

                        }
                    });


                    consumerSession.commit();
                    LOG.info("done commit");

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    commitDoneLatch.countDown();
                }
            }
        });

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                LOG.info("producer started");
                try {
                    produceMessage(producerSession, in, 1);
                } catch (javax.jms.IllegalStateException SessionClosedExpectedOnShutdown) {
                } catch (JMSException e) {
                    e.printStackTrace();
                    fail("unexpceted ex on producer: " + e);
                }
                LOG.info("producer done");
            }
        });

        // will be stopped by the plugin on TX ack
        broker.waitUntilStopped();
        //await for listener to detect disconnect
        brokerDisconnectedLatch.await();
        broker = createBroker(false, url);
        broker.start();

        assertTrue("message was recieved ", messagesReceived.await(20, TimeUnit.SECONDS));
        assertTrue("tx complete through failover", commitDoneLatch.await(40, TimeUnit.SECONDS));
        assertEquals("one delivery", 1, receivedCount.get());

        assertTrue("got disconnect/reconnect", gotDisconnect.get());
        assertTrue("got reconnect", gotReconnected.get());

        assertNull("No message produced", receiveMessage(cf, out));
    }


    private Message receiveMessage(ActiveMQConnectionFactory cf,
            Queue destination) throws Exception {
        final ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();
        final Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        final MessageConsumer consumer = consumerSession.createConsumer(destination);
        Message msg = consumer.receive(4000);
        consumerSession.commit();
        connection.close();
        return msg;
    }

    private void produceMessage(final Session producerSession, Queue destination, long count)
        throws JMSException {
        MessageProducer producer = producerSession.createProducer(destination);
        for (int i=0; i<count; i++) {
            TextMessage message = producerSession.createTextMessage(MESSAGE_TEXT + i);
            // have it expire so it will only be delivered once
            producer.send(message, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 500);
        }
        producer.close();
    }
}
