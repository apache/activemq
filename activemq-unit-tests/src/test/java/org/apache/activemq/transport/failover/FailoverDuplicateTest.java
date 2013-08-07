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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverDuplicateTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverDuplicateTest.class);
    private static final String QUEUE_NAME = "TestQueue";
    private static final String TRANSPORT_URI = "tcp://localhost:0";
    private String url;
    BrokerService broker;

    @Override
    public void tearDown() throws Exception {
        stopBroker();
    }

    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    public void startBroker(boolean deleteAllMessagesOnStartup) throws Exception {
        broker = createBroker(deleteAllMessagesOnStartup);
        broker.start();
    }

    public void startBroker(boolean deleteAllMessagesOnStartup, String bindAddress) throws Exception {
        broker = createBroker(deleteAllMessagesOnStartup, bindAddress);
        broker.start();
    }

    public BrokerService createBroker(boolean deleteAllMessagesOnStartup) throws Exception {
        return createBroker(deleteAllMessagesOnStartup, TRANSPORT_URI);
    }

    public BrokerService createBroker(boolean deleteAllMessagesOnStartup, String bindAddress) throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.addConnector(bindAddress);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessagesOnStartup);

        url = broker.getTransportConnectors().get(0).getConnectUri().toString();

        return broker;
    }

    public void configureConnectionFactory(ActiveMQConnectionFactory factory) {
        factory.setAuditMaximumProducerNumber(2048);
        factory.setOptimizeAcknowledge(true);
    }

    @SuppressWarnings("unchecked")
    public void testFailoverSendReplyLost() throws Exception {

        broker = createBroker(true);
        setDefaultPersistenceAdapter(broker);

        final CountDownLatch gotMessageLatch = new CountDownLatch(1);
        final CountDownLatch producersDone = new CountDownLatch(1);
        final AtomicBoolean first = new AtomicBoolean(false);
        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public void send(final ProducerBrokerExchange producerExchange,
                                     org.apache.activemq.command.Message messageSend)
                            throws Exception {
                        // so send will hang as if reply is lost
                        super.send(producerExchange, messageSend);
                        if (first.compareAndSet(false, true)) {
                            producerExchange.getConnectionContext().setDontSendReponse(true);
                            Executors.newSingleThreadExecutor().execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        LOG.info("Waiting for recepit");
                                        assertTrue("message received on time", gotMessageLatch.await(60, TimeUnit.SECONDS));
                                        assertTrue("new producers done on time", producersDone.await(120, TimeUnit.SECONDS));
                                        LOG.info("Stopping connection post send and receive and multiple producers");
                                        producerExchange.getConnectionContext().getConnection().stop();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }
                    }
                }
        });
        broker.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?jms.watchTopicAdvisories=false");
        configureConnectionFactory(cf);
        Connection sendConnection = cf.createConnection();
        sendConnection.start();

        final Session sendSession = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = sendSession.createQueue(QUEUE_NAME);


        final AtomicInteger receivedCount = new AtomicInteger();
        MessageListener listener = new MessageListener() {
            @Override
            public void onMessage(Message message) {
                gotMessageLatch.countDown();
                receivedCount.incrementAndGet();
            }
        };
        Connection receiveConnection;
        Session receiveSession = null;
        receiveConnection = cf.createConnection();
        receiveConnection.start();
        receiveSession = receiveConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        receiveSession.createConsumer(destination).setMessageListener(listener);

        final CountDownLatch sendDoneLatch = new CountDownLatch(1);
        // broker will die on send reply so this will hang till restart
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                LOG.info("doing async send...");
                try {
                    produceMessage(sendSession, destination, "will resend", 1);
                } catch (JMSException e) {
                    LOG.error("got send exception: ", e);
                    fail("got unexpected send exception" + e);
                }
                sendDoneLatch.countDown();
                LOG.info("done async send");
            }
        });


        assertTrue("one message got through on time", gotMessageLatch.await(20, TimeUnit.SECONDS));
        // send more messages, blow producer audit
        final int numProducers = 1050;
        final int numPerProducer = 2;
        final int totalSent = numPerProducer * numProducers + 1;
        for (int i=0; i<numProducers; i++) {
            produceMessage(receiveSession, destination, "new producer " + i, numPerProducer);
            // release resend when we half done, cursor audit exhausted
            // and concurrent dispatch with the resend
            if (i == 1025) {
                LOG.info("count down producers done");
                producersDone.countDown();
            }
        }

        assertTrue("message sent complete through failover", sendDoneLatch.await(30, TimeUnit.SECONDS));

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("received count:" + receivedCount.get());
                return totalSent <= receivedCount.get();
            }
        });
        assertEquals("we got all produced messages", totalSent, receivedCount.get());
        sendConnection.close();
        receiveConnection.close();

        // verify stats
        assertEquals("expect all messages are dequeued with one duplicate to dlq", totalSent + 2, ((RegionBroker) broker.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("dequeues : "   + ((RegionBroker) broker.getRegionBroker()).getDestinationStatistics().getDequeues().getCount());
                return  totalSent + 1 <= ((RegionBroker) broker.getRegionBroker()).getDestinationStatistics().getDequeues().getCount();
            }
        });
        assertEquals("dequeue correct, including duplicate dispatch poisoned", totalSent  + 1, ((RegionBroker) broker.getRegionBroker()).getDestinationStatistics().getDequeues().getCount());

        // ensure no dangling messages with fresh broker etc
        broker.stop();
        broker.waitUntilStopped();

        LOG.info("Checking for remaining/hung messages with second restart..");
        broker = createBroker(false, url);
        setDefaultPersistenceAdapter(broker);
        broker.start();

        // after restart, ensure no dangling messages
        cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        configureConnectionFactory(cf);
        sendConnection = cf.createConnection();
        sendConnection.start();
        Session session2 = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer  = session2.createConsumer(destination);
        Message msg = consumer.receive(1000);
        if (msg == null) {
            msg = consumer.receive(5000);
        }
        assertNull("no messges left dangling but got: " + msg, msg);

        sendConnection.close();
    }


    private void produceMessage(final Session producerSession, Queue destination, final String text, final int count)
            throws JMSException {
        MessageProducer producer = producerSession.createProducer(destination);
        for (int i=0; i<count; i++) {
            TextMessage message = producerSession.createTextMessage(text + ", count:" + i);
            producer.send(message);
        }
        producer.close();
    }
}
