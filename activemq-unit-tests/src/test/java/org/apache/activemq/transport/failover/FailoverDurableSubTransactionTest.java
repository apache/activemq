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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.*;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.*;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.*;
import javax.jms.Message;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class FailoverDurableSubTransactionTest {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverDurableSubTransactionTest.class);
    private static final String TOPIC_NAME = "Failover.WithTx";
    private static final String TRANSPORT_URI = "tcp://localhost:0";
    private String url;
    BrokerService broker;

    public enum FailType {
        ON_DISPATCH,
        ON_ACK,
        ON_COMMIT,
        ON_DISPACH_WITH_REPLAY_DELAY
    }

    @Parameterized.Parameter(0)
    public FailType failType;

    @Parameterized.Parameters(name ="failType=#{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {FailType.ON_DISPATCH},
                {FailType.ON_DISPACH_WITH_REPLAY_DELAY},
                {FailType.ON_ACK},
                {FailType.ON_COMMIT}
        });
    }

    @After
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

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);

        // faster redispatch
        broker.setKeepDurableSubsActive(true);

        url = broker.getTransportConnectors().get(0).getConnectUri().toString();

        return broker;
    }

    public void configureConnectionFactory(ActiveMQConnectionFactory factory) {
        factory.setWatchTopicAdvisories(false);
        factory.getRedeliveryPolicy().setMaximumRedeliveries(-1);

        if (!FailType.ON_DISPACH_WITH_REPLAY_DELAY.equals(failType)) {
            factory.getRedeliveryPolicy().setInitialRedeliveryDelay(0l);
            factory.getRedeliveryPolicy().setRedeliveryDelay(0l);
        }
    }


    @org.junit.Test
    public void testFailoverCommit() throws Exception {

        final AtomicInteger dispatchedCount = new AtomicInteger(0);
        final int errorAt = FailType.ON_COMMIT.equals(failType) ? 1 : 9;
        final int messageCount = 10;
        broker = createBroker(true);

        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
                        if (FailType.ON_COMMIT.equals(failType) && dispatchedCount.incrementAndGet() == errorAt) {
                            for (TransportConnection transportConnection : broker.getTransportConnectors().get(0).getConnections()) {
                                LOG.error("Whacking connection on commit: " + transportConnection);
                                transportConnection.serviceException(new IOException("ERROR NOW"));
                            }
                        } else {
                            super.commitTransaction(context, xid, onePhase);
                        }
                    }

                    @Override
                    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
                        if (FailType.ON_ACK.equals(failType) && ack.getAckType() == MessageAck.DELIVERED_ACK_TYPE && dispatchedCount.incrementAndGet() == errorAt) {
                            for (TransportConnection transportConnection : broker.getTransportConnectors().get(0).getConnections()) {
                                LOG.error("Whacking connection on ack: " + transportConnection);
                                transportConnection.serviceException(new IOException("ERROR NOW"));
                            }
                        }
                        super.acknowledge(consumerExchange, ack);
                    }

                    @Override
                    public void postProcessDispatch(MessageDispatch messageDispatch) {
                        super.postProcessDispatch(messageDispatch);
                        if ((FailType.ON_DISPATCH.equals(failType) || FailType.ON_DISPACH_WITH_REPLAY_DELAY.equals(failType)) && dispatchedCount.incrementAndGet() == errorAt) {
                            for (TransportConnection transportConnection : broker.getTransportConnectors().get(0).getConnections()) {
                                LOG.error("Whacking connection on dispatch: " + transportConnection);
                                transportConnection.serviceException(new IOException("ERROR NOW"));
                            }
                        }
                    }
                }
        });
        broker.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        cf.setAlwaysSyncSend(true);
        cf.setAlwaysSessionAsync(false);
        cf.getPrefetchPolicy().setDurableTopicPrefetch(FailType.ON_ACK.equals(failType) ? 2 : 100);
        configureConnectionFactory(cf);
        Connection connection = cf.createConnection();
        connection.setClientID("CID");
        connection.start();
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Topic destination = session.createTopic(TOPIC_NAME);

        MessageConsumer consumer = session.createDurableSubscriber(destination, "DS");
        consumer.close();

        produceMessage(destination, messageCount);
        LOG.info("Production done! " + broker.getDestination(ActiveMQDestination.transform(destination)));


        consumer = session.createDurableSubscriber(destination, "DS");

        AtomicBoolean success = new AtomicBoolean(false);

        HashSet<Integer> dupCheck = new HashSet<Integer>();
        while (!success.get()) {
            dupCheck.clear();
            int i = 0;
            for (i = 0; i < messageCount; i++) {
                Message msg = consumer.receive(5000);
                if (msg == null) {
                    LOG.info("Failed to receive on: " + i);
                    break;
                }
                LOG.info("Received: @" + i + ":" + msg.getJMSMessageID() + ", ID:" + msg.getIntProperty("ID"));
                assertTrue("single instance of: " +  i, dupCheck.add( msg.getIntProperty("ID")));
            }

            try {
                if (i == messageCount) {
                    session.commit();
                    success.set(true);
                } else {
                    session.rollback();
                }
            } catch (TransactionRolledBackException expected) {
                LOG.info("Got expected", expected);
                session.rollback();
            }
        }

        consumer.close();
        connection.close();

        org.apache.activemq.broker.region.Destination dlq = broker.getDestination(ActiveMQDestination.transform(new ActiveMQQueue(DEFAULT_DEAD_LETTER_QUEUE_NAME)));
        LOG.info("DLQ: " + dlq);
        assertEquals("DLQ empty ", 0, dlq.getDestinationStatistics().getMessages().getCount());

    }

    @org.junit.Test
    public void testFailoverCommitListener() throws Exception {

        final AtomicInteger dispatchedCount = new AtomicInteger(0);
        final int errorAt = FailType.ON_ACK.equals(failType) ? 1 : 1;
        final int messageCount = 10;
        broker = createBroker(true);

        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
                        LOG.info("commit request: " + xid);
                        if (FailType.ON_COMMIT.equals(failType) && dispatchedCount.incrementAndGet() == errorAt) {
                            for (TransportConnection transportConnection : broker.getTransportConnectors().get(0).getConnections()) {
                                LOG.error("Whacking connection on commit: " + transportConnection);
                                transportConnection.serviceException(new IOException("ERROR NOW"));
                            }
                        } else {
                            super.commitTransaction(context, xid, onePhase);
                        }
                    }


                    @Override
                    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
                        LOG.info("ack request: " + ack);
                        if (FailType.ON_ACK.equals(failType) /*&& ack.getAckType() == MessageAck.DELIVERED_ACK_TYPE*/ && dispatchedCount.incrementAndGet() == errorAt) {
                            for (TransportConnection transportConnection : broker.getTransportConnectors().get(0).getConnections()) {
                                LOG.error("Whacking connection on ack: " + transportConnection);
                                transportConnection.serviceException(new IOException("ERROR NOW"));
                            }
                        } else {
                            super.acknowledge(consumerExchange, ack);
                        }
                    }
                }

        });
        broker.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        cf.setAlwaysSyncSend(true);
        cf.setAlwaysSessionAsync(true);
        //cf.getPrefetchPolicy().setDurableTopicPrefetch(FailType.ON_ACK.equals(failType) ? 2 : 100);
        cf.setWatchTopicAdvisories(false);
        Connection connection = cf.createConnection();
        connection.setClientID("CID");
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Topic destination = session.createTopic(TOPIC_NAME);

        MessageConsumer consumer = session.createDurableSubscriber(destination, "DS");
        consumer.close();
        connection.close();

        produceMessage(destination, messageCount*2);
        LOG.info("Production done! " + broker.getDestination(ActiveMQDestination.transform(destination)));


        connection = cf.createConnection();
        connection.setClientID("CID");
        connection.start();
        final Session receiveSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        consumer = receiveSession.createDurableSubscriber(destination, "DS");

        final AtomicBoolean success = new AtomicBoolean(false);
        final HashSet<Integer> dupCheck = new HashSet<Integer>();
        final AtomicInteger receivedCount = new AtomicInteger();
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message msg) {
                    try {
                        int i = receivedCount.getAndIncrement();
                        LOG.info("Received: @" + i + ":" + msg.getJMSMessageID() + ", ID:" + msg.getIntProperty("ID"));
                        assertTrue("single instance of: " +  i, dupCheck.add( msg.getIntProperty("ID")));

                        if (receivedCount.get() == messageCount) {
                            receiveSession.commit();
                            success.set(true);
                        }
                    } catch (TransactionRolledBackException expected) {
                        LOG.info("Got expected", expected);
                        try {
                            receiveSession.rollback();
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                        dupCheck.clear();
                        receivedCount.set(0);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
            }
        });
        connection.start();

        try {

            assertTrue("committed ok", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return success.get();
                }
            }));
        } finally {
            consumer.close();
            connection.close();
        }

        org.apache.activemq.broker.region.Destination dlq = broker.getDestination(ActiveMQDestination.transform(new ActiveMQQueue(DEFAULT_DEAD_LETTER_QUEUE_NAME)));
        LOG.info("DLQ: " + dlq);
        assertEquals("DLQ empty ", 0, dlq.getDestinationStatistics().getMessages().getCount());

    }

    private void produceMessage(Topic destination, int count)
            throws JMSException {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url);
        configureConnectionFactory(cf);
        Connection connection = cf.createConnection();
        connection.start();
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        TextMessage message = producerSession.createTextMessage("Test message");
        for (int i=0; i<count; i++) {
            message.setIntProperty("ID", i);
            producer.send(message);
        }
        connection.close();
    }

}
