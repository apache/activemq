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
package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.AsyncCallback;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TransactionRolledBackException;
import javax.transaction.xa.XAException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AMQ3166Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ3166Test.class);

    private BrokerService brokerService;
    private AtomicInteger sendAttempts = new AtomicInteger(0);


    @Test
    public void testCommitThroughAsyncErrorNoForceRollback() throws Exception {
        startBroker(false);
        Connection connection = createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(session.createQueue("QAT"));

        for (int i=0; i<10; i++) {
            producer.send(session.createTextMessage("Hello A"));
        }

        session.commit();

        assertTrue("only one message made it through", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getTotalEnqueueCount() == 1;
            }
        }));

        connection.close();
    }

    @Test
    public void testCommitThroughAsyncErrorForceRollback() throws Exception {
        startBroker(true);
        Connection connection = createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(session.createQueue("QAT"));

        try {
            for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("Hello A"));
            }
            session.commit();
            fail("Expect TransactionRolledBackException");
        } catch (JMSException expected) {
            assertTrue(expected.getCause() instanceof XAException);
        }

        assertTrue("only one message made it through", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getTotalEnqueueCount() == 0;
            }
        }));

        connection.close();
    }

    @Test
    public void testAckCommitThroughAsyncErrorForceRollback() throws Exception {
        startBroker(true);
        Connection connection = createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue destination = session.createQueue("QAT");
        MessageProducer producer = session.createProducer(destination);
        producer.send(session.createTextMessage("Hello A"));
        producer.close();
        session.commit();

        MessageConsumer messageConsumer = session.createConsumer(destination);
        assertNotNull("got message", messageConsumer.receive(4000));

        try {
            session.commit();
            fail("Expect TransactionRolledBackException");
        } catch (JMSException expected) {
            assertTrue(expected.getCause() instanceof XAException);
            assertTrue(expected.getCause().getCause() instanceof TransactionRolledBackException);
            assertTrue(expected.getCause().getCause().getCause() instanceof RuntimeException);
        }

        assertTrue("one message still there!", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getTotalMessageCount() == 1;
            }
        }));

        connection.close();
    }


    @Test
    public void testErrorOnSyncSend() throws Exception {
        startBroker(false);
        ActiveMQConnection connection = (ActiveMQConnection) createConnection();
        connection.setAlwaysSyncSend(true);
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(session.createQueue("QAT"));

        try {
            for (int i = 0; i < 10; i++) {
                producer.send(session.createTextMessage("Hello A"));
            }
            session.commit();
        } catch (JMSException expectedSendFail) {
            LOG.info("Got expected: " + expectedSendFail);
            session.rollback();
        }

        assertTrue("only one message made it through", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getTotalEnqueueCount() == 0;
            }
        }));

        connection.close();
    }


    @Test
    public void testRollbackOnAsyncErrorAmqApi() throws Exception {
        startBroker(false);
        ActiveMQConnection connection = (ActiveMQConnection) createConnection();
        connection.start();
        final ActiveMQSession session = (ActiveMQSession) connection.createSession(true, Session.SESSION_TRANSACTED);
        int batchSize = 10;
        final CountDownLatch batchSent = new CountDownLatch(batchSize);
        ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(session.createQueue("QAT"));

        for (int i=0; i<batchSize; i++) {
            producer.send(session.createTextMessage("Hello A"), new AsyncCallback() {
                @Override
                public void onSuccess() {
                    batchSent.countDown();
                }

                @Override
                public void onException(JMSException e) {
                    session.getTransactionContext().setRollbackOnly(true);
                    batchSent.countDown();
                }
            });

            if (i==0) {
                // transaction context begun on first send
                session.getTransactionContext().addSynchronization(new Synchronization() {
                    @Override
                    public void beforeEnd() throws Exception {
                        // await response to all sends in the batch
                        if (!batchSent.await(10, TimeUnit.SECONDS)) {
                            LOG.error("TimedOut waiting for aync send requests!");
                            session.getTransactionContext().setRollbackOnly(true);
                        };
                        super.beforeEnd();
                    }
                });
            }
        }

        try {
            session.commit();
            fail("expect rollback on async error");
        } catch (TransactionRolledBackException expected) {
        }

        assertTrue("only one message made it through", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getTotalEnqueueCount() == 0;
            }
        }));

        connection.close();
    }


    private Connection createConnection() throws Exception {
        String connectionURI = brokerService.getTransportConnectors().get(0).getPublishableConnectString();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectionURI);
        cf.setWatchTopicAdvisories(false);
        return cf.createConnection();
    }

    public void startBroker(boolean forceRollbackOnAsyncSendException) throws Exception {
        brokerService = createBroker(forceRollbackOnAsyncSendException);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }

    protected BrokerService createBroker(boolean forceRollbackOnAsyncSendException) throws Exception {
        BrokerService answer = new BrokerService();

        answer.setPersistent(true);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.setAdvisorySupport(false);
        answer.setRollbackOnlyOnAsyncException(forceRollbackOnAsyncSendException);

        answer.addConnector("tcp://0.0.0.0:0");

        answer.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
                        if (ack.isStandardAck()) {
                            throw new RuntimeException("no way, won't allow any standard ack");
                        }
                        super.acknowledge(consumerExchange, ack);
                    }

                    @Override
                    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
                        if (sendAttempts.incrementAndGet() > 1) {
                            throw new RuntimeException("no way, won't accept any messages");
                        }
                        super.send(producerExchange, messageSend);
                    }
                }
        });

        return answer;
    }

}
