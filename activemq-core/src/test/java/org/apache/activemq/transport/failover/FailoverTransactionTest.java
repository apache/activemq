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

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.SocketProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TransactionRolledBackException;
import java.net.URI;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// see https://issues.apache.org/activemq/browse/AMQ-2473

// https://issues.apache.org/activemq/browse/AMQ-2590
public class FailoverTransactionTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverTransactionTest.class);
    private static final String QUEUE_NAME = "FailoverWithTx";
    private static final String TRANSPORT_URI = "tcp://localhost:0";
    private String url;
    BrokerService broker;

    public static Test suite() {
        return suite(FailoverTransactionTest.class);
    }

    public void setUp() throws Exception {
        super.setMaxTestTime(20 * 60 * 1000); // some boxes can be real slow
        super.setAutoFail(true);
        super.setUp();
    }

    public void tearDown() throws Exception {
        stopBroker();
    }

    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    private void startCleanBroker() throws Exception {
        startBroker(true);
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

    public void testFailoverProducerCloseBeforeTransaction() throws Exception {
        startCleanBroker();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
        produceMessage(session, destination);

        // restart to force failover and connection state recovery before the commit
        broker.stop();
        startBroker(false, url);

        session.commit();
        assertNotNull("we got the message", consumer.receive(20000));
        session.commit();
        connection.close();
    }

    public void initCombosForTestFailoverCommitReplyLost() {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.AMQ, PersistenceAdapterChoice.JDBC});
    }

    public void testFailoverCommitReplyLost() throws Exception {

        broker = createBroker(true);
        setDefaultPersistenceAdapter(broker);

        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public void commitTransaction(ConnectionContext context,
                                                  TransactionId xid, boolean onePhase) throws Exception {
                        super.commitTransaction(context, xid, onePhase);
                        // so commit will hang as if reply is lost
                        context.setDontSendReponse(true);
                        Executors.newSingleThreadExecutor().execute(new Runnable() {
                            public void run() {
                                LOG.info("Stopping broker post commit...");
                                try {
                                    broker.stop();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    }
                }
        });
        broker.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        Connection connection = cf.createConnection();
        connection.start();
        final Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
        produceMessage(session, destination);

        final CountDownLatch commitDoneLatch = new CountDownLatch(1);
        // broker will die on commit reply so this will hang till restart
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                LOG.info("doing async commit...");
                try {
                    session.commit();
                } catch (JMSException e) {
                    assertTrue(e instanceof TransactionRolledBackException);
                    LOG.info("got commit exception: ", e);
                }
                commitDoneLatch.countDown();
                LOG.info("done async commit");
            }
        });

        // will be stopped by the plugin
        broker.waitUntilStopped();
        broker = createBroker(false, url);
        setDefaultPersistenceAdapter(broker);
        broker.start();

        assertTrue("tx committed trough failover", commitDoneLatch.await(30, TimeUnit.SECONDS));

        // new transaction
        Message msg = consumer.receive(20000);
        LOG.info("Received: " + msg);
        assertNotNull("we got the message", msg);
        assertNull("we got just one message", consumer.receive(2000));
        session.commit();
        consumer.close();
        connection.close();

        // ensure no dangling messages with fresh broker etc
        broker.stop();
        broker.waitUntilStopped();

        LOG.info("Checking for remaining/hung messages..");
        broker = createBroker(false, url);
        setDefaultPersistenceAdapter(broker);
        broker.start();

        // after restart, ensure no dangling messages
        cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        connection = cf.createConnection();
        connection.start();
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session2.createConsumer(destination);
        msg = consumer.receive(1000);
        if (msg == null) {
            msg = consumer.receive(5000);
        }
        LOG.info("Received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }


    public void initCombosForTestFailoverSendReplyLost() {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB,
                        PersistenceAdapterChoice.JDBC
                        // not implemented for AMQ store
                });
    }

    public void testFailoverSendReplyLost() throws Exception {

        broker = createBroker(true);
        setDefaultPersistenceAdapter(broker);

        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public void send(ProducerBrokerExchange producerExchange,
                                     org.apache.activemq.command.Message messageSend)
                            throws Exception {
                        // so send will hang as if reply is lost
                        super.send(producerExchange, messageSend);
                        producerExchange.getConnectionContext().setDontSendReponse(true);
                        Executors.newSingleThreadExecutor().execute(new Runnable() {
                            public void run() {
                                LOG.info("Stopping broker post send...");
                                try {
                                    broker.stop();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    }
                }
        });
        broker.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?jms.watchTopicAdvisories=false");
        Connection connection = cf.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
        final CountDownLatch sendDoneLatch = new CountDownLatch(1);
        // broker will die on send reply so this will hang till restart
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                LOG.info("doing async send...");
                try {
                    produceMessage(session, destination);
                } catch (JMSException e) {
                    //assertTrue(e instanceof TransactionRolledBackException);
                    LOG.error("got send exception: ", e);
                    fail("got unexpected send exception" + e);
                }
                sendDoneLatch.countDown();
                LOG.info("done async send");
            }
        });

        // will be stopped by the plugin
        broker.waitUntilStopped();
        broker = createBroker(false, url);
        setDefaultPersistenceAdapter(broker);
        LOG.info("restarting....");
        broker.start();

        assertTrue("message sent through failover", sendDoneLatch.await(30, TimeUnit.SECONDS));

        // new transaction
        Message msg = consumer.receive(20000);
        LOG.info("Received: " + msg);
        assertNotNull("we got the message", msg);
        assertNull("we got just one message", consumer.receive(2000));
        consumer.close();
        connection.close();

        // verify stats
        assertEquals("no newly queued messages", 0, ((RegionBroker) broker.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());
        assertEquals("1 dequeue", 1, ((RegionBroker) broker.getRegionBroker()).getDestinationStatistics().getDequeues().getCount());

        // ensure no dangling messages with fresh broker etc
        broker.stop();
        broker.waitUntilStopped();

        LOG.info("Checking for remaining/hung messages with second restart..");
        broker = createBroker(false, url);
        setDefaultPersistenceAdapter(broker);
        broker.start();

        // after restart, ensure no dangling messages
        cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        connection = cf.createConnection();
        connection.start();
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session2.createConsumer(destination);
        msg = consumer.receive(1000);
        if (msg == null) {
            msg = consumer.receive(5000);
        }
        LOG.info("Received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }


    public void initCombosForTestFailoverConnectionSendReplyLost() {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB,
                        PersistenceAdapterChoice.JDBC
                        // last producer message id store feature not implemented for AMQ store
                });
    }

    public void testFailoverConnectionSendReplyLost() throws Exception {

        broker = createBroker(true);
        PersistenceAdapter store = setDefaultPersistenceAdapter(broker);
        if (store instanceof KahaDBPersistenceAdapter) {
            // duplicate checker not updated on canceled tasks, even it
            // it was, recovery of the audit would fail as the message is
            // not recorded in the store and the audit may not be up to date.
            // So if duplicate messages are a absolute no no after restarts,
            // ConcurrentStoreAndDispatchQueues must be disabled
            ((KahaDBPersistenceAdapter) store).setConcurrentStoreAndDispatchQueues(false);
        }

        final SocketProxy proxy = new SocketProxy();

        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    private boolean firstSend = true;

                    @Override
                    public void send(ProducerBrokerExchange producerExchange,
                                     org.apache.activemq.command.Message messageSend)
                            throws Exception {
                        // so send will hang as if reply is lost
                        super.send(producerExchange, messageSend);
                        if (firstSend) {
                            firstSend = false;

                            producerExchange.getConnectionContext().setDontSendReponse(true);
                            Executors.newSingleThreadExecutor().execute(new Runnable() {
                                public void run() {
                                    LOG.info("Stopping connection post send...");
                                    try {
                                        proxy.close();
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

        proxy.setTarget(new URI(url));
        proxy.open();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + proxy.getUrl().toASCIIString() + ")?jms.watchTopicAdvisories=false");
        Connection connection = cf.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
        final CountDownLatch sendDoneLatch = new CountDownLatch(1);
        // proxy connection will die on send reply so this will hang on failover reconnect till open
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                LOG.info("doing async send...");
                try {
                    produceMessage(session, destination);
                } catch (JMSException e) {
                    //assertTrue(e instanceof TransactionRolledBackException);
                    LOG.info("got send exception: ", e);
                }
                sendDoneLatch.countDown();
                LOG.info("done async send");
            }
        });

        // will be closed by the plugin
        assertTrue("proxy was closed", proxy.waitUntilClosed(30));
        LOG.info("restarting proxy");
        proxy.open();

        assertTrue("message sent through failover", sendDoneLatch.await(30, TimeUnit.SECONDS));

        Message msg = consumer.receive(20000);
        LOG.info("Received: " + msg);
        assertNotNull("we got the message", msg);
        assertNull("we got just one message", consumer.receive(2000));
        consumer.close();
        connection.close();

        // verify stats, connection dup suppression means dups don't get to broker
        assertEquals("one queued message", 1, ((RegionBroker) broker.getRegionBroker()).getDestinationStatistics().getEnqueues().getCount());

        // ensure no dangling messages with fresh broker etc
        broker.stop();
        broker.waitUntilStopped();

        LOG.info("Checking for remaining/hung messages with restart..");
        broker = createBroker(false, url);
        setDefaultPersistenceAdapter(broker);
        broker.start();

        // after restart, ensure no dangling messages
        cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        connection = cf.createConnection();
        connection.start();
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session2.createConsumer(destination);
        msg = consumer.receive(1000);
        if (msg == null) {
            msg = consumer.receive(5000);
        }
        LOG.info("Received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }

    public void testFailoverProducerCloseBeforeTransactionFailWhenDisabled() throws Exception {
        startCleanBroker();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?trackTransactionProducers=false");
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
        produceMessage(session, destination);

        // restart to force failover and connection state recovery before the commit
        broker.stop();
        startBroker(false, url);

        session.commit();

        // without tracking producers, message will not be replayed on recovery
        assertNull("we got the message", consumer.receive(5000));
        session.commit();
        connection.close();
    }

    public void testFailoverMultipleProducerCloseBeforeTransaction() throws Exception {
        startCleanBroker();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer;
        TextMessage message;
        final int count = 10;
        for (int i = 0; i < count; i++) {
            producer = session.createProducer(destination);
            message = session.createTextMessage("Test message: " + count);
            producer.send(message);
            producer.close();
        }

        // restart to force failover and connection state recovery before the commit
        broker.stop();
        startBroker(false, url);

        session.commit();
        for (int i = 0; i < count; i++) {
            assertNotNull("we got all the message: " + count, consumer.receive(20000));
        }
        session.commit();
        connection.close();
    }

    // https://issues.apache.org/activemq/browse/AMQ-2772
    public void testFailoverWithConnectionConsumer() throws Exception {
        startCleanBroker();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        Connection connection = cf.createConnection();
        connection.start();

        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(QUEUE_NAME);

        final CountDownLatch connectionConsumerGotOne = new CountDownLatch(1);
        final Session poolSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.createConnectionConsumer(destination, null, new ServerSessionPool() {
            public ServerSession getServerSession() throws JMSException {
                return new ServerSession() {
                    public Session getSession() throws JMSException {
                        return poolSession;
                    }

                    public void start() throws JMSException {
                        connectionConsumerGotOne.countDown();
                        poolSession.run();
                    }
                };
            }
        }, 1);

        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer;
        TextMessage message;
        final int count = 10;
        for (int i = 0; i < count; i++) {
            producer = session.createProducer(destination);
            message = session.createTextMessage("Test message: " + count);
            producer.send(message);
            producer.close();
        }

        // restart to force failover and connection state recovery before the commit
        broker.stop();
        startBroker(false, url);

        session.commit();
        for (int i = 0; i < count - 1; i++) {
            assertNotNull("we got all the message: " + count, consumer.receive(20000));
        }
        session.commit();
        connection.close();

        assertTrue("connectionconsumer got a message", connectionConsumerGotOne.await(10, TimeUnit.SECONDS));
    }

    public void testFailoverConsumerAckLost() throws Exception {
        // as failure depends on hash order of state tracker recovery, do a few times
        for (int i = 0; i < 3; i++) {
            try {
                doTestFailoverConsumerAckLost(i);
            } finally {
                stopBroker();
            }
        }
    }

    public void doTestFailoverConsumerAckLost(final int pauseSeconds) throws Exception {
        broker = createBroker(true);
        setDefaultPersistenceAdapter(broker);

        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {

                    // broker is killed on delivered ack as prefetch is 1
                    @Override
                    public void acknowledge(
                            ConsumerBrokerExchange consumerExchange,
                            final MessageAck ack) throws Exception {

                        consumerExchange.getConnectionContext().setDontSendReponse(true);
                        Executors.newSingleThreadExecutor().execute(new Runnable() {
                            public void run() {
                                LOG.info("Stopping broker on ack: " + ack);
                                try {
                                    broker.stop();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    }
                }
        });
        broker.start();

        Vector<Connection> connections = new Vector<Connection>();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        Connection connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = producerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=1");

        connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session consumerSession1 = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

        connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session consumerSession2 = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

        final MessageConsumer consumer1 = consumerSession1.createConsumer(destination);
        final MessageConsumer consumer2 = consumerSession2.createConsumer(destination);

        produceMessage(producerSession, destination);
        produceMessage(producerSession, destination);

        final Vector<Message> receivedMessages = new Vector<Message>();
        final CountDownLatch commitDoneLatch = new CountDownLatch(1);
        final AtomicBoolean gotTransactionRolledBackException = new AtomicBoolean(false);
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                LOG.info("doing async commit after consume...");
                try {
                    Message msg = consumer1.receive(20000);
                    LOG.info("consumer1 first attempt got message: " + msg);
                    receivedMessages.add(msg);

                    // give some variance to the runs
                    TimeUnit.SECONDS.sleep(pauseSeconds * 2);

                    // should not get a second message as there are two messages and two consumers
                    // and prefetch=1, but with failover and unordered connection restore it can get the second
                    // message.

                    // For the transaction to complete it needs to get the same one or two messages
                    // again so that the acks line up.
                    // If redelivery order is different, the commit should fail with an ex
                    //
                    msg = consumer1.receive(5000);
                    LOG.info("consumer1 second attempt got message: " + msg);
                    if (msg != null) {
                        receivedMessages.add(msg);
                    }

                    LOG.info("committing consumer1 session: " + receivedMessages.size() + " messsage(s)");
                    try {
                        consumerSession1.commit();
                    } catch (JMSException expectedSometimes) {
                        LOG.info("got exception ex on commit", expectedSometimes);
                        if (expectedSometimes instanceof TransactionRolledBackException) {
                            gotTransactionRolledBackException.set(true);
                            // ok, message one was not replayed so we expect the rollback
                        } else {
                            throw expectedSometimes;
                        }

                    }
                    commitDoneLatch.countDown();
                    LOG.info("done async commit");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });


        // will be stopped by the plugin
        broker.waitUntilStopped();
        broker = createBroker(false, url);
        setDefaultPersistenceAdapter(broker);
        broker.start();

        assertTrue("tx committed trough failover", commitDoneLatch.await(30, TimeUnit.SECONDS));

        LOG.info("received message count: " + receivedMessages.size());

        // new transaction
        Message msg = consumer1.receive(gotTransactionRolledBackException.get() ? 5000 : 20000);
        LOG.info("post: from consumer1 received: " + msg);
        if (gotTransactionRolledBackException.get()) {
            assertNotNull("should be available again after commit rollback ex", msg);
        } else {
            assertNull("should be nothing left for consumer as recieve should have committed", msg);
        }
        consumerSession1.commit();

        if (gotTransactionRolledBackException.get() ||
                !gotTransactionRolledBackException.get() && receivedMessages.size() == 1) {
            // just one message successfully consumed or none consumed
            // consumer2 should get other message
            msg = consumer2.receive(10000);
            LOG.info("post: from consumer2 received: " + msg);
            assertNotNull("got second message on consumer2", msg);
            consumerSession2.commit();
        }

        for (Connection c : connections) {
            c.close();
        }

        // ensure no dangling messages with fresh broker etc
        broker.stop();
        broker.waitUntilStopped();

        LOG.info("Checking for remaining/hung messages..");
        broker = createBroker(false, url);
        setDefaultPersistenceAdapter(broker);
        broker.start();

        // after restart, ensure no dangling messages
        cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        connection = cf.createConnection();
        connection.start();
        Session sweeperSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer sweeper = sweeperSession.createConsumer(destination);
        msg = sweeper.receive(1000);
        if (msg == null) {
            msg = sweeper.receive(5000);
        }
        LOG.info("Sweep received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }

    public void testAutoRollbackWithMissingRedeliveries() throws Exception {
        broker = createBroker(true);
        broker.start();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        Connection connection = cf.createConnection();
        connection.start();
        final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = producerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=1");
        final Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumerSession.createConsumer(destination);

        produceMessage(producerSession, destination);

        Message msg = consumer.receive(20000);
        assertNotNull(msg);

        broker.stop();
        broker = createBroker(false, url);
        // use empty jdbc store so that default wait(0) for redeliveries will timeout after failover
        setPersistenceAdapter(broker, PersistenceAdapterChoice.JDBC);
        broker.start();

        try {
            consumerSession.commit();
            fail("expected transaciton rolledback ex");
        } catch (TransactionRolledBackException expected) {
        }

        broker.stop();
        broker = createBroker(false, url);
        broker.start();

        assertNotNull("should get rolledback message from original restarted broker", consumer.receive(20000));
        connection.close();
    }


    public void testWaitForMissingRedeliveries() throws Exception {
        LOG.info("testWaitForMissingRedeliveries()");
        broker = createBroker(true);
        broker.start();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?jms.consumerFailoverRedeliveryWaitPeriod=30000");
        Connection connection = cf.createConnection();
        connection.start();
        final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = producerSession.createQueue(QUEUE_NAME);
        final Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumerSession.createConsumer(destination);

        produceMessage(producerSession, destination);
        Message msg = consumer.receive(20000);
        if (msg == null) {
            AutoFailTestSupport.dumpAllThreads("missing-");
        }
        assertNotNull("got message just produced", msg);

        broker.stop();
        broker = createBroker(false, url);
        // use empty jdbc store so that wait for re-deliveries occur when failover resumes
        setPersistenceAdapter(broker, PersistenceAdapterChoice.JDBC);
        broker.start();

        final CountDownLatch commitDone = new CountDownLatch(1);
        // will block pending re-deliveries
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                LOG.info("doing async commit...");
                try {
                    consumerSession.commit();
                    commitDone.countDown();
                } catch (JMSException ignored) {
                }
            }
        });

        broker.stop();
        broker = createBroker(false, url);
        broker.start();

        assertTrue("commit was successful", commitDone.await(30, TimeUnit.SECONDS));

        assertNull("should not get committed message", consumer.receive(5000));
        connection.close();
    }


    public void testPoisonOnDeliveryWhilePending() throws Exception {
        LOG.info("testPoisonOnDeliveryWhilePending()");
        broker = createBroker(true);
        broker.start();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?jms.consumerFailoverRedeliveryWaitPeriod=10000");
        Connection connection = cf.createConnection();
        connection.start();
        final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = producerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=0");
        final Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumerSession.createConsumer(destination);

        produceMessage(producerSession, destination);
        Message msg = consumer.receive(20000);
        if (msg == null) {
            AutoFailTestSupport.dumpAllThreads("missing-");
        }
        assertNotNull("got message just produced", msg);

        // add another consumer into the mix that may get the message after restart
        MessageConsumer consumer2 = consumerSession.createConsumer(consumerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=1"));

        broker.stop();
        broker = createBroker(false, url);
        broker.start();

        final CountDownLatch commitDone = new CountDownLatch(1);

        final Vector<Exception> exceptions = new Vector<Exception>();

        // commit may fail if other consumer gets the message on restart, it will be seen a a duplicate on teh connection
        // but with no transaciton and it pending on another consumer it will be posion
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                LOG.info("doing async commit...");
                try {
                    consumerSession.commit();
                } catch (JMSException ex) {
                    exceptions.add(ex);
                } finally {
                    commitDone.countDown();
                }
            }
        });

        assertNull("consumer2 not get a message while pending to 1 or consumed by 1", consumer2.receive(2000));

        assertTrue("commit completed ", commitDone.await(15, TimeUnit.SECONDS));

        // either message consumed or sent to dlq via poison on redelivery to wrong consumer
        // message should not be available again in any event
        assertNull("consumer should not get rolledback on non redelivered message or duplicate", consumer.receive(5000));

        // consumer replay is hashmap order dependent on a failover connection state recover so need to deal with both cases
        if (exceptions.isEmpty()) {
            // commit succeeded, message was redelivered to the correct consumer after restart so commit was fine
        } else {
            // message should be in dlq
            MessageConsumer dlqConsumer = consumerSession.createConsumer(consumerSession.createQueue("ActiveMQ.DLQ"));
            TextMessage dlqMessage = (TextMessage) dlqConsumer.receive(5000);
            assertNotNull("found message in dlq", dlqMessage);
            assertEquals("text matches", "Test message", dlqMessage.getText());
            consumerSession.commit();
        }
        connection.close();
    }

    private void produceMessage(final Session producerSession, Queue destination)
            throws JMSException {
        MessageProducer producer = producerSession.createProducer(destination);
        TextMessage message = producerSession.createTextMessage("Test message");
        producer.send(message);
        producer.close();
    }

}
