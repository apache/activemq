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
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.util.DestinationPathSeparatorBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.transport.TransportListener;
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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// see https://issues.apache.org/activemq/browse/AMQ-2473

// https://issues.apache.org/activemq/browse/AMQ-2590
public class FailoverTransactionTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverTransactionTest.class);
    private static final String QUEUE_NAME = "Failover.WithTx";
    private static final String TRANSPORT_URI = "tcp://localhost:0";
    private String url;
    BrokerService broker;
    final Random random = new Random();

    public static Test suite() {
        return suite(FailoverTransactionTest.class);
    }

    public void setUp() throws Exception {
        super.setMaxTestTime(2 * 60 * 1000); // some boxes can be real slow
        super.setAutoFail(true);
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
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

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setUsePrefetchExtension(false);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);

        url = broker.getTransportConnectors().get(0).getConnectUri().toString();

        return broker;
    }

    public void configureConnectionFactory(ActiveMQConnectionFactory factory) {
        // nothing to do
    }

    public void testFailoverProducerCloseBeforeTransaction() throws Exception {
        startCleanBroker();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        configureConnectionFactory(cf);
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
        String osName = System.getProperty("os.name");
        Object[] persistenceAdapters;
        if (!osName.equalsIgnoreCase("AIX") && !osName.equalsIgnoreCase("SunOS")) {
            persistenceAdapters = new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB, PersistenceAdapterChoice.JDBC};
        } else {
            persistenceAdapters = new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.JDBC};
        }
        addCombinationValues("defaultPersistenceAdapter",persistenceAdapters);
    }

    @SuppressWarnings("unchecked")
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
        configureConnectionFactory(cf);
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

        assertTrue("tx committed through failover", commitDoneLatch.await(30, TimeUnit.SECONDS));

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
        configureConnectionFactory(cf);
        connection = cf.createConnection();
        connection.start();
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session2.createConsumer(destination);
        msg = consumer.receive(1000);
        LOG.info("Received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }

    @SuppressWarnings("unchecked")
    public void testFailoverCommitReplyLostWithDestinationPathSeparator() throws Exception {

        broker = createBroker(true);
        setDefaultPersistenceAdapter(broker);

        broker.setPlugins(new BrokerPlugin[]{
                new DestinationPathSeparatorBroker(),
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
        configureConnectionFactory(cf);
        Connection connection = cf.createConnection();
        connection.start();
        final Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(QUEUE_NAME.replace('.','/') + "?consumer.prefetchSize=0");

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
        broker.setPlugins(new BrokerPlugin[]{new DestinationPathSeparatorBroker()});
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
        broker.setPlugins(new BrokerPlugin[]{new DestinationPathSeparatorBroker()});
        broker.start();

        // after restart, ensure no dangling messages
        cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        configureConnectionFactory(cf);
        connection = cf.createConnection();
        connection.start();
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session2.createConsumer(destination);
        msg = consumer.receive(1000);
        LOG.info("Received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();

        ActiveMQDestination[] destinations = broker.getRegionBroker().getDestinations();
        for (ActiveMQDestination dest : destinations) {
            LOG.info("Destinations list: " + dest);
        }
        assertEquals("Only one destination", 1, broker.getRegionBroker().getDestinations().length);
    }

    public void initCombosForTestFailoverSendReplyLost() {
        addCombinationValues("defaultPersistenceAdapter",
            new Object[]{PersistenceAdapterChoice.KahaDB,
                    PersistenceAdapterChoice.JDBC
                    // not implemented for AMQ store or PersistenceAdapterChoice.LevelDB
            });
    }

    @SuppressWarnings("unchecked")
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
        configureConnectionFactory(cf);
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
        configureConnectionFactory(cf);
        connection = cf.createConnection();
        connection.start();
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session2.createConsumer(destination);
        msg = consumer.receive(1000);
        LOG.info("Received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }

    public void initCombosForTestFailoverConnectionSendReplyLost() {
        addCombinationValues("defaultPersistenceAdapter",
            new Object[]{PersistenceAdapterChoice.KahaDB,
                    PersistenceAdapterChoice.JDBC
                    // last producer message id store feature not implemented for AMQ store
                    // or PersistenceAdapterChoice.LevelDB
            });
    }

    @SuppressWarnings("unchecked")
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
        configureConnectionFactory(cf);
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
        configureConnectionFactory(cf);
        connection = cf.createConnection();
        connection.start();
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session2.createConsumer(destination);
        msg = consumer.receive(1000);
        LOG.info("Received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }

    public void testFailoverProducerCloseBeforeTransactionFailWhenDisabled() throws Exception {
        startCleanBroker();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?trackTransactionProducers=false");
        configureConnectionFactory(cf);
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
        produceMessage(session, destination);

        // restart to force failover and connection state recovery before the commit
        broker.stop();
        startBroker(false, url);

        try {
            session.commit();
            fail("expect ex for rollback only on async exc");
        } catch (JMSException expected) {
        }

        // without tracking producers, message will not be replayed on recovery
        assertNull("we got the message", consumer.receive(5000));
        session.commit();
        connection.close();
    }

    public void testFailoverMultipleProducerCloseBeforeTransaction() throws Exception {
        startCleanBroker();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        configureConnectionFactory(cf);
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
        configureConnectionFactory(cf);
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
            assertNotNull("Failed to get message: " + count, consumer.receive(20000));
        }
        session.commit();
        connection.close();

        assertTrue("connectionconsumer did not get a message", connectionConsumerGotOne.await(10, TimeUnit.SECONDS));
    }

    public void testFailoverConsumerAckLost() throws Exception {
        // as failure depends on hash order of state tracker recovery, do a few times
        for (int i = 0; i < 3; i++) {
            try {
                LOG.info("Iteration: " + i);
                doTestFailoverConsumerAckLost(i);
            } finally {
                stopBroker();
            }
        }
    }

    @SuppressWarnings("unchecked")
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
        configureConnectionFactory(cf);
        Connection connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = producerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=1");

        connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session consumerSession1 = connection.createSession(true, Session.SESSION_TRANSACTED);

        connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session consumerSession2 = connection.createSession(true, Session.SESSION_TRANSACTED);

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
                    TimeUnit.SECONDS.sleep(random.nextInt(5));

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
                    } catch (TransactionRolledBackException expected) {
                        LOG.info("got exception ex on commit", expected);
                        gotTransactionRolledBackException.set(true);
                        // ok, message one was not replayed so we expect the rollback
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

        assertTrue("tx committed through failover", commitDoneLatch.await(30, TimeUnit.SECONDS));

        LOG.info("received message count: " + receivedMessages.size());

        // new transaction to get both messages from either consumer
        for (int i=0; i<2; i++) {
            Message msg = consumer1.receive(5000);
            LOG.info("post: from consumer1 received: " + msg);
            consumerSession1.commit();
            if (msg == null) {
                msg = consumer2.receive(10000);
                LOG.info("post: from consumer2 received: " + msg);
                consumerSession2.commit();
            }
            assertNotNull("got message [" + i + "]", msg);
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
        configureConnectionFactory(cf);
        connection = cf.createConnection();
        connection.start();
        Session sweeperSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer sweeper = sweeperSession.createConsumer(destination);
        Message msg = sweeper.receive(1000);
        LOG.info("Sweep received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }

    public void testPoolingNConsumesAfterReconnect() throws Exception {
        broker = createBroker(true);
        setDefaultPersistenceAdapter(broker);

        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    int count = 0;

                    @Override
                    public void removeConsumer(ConnectionContext context, final ConsumerInfo info) throws Exception {
                        if (count++ == 1) {
                            Executors.newSingleThreadExecutor().execute(new Runnable() {
                                public void run() {
                                    LOG.info("Stopping broker on removeConsumer: " + info);
                                    try {
                                        broker.stop();
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

        Vector<Connection> connections = new Vector<Connection>();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        configureConnectionFactory(cf);
        Connection connection = cf.createConnection();
        connection.start();
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = producerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=1");

        produceMessage(producerSession, destination);
        connection.close();

        connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        final int sessionCount = 10;
        final Stack<Session> sessions = new Stack<Session>();
        for (int i = 0; i < sessionCount; i++) {
            sessions.push(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
        }

        final int consumerCount = 1000;
        final Deque<MessageConsumer> consumers = new ArrayDeque<MessageConsumer>();
        for (int i = 0; i < consumerCount; i++) {
            consumers.push(consumerSession.createConsumer(destination));
        }
        final ExecutorService executorService = Executors.newCachedThreadPool();

        final FailoverTransport failoverTransport = ((ActiveMQConnection) connection).getTransport().narrow(FailoverTransport.class);
        final TransportListener delegate = failoverTransport.getTransportListener();
        failoverTransport.setTransportListener(new TransportListener() {
            @Override
            public void onCommand(Object command) {
                delegate.onCommand(command);
            }

            @Override
            public void onException(IOException error) {
                delegate.onException(error);
            }

            @Override
            public void transportInterupted() {

                LOG.error("Transport interrupted: " + failoverTransport, new RuntimeException("HERE"));
                for (int i = 0; i < consumerCount && !consumers.isEmpty(); i++) {

                    executorService.execute(new Runnable() {
                        public void run() {
                            MessageConsumer localConsumer = null;
                            try {
                                synchronized (delegate) {
                                    localConsumer = consumers.pop();
                                }
                                localConsumer.receive(1);

                                LOG.info("calling close() " + ((ActiveMQMessageConsumer) localConsumer).getConsumerId());
                                localConsumer.close();
                            } catch (NoSuchElementException nse) {
                            } catch (Exception ignored) {
                                LOG.error("Ex on: " + ((ActiveMQMessageConsumer) localConsumer).getConsumerId(), ignored);
                            }
                        }
                    });
                }

                delegate.transportInterupted();
            }

            @Override
            public void transportResumed() {
                delegate.transportResumed();
            }
        });


        MessageConsumer consumer = null;
        synchronized (delegate) {
            consumer = consumers.pop();
        }
        LOG.info("calling close to trigger broker stop " + ((ActiveMQMessageConsumer) consumer).getConsumerId());
        consumer.close();

        // will be stopped by the plugin
        broker.waitUntilStopped();
        broker = createBroker(false, url);
        setDefaultPersistenceAdapter(broker);
        broker.start();

        consumer = consumerSession.createConsumer(destination);
        LOG.info("finally consuming message: " + ((ActiveMQMessageConsumer) consumer).getConsumerId());

        Message msg = null;
        for (int i = 0; i < 4 && msg == null; i++) {
            msg = consumer.receive(1000);
        }
        LOG.info("post: from consumer1 received: " + msg);
        assertNotNull("got message after failover", msg);
        msg.acknowledge();

        for (Connection c : connections) {
            c.close();
        }
    }

    public void testAutoRollbackWithMissingRedeliveries() throws Exception {
        broker = createBroker(true);
        broker.start();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        configureConnectionFactory(cf);
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
        configureConnectionFactory(cf);
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
        final CountDownLatch gotException = new CountDownLatch(1);
        // will block pending re-deliveries
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                LOG.info("doing async commit...");
                try {
                    consumerSession.commit();
                } catch (JMSException ignored) {
                    ignored.printStackTrace();
                    gotException.countDown();
                } finally {
                    commitDone.countDown();
                }

            }
        });

        broker.stop();
        broker = createBroker(false, url);
        broker.start();

        assertTrue("commit was successful", commitDone.await(30, TimeUnit.SECONDS));
        assertTrue("got exception on commit", gotException.await(30, TimeUnit.SECONDS));

        assertNotNull("should get failed committed message", consumer.receive(5000));
        connection.close();
    }

    public void testReDeliveryWhilePending() throws Exception {
        LOG.info("testReDeliveryWhilePending()");
        broker = createBroker(true);
        broker.start();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?jms.consumerFailoverRedeliveryWaitPeriod=10000");
        configureConnectionFactory(cf);
        Connection connection = cf.createConnection();
        connection.start();
        final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = producerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=0");
        final Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        final Session secondConsumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);

        MessageConsumer consumer = consumerSession.createConsumer(destination);

        produceMessage(producerSession, destination);
        Message msg = consumer.receive(20000);
        if (msg == null) {
            AutoFailTestSupport.dumpAllThreads("missing-");
        }
        assertNotNull("got message just produced", msg);

        // add another consumer into the mix that may get the message after restart
        MessageConsumer consumer2 = secondConsumerSession.createConsumer(consumerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=1"));

        broker.stop();
        broker = createBroker(false, url);
        broker.start();

        final CountDownLatch commitDone = new CountDownLatch(1);
        final CountDownLatch gotRollback = new CountDownLatch(1);

        final Vector<Exception> exceptions = new Vector<Exception>();

        // commit will fail due to failover with outstanding ack
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                LOG.info("doing async commit...");
                try {
                    consumerSession.commit();
                } catch (TransactionRolledBackException ex) {
                    gotRollback.countDown();
                } catch (JMSException ex) {
                    exceptions.add(ex);
                } finally {
                    commitDone.countDown();
                }
            }
        });


        assertTrue("commit completed ", commitDone.await(15, TimeUnit.SECONDS));
        assertTrue("got Rollback", gotRollback.await(15, TimeUnit.SECONDS));

        assertTrue("no other exceptions", exceptions.isEmpty());

        // consumer replay is hashmap order dependent on a failover connection state recover so need to deal with both cases
        // consume message from one of the consumers
        Message message = consumer2.receive(2000);
        if (message == null) {
            message = consumer.receive(2000);
        }
        consumerSession.commit();
        secondConsumerSession.commit();

        assertNotNull("got message after rollback", message);

        // no message should be in dlq
        MessageConsumer dlqConsumer = consumerSession.createConsumer(consumerSession.createQueue("ActiveMQ.DLQ"));
        assertNull("nothing in the dlq", dlqConsumer.receive(2000));
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
