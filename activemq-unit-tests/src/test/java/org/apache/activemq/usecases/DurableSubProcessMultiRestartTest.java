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
 * WITHOUT WARRANTIES OR ONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.usecases;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurableSubProcessMultiRestartTest {
    private static final Logger LOG = LoggerFactory.getLogger(DurableSubProcessMultiRestartTest.class);

    public static final long RUNTIME = 1 * 60 * 1000;

    private BrokerService broker;
    private ActiveMQTopic topic;

    private final ReentrantReadWriteLock processLock = new ReentrantReadWriteLock(true);

    private int restartCount = 0;
    private final int SUBSCRIPTION_ID = 1;

    static final Vector<Throwable> exceptions = new Vector<Throwable>();

    /**
     * The test creates a durable subscriber and producer with a broker that is
     * continually restarted.
     *
     * Producer creates a message every .5 seconds -creates a new connection for
     * each message
     *
     * durable subscriber - comes online for 10 seconds, - then goes offline for
     * a "moment" - repeats the cycle
     *
     * approx every 10 seconds the broker restarts. Subscriber and Producer
     * connections will be closed BEFORE the restart.
     *
     * The Durable subscriber is "unsubscribed" before the the end of the test.
     *
     * checks for number of kahaDB files left on filesystem.
     *
     * @throws Exception
     */
    @Test
    public void testProcess() throws Exception {

        DurableSubscriber durableSubscriber = new DurableSubscriber(SUBSCRIPTION_ID);
        MsgProducer msgProducer = new MsgProducer();

        try {
            // register the subscription & start messages
            durableSubscriber.start();
            msgProducer.start();

            long endTime = System.currentTimeMillis() + RUNTIME;

            while (endTime > System.currentTimeMillis()) {
                Thread.sleep(10000);
                restartBroker();
            }
        } catch (Throwable e) {
            exit("ProcessTest.testProcess failed.", e);
        }

        // wait for threads to finish
        try {
            msgProducer.join();
            durableSubscriber.join();
        } catch (InterruptedException e) {
            e.printStackTrace(System.out);
        }

        // restart broker one last time
        restartBroker();

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());

        final KahaDBPersistenceAdapter pa = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        assertTrue("only less than two journal files should be left: " + pa.getStore().getJournal().getFileMap().size(),
            Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return pa.getStore().getJournal().getFileMap().size() <= 2;
                }
            }, TimeUnit.MINUTES.toMillis(3))
        );

        LOG.info("DONE.");
    }

    private void restartBroker() throws Exception {
        LOG.info("Broker restart: waiting for components.");

        processLock.writeLock().lock();
        try {
            destroyBroker();
            startBroker(false);

            restartCount++;
            LOG.info("Broker restarted. count: " + restartCount);
        } finally {
            processLock.writeLock().unlock();
        }
    }

    /**
     * Producers messages
     *
     */
    final class MsgProducer extends Thread {

        String url = "vm://" + DurableSubProcessMultiRestartTest.getName();

        final ConnectionFactory cf = new ActiveMQConnectionFactory(url);

        private long msgCount;
        int messageRover = 0;

        public MsgProducer() {
            super("MsgProducer");
            setDaemon(true);
        }

        @Override
        public void run() {

            long endTime = RUNTIME + System.currentTimeMillis();

            try {
                while (endTime > System.currentTimeMillis()) {

                    Thread.sleep(500);

                    processLock.readLock().lock();
                    try {
                        send();
                    } finally {
                        processLock.readLock().unlock();
                    }
                    LOG.info("MsgProducer msgCount=" + msgCount);
                }
            } catch (Throwable e) {
                exit("Server.run failed", e);
            }
        }

        public void send() throws JMSException {

            LOG.info("Sending ... ");

            Connection con = cf.createConnection();

            Session sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer prod = sess.createProducer(null);
            Message message = sess.createMessage();
            message.setIntProperty("ID", ++messageRover);
            message.setBooleanProperty("COMMIT", true);
            prod.send(topic, message);

            msgCount++;
            LOG.info("Message Sent.");

            sess.close();
            con.close();
        }
    }

    /**
     * Consumes massages from a durable subscription. Goes online/offline
     * periodically.
     */
    private final class DurableSubscriber extends Thread {

        String url = "tcp://localhost:61656";

        final ConnectionFactory cf = new ActiveMQConnectionFactory(url);

        public static final String SUBSCRIPTION_NAME = "subscription";

        private final int id;
        private final String conClientId;
        private long msgCount;

        public DurableSubscriber(int id) throws JMSException {
            super("DurableSubscriber" + id);
            setDaemon(true);

            this.id = id;
            conClientId = "cli" + id;

            subscribe();
        }

        @Override
        public void run() {

            long end = System.currentTimeMillis() + RUNTIME;

            try {

                // while (true) {
                while (end > System.currentTimeMillis()) {

                    processLock.readLock().lock();
                    try {
                        process(5000);
                    } finally {
                        processLock.readLock().unlock();
                    }
                }

                unsubscribe();

            } catch (JMSException maybe) {
                if (maybe.getCause() instanceof IOException) {
                    // ok on broker shutdown;
                } else {
                    exit(toString() + " failed with JMSException", maybe);
                }
            } catch (Throwable e) {
                exit(toString() + " failed.", e);
            }

            LOG.info(toString() + " DONE. MsgCout=" + msgCount);
        }

        private void process(long duration) throws JMSException {
            LOG.info(toString() + " ONLINE.");

            Connection con = openConnection();
            Session sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer = sess.createDurableSubscriber(topic, SUBSCRIPTION_NAME);

            long end = System.currentTimeMillis() + duration;

            try {
                while (end > System.currentTimeMillis()) {
                    Message message = consumer.receive(100);
                    if (message != null) {
                        LOG.info(toString() + "received message...");
                        msgCount++;
                    }
                }
            } finally {
                sess.close();
                con.close();
                LOG.info(toString() + " OFFLINE.");
            }
        }

        private Connection openConnection() throws JMSException {
            Connection con = cf.createConnection();
            con.setClientID(conClientId);
            con.start();
            return con;
        }

        private void subscribe() throws JMSException {
            Connection con = openConnection();
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

            session.createDurableSubscriber(topic, SUBSCRIPTION_NAME);
            LOG.info(toString() + " SUBSCRIBED");

            session.close();
            con.close();
        }

        private void unsubscribe() throws JMSException {
            Connection con = openConnection();
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.unsubscribe(SUBSCRIPTION_NAME);
            LOG.info(toString() + " UNSUBSCRIBED");

            session.close();
            con.close();
        }

        @Override
        public String toString() {
            return "DurableSubscriber[id=" + id + "]";
        }
    }

    // -------- helper methods -----------

    public static void exit(String message) {
        exit(message, null);
    }

    public static void exit(String message, Throwable e) {
        Throwable cause = new RuntimeException(message, e);
        LOG.error(message, cause);
        exceptions.add(cause);
        fail(cause.toString());
    }

    @Before
    public void setUp() throws Exception {
        topic = new ActiveMQTopic("TopicT");
        startBroker();
    }

    @After
    public void tearDown() throws Exception {
        destroyBroker();
    }

    private void startBroker() throws Exception {
        startBroker(true);
    }

    private void startBroker(boolean deleteAllMessages) throws Exception {
        if (broker != null)
            return;

        broker = BrokerFactory.createBroker("broker:(vm://" + getName() + ")");
        broker.setBrokerName(getName());
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);

        broker.setKeepDurableSubsActive(true);

        File kahadbData = new File("activemq-data/" + getName() + "-kahadb");
        if (deleteAllMessages)
            delete(kahadbData);

        broker.setPersistent(true);
        KahaDBPersistenceAdapter kahadb = new KahaDBPersistenceAdapter();
        kahadb.setDirectory(kahadbData);
        kahadb.setJournalMaxFileLength(20 * 1024);
        broker.setPersistenceAdapter(kahadb);

        broker.addConnector("tcp://localhost:61656");

        broker.getSystemUsage().getMemoryUsage().setLimit(256 * 1024 * 1024);
        broker.getSystemUsage().getTempUsage().setLimit(256 * 1024 * 1024);
        broker.getSystemUsage().getStoreUsage().setLimit(256 * 1024 * 1024);

        broker.start();
    }

    protected static String getName() {
        return "DurableSubProcessMultiRestartTest";
    }

    private static boolean delete(File path) {
        if (path == null)
            return true;

        if (path.isDirectory()) {
            for (File file : path.listFiles()) {
                delete(file);
            }
        }
        return path.delete();
    }

    private void destroyBroker() throws Exception {
        if (broker == null)
            return;

        broker.stop();
        broker = null;
    }
}
