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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A cut down version of DurableSubProcessWithRestartTest that focuses on kahaDB file retention
 */
public class DurableSubDelayedUnsubscribeTest {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubDelayedUnsubscribeTest.class);

    private static final long RUNTIME = 2 * 60 * 1000;
    private static final int CARGO_SIZE = 400; // max
    private static final int MAX_CLIENTS = 15;
    private static boolean ALLOW_SUBSCRIPTION_ABANDONMENT = true;
    private static final Vector<Throwable> exceptions = new Vector<Throwable>();

    private BrokerService broker;
    private ActiveMQTopic topic;

    private ClientManager clientManager;
    private Server server;
    private HouseKeeper houseKeeper;

    private final ReentrantReadWriteLock processLock = new ReentrantReadWriteLock(true);

    @Test
    public void testProcess() throws Exception {

        server.start();
        clientManager.start();

        houseKeeper.start();

        // Sleep to
        Thread.sleep(RUNTIME);

        // inform message producer to stop
        server.stopped = true;

        // add one Subscriber to the topic that will not be unsubscribed.
        // should not have any pending messages in the kahadb store
        Client lastClient = new Client(32000, ClientType.A);
        lastClient.process(1000);

        // stop client manager from creating any more clients
        clientManager.stopped = true;

        final BrokerService brokerService = this.broker;

        // Wait for all client subscription to be unsubscribed or swept away.
        // Ensure we sleep longer than the housekeeper's sweep delay otherwise we can
        // miss the fact that all durables that were abandoned do finally get cleaned up.

        // Wait for all clients to stop
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return clientManager.getClientCount() == 0;
            }
        }, Client.lifetime + TimeUnit.SECONDS.toMillis(10));

        assertTrue("should have only one inactiveSubscriber subscribed but was: " + brokerService.getAdminView().getInactiveDurableTopicSubscribers().length,
            Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return brokerService.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
                }
            }, houseKeeper.SWEEP_DELAY * 2));

        assertTrue("should be no subscribers subscribed but was: " + brokerService.getAdminView().getDurableTopicSubscribers().length,
            Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return brokerService.getAdminView().getDurableTopicSubscribers().length == 0;
                }
            }, TimeUnit.MINUTES.toMillis(3)));

        processLock.writeLock().lock();

        // check outcome.

        ObjectName[] subscribers = broker.getAdminView().getDurableTopicSubscribers();
        ObjectName[] inactiveSubscribers = broker.getAdminView().getInactiveDurableTopicSubscribers();
        final KahaDBPersistenceAdapter persistenceAdapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();

        printDebugClientInfo(subscribers, inactiveSubscribers, persistenceAdapter);

        assertEquals("should have only one inactiveSubscriber subscribed", 1, broker.getAdminView().getInactiveDurableTopicSubscribers().length);
        assertEquals("should be no subscribers subscribed", 0, broker.getAdminView().getDurableTopicSubscribers().length);

        final KahaDBPersistenceAdapter pa = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        assertTrue("should be less than 3 journal file left but was: " + persistenceAdapter.getStore().getJournal().getFileMap().size(),
            Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return pa.getStore().getJournal().getFileMap().size() <= 3;
                }
            }, TimeUnit.MINUTES.toMillis(3)));

        // Be good and cleanup our mess a bit.
        this.houseKeeper.shutdown();

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());

        LOG.info("DONE.");
    }

    private void printDebugClientInfo(ObjectName[] subscribers, ObjectName[] inactiveSubscribers, final KahaDBPersistenceAdapter pa) throws IOException {

        LOG.info("====>>> START DEBUG Subscriber INFO");

        LOG.info("Number of subscribers subscribed as seen through JMX is" + subscribers.length);

        for (int i = 0; i < subscribers.length; i++) {
            LOG.info("subscribers subscribed as seen throngh JMX: " + subscribers[i]);
        }

        LOG.info("Number of inactiveSubscribers subscribed as seen through JMX is" + inactiveSubscribers.length);

        for (int i = 0; i < inactiveSubscribers.length; i++) {
            LOG.info("subscribers subscribed as seen throngh JMX: " + inactiveSubscribers[i]);
        }

        LOG.info("ClientManager.clients size is" + clientManager.clients.size());

        for (int i = 0; i < clientManager.clients.size(); i++) {
            LOG.info("clients is: " + clientManager.clients.get(i));
        }

        LOG.info("housekeep.subscriptions size is " + houseKeeper.abandonedSubscriptions.size());

        for (int i = 0; i < houseKeeper.abandonedSubscriptions.size(); i++) {
            LOG.info("housekeep is: " + houseKeeper.abandonedSubscriptions.get(i));
        }

        LOG.info("number of journal files left" + pa.getStore().getJournal().getFileMap().size());

        LOG.info("====>>> END DEBUG Subscriber INFO");
    }

    /**
     * Creates batch of messages in a transaction periodically. The last message
     * in the transaction is always a special message what contains info about
     * the whole transaction.
     * <p>
     * Notifies the clients about the created messages also.
     */
    final class Server extends Thread {

        public boolean stopped;
        final String url = "vm://" + DurableSubDelayedUnsubscribeTest.getName() + "?"
                + "jms.redeliveryPolicy.maximumRedeliveries=2&jms.redeliveryPolicy.initialRedeliveryDelay=500&"
                + "jms.producerWindowSize=20971520&jms.prefetchPolicy.all=100&" + "jms.copyMessageOnSend=false&jms.disableTimeStampsByDefault=false&"
                + "jms.alwaysSyncSend=true&jms.dispatchAsync=false&" + "jms.watchTopicAdvisories=false&" + "waitForStart=200&create=false";
        final ConnectionFactory cf = new ActiveMQConnectionFactory(url);

        final Object sendMutex = new Object();
        final String[] cargos = new String[500];

        int transRover = 0;
        int messageRover = 0;

        public Server() {
            super("Server");
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while (true) {

                    if (stopped) {
                        // server should stop producing
                        break;
                    }

                    Thread.sleep(500);
                    processLock.readLock().lock();
                    try {
                        send();
                    } finally {
                        processLock.readLock().unlock();
                    }
                }
            } catch (Throwable e) {
                exit("Server.run failed", e);
            }
        }

        public void send() throws JMSException {
            // do not create new clients now
            // ToDo: Test this case later.
            synchronized (sendMutex) {
                int trans = ++transRover;
                boolean relevantTrans = random(2) > 1;
                ClientType clientType = relevantTrans ? ClientType.randomClientType() : null; // sends
                                                                                              // this
                                                                                              // types
                int count = random(200);

                LOG.info("Sending Trans[id=" + trans + ", count=" + count + ", clientType=" + clientType + "]");

                Connection con = cf.createConnection();
                Session sess = con.createSession(true, Session.SESSION_TRANSACTED);
                MessageProducer prod = sess.createProducer(null);

                for (int i = 0; i < count; i++) {
                    Message message = sess.createMessage();
                    message.setIntProperty("ID", ++messageRover);
                    message.setIntProperty("TRANS", trans);
                    String type = clientType != null ? clientType.randomMessageType() : ClientType.randomNonRelevantMessageType();
                    message.setStringProperty("TYPE", type);

                    if (CARGO_SIZE > 0)
                        message.setStringProperty("CARGO", getCargo(random(CARGO_SIZE)));

                    prod.send(topic, message);

                }

                Message message = sess.createMessage();
                message.setIntProperty("ID", ++messageRover);
                message.setIntProperty("TRANS", trans);
                message.setBooleanProperty("COMMIT", true);
                message.setBooleanProperty("RELEVANT", relevantTrans);
                prod.send(topic, message);

                sess.commit();
                LOG.info("Committed Trans[id=" + trans + ", count=" + count + ", clientType=" + clientType + "], ID=" + messageRover);

                sess.close();
                con.close();
            }
        }

        private String getCargo(int length) {
            if (length == 0)
                return null;

            if (length < cargos.length) {
                String result = cargos[length];
                if (result == null) {
                    result = getCargoImpl(length);
                    cargos[length] = result;
                }
                return result;
            }
            return getCargoImpl(length);
        }

        private String getCargoImpl(int length) {
            StringBuilder sb = new StringBuilder(length);
            for (int i = length; --i >= 0;) {
                sb.append('a');
            }
            return sb.toString();
        }
    }

    /**
     * Clients listen on different messages in the topic. The 'TYPE' property
     * helps the client to select the proper messages.
     */
    private enum ClientType {
        A("a", "b", "c"), B("c", "d", "e"), C("d", "e", "f"), D("g", "h");

        public final String[] messageTypes;

        public final String selector;

        ClientType(String... messageTypes) {
            this.messageTypes = messageTypes;

            StringBuilder sb = new StringBuilder("TYPE in (");
            for (int i = 0; i < messageTypes.length; i++) {
                if (i > 0)
                    sb.append(", ");
                sb.append('\'').append(messageTypes[i]).append('\'');
            }
            sb.append(')');
            selector = sb.toString();
        }

        public static ClientType randomClientType() {
            return values()[DurableSubDelayedUnsubscribeTest.random(values().length - 1)];
        }

        public final String randomMessageType() {
            return messageTypes[DurableSubDelayedUnsubscribeTest.random(messageTypes.length - 1)];
        }

        public static String randomNonRelevantMessageType() {
            return Integer.toString(DurableSubDelayedUnsubscribeTest.random(20));
        }

        @Override
        public final String toString() {
            return this.name() /* + '[' + selector + ']' */;
        }
    }

    /**
     * Creates new cliens.
     */
    private final class ClientManager extends Thread {

        private int clientRover = 0;

        private final CopyOnWriteArrayList<Client> clients = new CopyOnWriteArrayList<Client>();

        private boolean stopped;

        public ClientManager() {
            super("ClientManager");
            setDaemon(true);
        }

        public int getClientCount() {
            return clients.size();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if (clients.size() < MAX_CLIENTS) {

                        if (stopped) {
                            // get out, don't start any more threads
                            break;
                        }
                        processLock.readLock().lock();
                        try {
                            createNewClient();
                        } finally {
                            processLock.readLock().unlock();
                        }
                    }

                    int size = clients.size();
                    sleepRandom(size * 3 * 1000, size * 6 * 1000);
                }
            } catch (Throwable e) {
                exit("ClientManager.run failed.", e);
            }
        }

        private void createNewClient() throws JMSException {
            ClientType type = ClientType.randomClientType();

            Client client;
            synchronized (server.sendMutex) {
                client = new Client(++clientRover, type);
                clients.add(client);
            }
            client.start();

            LOG.info(client.toString() + " created. " + this);
        }

        public void removeClient(Client client) {
            clients.remove(client);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ClientManager[count=");
            sb.append(clients.size());
            sb.append(", clients=");
            boolean sep = false;
            for (Client client : clients) {
                if (sep)
                    sb.append(", ");
                else
                    sep = true;
                sb.append(client.toString());
            }
            sb.append(']');
            return sb.toString();
        }
    }

    /**
     * Consumes massages from a durable subscription. Goes online/offline
     * periodically. Checks the incoming messages against the sent messages of
     * the server.
     */
    private final class Client extends Thread {

        String url = "failover:(tcp://localhost:61656?wireFormat.maxInactivityDuration=0)?" + "jms.watchTopicAdvisories=false&"
                + "jms.alwaysSyncSend=true&jms.dispatchAsync=true&" + "jms.producerWindowSize=20971520&" + "jms.copyMessageOnSend=false&"
                + "initialReconnectDelay=100&maxReconnectDelay=30000&" + "useExponentialBackOff=true";
        final ConnectionFactory cf = new ActiveMQConnectionFactory(url);

        public static final String SUBSCRIPTION_NAME = "subscription";

        private final int id;
        private final String conClientId;

        public static final int lifetime = 60 * 1000;
        private final int online = 1 * 1000;
        private final int offline = 59 * 1000;

        private final ClientType clientType;
        private final String selector;

        public Client(int id, ClientType clientType) throws JMSException {
            super("Client" + id);
            setDaemon(true);

            this.id = id;
            conClientId = "cli" + id;
            this.clientType = clientType;
            selector = "(COMMIT = true and RELEVANT = true) or " + clientType.selector;

            subscribe();
        }

        @Override
        public void run() {
            // long end = System.currentTimeMillis() + lifetime.next();
            long end = System.currentTimeMillis() + lifetime;
            try {
                boolean sleep = false;
                while (true) {
                    long max = end - System.currentTimeMillis();
                    if (max <= 0)
                        break;

                    if (sleep)
                        Thread.sleep(offline);
                    // offline.sleepRandom();
                    else
                        sleep = true;

                    processLock.readLock().lock();
                    try {
                        process(online);
                    } finally {
                        processLock.readLock().unlock();
                    }
                }

                // 50% unsubscribe, 50% abondon subscription
                if (!ALLOW_SUBSCRIPTION_ABANDONMENT) {
                    unsubscribe();
                    ALLOW_SUBSCRIPTION_ABANDONMENT = true;
                } else {

                    LOG.info("Client abandon the subscription. " + this);

                    // housekeeper should sweep these abandoned subscriptions
                    houseKeeper.abandonedSubscriptions.add(conClientId);
                    ALLOW_SUBSCRIPTION_ABANDONMENT = false;
                }
            } catch (Throwable e) {
                exit(toString() + " failed.", e);
            }

            clientManager.removeClient(this);
            LOG.info(toString() + " DONE.");
        }

        private void process(long processingTime) throws JMSException {
            long end = System.currentTimeMillis() + processingTime;
            long hardEnd = end + 20000; // wait to finish the transaction.
            boolean inTransaction = false;
            int transCount = 0;

            LOG.info(toString() + " ONLINE.");
            Connection con = openConnection();
            Session sess = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = sess.createDurableSubscriber(topic, SUBSCRIPTION_NAME, selector, false);
            try {
                do {
                    long max = end - System.currentTimeMillis();
                    if (max <= 0) {
                        if (!inTransaction)
                            break;

                        max = hardEnd - System.currentTimeMillis();
                        if (max <= 0)
                            exit("" + this + " failed: Transaction is not finished.");
                    }

                    Message message = consumer.receive(max);
                    if (message == null)
                        continue;

                    if (message.propertyExists("COMMIT")) {
                        message.acknowledge(); // CLIENT_ACKNOWLEDGE

                        LOG.info("Received Trans[id=" + message.getIntProperty("TRANS") + ", count=" + transCount + "] in " + this + ".");

                        inTransaction = false;
                        transCount = 0;
                    } else {
                        inTransaction = true;
                        transCount++;
                    }
                } while (true);
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
            session.createDurableSubscriber(topic, SUBSCRIPTION_NAME, selector, true);
            session.close();
            con.close();
        }

        private void unsubscribe() throws JMSException {
            Connection con = openConnection();
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.unsubscribe(SUBSCRIPTION_NAME);
            session.close();
            con.close();
        }

        @Override
        public String toString() {
            return "Client[id=" + id + ", type=" + clientType + "]";
        }
    }

    /**
     * Sweeps out not-used durable subscriptions.
     */
    private final class HouseKeeper extends Thread {

        private final AtomicBoolean done = new AtomicBoolean();

        public final long SWEEP_DELAY = TimeUnit.MINUTES.toMillis(3);

        private HouseKeeper() {
            super("HouseKeeper");
            setDaemon(true);
        }

        public final CopyOnWriteArrayList<String> abandonedSubscriptions = new CopyOnWriteArrayList<String>();

        public void shutdown() throws Exception {
            done.set(true);

            // In case we are sleeping, abort the sleep.
            this.interrupt();

            // Wait for run to complete.
            this.join(TimeUnit.MINUTES.toMillis(SWEEP_DELAY));

            // Ensure all abandoned subscriptions get wiped.
            if (!abandonedSubscriptions.isEmpty()) {
                this.sweep();
            }
        }

        @Override
        public void run() {
            while (!done.get()) {
                try {
                    Thread.sleep(SWEEP_DELAY);

                    processLock.readLock().lock();
                    try {
                        sweep();
                    } finally {
                        processLock.readLock().unlock();
                    }
                } catch (InterruptedException ex) {
                    break;
                } catch (Throwable e) {
                    Exception log = new Exception("HouseKeeper failed.", e);
                    log.printStackTrace();
                }
            }
        }

        private void sweep() throws Exception {
            LOG.info("Housekeeper sweeping.");

            int closed = 0;
            ArrayList<String> sweeped = new ArrayList<String>();
            try {
                for (String clientId : abandonedSubscriptions) {
                    LOG.info("Sweeping out subscription of " + clientId + ".");
                    broker.getAdminView().destroyDurableSubscriber(clientId, Client.SUBSCRIPTION_NAME);
                    sweeped.add(clientId);
                    closed++;
                }
            } catch (Exception ignored) {
                LOG.info("Ex on destroy sub " + ignored);
            } finally {
                abandonedSubscriptions.removeAll(sweeped);
            }

            LOG.info("Housekeeper sweeped out " + closed + " subscriptions.");
        }
    }

    public static int random(int max) {
        return (int) (Math.random() * (max + 1));
    }

    public static int random(int min, int max) {
        return random(max - min) + min;
    }

    public static void sleepRandom(int maxMillis) throws InterruptedException {
        Thread.sleep(random(maxMillis));
    }

    public static void sleepRandom(int minMillis, int maxMillis) throws InterruptedException {
        Thread.sleep(random(minMillis, maxMillis));
    }

    public static final class Random {

        final int min;
        final int max;

        Random(int min, int max) {
            this.min = min;
            this.max = max;
        }

        public int next() {
            return random(min, max);
        }

        public void sleepRandom() throws InterruptedException {
            DurableSubDelayedUnsubscribeTest.sleepRandom(min, max);
        }
    }

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

        clientManager = new ClientManager();
        server = new Server();
        houseKeeper = new HouseKeeper();
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

        File kahadbData = new File("activemq-data/" + getName() + "-kahadb");
        if (deleteAllMessages)
            delete(kahadbData);

        broker.setPersistent(true);
        KahaDBPersistenceAdapter kahadb = new KahaDBPersistenceAdapter();
        kahadb.setDirectory(kahadbData);
        kahadb.setJournalMaxFileLength(512 * 1024);
        broker.setPersistenceAdapter(kahadb);

        broker.addConnector("tcp://localhost:61656");

        broker.getSystemUsage().getMemoryUsage().setLimit(256 * 1024 * 1024);
        broker.getSystemUsage().getTempUsage().setLimit(256 * 1024 * 1024);
        broker.getSystemUsage().getStoreUsage().setLimit(256 * 1024 * 1024);

        broker.start();
    }

    protected static String getName() {
        return "DurableSubProcessWithRestartTest";
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
