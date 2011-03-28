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

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.apache.activemq.store.amq.AMQPersistenceAdapter;
import org.apache.activemq.store.kahadaptor.KahaPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DurableSubProcessWithRestartTest {
    private static final Logger LOG = LoggerFactory.getLogger(DurableSubProcessWithRestartTest.class);
    public static final long RUNTIME = 5 * 60 * 1000;

    public static final int SERVER_SLEEP = 2 * 1000; // max
    public static final int CARGO_SIZE = 400; // max

    public static final int MAX_CLIENTS = 5;
    public static final Random CLIENT_LIFETIME = new Random(5 * 1000,
            2 * 5 * 1000);
    public static final Random CLIENT_ONLINE = new Random(2 * 1000, 2 * 1000);
    public static final Random CLIENT_OFFLINE = new Random(10 * 1000, 10 * 1000);

    public static final Persistence PERSISTENT_ADAPTER = Persistence.KAHADB;
    public static final long BROKER_RESTART = 1 * 10 * 1000;

    public static final boolean ALLOW_SUBSCRIPTION_ABANDONMENT = true;
    public static final boolean CHECK_REDELIVERY = true;

    private BrokerService broker;
    private ActiveMQTopic topic;

    private ClientManager clientManager;
    private Server server;
    private HouseKeeper houseKeeper;

    private final ReentrantReadWriteLock processLock = new ReentrantReadWriteLock(
            true);
    private int restartCount = 0;
    static final Vector<Throwable> exceptions = new Vector<Throwable>();

    // this is a nice test but it takes 5mins, may be handy in the future
    // resulting bug https://issues.apache.org/jira/browse/AMQ-3190
    @Ignore("covered by org.apache.activemq.usecases.DurableSubscriptionOfflineTest.testNoMissOnMatchingSubAfterRestart()") @Test
    public void testProcess() {
        try {
            server.start();
            clientManager.start();

            if (ALLOW_SUBSCRIPTION_ABANDONMENT)
                houseKeeper.start();

            if (BROKER_RESTART <= 0)
                Thread.sleep(RUNTIME);
            else {
                long end = System.currentTimeMillis() + RUNTIME;

                while (true) {
                    long now = System.currentTimeMillis();
                    if (now > end)
                        break;

                    now = end - now;
                    now = now < BROKER_RESTART ? now : BROKER_RESTART;
                    Thread.sleep(now);

                    restartBroker();
                }
            }
        } catch (Throwable e) {
            exit("ProcessTest.testProcess failed.", e);
        }

        processLock.writeLock().lock();
        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
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
     * Creates batch of messages in a transaction periodically. The last message
     * in the transaction is always a special message what contains info about
     * the whole transaction.
     * <p>
     * Notifies the clients about the created messages also.
     */
    final class Server extends Thread {

        final String url = "vm://"
                + DurableSubProcessWithRestartTest.getName()
                + "?"
                + "jms.redeliveryPolicy.maximumRedeliveries=2&jms.redeliveryPolicy.initialRedeliveryDelay=500&"
                + "jms.producerWindowSize=20971520&jms.prefetchPolicy.all=100&"
                + "jms.copyMessageOnSend=false&jms.disableTimeStampsByDefault=false&"
                + "jms.alwaysSyncSend=true&jms.dispatchAsync=false&"
                + "jms.watchTopicAdvisories=false&"
                + "waitForStart=200&create=false";
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
                    DurableSubProcessWithRestartTest.sleepRandom(SERVER_SLEEP);

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
                ClientType clientType = relevantTrans ? ClientType
                        .randomClientType() : null; // sends this types
                int count = random(200);

                LOG.info("Sending Trans[id=" + trans + ", count="
                        + count + ", clientType=" + clientType + "]");

                Connection con = cf.createConnection();
                Session sess = con
                        .createSession(true, Session.SESSION_TRANSACTED);
                MessageProducer prod = sess.createProducer(null);

                for (int i = 0; i < count; i++) {
                    Message message = sess.createMessage();
                    message.setIntProperty("ID", ++messageRover);
                    message.setIntProperty("TRANS", trans);
                    String type = clientType != null ? clientType
                            .randomMessageType() : ClientType
                            .randomNonRelevantMessageType();
                    message.setStringProperty("TYPE", type);

                    if (CARGO_SIZE > 0)
                        message.setStringProperty("CARGO",
                                getCargo(random(CARGO_SIZE)));

                    prod.send(topic, message);
                    clientManager.onServerMessage(message);
                }

                Message message = sess.createMessage();
                message.setIntProperty("ID", ++messageRover);
                message.setIntProperty("TRANS", trans);
                message.setBooleanProperty("COMMIT", true);
                message.setBooleanProperty("RELEVANT", relevantTrans);
                prod.send(topic, message);
                clientManager.onServerMessage(message);

                sess.commit();
                LOG.info("Committed Trans[id=" + trans + ", count="
                        + count + ", clientType=" + clientType + "], ID=" + messageRover);

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
        public final HashSet<String> messageTypeSet;
        public final String selector;

        ClientType(String... messageTypes) {
            this.messageTypes = messageTypes;
            messageTypeSet = new HashSet<String>(Arrays.asList(messageTypes));

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
            return values()[DurableSubProcessWithRestartTest
                    .random(values().length - 1)];
        }

        public final String randomMessageType() {
            return messageTypes[DurableSubProcessWithRestartTest
                    .random(messageTypes.length - 1)];
        }

        public static String randomNonRelevantMessageType() {
            return Integer
                    .toString(DurableSubProcessWithRestartTest.random(20));
        }

        public final boolean isRelevant(String messageType) {
            return messageTypeSet.contains(messageType);
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

        public ClientManager() {
            super("ClientManager");
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if (clients.size() < MAX_CLIENTS) {
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
                client = new Client(++clientRover, type, CLIENT_LIFETIME,
                        CLIENT_ONLINE, CLIENT_OFFLINE);
                clients.add(client);
            }
            client.start();

            LOG.info(client.toString() + " created. " + this);
        }

        public void removeClient(Client client) {
            clients.remove(client);
        }

        public void onServerMessage(Message message) throws JMSException {
            for (Client client : clients) {
                client.onServerMessage(message);
            }
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

        String url = "failover:(tcp://localhost:61656?wireFormat.maxInactivityDuration=0)?"
                + "jms.watchTopicAdvisories=false&"
                + "jms.alwaysSyncSend=true&jms.dispatchAsync=true&"
                + "jms.producerWindowSize=20971520&"
                + "jms.copyMessageOnSend=false&"
                + "initialReconnectDelay=100&maxReconnectDelay=30000&maxReconnectAttempts=0&"
                + "useExponentialBackOff=true";
        final ConnectionFactory cf = new ActiveMQConnectionFactory(url);

        public static final String SUBSCRIPTION_NAME = "subscription";

        private final int id;
        private final String conClientId;

        private final Random lifetime;
        private final Random online;
        private final Random offline;

        private final ClientType clientType;
        private final String selector;

        private final ConcurrentLinkedQueue<Message> waitingList = new ConcurrentLinkedQueue<Message>();
        private final HashSet<Integer> processed = CHECK_REDELIVERY ? new HashSet<Integer>(
                10000) : null;

        public Client(int id, ClientType clientType, Random lifetime,
                Random online, Random offline) throws JMSException {
            super("Client" + id);
            setDaemon(true);

            this.id = id;
            conClientId = "cli" + id;
            this.clientType = clientType;
            selector = "(COMMIT = true and RELEVANT = true) or "
                    + clientType.selector;

            this.lifetime = lifetime;
            this.online = online;
            this.offline = offline;

            subscribe();
        }

        @Override
        public void run() {
            long end = System.currentTimeMillis() + lifetime.next();
            try {
                boolean sleep = false;
                while (true) {
                    long max = end - System.currentTimeMillis();
                    if (max <= 0)
                        break;

                    if (sleep)
                        offline.sleepRandom();
                    else
                        sleep = true;

                    processLock.readLock().lock();
                    try {
                        process(online.next());
                    } finally {
                        processLock.readLock().unlock();
                    }
                }

                if (!ALLOW_SUBSCRIPTION_ABANDONMENT || random(1) > 0)
                    unsubscribe();
                else {
                    LOG.info("Client abandon the subscription. "
                            + this);

                    // housekeeper should sweep these abandoned subscriptions
                    houseKeeper.abandonedSubscriptions.add(conClientId);
                }
            } catch (Throwable e) {
                exit(toString() + " failed.", e);
            }

            clientManager.removeClient(this);
            LOG.info(toString() + " DONE.");
        }

        private void process(long millis) throws JMSException {
            long end = System.currentTimeMillis() + millis;
            long hardEnd = end + 20000; // wait to finish the transaction.
            boolean inTransaction = false;
            int transCount = 0;

            LOG.info(toString() + " ONLINE.");
            Connection con = openConnection();
            Session sess = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = sess.createDurableSubscriber(topic,
                    SUBSCRIPTION_NAME, selector, false);
            try {
                do {
                    long max = end - System.currentTimeMillis();
                    if (max <= 0) {
                        if (!inTransaction)
                            break;

                        max = hardEnd - System.currentTimeMillis();
                        if (max <= 0)
                            exit("" + this
                                    + " failed: Transaction is not finished.");
                    }

                    Message message = consumer.receive(max);
                    if (message == null)
                        continue;

                    onClientMessage(message);

                    if (message.propertyExists("COMMIT")) {
                        message.acknowledge(); // CLIENT_ACKNOWLEDGE

                        LOG.info("Received Trans[id="
                                + message.getIntProperty("TRANS") + ", count="
                                + transCount + "] in " + this + ".");

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

                // Check if the messages are in the waiting
                // list for long time.
                Message topMessage = waitingList.peek();
                if (topMessage != null)
                    checkDeliveryTime(topMessage);
            }
        }

        public void onServerMessage(Message message) throws JMSException {
            if (Boolean.TRUE.equals(message.getObjectProperty("COMMIT"))) {
                if (Boolean.TRUE.equals(message.getObjectProperty("RELEVANT")))
                    waitingList.add(message);
            } else {
                String messageType = message.getStringProperty("TYPE");
                if (clientType.isRelevant(messageType))
                    waitingList.add(message);
            }
        }

        public void onClientMessage(Message message) {
            Message serverMessage = waitingList.poll();
            try {
                Integer receivedId = (Integer) message.getObjectProperty("ID");
                if (processed != null && processed.contains(receivedId))
                    LOG.info("! Message has been processed before. "
                            + this + " message = " + message);

                if (serverMessage == null)
                    exit(""
                            + this
                            + " failed: There is no next server message, but received: "
                            + message);

                Integer serverId = (Integer) serverMessage
                        .getObjectProperty("ID");
                if (receivedId == null || serverId == null)
                    exit("" + this + " failed: message ID not found.\r\n"
                            + " received: " + message + "\r\n" + "   server: "
                            + serverMessage);

                if (!serverId.equals(receivedId)) {
                    String detail = processed != null ?
                        Arrays.toString(processed.toArray()) + "\n"
                        : "";
                    exit(detail + this + " failed: Received wrong message.\r\n"
                            + " received: " + message + "\r\n" + "   server: "
                            + serverMessage);
                }

                checkDeliveryTime(message);

                if (processed != null)
                    processed.add(receivedId);
            } catch (Throwable e) {
                exit("" + this + ".onClientMessage failed.\r\n" + " received: "
                        + message + "\r\n" + "   server: " + serverMessage, e);
            }
        }

        /**
         * Checks if the message was not delivered fast enough.
         */
        public void checkDeliveryTime(Message message) throws JMSException {
            long creation = message.getJMSTimestamp();
            long min = System.currentTimeMillis() - (offline.max + online.min)
                    * (BROKER_RESTART > 0 ? 4 : 1);

            if (false && min > creation) {
                SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
                exit("" + this + ".checkDeliveryTime failed. Message time: "
                        + df.format(new Date(creation)) + ", min: "
                        + df.format(new Date(min)) + "\r\n" + message);
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
            Session session = con
                    .createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, SUBSCRIPTION_NAME, selector,
                    true);
            session.close();
            con.close();
        }

        private void unsubscribe() throws JMSException {
            Connection con = openConnection();
            Session session = con
                    .createSession(false, Session.AUTO_ACKNOWLEDGE);
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

        private HouseKeeper() {
            super("HouseKeeper");
            setDaemon(true);
        }

        public final CopyOnWriteArrayList<String> abandonedSubscriptions = new CopyOnWriteArrayList<String>();

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(3 * 60 * 1000);

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
                    LOG.info("Sweeping out subscription of "
                            + clientId + ".");
                    broker.getAdminView().destroyDurableSubscriber(clientId,
                            Client.SUBSCRIPTION_NAME);
                    sweeped.add(clientId);
                    closed++;
                }
            } catch (Exception ignored) {
                LOG.info("Ex on destroy sub " + ignored);
            } finally {
                abandonedSubscriptions.removeAll(sweeped);
            }

            LOG.info("Housekeeper sweeped out " + closed
                    + " subscriptions.");
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

    public static void sleepRandom(int minMillis, int maxMillis)
            throws InterruptedException {
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
            DurableSubProcessWithRestartTest.sleepRandom(min, max);
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

    private enum Persistence {
        MEMORY, AMQ, KAHA, KAHADB
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

        switch (PERSISTENT_ADAPTER) {
        case MEMORY:
            broker.setPersistent(false);
            break;

        case AMQ:
            File amqData = new File("activemq-data/" + getName() + "-amq");
            if (deleteAllMessages)
                delete(amqData);

            broker.setPersistent(true);
            AMQPersistenceAdapter amq = new AMQPersistenceAdapter();
            amq.setDirectory(amqData);
            broker.setPersistenceAdapter(amq);
            break;

        case KAHA:
            File kahaData = new File("activemq-data/" + getName() + "-kaha");
            if (deleteAllMessages)
                delete(kahaData);

            broker.setPersistent(true);
            KahaPersistenceAdapter kaha = new KahaPersistenceAdapter();
            kaha.setDirectory(kahaData);
            broker.setPersistenceAdapter(kaha);
            break;

        case KAHADB:
            File kahadbData = new File("activemq-data/" + getName() + "-kahadb");
            if (deleteAllMessages)
                delete(kahadbData);

            broker.setPersistent(true);
            KahaDBPersistenceAdapter kahadb = new KahaDBPersistenceAdapter();
            kahadb.setDirectory(kahadbData);
            kahadb.setJournalMaxFileLength(5 * 1024 * 1024);
            broker.setPersistenceAdapter(kahadb);
            break;
        }

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
