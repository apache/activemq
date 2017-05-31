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

import java.io.File;
import java.util.concurrent.TimeUnit;

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
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurableSubSelectorDelayWithRestartTest {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubSelectorDelayWithRestartTest.class);

    public static final long RUNTIME = 1 * 60 * 1000;

    private boolean RESTART = true;
    private int NUMBER_SUBSCRIBERS = 3;

    private BrokerService broker;
    private ActiveMQTopic topic;

    @Test
    public void testProcess() throws Exception {

        MsgProducer msgProducer = new MsgProducer();
        msgProducer.start();

        DurableSubscriber subscribers[] = new DurableSubscriber[NUMBER_SUBSCRIBERS];

        for (int i = 0; i < subscribers.length - 1; i++) {
            subscribers[i] = new DurableSubscriber(i);
            subscribers[i].process();
        }

        // wait for server to finish
        msgProducer.join();

        //for the last subscriber pop one message into the topic.
        subscribers[(subscribers.length - 1)] = new DurableSubscriber((subscribers.length - 1));
        subscribers[(subscribers.length - 1)].subscribe();
        MsgProducer msgProducer2 = new MsgProducer();
        msgProducer2.send();
        subscribers[(subscribers.length - 1)].process();

        // unsubscribe all, but the last subscriber.
        for (int j = 0; j < (subscribers.length - 1); j++) {
            LOG.info("Unsubscribing subscriber " + subscribers[j]);
            subscribers[j].unsubscribe();
        }

        final KahaDBPersistenceAdapter pa = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        assertTrue("small number of journal files should be left ", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("journal data file count - expected {} actual {}", 4, pa.getStore().getJournal().getFileMap().size());
                return pa.getStore().getJournal().getFileMap().size() <= 4;
            }
        }, TimeUnit.MINUTES.toMillis(3)));

        LOG.info("DONE.");
    }

    /**
     * Message Producer
     */
    final class MsgProducer extends Thread {

         final String url = "failover:(tcp://localhost:61656)";

        final ConnectionFactory cf = new ActiveMQConnectionFactory(url);

        int transRover = 0;
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
                    Thread.sleep(400);
                    send();

                    //restart broker all the time
                    if(RESTART){
                     destroyBroker();
                     startBroker(false);
                    }
                }
            } catch (Throwable e) {
               e.printStackTrace(System.out);
               throw new RuntimeException(e);
            }
        }

        public void send() throws JMSException {

            int trans = ++transRover;
            boolean relevantTrans = true;
            int count = 40;

            LOG.info("Sending Trans[id=" + trans + ", count="
                    + count + "]");

            Connection con = cf.createConnection();

            Session sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer prod = sess.createProducer(null);

            for (int i = 0; i < count; i++) {
                Message message = sess.createMessage();
                message.setIntProperty("ID", ++messageRover);
                message.setIntProperty("TRANS", trans);
                message.setBooleanProperty("RELEVANT", false);
                prod.send(topic, message);
            }

            Message message = sess.createMessage();
            message.setIntProperty("ID", ++messageRover);
            message.setIntProperty("TRANS", trans);
            message.setBooleanProperty("COMMIT", true);
            message.setBooleanProperty("RELEVANT", relevantTrans);
            prod.send(topic, message);

            LOG.info("Committed Trans[id=" + trans + ", count="
                    + count + "], ID=" + messageRover);

            sess.close();
            con.close();
        }
    }

    /**
     * Consumes massages from a durable subscription. Goes online/offline
     * periodically. Checks the incoming messages against the sent messages of
     * the server.
     */
    private final class DurableSubscriber {

        final String url = "failover:(tcp://localhost:61656)";

        final ConnectionFactory cf = new ActiveMQConnectionFactory(url);

        private final String subName ;

        private final int id;
        private final String conClientId;
        private final String selector;

        public DurableSubscriber(int id) throws JMSException {
            this.id = id;
            conClientId = "cli" + id;
            subName = "subscription"+ id;
            selector ="RELEVANT = true";
        }

        private void process() throws JMSException {
            long end = System.currentTimeMillis() + 20000;
            int transCount = 0;

            LOG.info(toString() + " ONLINE.");
            Connection con = openConnection();

            Session sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = sess.createDurableSubscriber(topic, subName, selector, false);

            try {

                do {
                    long max = end - System.currentTimeMillis();

                    if (max <= 0) {
                            break;
                    }

                    Message message = consumer.receive(max);
                    if (message == null) {
                        continue;
                    }

                    LOG.info("Received Trans[id="
                            + message.getIntProperty("TRANS") + ", count="
                            + transCount + "] in " + this + ".");

                } while (true);

            } finally {
                try {
                    sess.close();
                    con.close();
                } catch (Exception e) {}

                LOG.info(toString() + " OFFLINE.");
            }
        }

        private Connection openConnection() throws JMSException {
            Connection con = cf.createConnection();
            con.setClientID(conClientId);
            con.start();
            return con;
        }

        public void subscribe() throws JMSException{
            LOG.info(toString() + "SUBSCRIBING");
            Connection con = openConnection();

            Session sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            sess.createDurableSubscriber(topic, subName, selector, false);

            sess.close();
            con.close();
        }

        private void unsubscribe() throws JMSException {
            Connection con = openConnection();
            Session session = con
                    .createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.unsubscribe(subName);
            session.close();
            con.close();
        }

        @Override
        public String toString() {
            return "DurableSubscriber[id=" + id + "]";
        }
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

        File kahadbData = new File("activemq-data/" + getName() + "-kahadb");
        if (deleteAllMessages)
            delete(kahadbData);

        broker.setPersistent(true);
        KahaDBPersistenceAdapter kahadb = new KahaDBPersistenceAdapter();
        kahadb.setDirectory(kahadbData);
        kahadb.setJournalMaxFileLength(  10 * 1024);
        kahadb.setCleanupInterval(5000);
        broker.setPersistenceAdapter(kahadb);

        broker.addConnector("tcp://localhost:61656");

        broker.getSystemUsage().getMemoryUsage().setLimit(256 * 1024 * 1024);
        broker.getSystemUsage().getTempUsage().setLimit(256 * 1024 * 1024);
        broker.getSystemUsage().getStoreUsage().setLimit(256 * 1024 * 1024);

        LOG.info(toString() + "Starting Broker...");
        broker.start();
        broker.waitUntilStarted();

        LOG.info(toString() + " Broker started!!");
    }

    protected static String getName() {
        return "DurableSubSelectorDelayTest";
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
