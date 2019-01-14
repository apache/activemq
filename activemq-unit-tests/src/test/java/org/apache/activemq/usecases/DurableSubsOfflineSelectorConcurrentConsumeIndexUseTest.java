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
package org.apache.activemq.usecases;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurableSubsOfflineSelectorConcurrentConsumeIndexUseTest extends org.apache.activemq.TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubsOfflineSelectorConcurrentConsumeIndexUseTest.class);
    public int messageCount = 10000;
    private BrokerService broker;
    private ActiveMQTopic topic;
    private final List<Throwable> exceptions = new ArrayList<Throwable>();

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://" + getName(true));
        connectionFactory.setWatchTopicAdvisories(false);
        return connectionFactory;
    }

    @Override
    protected Connection createConnection() throws Exception {
        return createConnection("id");
    }

    protected Connection createConnection(String name) throws Exception {
        Connection con = getConnectionFactory().createConnection();
        con.setClientID(name);
        con.start();
        return con;
    }

    public static Test suite() {
        return suite(DurableSubsOfflineSelectorConcurrentConsumeIndexUseTest.class);
    }

    @Override
    protected void setUp() throws Exception {
        exceptions.clear();
        topic = (ActiveMQTopic) createDestination();
        createBroker();
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    private void createBroker() throws Exception {
        createBroker(true);
    }

    private void createBroker(boolean deleteAllMessages) throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://" + getName(true) + ")");
        broker.setBrokerName(getName(true));
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.getManagementContext().setCreateConnector(false);
        broker.setAdvisorySupport(false);
        broker.addConnector("tcp://0.0.0.0:0");

        setDefaultPersistenceAdapter(broker);

        ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).getStore().getPageFile().setPageSize(1024);

        broker.start();
    }

    private void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    public void testIndexPageUsage() throws Exception {
        Connection con = createConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "true", "filter = 'true'", true);
        session.close();

        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "false", "filter = 'false'", true);
        session.close();

        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "all", null, true);
        session.close();

        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "all2", null, true);
        session.close();

        con.close();

        // send messages

        final CountDownLatch goOn = new CountDownLatch(1);
        Thread sendThread = new Thread() {
            @Override
            public void run() {
                try {

                    final Connection sendCon = createConnection("send");
                    final Session sendSession = sendCon.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    final MessageProducer producer = sendSession.createProducer(null);

                    for (int i = 0; i < messageCount; i++) {
                        boolean filter = i % 2 == 1;
                        Message message = sendSession.createMessage();
                        message.setStringProperty("filter", filter ? "true" : "false");
                        producer.send(topic, message);

                        if (i > 0 && i % 10000 == 0) {
                            LOG.info("Sent:" + i);
                        }
                        if (i> messageCount/2) {
                            goOn.countDown();
                        }
                    }
                    sendSession.close();
                    sendCon.close();
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        };
        sendThread.start();

        goOn.await(5, TimeUnit.MINUTES);
        LOG.info("Activating consumers");

        // consume messages in parallel
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumerTrue = session.createDurableSubscriber(topic, "true", "filter = 'true'", true);
        Listener listenerT = new Listener();
        consumerTrue.setMessageListener(listenerT);

        MessageConsumer consumerFalse = session.createDurableSubscriber(topic, "false", "filter = 'false'", true);
        Listener listenerF = new Listener();
        consumerFalse.setMessageListener(listenerF);

        MessageConsumer consumerAll = session.createDurableSubscriber(topic, "all", null, true);
        Listener listenerA = new Listener();
        consumerAll.setMessageListener(listenerA);

        MessageConsumer consumerAll2 = session.createDurableSubscriber(topic, "all2", null, true);
        Listener listenerA2 = new Listener();
        consumerAll2.setMessageListener(listenerA2);

        waitFor(listenerA, messageCount);
        assertEquals(messageCount, listenerA.count);

        waitFor(listenerA2, messageCount);
        assertEquals(messageCount, listenerA2.count);

        assertEquals(messageCount / 2, listenerT.count);
        assertEquals(messageCount / 2, listenerF.count);

        consumerTrue.close();
        session.unsubscribe("true");

        consumerFalse.close();
        session.unsubscribe("false");

        consumerAll.close();
        session.unsubscribe("all");

        session.close();
        con.close();

        PersistenceAdapter persistenceAdapter = broker.getPersistenceAdapter();
        if( persistenceAdapter instanceof KahaDBPersistenceAdapter) {
            final KahaDBStore store = ((KahaDBPersistenceAdapter) persistenceAdapter).getStore();
            LOG.info("Store page count: " + store.getPageFile().getPageCount());
            LOG.info("Store free page count: " + store.getPageFile().getFreePageCount());
            LOG.info("Store page in-use: " + (store.getPageFile().getPageCount() - store.getPageFile().getFreePageCount()));

            assertTrue("no leak of pages, always use just 11", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return 12 == store.getPageFile().getPageCount() -
                            store.getPageFile().getFreePageCount();
                }
            }, TimeUnit.SECONDS.toMillis(10)));
        }
    }

    private void waitFor(final Listener listener, final int count) throws Exception {

        assertTrue("got all messages on time", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return listener.count == count;
            }
        }, TimeUnit.MINUTES.toMillis(10)));

    }

    public static class Listener implements MessageListener {
        int count = 0;
        String id = null;

        Listener() {
        }

        @Override
        public void onMessage(Message message) {
            count++;
            if (id != null) {
                try {
                    LOG.info(id + ", " + message.getJMSMessageID());
                } catch (Exception ignored) {
                }
            }
        }
    }
}
