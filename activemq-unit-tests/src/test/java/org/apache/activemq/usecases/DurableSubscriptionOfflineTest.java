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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DurableSubscriptionOfflineTest extends DurableSubscriptionOfflineTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionOfflineTest.class);

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://" + getName(true));
        connectionFactory.setWatchTopicAdvisories(false);
        return connectionFactory;
    }

    @Test(timeout = 60 * 1000)
    public void testConsumeAllMatchedMessages() throws Exception {
        // create durable subscription
        Connection con = createConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);

        session.close();
        con.close();

        // consume messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals(sent, listener.count);
    }

    @Test(timeout = 60 * 1000)
    public void testBrowseOfflineSub() throws Exception {
        // create durable subscription
        Connection con = createConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId");
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        for (int i = 0; i < 10; i++) {
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);

        session.close();
        con.close();

        // browse the durable sub
        ObjectName[] subs = broker.getAdminView().getInactiveDurableTopicSubscribers();
        assertEquals(1, subs.length);
        ObjectName subName = subs[0];
        DurableSubscriptionViewMBean sub = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(subName, DurableSubscriptionViewMBean.class, true);
        CompositeData[] data  = sub.browse();
        assertNotNull(data);
        assertEquals(10, data.length);

        TabularData tabularData = sub.browseAsTable();
        assertNotNull(tabularData);
        assertEquals(10, tabularData.size());

    }

    @Test(timeout = 60 * 1000)
    public void testTwoOfflineSubscriptionCanConsume() throws Exception {
        // create durable subscription 1
        Connection con = createConnection("cliId1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // create durable subscription 2
        Connection con2 = createConnection("cliId2");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        DurableSubscriptionOfflineTestListener listener2 = new DurableSubscriptionOfflineTestListener();
        consumer2.setMessageListener(listener2);

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test online subs
        Thread.sleep(3 * 1000);
        session2.close();
        con2.close();

        assertEquals(sent, listener2.count);

        // consume messages
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals("offline consumer got all", sent, listener.count);
    }

    @Test(timeout = 60 * 1000)
    public void testRemovedDurableSubDeletes() throws Exception {
        String filter = "$a='A1' AND (($b=true AND $c=true) OR ($d='D1' OR $d='D2'))";
        // create durable subscription 1
        Connection con = createConnection("cliId1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        for (int i = 0; i < 10; i++) {
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);

        Connection con2 = createConnection("cliId1");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2.unsubscribe("SubsId");
        session2.close();
        con2.close();

        // see if retroactive can consumer any
        topic = new ActiveMQTopic(topic.getPhysicalName() + "?consumer.retroactive=true");
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);
        session.close();
        con.close();
        assertEquals(0, listener.count);
    }

    @Test(timeout = 60 * 1000)
    public void testRemovedDurableSubDeletesFromIndex() throws Exception {

        if (! (broker.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter)) {
            return;
        }

        final int numMessages = 2750;

        KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter)broker.getPersistenceAdapter();
        PageFile pageFile = kahaDBPersistenceAdapter.getStore().getPageFile();
        LOG.info("PageCount " + pageFile.getPageCount() + " f:" + pageFile.getFreePageCount() + ", fileSize:" + pageFile.getFile().length());

        long lastDiff = 0;
        for (int repeats=0; repeats<2; repeats++) {

            LOG.info("Iteration: "+ repeats  + " Count:" + pageFile.getPageCount() + " f:" + pageFile.getFreePageCount());

            Connection con = createConnection("cliId1" + "-" + repeats);
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
            session.close();
            con.close();

            // send messages
            con = createConnection();
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null);

            for (int i = 0; i < numMessages; i++) {
                Message message = session.createMessage();
                message.setStringProperty("filter", "true");
                producer.send(topic, message);
            }
            con.close();

            Connection con2 = createConnection("cliId1" + "-" + repeats);
            Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session2.unsubscribe("SubsId");
            session2.close();
            con2.close();

            LOG.info("PageCount " + pageFile.getPageCount() + " f:" + pageFile.getFreePageCount() +  " diff: " + (pageFile.getPageCount() - pageFile.getFreePageCount()) + " fileSize:" + pageFile.getFile().length());

            if (lastDiff != 0) {
                assertEquals("Only use X pages per iteration: " + repeats, lastDiff, pageFile.getPageCount() - pageFile.getFreePageCount());
            }
            lastDiff = pageFile.getPageCount() - pageFile.getFreePageCount();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testInterleavedOfflineSubscriptionCanConsumeAfterUnsub() throws Exception {
        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // create offline subs 2
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", null, true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            boolean filter = (int) (Math.random() * 2) >= 1;

            sent++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);

        Connection con2 = createConnection("offCli1");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2.unsubscribe("SubsId");
        session2.close();
        con2.close();

        // consume all messages
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener("SubsId");
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals("offline consumer got all", sent, listener.count);
    }

    @Test(timeout = 60 * 1000)
    public void testNoDuplicateOnConcurrentSendTranCommitAndActivate() throws Exception {
        final int messageCount = 1000;
        Connection con = null;
        Session session = null;
        final int numConsumers = 10;
        for (int i = 0; i <= numConsumers; i++) {
            con = createConnection("cli" + i);
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId", null, true);
            session.close();
            con.close();
        }

        class CheckForDupsClient implements Runnable {
            HashSet<Long> ids = new HashSet<Long>();
            final int id;

            public CheckForDupsClient(int id) {
                this.id = id;
            }

            @Override
            public void run() {
                try {
                    Connection con = createConnection("cli" + id);
                    Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    for (int j=0;j<2;j++) {
                        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
                        for (int i = 0; i < messageCount/2; i++) {
                            Message message = consumer.receive(4000);
                            assertNotNull(message);
                            long producerSequenceId = new MessageId(message.getJMSMessageID()).getProducerSequenceId();
                            assertTrue("ID=" + id + " not a duplicate: " + producerSequenceId, ids.add(producerSequenceId));
                        }
                        consumer.close();
                    }

                    // verify no duplicates left
                    MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
                    Message message = consumer.receive(4000);
                    if (message != null) {
                        long producerSequenceId = new MessageId(message.getJMSMessageID()).getProducerSequenceId();
                        assertTrue("ID=" + id + " not a duplicate: " + producerSequenceId, ids.add(producerSequenceId));
                    }
                    assertNull(message);


                    session.close();
                    con.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                    exceptions.add(e);
                }
            }
        }

        final String payLoad = new String(new byte[1000]);
        con = createConnection();
        final Session sendSession = con.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = sendSession.createProducer(topic);
        for (int i = 0; i < messageCount; i++) {
            producer.send(sendSession.createTextMessage(payLoad));
        }

        ExecutorService executorService = Executors.newCachedThreadPool();

        // concurrent commit and activate
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sendSession.commit();
                } catch (JMSException e) {
                    e.printStackTrace();
                    exceptions.add(e);
                }
            }
        });
        for (int i = 0; i < numConsumers; i++) {
            executorService.execute(new CheckForDupsClient(i));
        }

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);
        con.close();

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }


    @Test(timeout = 2 * 60 * 1000)
    public void testOrderOnActivateDeactivate() throws Exception {
        for (int i=0;i<10;i++) {
            LOG.info("Iteration: " + i);
            doTestOrderOnActivateDeactivate();
            broker.stop();
            broker.waitUntilStopped();
            createBroker(true /*deleteAllMessages*/);
        }
    }


    public void doTestOrderOnActivateDeactivate() throws Exception {
        final int messageCount = 1000;
        Connection con = null;
        Session session = null;
        final int numConsumers = 4;
        for (int i = 0; i <= numConsumers; i++) {
            con = createConnection("cli" + i);
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(topic, "SubsId", null, true);
            session.close();
            con.close();
        }

        final String url = "failover:(tcp://localhost:"
                + (broker.getTransportConnectors().get(1).getConnectUri()).getPort()
                + "?wireFormat.maxInactivityDuration=0)?"
                + "jms.watchTopicAdvisories=false&"
                + "jms.alwaysSyncSend=true&jms.dispatchAsync=true&"
                + "jms.sendAcksAsync=true&"
                + "initialReconnectDelay=100&maxReconnectDelay=30000&"
                + "useExponentialBackOff=true";
        final ActiveMQConnectionFactory clientFactory = new ActiveMQConnectionFactory(url);

        class CheckOrderClient implements Runnable {
            final int id;
            int runCount = 0;

            public CheckOrderClient(int id) {
                this.id = id;
            }

            @Override
            public void run() {
                try {
                    synchronized (this) {
                        Connection con = clientFactory.createConnection();
                        con.setClientID("cli" + id);
                        con.start();
                        Session session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
                        int nextId = 0;

                        ++runCount;
                        int i=0;
                        for (; i < messageCount/2; i++) {
                            Message message = consumer.receiveNoWait();
                            if (message == null) {
                                break;
                            }
                            long producerSequenceId = new MessageId(message.getJMSMessageID()).getProducerSequenceId();
                            assertEquals(id + " expected order: runCount: " + runCount  + " id: " + message.getJMSMessageID(), ++nextId, producerSequenceId);
                        }
                        LOG.info(con.getClientID() + " peeked " + i);
                        session.close();
                        con.close();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    exceptions.add(e);
                }
            }
        }

        Runnable producer = new Runnable() {
            final String payLoad = new String(new byte[600]);

            @Override
            public void run() {
                try {
                    Connection con = createConnection();
                    final Session sendSession = con.createSession(true, Session.SESSION_TRANSACTED);
                    MessageProducer producer = sendSession.createProducer(topic);
                    for (int i = 0; i < messageCount; i++) {
                        producer.send(sendSession.createTextMessage(payLoad));
                    }
                    LOG.info("About to commit: " + messageCount);
                    sendSession.commit();
                    LOG.info("committed: " + messageCount);
                    con.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptions.add(e);
                }
            }
        };

        ExecutorService executorService = Executors.newCachedThreadPool();

        // concurrent commit and activate
        for (int i = 0; i < numConsumers; i++) {
            final CheckOrderClient client = new CheckOrderClient(i);
            for (int j=0; j<100; j++) {
                executorService.execute(client);
            }
        }
        executorService.execute(producer);

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);
        con.close();

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testUnmatchedSubUnsubscribeDeletesAll() throws Exception {
        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int filtered = 0;
        for (int i = 0; i < 10; i++) {
            boolean filter = (i %2 == 0); //(int) (Math.random() * 2) >= 1;
            if (filter)
                filtered++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        LOG.info("sent: " + filtered);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test offline subs
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.unsubscribe("SubsId");
        session.close();
        con.close();

        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals(0, listener.count);
    }

    @Test(timeout = 60 * 1000)
    public void testAllConsumed() throws Exception {
        final String filter = "filter = 'true'";
        Connection con = createConnection("cli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
            sent++;
        }

        LOG.info("sent: " + sent);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        con = createConnection("cli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);
        Thread.sleep(3 * 1000);
        session.close();
        con.close();

        assertEquals(sent, listener.count);

        LOG.info("cli2 pull 2");
        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        assertNotNull("got message", consumer.receive(2000));
        assertNotNull("got message", consumer.receive(2000));
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);

        sent = 0;
        for (int i = 0; i < 2; i++) {
            Message message = session.createMessage();
            message.setStringProperty("filter", i==1 ? "true" : "false");
            producer.send(topic, message);
            sent++;
        }
        LOG.info("sent: " + sent);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        LOG.info("cli1 again, should get 1 new ones");
        con = createConnection("cli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);
        Thread.sleep(3 * 1000);
        session.close();
        con.close();

        assertEquals(1, listener.count);
    }

    // https://issues.apache.org/jira/browse/AMQ-3190
    @Test(timeout = 60 * 1000)
    public void testNoMissOnMatchingSubAfterRestart() throws Exception {

        final String filter = "filter = 'true'";
        Connection con = createConnection("cli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        // send unmatched messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        // message for cli1 to keep it interested
        Message message = session.createMessage();
        message.setStringProperty("filter", "true");
        message.setIntProperty("ID", 0);
        producer.send(topic, message);
        sent++;

        for (int i = sent; i < 10; i++) {
            message = session.createMessage();
            message.setStringProperty("filter", "false");
            message.setIntProperty("ID", i);
            producer.send(topic, message);
            sent++;
        }
        con.close();
        LOG.info("sent: " + sent);

        // new sub at id 10
        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        destroyBroker();
        createBroker(false);

        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);

        for (int i = sent; i < 30; i++) {
            message = session.createMessage();
            message.setStringProperty("filter", "true");
            message.setIntProperty("ID", i);
            producer.send(topic, message);
            sent++;
        }
        con.close();
        LOG.info("sent: " + sent);

        // pick up the first of the next twenty messages
        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        Message m = consumer.receive(3000);
        assertEquals("is message 10", 10, m.getIntProperty("ID"));

        session.close();
        con.close();

        // pick up the first few messages for client1
        con = createConnection("cli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        m = consumer.receive(3000);
        assertEquals("is message 0", 0, m.getIntProperty("ID"));
        m = consumer.receive(3000);
        assertEquals("is message 10", 10, m.getIntProperty("ID"));

        session.close();
        con.close();
    }

    @org.junit.Test(timeout = 640000)
    public void testInactiveSubscribeAfterBrokerRestart() throws Exception {
        final int messageCount = 20;
        Connection alwaysOnCon = createConnection("subs1");
        Connection tearDownFacCon = createConnection("subs2");
        Session awaysOnCon = alwaysOnCon.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session tearDownCon = tearDownFacCon.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQTopic topic = new ActiveMQTopic("TEST.FOO");
        String consumerName = "consumerName";
        String tearDownconsumerName = "tearDownconsumerName";
        // Setup consumers
        MessageConsumer remoteConsumer = awaysOnCon.createDurableSubscriber(topic, consumerName);
        MessageConsumer remoteConsumer2 = tearDownCon.createDurableSubscriber(topic, tearDownconsumerName);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener("listener");
        remoteConsumer.setMessageListener(listener);
        remoteConsumer2.setMessageListener(listener);
        // Setup producer
        MessageProducer localProducer = awaysOnCon.createProducer(topic);
        localProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        // Send messages
        for (int i = 0; i < messageCount; i++) {
            if (i == 10) {
                remoteConsumer2.close();
                tearDownFacCon.close();
            }
            Message test = awaysOnCon.createTextMessage("test-" + i);
            localProducer.send(test);
        }
        destroyBroker();
        createBroker(false);
        Connection reconnectCon = createConnection("subs2");
        Session reconnectSession = reconnectCon.createSession(false, Session.AUTO_ACKNOWLEDGE);
        remoteConsumer2 = reconnectSession.createDurableSubscriber(topic, tearDownconsumerName);
        remoteConsumer2.setMessageListener(listener);
        LOG.info("waiting for messages to flow");
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return listener.count >= messageCount * 2;
            }
        });
        assertTrue("At least message " + messageCount * 2 +
                        " must be received, count=" + listener.count,
                messageCount * 2 <= listener.count);
        awaysOnCon.close();
        reconnectCon.close();
    }


//    // https://issues.apache.org/jira/browse/AMQ-3768
//    public void testPageReuse() throws Exception {
//        Connection con = null;
//        Session session = null;
//
//        final int numConsumers = 115;
//        for (int i=0; i<=numConsumers;i++) {
//            con = createConnection("cli" + i);
//            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            session.createDurableSubscriber(topic, "SubsId", null, true);
//            session.close();
//            con.close();
//        }
//
//        // populate ack locations
//        con = createConnection();
//        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//        MessageProducer producer = session.createProducer(null);
//        Message message = session.createTextMessage(new byte[10].toString());
//        producer.send(topic, message);
//        con.close();
//
//        // we have a split, remove all but the last so that
//        // the head pageid changes in the acklocations listindex
//        for (int i=0; i<=numConsumers -1; i++) {
//            con = createConnection("cli" + i);
//            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            session.unsubscribe("SubsId");
//            session.close();
//            con.close();
//        }
//
//        destroyBroker();
//        createBroker(false);
//
//        // create a bunch more subs to reuse the freed page and get us in a knot
//        for (int i=1; i<=numConsumers;i++) {
//            con = createConnection("cli" + i);
//            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            session.createDurableSubscriber(topic, "SubsId", filter, true);
//            session.close();
//            con.close();
//        }
//    }
//
//    public void testRedeliveryFlag() throws Exception {
//
//        Connection con;
//        Session session;
//        final int numClients = 2;
//        for (int i=0; i<numClients; i++) {
//            con = createConnection("cliId" + i);
//            session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//            session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//            session.close();
//            con.close();
//        }
//
//        final Random random = new Random();
//
//        // send messages
//        con = createConnection();
//        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//        MessageProducer producer = session.createProducer(null);
//
//        final int count = 1000;
//        for (int i = 0; i < count; i++) {
//            Message message = session.createMessage();
//            message.setStringProperty("filter", "true");
//            producer.send(topic, message);
//        }
//        session.close();
//        con.close();
//
//        class Client implements Runnable {
//            Connection con;
//            Session session;
//            String clientId;
//            Client(String id) {
//                this.clientId = id;
//            }
//
//            @Override
//            public void run() {
//                MessageConsumer consumer = null;
//                Message message = null;
//
//                try {
//                    for (int i = -1; i < random.nextInt(10); i++) {
//                        // go online and take none
//                        con = createConnection(clientId);
//                        session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                        consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//                        session.close();
//                        con.close();
//                    }
//
//                    // consume 1
//                    con = createConnection(clientId);
//                    session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                    consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//                    message = consumer.receive(4000);
//                    assertNotNull("got message", message);
//                    // it is not reliable as it depends on broker dispatch rather than client receipt
//                    // and delivered ack
//                    //  assertFalse("not redelivered", message.getJMSRedelivered());
//                    message.acknowledge();
//                    session.close();
//                    con.close();
//
//                    // peek all
//                    for (int j = -1; j < random.nextInt(10); j++) {
//                        con = createConnection(clientId);
//                        session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                        consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//
//                        for (int i = 0; i < count - 1; i++) {
//                            assertNotNull("got message", consumer.receive(4000));
//                        }
//                        // no ack
//                        session.close();
//                        con.close();
//                    }
//
//                    // consume remaining
//                    con = createConnection(clientId);
//                    session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                    consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//
//                    for (int i = 0; i < count - 1; i++) {
//                        message = consumer.receive(4000);
//                        assertNotNull("got message", message);
//                        assertTrue("is redelivered", message.getJMSRedelivered());
//                    }
//                    message.acknowledge();
//                    session.close();
//                    con.close();
//
//                    con = createConnection(clientId);
//                    session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//                    consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
//                    assertNull("no message left", consumer.receive(2000));
//                } catch (Throwable throwable) {
//                    throwable.printStackTrace();
//                    exceptions.add(throwable);
//                }
//            }
//        }
//        ExecutorService executorService = Executors.newCachedThreadPool();
//        for (int i=0; i<numClients; i++) {
//            executorService.execute(new Client("cliId" + i));
//        }
//        executorService.shutdown();
//        executorService.awaitTermination(10, TimeUnit.MINUTES);
//        assertTrue("No exceptions expected, but was: " + exceptions, exceptions.isEmpty());
//    }

}
