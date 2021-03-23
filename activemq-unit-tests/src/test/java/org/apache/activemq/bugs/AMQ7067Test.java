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
package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnection;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.PersistenceAdapterViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.RecoveredXATransactionViewMBean;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MessageDatabase;
import org.apache.activemq.util.JMXSupport;
import org.apache.activemq.util.Wait;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static javax.transaction.xa.XAResource.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AMQ7067Test {

    protected static Random r = new Random();
    final static String WIRE_LEVEL_ENDPOINT = "tcp://localhost:61616";
    protected BrokerService broker;
    protected ActiveMQXAConnection connection;
    protected XASession xaSession;
    protected XAResource xaRes;
    private final String xbean = "xbean:";
    private final String confBase = "src/test/resources/org/apache/activemq/bugs/amq7067";


    private static final ActiveMQXAConnectionFactory ACTIVE_MQ_CONNECTION_FACTORY;
    private static final ActiveMQConnectionFactory ACTIVE_MQ_NON_XA_CONNECTION_FACTORY;

    static {
        ACTIVE_MQ_CONNECTION_FACTORY = new ActiveMQXAConnectionFactory(WIRE_LEVEL_ENDPOINT);
        ACTIVE_MQ_NON_XA_CONNECTION_FACTORY = new ActiveMQConnectionFactory(WIRE_LEVEL_ENDPOINT);
    }

    @Before
    public void setup() throws Exception {
        deleteData(new File("target/data"));
        createBroker();
    }

    @After
    public void shutdown() throws Exception {
        broker.stop();
    }

    public void setupXAConnection() throws Exception {
        connection = (ActiveMQXAConnection) ACTIVE_MQ_CONNECTION_FACTORY.createXAConnection();
        connection.start();
        xaSession = connection.createXASession();
        xaRes = xaSession.getXAResource();
    }

    private void createBroker() throws Exception {
        broker = new BrokerService();
        broker = BrokerFactory.createBroker(xbean + confBase + "/activemq.xml");
        broker.start();
    }

    @Test
    public void testXAPrepare() throws Exception {

        setupXAConnection();

        Queue holdKahaDb = xaSession.createQueue("holdKahaDb");

        MessageProducer holdKahaDbProducer = xaSession.createProducer(holdKahaDb);

        XATransactionId txid = createXATransaction();
        System.out.println("****** create new txid = " + txid);
        xaRes.start(txid, TMNOFLAGS);

        TextMessage helloMessage = xaSession.createTextMessage(StringUtils.repeat("a", 10));
        holdKahaDbProducer.send(helloMessage);
        xaRes.end(txid, TMSUCCESS);

        Queue queue = xaSession.createQueue("test");

        produce(xaRes, xaSession, queue, 100, 512 * 1024);

        xaRes.prepare(txid);

        produce(xaRes, xaSession, queue, 100, 512 * 1024);

        ((org.apache.activemq.broker.region.Queue) broker.getRegionBroker().getDestinationMap().get(queue)).purge();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == getQueueSize(queue.getQueueName());
            }
        });

        // force gc
        broker.getPersistenceAdapter().checkpoint(true);

        Xid[] xids = xaRes.recover(TMSTARTRSCAN);

        //Should be 1 since we have only 1 prepared
        assertEquals(1, xids.length);
        connection.close();

        broker.stop();
        broker.waitUntilStopped();
        createBroker();

        setupXAConnection();
        xids = xaRes.recover(TMSTARTRSCAN);

        System.out.println("****** recovered = " + xids);

        // THIS SHOULD NOT FAIL AS THERE SHOULD DBE ONLY 1 TRANSACTION!
        assertEquals(1, xids.length);
    }

    @Test
    public void testXAPrepareWithAckCompactionDoesNotLooseInflight() throws Exception {

        // investigate liner gc issue - store usage not getting released
        org.apache.log4j.Logger.getLogger(MessageDatabase.class).setLevel(Level.TRACE);


        setupXAConnection();

        Queue holdKahaDb = xaSession.createQueue("holdKahaDb");

        MessageProducer holdKahaDbProducer = xaSession.createProducer(holdKahaDb);

        XATransactionId txid = createXATransaction();
        System.out.println("****** create new txid = " + txid);
        xaRes.start(txid, TMNOFLAGS);

        TextMessage helloMessage = xaSession.createTextMessage(StringUtils.repeat("a", 10));
        holdKahaDbProducer.send(helloMessage);
        xaRes.end(txid, TMSUCCESS);

        Queue queue = xaSession.createQueue("test");

        produce(xaRes, xaSession, queue, 100, 512 * 1024);

        xaRes.prepare(txid);

        produce(xaRes, xaSession, queue, 100, 512 * 1024);

        ((org.apache.activemq.broker.region.Queue) broker.getRegionBroker().getDestinationMap().get(queue)).purge();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == getQueueSize(queue.getQueueName());
            }
        });

        // force gc, two data files requires two cycles
        int limit = ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).getCompactAcksAfterNoGC() + 1;
        for (int i=0; i<limit*2; i++) {
            broker.getPersistenceAdapter().checkpoint(true);
        }

        // ack compaction task operates in the background
        TimeUnit.SECONDS.sleep(5);

        Xid[] xids = xaRes.recover(TMSTARTRSCAN);

        //Should be 1 since we have only 1 prepared
        assertEquals(1, xids.length);
        connection.close();

        broker.stop();
        broker.waitUntilStopped();
        createBroker();

        setupXAConnection();
        xids = xaRes.recover(TMSTARTRSCAN);

        System.out.println("****** recovered = " + xids);

        // THIS SHOULD NOT FAIL AS THERE SHOULD DBE ONLY 1 TRANSACTION!
        assertEquals(1, xids.length);
    }

    @Test
    public void testXACommitWithAckCompactionDoesNotLooseOutcomeOnFullRecovery() throws Exception {
        doTestXACompletionWithAckCompactionDoesNotLooseOutcomeOnFullRecovery(true);
    }

    @Test
    public void testXARollbackWithAckCompactionDoesNotLooseOutcomeOnFullRecovery() throws Exception {
        doTestXACompletionWithAckCompactionDoesNotLooseOutcomeOnFullRecovery(false);
    }

    protected void doTestXACompletionWithAckCompactionDoesNotLooseOutcomeOnFullRecovery(boolean commit) throws Exception {

        ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).setCompactAcksAfterNoGC(2);
        // investigate liner gc issue - store usage not getting released
        org.apache.log4j.Logger.getLogger(MessageDatabase.class).setLevel(Level.TRACE);


        setupXAConnection();

        Queue holdKahaDb = xaSession.createQueue("holdKahaDb");

        MessageProducer holdKahaDbProducer = xaSession.createProducer(holdKahaDb);

        XATransactionId txid = createXATransaction();
        System.out.println("****** create new txid = " + txid);
        xaRes.start(txid, TMNOFLAGS);

        TextMessage helloMessage = xaSession.createTextMessage(StringUtils.repeat("a", 10));
        holdKahaDbProducer.send(helloMessage);
        xaRes.end(txid, TMSUCCESS);

        Queue queue = xaSession.createQueue("test");

        produce(xaRes, xaSession, queue, 100, 512 * 1024);
        ((org.apache.activemq.broker.region.Queue) broker.getRegionBroker().getDestinationMap().get(queue)).purge();

        xaRes.prepare(txid);

        // hold onto data file with prepare record
        produce(xaRes, xaSession, holdKahaDb, 1, 10);

        produce(xaRes, xaSession, queue, 50, 512 * 1024);
        ((org.apache.activemq.broker.region.Queue) broker.getRegionBroker().getDestinationMap().get(queue)).purge();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == getQueueSize(queue.getQueueName());
            }
        });

        if (commit) {
            xaRes.commit(txid, false);
        } else {
            xaRes.rollback(txid);
        }

        produce(xaRes, xaSession, queue, 50, 512 * 1024);
        ((org.apache.activemq.broker.region.Queue) broker.getRegionBroker().getDestinationMap().get(queue)).purge();


        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == getQueueSize(queue.getQueueName());
            }
        });

        int limit = ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).getCompactAcksAfterNoGC() + 1;
        // force gc, n data files requires n cycles
        for (int dataFilesToMove = 0; dataFilesToMove < 4; dataFilesToMove++) {
            for (int i = 0; i < limit; i++) {
                broker.getPersistenceAdapter().checkpoint(true);
            }
            // ack compaction task operates in the background
            TimeUnit.SECONDS.sleep(2);
        }


        Xid[] xids = xaRes.recover(TMSTARTRSCAN);

        //Should be 0 since we have delivered the outcome
        assertEquals(0, xids.length);
        connection.close();

        // need full recovery to see lost commit record
        curruptIndexFile(getDataDirectory());

        broker.stop();
        broker.waitUntilStopped();
        createBroker();

        setupXAConnection();
        xids = xaRes.recover(TMSTARTRSCAN);

        System.out.println("****** recovered = " + xids);

        assertEquals(0, xids.length);
    }

    @Test
    public void testXAcommit() throws Exception {

        setupXAConnection();

        Queue holdKahaDb = xaSession.createQueue("holdKahaDb");
        createDanglingTransaction(xaRes, xaSession, holdKahaDb);

        MessageProducer holdKahaDbProducer = xaSession.createProducer(holdKahaDb);

        XATransactionId txid = createXATransaction();
        System.out.println("****** create new txid = " + txid);
        xaRes.start(txid, TMNOFLAGS);

        TextMessage helloMessage = xaSession.createTextMessage(StringUtils.repeat("a", 10));
        holdKahaDbProducer.send(helloMessage);
        xaRes.end(txid, TMSUCCESS);
        xaRes.prepare(txid);

        Queue queue = xaSession.createQueue("test");

        produce(xaRes, xaSession, queue, 100, 512 * 1024);
        xaRes.commit(txid, false);
        produce(xaRes, xaSession, queue, 100, 512 * 1024);

        ((org.apache.activemq.broker.region.Queue) broker.getRegionBroker().getDestinationMap().get(queue)).purge();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == getQueueSize(queue.getQueueName());
            }
        });

        // force gc
        broker.getPersistenceAdapter().checkpoint(true);

        Xid[] xids = xaRes.recover(TMSTARTRSCAN);

        //Should be 1 since we have only 1 prepared
        assertEquals(1, xids.length);
        connection.close();

        broker.stop();
        broker.waitUntilStopped();
        createBroker();

        setupXAConnection();
        xids = xaRes.recover(TMSTARTRSCAN);

        // THIS SHOULD NOT FAIL AS THERE SHOULD DBE ONLY 1 TRANSACTION!
        assertEquals(1, xids.length);

    }

    @Test
    public void testXArollback() throws Exception {

        setupXAConnection();

        Queue holdKahaDb = xaSession.createQueue("holdKahaDb");
        createDanglingTransaction(xaRes, xaSession, holdKahaDb);

        MessageProducer holdKahaDbProducer = xaSession.createProducer(holdKahaDb);

        XATransactionId txid = createXATransaction();
        System.out.println("****** create new txid = " + txid);
        xaRes.start(txid, TMNOFLAGS);

        TextMessage helloMessage = xaSession.createTextMessage(StringUtils.repeat("a", 10));
        holdKahaDbProducer.send(helloMessage);
        xaRes.end(txid, TMSUCCESS);
        xaRes.prepare(txid);

        Queue queue = xaSession.createQueue("test");

        produce(xaRes, xaSession, queue, 100, 512 * 1024);
        xaRes.rollback(txid);
        produce(xaRes, xaSession, queue, 100, 512 * 1024);

        ((org.apache.activemq.broker.region.Queue) broker.getRegionBroker().getDestinationMap().get(queue)).purge();

        Xid[] xids = xaRes.recover(TMSTARTRSCAN);

        //Should be 1 since we have only 1 prepared
        assertEquals(1, xids.length);
        connection.close();

        broker.stop();
        broker.waitUntilStopped();
        createBroker();

        setupXAConnection();
        xids = xaRes.recover(TMSTARTRSCAN);

        // THIS SHOULD NOT FAIL AS THERE SHOULD BE ONLY 1 TRANSACTION!
        assertEquals(1, xids.length);

    }

    @Test
    public void testCommit() throws Exception {
        final Connection connection = ACTIVE_MQ_NON_XA_CONNECTION_FACTORY.createConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue holdKahaDb = session.createQueue("holdKahaDb");
        MessageProducer holdKahaDbProducer = session.createProducer(holdKahaDb);
        TextMessage helloMessage = session.createTextMessage(StringUtils.repeat("a", 10));
        holdKahaDbProducer.send(helloMessage);
        Queue queue = session.createQueue("test");
        produce(connection, queue, 100, 512*1024);
        session.commit();
        produce(connection, queue, 100, 512*1024);

        System.out.println(String.format("QueueSize %s: %d", holdKahaDb.getQueueName(), getQueueSize(holdKahaDb.getQueueName())));
        purgeQueue(queue.getQueueName());
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == getQueueSize(queue.getQueueName());
            }
        });

        // force gc
        broker.getPersistenceAdapter().checkpoint(true);


        connection.close();
        curruptIndexFile(getDataDirectory());

        broker.stop();
        broker.waitUntilStopped();
        createBroker();
        broker.waitUntilStarted();

        while(true) {
            try {
                TimeUnit.SECONDS.sleep(1);
                System.out.println(String.format("QueueSize %s: %d", holdKahaDb.getQueueName(), getQueueSize(holdKahaDb.getQueueName())));
                break;
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
                break;
            }
        }

        // THIS SHOULD NOT FAIL AS THERE SHOULD BE ONLY 1 TRANSACTION!
        assertEquals(1, getQueueSize(holdKahaDb.getQueueName()));
    }

    @Test
    public void testRollback() throws Exception {
        final Connection connection = ACTIVE_MQ_NON_XA_CONNECTION_FACTORY.createConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue holdKahaDb = session.createQueue("holdKahaDb");
        MessageProducer holdKahaDbProducer = session.createProducer(holdKahaDb);
        TextMessage helloMessage = session.createTextMessage(StringUtils.repeat("a", 10));
        holdKahaDbProducer.send(helloMessage);
        Queue queue = session.createQueue("test");
        produce(connection, queue, 100, 512*1024);
        session.rollback();
        produce(connection, queue, 100, 512*1024);

        System.out.println(String.format("QueueSize %s: %d", holdKahaDb.getQueueName(), getQueueSize(holdKahaDb.getQueueName())));
        purgeQueue(queue.getQueueName());

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == getQueueSize(queue.getQueueName());
            }
        });

        // force gc
        broker.getPersistenceAdapter().checkpoint(true);

        connection.close();
        curruptIndexFile(getDataDirectory());

        broker.stop();
        broker.waitUntilStopped();
        createBroker();
        broker.waitUntilStarted();


        // no sign of the test queue on recovery, rollback is the default for any inflight
        // this test serves as a sanity check on existing behaviour
        try {
            getQueueSize(holdKahaDb.getQueueName());
            fail("expect InstanceNotFoundException");
        } catch (UndeclaredThrowableException expected) {
            assertTrue(expected.getCause() instanceof InstanceNotFoundException);
        }
    }

    @Test
    public void testForwardAcksAndCommitsWithLocalTransaction() throws Exception {
        ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).setCompactAcksAfterNoGC(2);
        final Connection connection = ACTIVE_MQ_NON_XA_CONNECTION_FACTORY.createConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue holdKahaDb = session.createQueue("holdKahaDb");
        MessageProducer holdKahaDbProducer = session.createProducer(holdKahaDb);
        TextMessage helloMessage = session.createTextMessage(StringUtils.repeat("a", 10));
        holdKahaDbProducer.send(helloMessage);
        session.commit();
        Queue queue = session.createQueue("test");

        for (int i = 0; i < 5; i++) {
            produce(connection, queue, 60, 512 * 1024);
            consume(connection, queue, 30, true);
        }

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 150 == getQueueSize(queue.getQueueName());
            }
        });

        // force gc, n data files requires n cycles
        int limit = ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).getCompactAcksAfterNoGC() + 1;
        for (int dataFilesToMove = 0; dataFilesToMove < 10; dataFilesToMove++) {
            for (int i = 0; i < limit; i++) {
                broker.getPersistenceAdapter().checkpoint(true);
            }
            // ack compaction task operates in the background
            TimeUnit.SECONDS.sleep(2);
        }

        session.commit();

        connection.close();
        curruptIndexFile(getDataDirectory());

        broker.stop();
        broker.waitUntilStopped();
        createBroker();
        broker.waitUntilStarted();

        while(true) {
            try {
                TimeUnit.SECONDS.sleep(1);
                System.out.println(String.format("QueueSize %s: %d", holdKahaDb.getQueueName(), getQueueSize(holdKahaDb.getQueueName())));
                break;
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
                break;
            }
        }

        assertEquals(1, getQueueSize(holdKahaDb.getQueueName()));
        assertEquals(150, getQueueSize(queue.getQueueName()));
    }

    private static void consume(Connection connection, Queue queue, int messageCount, boolean transacted) throws JMSException {
        final Session session = connection.createSession(transacted, transacted ? 0 : Session.AUTO_ACKNOWLEDGE);
        final MessageConsumer consumer = session.createConsumer(queue);

        int messagesConsumed = 0;

        while (consumer.receive(1000) != null && messagesConsumed < messageCount) {
            messagesConsumed++;
            session.commit();
        }

        System.out.println(messagesConsumed + " messages consumed from " + queue.getQueueName());
        session.close();
    }

    protected static void createDanglingTransaction(XAResource xaRes, XASession xaSession, Queue queue) throws JMSException, IOException, XAException {
        MessageProducer producer = xaSession.createProducer(queue);
        XATransactionId txId = createXATransaction();
        xaRes.start(txId, TMNOFLAGS);

        TextMessage helloMessage = xaSession.createTextMessage(StringUtils.repeat("dangler", 10));
        producer.send(helloMessage);
        xaRes.end(txId, TMSUCCESS);
        xaRes.prepare(txId);
        System.out.println("****** createDanglingTransaction txId = " + txId);
    }

    protected static void produce(XAResource xaRes, XASession xaSession, Queue queue, int messageCount, int messageSize) throws JMSException, IOException, XAException {
        MessageProducer producer = xaSession.createProducer(queue);

        for (int i = 0; i < messageCount; i++) {
            XATransactionId txid = createXATransaction();
            xaRes.start(txid, TMNOFLAGS);

            TextMessage helloMessage = xaSession.createTextMessage(StringUtils.repeat("a", messageSize));
            producer.send(helloMessage);
            xaRes.end(txid, TMSUCCESS);
            xaRes.commit(txid, true);
        }
    }

    protected static void produce(Connection connection, Queue queue, int messageCount, int messageSize) throws JMSException, IOException, XAException {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < messageCount; i++) {
            TextMessage helloMessage = session.createTextMessage(StringUtils.repeat("a", messageSize));
            producer.send(helloMessage);
            session.commit();

        }
    }

    protected static XATransactionId createXATransaction() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(r.nextInt());
        os.close();
        byte[] bs = baos.toByteArray();

        XATransactionId xid = new XATransactionId();
        xid.setBranchQualifier(bs);
        xid.setGlobalTransactionId(bs);
        xid.setFormatId(55);
        return xid;
    }

    private RecoveredXATransactionViewMBean getProxyToPreparedTransactionViewMBean(TransactionId xid) throws MalformedObjectNameException, JMSException {

        ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,transactionType=RecoveredXaTransaction,xid=" +
                JMXSupport.encodeObjectNamePart(xid.toString()));
        RecoveredXATransactionViewMBean proxy = (RecoveredXATransactionViewMBean) broker.getManagementContext().newProxyInstance(objectName,
                RecoveredXATransactionViewMBean.class, true);
        return proxy;
    }

    private PersistenceAdapterViewMBean getProxyToPersistenceAdapter(String name) throws MalformedObjectNameException, JMSException {
        return (PersistenceAdapterViewMBean) broker.getManagementContext().newProxyInstance(
                BrokerMBeanSupport.createPersistenceAdapterName(broker.getBrokerObjectName().toString(), name),
                PersistenceAdapterViewMBean.class, true);
    }

    private void deleteData(File file) throws Exception {
        String[] entries = file.list();
        if (entries == null) return;
        for (String s : entries) {
            File currentFile = new File(file.getPath(), s);
            if (currentFile.isDirectory()) {
                deleteData(currentFile);
            }
            currentFile.delete();
        }
        file.delete();
    }

    private long getQueueSize(final String queueName) throws MalformedObjectNameException {
        ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + JMXSupport.encodeObjectNamePart(queueName));
        DestinationViewMBean proxy = (DestinationViewMBean) broker.getManagementContext().newProxyInstance(objectName, DestinationViewMBean.class, true);
        return proxy.getQueueSize();
    }

    private void purgeQueue(final String queueName) throws MalformedObjectNameException, Exception {
        ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + JMXSupport.encodeObjectNamePart(queueName));
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext().newProxyInstance(objectName, QueueViewMBean.class, true);
        proxy.purge();
    }

    private String getDataDirectory() throws MalformedObjectNameException {
        ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean proxy = (BrokerViewMBean) broker.getManagementContext().newProxyInstance(objectName, BrokerViewMBean.class, true);
        return proxy.getDataDirectory();
    }

    protected static void curruptIndexFile(final String dataPath) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(String.format("%s/kahadb/db.data", dataPath), "UTF-8");
        writer.println("asdasdasd");
        writer.close();
    }
}