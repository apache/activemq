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
import org.apache.activemq.util.JMXSupport;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
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
import java.net.URI;
import java.util.Random;

import static javax.transaction.xa.XAResource.*;
import static org.junit.Assert.assertEquals;

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
    public void testAMQ7067XAcommit() throws Exception {

        PersistenceAdapterViewMBean kahadbView = getProxyToPersistenceAdapter(broker.getPersistenceAdapter().toString());
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

        Xid[] xids = xaRes.recover(TMSTARTRSCAN);

        //Should be 1 since we have only 1 prepared
        assertEquals(1, xids.length);
        connection.close();

        broker.stop();
        broker.waitUntilStopped();
        createBroker();

        setupXAConnection();
        xids = xaRes.recover(TMSTARTRSCAN);

        // THIS SHOULD NOT FAIL AS THERE SHOUL DBE ONLY 1 TRANSACTION!
        assertEquals(1, xids.length);

    }

    @Test
    public void testAMQ7067XArollback() throws Exception {

        PersistenceAdapterViewMBean kahadbView = getProxyToPersistenceAdapter(broker.getPersistenceAdapter().toString());
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
    public void testAMQ7067commit() throws Exception {
        final Connection connection = ACTIVE_MQ_NON_XA_CONNECTION_FACTORY.createConnection();
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
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
        Thread.sleep(10000);

        curruptIndexFile(getDataDirectory());


        while(true) {
            try {
                Thread.sleep(10000);
                System.out.println(String.format("QueueSize %s: %d", holdKahaDb.getQueueName(), getQueueSize(holdKahaDb.getQueueName())));
                break;
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
                break;
            }
        }

        connection.close();

        // THIS SHOULD NOT FAIL AS THERE SHOULD BE ONLY 1 TRANSACTION!
        assertEquals(1, getQueueSize(holdKahaDb.getQueueName()));
    }

    @Test
    public void testAMQ7067rollback() throws Exception {
        final Connection connection = ACTIVE_MQ_NON_XA_CONNECTION_FACTORY.createConnection();
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
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
        Thread.sleep(10000);

        curruptIndexFile(getDataDirectory());


        while(true) {
            try {
                Thread.sleep(10000);
                System.out.println(String.format("QueueSize %s: %d", holdKahaDb.getQueueName(), getQueueSize(holdKahaDb.getQueueName())));
                break;
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
                break;
            }
        }

        connection.close();

        // THIS SHOULD NOT FAIL AS THERE SHOULD ZERO TRANSACTION!
        assertEquals(0, getQueueSize(holdKahaDb.getQueueName()));
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
            xaRes.prepare(txid);
            xaRes.commit(txid, true);
        }
    }

    protected static void produce(Connection connection, Queue queue, int messageCount, int messageSize) throws JMSException, IOException, XAException {
        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
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