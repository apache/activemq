package org.apache.activemq.broker.replica;

import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class ReplicaPluginTopicTest extends ReplicaPluginTestSupport {

    private static final String CLIENT_ID_ONE = "one";
    private static final String CLIENT_ID_TWO = "two";
    private static final String CLIENT_ID_XA = "xa";

    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    protected Connection firstBrokerConnection2;
    protected Connection secondBrokerConnection2;

    protected XAConnection firstBrokerXAConnection;
    protected Connection secondBrokerXAConnection;

    private static long txGenerator = 67;

    @Override
    protected void setUp() throws Exception {
        useTopic = true;

        super.setUp();
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.setClientID(CLIENT_ID_ONE);
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.setClientID(CLIENT_ID_ONE);
        secondBrokerConnection.start();

        firstBrokerConnection2 = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection2.setClientID(CLIENT_ID_TWO);
        firstBrokerConnection2.start();

        secondBrokerConnection2 = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection2.setClientID(CLIENT_ID_TWO);
        secondBrokerConnection2.start();

        firstBrokerXAConnection = firstBrokerXAConnectionFactory.createXAConnection();
        firstBrokerXAConnection.setClientID(CLIENT_ID_XA);
        firstBrokerXAConnection.start();

        secondBrokerXAConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerXAConnection.setClientID(CLIENT_ID_XA);
        secondBrokerXAConnection.start();
    }

    @Override
    protected void tearDown() throws Exception {
        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }

        if (firstBrokerXAConnection != null) {
            firstBrokerXAConnection.close();
            firstBrokerXAConnection = null;
        }
        if (secondBrokerXAConnection != null) {
            secondBrokerXAConnection.close();
            secondBrokerXAConnection = null;
        }

        super.tearDown();
    }

    public void testSendMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testAcknowledgeMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);
        firstBrokerConnection2.createSession(false, Session.CLIENT_ACKNOWLEDGE).createDurableSubscriber((Topic) destination, CLIENT_ID_TWO);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        secondBrokerSession.close();

        receivedMessage = firstBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        receivedMessage.acknowledge();

        Thread.sleep(LONG_TIMEOUT);

        secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        receivedMessage = secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNull(receivedMessage);
        secondBrokerSession.close();


        secondBrokerSession = secondBrokerConnection2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_TWO);
        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        secondBrokerSession.close();

        firstBrokerSession.close();
        secondBrokerSession.close();
    }


    public void testSendMessageTransactionCommit() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.commit();

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageTransactionRollback() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.rollback();

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageXATransactionCommit() throws Exception {
        XASession firstBrokerSession = firstBrokerXAConnection.createXASession();
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        XAResource xaRes = firstBrokerSession.getXAResource();
        Xid xid = createXid();
        xaRes.start(xid, XAResource.TMNOFLAGS);

        TextMessage message  = firstBrokerSession.createTextMessage(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerXAConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        xaRes.end(xid, XAResource.TMSUCCESS);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.prepare(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.commit(xid, false);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageXATransactionRollback() throws Exception {
        XASession firstBrokerSession = firstBrokerXAConnection.createXASession();
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        XAResource xaRes = firstBrokerSession.getXAResource();
        Xid xid = createXid();
        xaRes.start(xid, XAResource.TMNOFLAGS);

        TextMessage message  = firstBrokerSession.createTextMessage(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerXAConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        xaRes.end(xid, XAResource.TMSUCCESS);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.prepare(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.rollback(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testExpireMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        firstBrokerProducer.setTimeToLive(LONG_TIMEOUT);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT + SHORT_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }
}
