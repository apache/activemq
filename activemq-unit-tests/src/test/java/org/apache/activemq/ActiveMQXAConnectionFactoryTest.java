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
package org.apache.activemq;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnection;
import javax.jms.XASession;
import javax.jms.XATopicConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransactionBroker;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.management.JMSConnectionStatsImpl;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQXAConnectionFactoryTest extends CombinationTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQXAConnectionFactoryTest.class);
    long txGenerator = System.currentTimeMillis();
    private ActiveMQConnection connection;
    private BrokerService broker;

    @Override
    public void tearDown() throws Exception {
        // Try our best to close any previously opend connection.
        try {
            connection.close();
        } catch (Throwable ignore) {
        }
        // Try our best to stop any previously started broker.
        try {
            broker.stop();
        } catch (Throwable ignore) {
        }
    }

    protected ActiveMQConnectionFactory getXAConnectionFactory(String brokerUrl) {
        return new ActiveMQXAConnectionFactory(brokerUrl);
    }

    protected ActiveMQConnectionFactory getXAConnectionFactory(URI uri) {
        return new ActiveMQXAConnectionFactory(uri);
    }

    public void testCopy() throws URISyntaxException, JMSException {
        ActiveMQConnectionFactory cf = getXAConnectionFactory("vm://localhost?");
        ActiveMQConnectionFactory copy = cf.copy();
        assertTrue("Should be an ActiveMQXAConnectionFactory", copy.getClass().equals(cf.getClass()));
    }

    public void testUseURIToSetOptionsOnConnectionFactory() throws URISyntaxException, JMSException {
        ActiveMQConnectionFactory cf = getXAConnectionFactory("vm://localhost?jms.useAsyncSend=true");
        assertTrue(cf.isUseAsyncSend());
        // the broker url have been adjusted.
        assertEquals("vm://localhost", cf.getBrokerURL());

        cf = getXAConnectionFactory("vm://localhost?jms.useAsyncSend=false");
        assertFalse(cf.isUseAsyncSend());
        // the broker url have been adjusted.
        assertEquals("vm://localhost", cf.getBrokerURL());

        cf = getXAConnectionFactory("vm:(broker:()/localhost)?jms.useAsyncSend=true");
        assertTrue(cf.isUseAsyncSend());
        // the broker url have been adjusted.
        assertEquals("vm:(broker:()/localhost)", cf.getBrokerURL());

        cf = getXAConnectionFactory(
                "vm://localhost?jms.redeliveryPolicy.maximumRedeliveries=10&" +
                               "jms.redeliveryPolicy.initialRedeliveryDelay=10000&" +
                               "jms.redeliveryPolicy.redeliveryDelay=10000&" +
                               "jms.redeliveryPolicy.useExponentialBackOff=true&" +
                               "jms.redeliveryPolicy.backOffMultiplier=2");
        assertEquals(10, cf.getRedeliveryPolicy().getMaximumRedeliveries());
        assertEquals(10000, cf.getRedeliveryPolicy().getInitialRedeliveryDelay());
        assertEquals(10000, cf.getRedeliveryPolicy().getRedeliveryDelay());
        assertEquals(true, cf.getRedeliveryPolicy().isUseExponentialBackOff());
        assertEquals(2.0, cf.getRedeliveryPolicy().getBackOffMultiplier(), 0.1);

        // the broker url have been adjusted.
        assertEquals("vm://localhost", cf.getBrokerURL());
    }

    public void testCreateVMConnectionWithEmbdeddBroker() throws URISyntaxException, JMSException {
        ActiveMQConnectionFactory cf = getXAConnectionFactory("vm://myBroker?broker.persistent=false");
        // Make sure the broker is not created until the connection is
        // instantiated.
        assertNull(BrokerRegistry.getInstance().lookup("myBroker"));
        connection = (ActiveMQConnection) cf.createConnection();
        // This should create the connection.
        assertNotNull(connection);
        // Verify the broker was created.
        assertNotNull(BrokerRegistry.getInstance().lookup("myBroker"));
        connection.close();
        // Verify the broker was destroyed.
        assertNull(BrokerRegistry.getInstance().lookup("myBroker"));

        connection.close();
    }

    public void testGetBrokerName() throws URISyntaxException, JMSException {
        ActiveMQConnectionFactory cf = getXAConnectionFactory("vm://localhost?broker.persistent=false");
        connection = (ActiveMQConnection)cf.createConnection();
        connection.start();

        String brokerName = connection.getBrokerName();
        LOG.info("Got broker name: " + brokerName);

        assertNotNull("No broker name available!", brokerName);
        connection.close();
    }

    public void testCreateTcpConnectionUsingAllocatedPort() throws Exception {
        assertCreateConnection("tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true");
    }

    public void testCreateTcpConnectionUsingKnownPort() throws Exception {
        assertCreateConnection("tcp://localhost:61610?wireFormat.tcpNoDelayEnabled=true");
    }

    public void testIsSameRM() throws URISyntaxException, JMSException, XAException {

        XAConnection connection1 = null;
        XAConnection connection2 = null;
        try {
            ActiveMQConnectionFactory cf1 = getXAConnectionFactory("vm://localhost?broker.persistent=false");
            connection1 = (XAConnection)cf1.createConnection();
            XASession session1 = connection1.createXASession();
            XAResource resource1 = session1.getXAResource();

            ActiveMQConnectionFactory cf2 = getXAConnectionFactory("vm://localhost?broker.persistent=false");
            connection2 = (XAConnection)cf2.createConnection();
            XASession session2 = connection2.createXASession();
            XAResource resource2 = session2.getXAResource();

            assertTrue(resource1.isSameRM(resource2));
            session1.close();
            session2.close();
        } finally {
            if (connection1 != null) {
                try {
                    connection1.close();
                } catch (Exception e) {
                    // ignore
                }
            }
            if (connection2 != null) {
                try {
                    connection2.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    public void testIsSameRMOverride() throws URISyntaxException, JMSException, XAException {

        XAConnection connection1 = null;
        XAConnection connection2 = null;
        try {
            ActiveMQConnectionFactory cf1 = getXAConnectionFactory("vm://localhost?broker.persistent=false&jms.rmIdFromConnectionId=true");
            connection1 = (XAConnection)cf1.createConnection();
            XASession session1 = connection1.createXASession();
            XAResource resource1 = session1.getXAResource();

            ActiveMQConnectionFactory cf2 = getXAConnectionFactory("vm://localhost?broker.persistent=false");
            connection2 = (XAConnection)cf2.createConnection();
            XASession session2 = connection2.createXASession();
            XAResource resource2 = session2.getXAResource();

            assertFalse(resource1.isSameRM(resource2));

            // ensure identity is preserved
            XASession session1a = connection1.createXASession();
            assertTrue(resource1.isSameRM(session1a.getXAResource()));
            session1.close();
            session2.close();
        } finally {
            if (connection1 != null) {
                try {
                    connection1.close();
                } catch (Exception e) {
                    // ignore
                }
            }
            if (connection2 != null) {
                try {
                    connection2.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    public void testVanilaTransactionalProduceReceive() throws Exception {

        XAConnection connection1 = null;
        try {
            ActiveMQConnectionFactory cf1 = getXAConnectionFactory("vm://localhost?broker.persistent=false");
            connection1 = (XAConnection)cf1.createConnection();
            connection1.start();
            XASession session = connection1.createXASession();
            XAResource resource = session.getXAResource();
            Destination dest = new ActiveMQQueue(getName());

            // publish a message
            Xid tid = createXid();
            resource.start(tid, XAResource.TMNOFLAGS);
            MessageProducer producer = session.createProducer(dest);
            ActiveMQTextMessage message  = new ActiveMQTextMessage();
            message.setText(getName());
            producer.send(message);
            resource.end(tid, XAResource.TMSUCCESS);
            resource.commit(tid, true);
            session.close();

            session = connection1.createXASession();
            MessageConsumer consumer = session.createConsumer(dest);
            tid = createXid();
            resource = session.getXAResource();
            resource.start(tid, XAResource.TMNOFLAGS);
            TextMessage receivedMessage = (TextMessage) consumer.receive(1000);
            assertNotNull(receivedMessage);
            assertEquals(getName(), receivedMessage.getText());
            resource.end(tid, XAResource.TMSUCCESS);
            resource.commit(tid, true);
            session.close();

        } finally {
            if (connection1 != null) {
                try {
                    connection1.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    public void testConsumerCloseTransactionalSendReceive() throws Exception {

        ActiveMQConnectionFactory cf1 = getXAConnectionFactory("vm://localhost?broker.persistent=false");
        XAConnection connection1 = (XAConnection)cf1.createConnection();
        connection1.start();
        XASession session = connection1.createXASession();
        XAResource resource = session.getXAResource();
        Destination dest = new ActiveMQQueue(getName());

        // publish a message
        Xid tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);
        MessageProducer producer = session.createProducer(dest);
        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        producer.send(message);
        producer.close();
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);
        session.close();

        session = connection1.createXASession();
        MessageConsumer consumer = session.createConsumer(dest);
        tid = createXid();
        resource = session.getXAResource();
        resource.start(tid, XAResource.TMNOFLAGS);
        TextMessage receivedMessage = (TextMessage) consumer.receive(1000);
        consumer.close();
        assertNotNull(receivedMessage);
        assertEquals(getName(), receivedMessage.getText());
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);

        session = connection1.createXASession();
        consumer = session.createConsumer(dest);
        tid = createXid();
        resource = session.getXAResource();
        resource.start(tid, XAResource.TMNOFLAGS);
        assertNull(consumer.receive(1000));
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);

    }

    public void testSessionCloseTransactionalSendReceive() throws Exception {

        ActiveMQConnectionFactory cf1 = getXAConnectionFactory("vm://localhost?broker.persistent=false");
        XAConnection connection1 = (XAConnection)cf1.createConnection();
        connection1.start();
        XASession session = connection1.createXASession();
        XAResource resource = session.getXAResource();
        Destination dest = new ActiveMQQueue(getName());

        // publish a message
        Xid tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);
        MessageProducer producer = session.createProducer(dest);
        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        producer.send(message);
        session.close();
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);


        session = connection1.createXASession();
        MessageConsumer consumer = session.createConsumer(dest);
        tid = createXid();
        resource = session.getXAResource();
        resource.start(tid, XAResource.TMNOFLAGS);
        TextMessage receivedMessage = (TextMessage) consumer.receive(1000);
        session.close();
        assertNotNull(receivedMessage);
        assertEquals(getName(), receivedMessage.getText());
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);

        session = connection1.createXASession();
        consumer = session.createConsumer(dest);
        tid = createXid();
        resource = session.getXAResource();
        resource.start(tid, XAResource.TMNOFLAGS);
        assertNull(consumer.receive(1000));
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);
    }


    public void testReadonlyNoLeak() throws Exception {
        final String brokerName = "readOnlyNoLeak";
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:0)/" + brokerName));
        broker.setPersistent(false);
        broker.start();
        ActiveMQConnectionFactory cf1 = getXAConnectionFactory("failover:(" + broker.getTransportConnectors().get(0).getConnectUri() + ")");
        cf1.setStatsEnabled(true);
        ActiveMQXAConnection xaConnection = (ActiveMQXAConnection)cf1.createConnection();
        xaConnection.start();
        XASession session = xaConnection.createXASession();
        XAResource resource = session.getXAResource();
        Xid tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);
        session.close();
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);

        assertTransactionGoneFromBroker(tid);
        assertTransactionGoneFromConnection(brokerName, xaConnection.getClientID(), xaConnection.getConnectionInfo().getConnectionId(), tid);
        assertSessionGone(xaConnection, session);
        assertTransactionGoneFromFailoverState(xaConnection, tid);

        // two phase
        session = xaConnection.createXASession();
        resource = session.getXAResource();
        tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);
        session.close();
        resource.end(tid, XAResource.TMSUCCESS);
        assertEquals(XAResource.XA_RDONLY, resource.prepare(tid));

        // no need for a commit on read only
        assertTransactionGoneFromBroker(tid);
        assertTransactionGoneFromConnection(brokerName, xaConnection.getClientID(), xaConnection.getConnectionInfo().getConnectionId(), tid);
        assertSessionGone(xaConnection, session);
        assertTransactionGoneFromFailoverState(xaConnection, tid);

        xaConnection.close();
        broker.stop();

    }

    public void testCloseSendConnection() throws Exception {
        String brokerName = "closeSend";
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:0)/" + brokerName));
        broker.start();
        broker.waitUntilStarted();
        ActiveMQConnectionFactory cf = getXAConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri());
        XAConnection connection = (XAConnection)cf.createConnection();
        connection.start();
        XASession session = connection.createXASession();
        XAResource resource = session.getXAResource();
        Destination dest = new ActiveMQQueue(getName());

        // publish a message
        Xid tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);
        MessageProducer producer = session.createProducer(dest);
        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        producer.send(message);

        connection.close();

        assertTransactionGoneFromBroker(tid);

        broker.stop();
    }

    public void testExceptionAfterClose() throws Exception {

        ActiveMQConnectionFactory cf1 = getXAConnectionFactory("vm://localhost?broker.persistent=false");
        XAConnection connection1 = (XAConnection)cf1.createConnection();
        connection1.start();

        XASession session = connection1.createXASession();
        session.close();
        try {
            session.commit();
            fail("expect exception after close");
        } catch (javax.jms.IllegalStateException expected) {}

        try {
            session.rollback();
            fail("expect exception after close");
        } catch (javax.jms.IllegalStateException expected) {}

        try {
            session.getTransacted();
            fail("expect exception after close");
        } catch (javax.jms.IllegalStateException expected) {}
    }

    public void testProducerFailAfterRollbackOnly() throws Exception {

        ActiveMQConnectionFactory cf1 = getXAConnectionFactory("vm://localhost?broker.persistent=false");
        XAConnection connection1 = (XAConnection)cf1.createConnection();
        connection1.start();

        XASession session = connection1.createXASession();
        XAResource resource = session.getXAResource();
        Destination dest = new ActiveMQQueue(getName());

        // publish a message
        Xid tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);
        MessageProducer producer = session.createProducer(dest);
        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());

        // can happen out of band with XA via RAR
        resource.end(tid, XAResource.TMFAIL);
        ((ActiveMQSession)session).getTransactionContext().setRollbackOnly(true);
        try {
            producer.send(message);
            fail("expect error on setRollbackOnly");
        } catch (JMSException expected) {}

        // rollback only state does not linger
        tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);
        producer.send(message);
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);
        connection1.close();
    }

    public void testRollbackXaErrorCode() throws Exception {
        String brokerName = "rollbackErrorCode";
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:0)/" + brokerName));
        broker.start();
        broker.waitUntilStarted();
        ActiveMQConnectionFactory cf = getXAConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri());
        XAConnection connection = (XAConnection)cf.createConnection();
        connection.start();
        XASession session = connection.createXASession();
        XAResource resource = session.getXAResource();

        Xid tid = createXid();
        try {
            resource.rollback(tid);
            fail("Expected xa exception on no tx");
        } catch (XAException expected) {
            LOG.info("got expected xa", expected);
            assertEquals("no tx", XAException.XAER_NOTA, expected.errorCode);
        }
        connection.close();
        broker.stop();
    }

    private void assertTransactionGoneFromFailoverState(
            ActiveMQXAConnection connection1, Xid tid) throws Exception {

        FailoverTransport transport = (FailoverTransport) connection1.getTransport().narrow(FailoverTransport.class);
        TransactionInfo info = new TransactionInfo(connection1.getConnectionInfo().getConnectionId(), new XATransactionId(tid), TransactionInfo.COMMIT_ONE_PHASE);
        assertNull("transaction should not exist in the state tracker",
                transport.getStateTracker().processCommitTransactionOnePhase(info));
    }

    private void assertSessionGone(ActiveMQXAConnection connection1,
            XASession session) {
        JMSConnectionStatsImpl stats = (JMSConnectionStatsImpl)connection1.getStats();
        // should be no dangling sessions maintained by the transaction
        assertEquals("should be no sessions", 0, stats.getSessions().length);
    }

    private void assertTransactionGoneFromConnection(String brokerName, String clientId, ConnectionId connectionId, Xid tid) throws Exception {
        BrokerService broker = BrokerRegistry.getInstance().lookup(brokerName);
        CopyOnWriteArrayList<TransportConnection> connections = broker.getTransportConnectors().get(0).getConnections();
        for (TransportConnection connection: connections) {
            if (connection.getConnectionId().equals(clientId)) {
                try {
                    connection.processPrepareTransaction(new TransactionInfo(connectionId, new XATransactionId(tid), TransactionInfo.PREPARE));
                    fail("did not get expected excepton on missing transaction, it must be still there in error!");
                } catch (IllegalStateException expectedOnNoTransaction) {
                }
            }
        }
    }

    private void assertTransactionGoneFromBroker(Xid tid) throws Exception {
        BrokerService broker = BrokerRegistry.getInstance().lookup("localhost");
        TransactionBroker transactionBroker = (TransactionBroker)broker.getBroker().getAdaptor(TransactionBroker.class);
        try {
            transactionBroker.getTransaction(null, new XATransactionId(tid), false);
            fail("expected exception on tx not found");
        } catch (XAException expectedOnNotFound) {
        }
    }

    protected void assertCreateConnection(String uri) throws Exception {
        // Start up a broker with a tcp connector.
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        TransportConnector connector = broker.addConnector(uri);
        broker.start();

        URI temp = new URI(uri);
        // URI connectURI = connector.getServer().getConnectURI();
        // TODO this sometimes fails when using the actual local host name
        URI currentURI = new URI(connector.getPublishableConnectString());

        // sometimes the actual host name doesn't work in this test case
        // e.g. on OS X so lets use the original details but just use the actual
        // port
        URI connectURI = new URI(temp.getScheme(), temp.getUserInfo(), temp.getHost(), currentURI.getPort(),
                                 temp.getPath(), temp.getQuery(), temp.getFragment());

        LOG.info("connection URI is: " + connectURI);

        // This should create the connection.
        ActiveMQConnectionFactory cf = getXAConnectionFactory(connectURI);
        Connection connection = cf.createConnection();

        assertXAConnection(connection);

        assertNotNull(connection);
        connection.close();

        connection = ((XAConnectionFactory)cf).createXAConnection();

        assertXAConnection(connection);

        assertNotNull(connection);
    }

    private void assertXAConnection(Connection connection) {
        assertTrue("Should be an XAConnection", connection instanceof XAConnection);
        assertTrue("Should be an XATopicConnection", connection instanceof XATopicConnection);
        assertTrue("Should be an XAQueueConnection", connection instanceof XAQueueConnection);
    }

    public Xid createXid() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {
            @Override
            public int getFormatId() {
                return 86;
            }

            @Override
            public byte[] getGlobalTransactionId() {
                return bs;
            }

            @Override
            public byte[] getBranchQualifier() {
                return bs;
            }
        };

    }

}
