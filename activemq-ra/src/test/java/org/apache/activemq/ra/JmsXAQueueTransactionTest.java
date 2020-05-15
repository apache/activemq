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
package org.apache.activemq.ra;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.resource.spi.ManagedConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.JmsQueueTransactionTest;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsXAQueueTransactionTest extends JmsQueueTransactionTest {
    private static final Logger LOG = LoggerFactory.getLogger(JmsXAQueueTransactionTest.class);
    private ConnectionManagerAdapter connectionManager = new ConnectionManagerAdapter();
    private ActiveMQManagedConnectionFactory managedConnectionFactory;
    private XAResource xaResource;
    private static long txGenerator;
    private Xid xid;

    @Override
    protected void setUp() throws Exception {
        System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES", "java.util");
        super.setUp();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false&useJmx=false"));
    }

    @Override
    protected void setSessionTransacted() {
        resourceProvider.setTransacted(false);
        resourceProvider.setAckMode(Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    protected ConnectionFactory newConnectionFactory() throws Exception {
        managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnectionFactory.setServerUrl("vm://localhost?create=false&waitForStart=5000");
        managedConnectionFactory.setUserName(org.apache.activemq.ActiveMQConnectionFactory.DEFAULT_USER);
        managedConnectionFactory.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);

        return (ConnectionFactory)managedConnectionFactory.createConnectionFactory(connectionManager);
    }

    /**
     * Recreates the connection.
     *
     * @throws javax.jms.JMSException
     */
    @Override
    protected void reconnect() throws Exception {
        super.reconnect();
        ManagedConnectionProxy proxy = (ManagedConnectionProxy) connection;
        ManagedConnection mc = proxy.getManagedConnection();
        xaResource = mc.getXAResource();
    }

    @Override
    protected ActiveMQPrefetchPolicy getPrefetchPolicy() {
        ManagedConnectionProxy proxy = (ManagedConnectionProxy) connection;
        ActiveMQManagedConnection mc = proxy.getManagedConnection();
        ActiveMQConnection conn = (ActiveMQConnection) mc.getPhysicalConnection();
        return conn.getPrefetchPolicy();
    }

    @Override
    protected void beginTx() throws Exception {
        xid = createXid();
        xaResource.start(xid, XAResource.TMNOFLAGS);
    }

    @Override
    protected void commitTx() throws Exception {
        xaResource.end(xid, XAResource.TMSUCCESS);
        int result = xaResource.prepare(xid);
        if (result == XAResource.XA_OK) {
            xaResource.commit(xid, false);
        }
        xid = null;
    }

    @Override
    protected void rollbackTx() throws Exception {
        xaResource.end(xid, XAResource.TMSUCCESS);
        xaResource.rollback(xid);
        xid = null;
    }

    protected void abortTx() throws Exception {
        xaResource.end(xid, XAResource.TMFAIL);
        xaResource.rollback(xid);
        xid = null;
    }

    //This test won't work with xa tx it is overridden to do nothing here
    @Override
    public void testMessageListener() throws Exception {
    }

    /**
     * Sends a batch of messages and validates that the message sent before
     * session close is not consumed.
     * <p/>
     * This test only works with local transactions, not xa. so its commented out here
     *
     * @throws Exception
     */
    @Override
    public void testSendSessionClose() throws Exception {
    }

    public void testSendOnAbortedXATx() throws Exception {
        connection.close();

        ConnectionFactory connectionFactory = newConnectionFactory();
        connection = connectionFactory.createConnection();
        connection.start();

        ManagedConnectionProxy proxy = (ManagedConnectionProxy) connection;
        ManagedConnection mc = proxy.getManagedConnection();
        xaResource = mc.getXAResource();

        beginTx();

        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(destination);

        abortTx();

        try {
            producer.send(session.createTextMessage("my tx aborted!"));
            fail("expect error on send with rolled back tx");
        } catch (JMSException expected) {
            assertTrue("matches expected message", expected.getLocalizedMessage().contains("rollback only"));
            expected.printStackTrace();
        }

        connection.close();
    }

    public void testReceiveTwoThenAbort() throws Exception {
        Message[] outbound = new Message[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message")};

        // lets consume any outstanding messages from prev test runs
        beginTx();
        while (consumer.receive(1000) != null) {
        }
        commitTx();

        //
        beginTx();
        producer.send(outbound[0]);
        producer.send(outbound[1]);
        commitTx();

        LOG.info("Sent 0: " + outbound[0]);
        LOG.info("Sent 1: " + outbound[1]);

        ArrayList<Message> messages = new ArrayList<Message>();
        beginTx();
        Message message = consumer.receive(1000);
        assertEquals(outbound[0], message);

        message = consumer.receive(1000);
        assertNotNull(message);
        assertEquals(outbound[1], message);
        abortTx();

        // Consume again.. the prev message should
        // get redelivered.
        beginTx();
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the first message again!", message);
        messages.add(message);
        assertEquals(outbound[0], message);
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the second message again!", message);
        messages.add(message);
        assertEquals(outbound[1], message);

        assertNull(consumer.receiveNoWait());
        commitTx();

        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);
        assertTextMessagesEqual("Rollback did not work", outbound, inbound);
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
