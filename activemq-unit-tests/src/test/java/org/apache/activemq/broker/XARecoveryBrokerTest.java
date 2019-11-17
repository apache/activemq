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
package org.apache.activemq.broker;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.TransactionContext;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.PersistenceAdapterViewMBean;
import org.apache.activemq.broker.jmx.RecoveredXATransactionViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.*;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.JMXSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to simulate the recovery that occurs when a broker shuts down.
 * 
 * 
 */
public class XARecoveryBrokerTest extends BrokerRestartTestSupport {
    protected static final Logger LOG = LoggerFactory.getLogger(XARecoveryBrokerTest.class);
    public boolean prioritySupport = true;
    public boolean keepDurableSubsActive = false;

    public void testPreparedJmxView() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Prepare 4 message sends.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            XATransactionId txid = createXATransaction(sessionInfo);
            connection.send(createBeginTransaction(connectionInfo, txid));

            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);

            // Prepare
            connection.send(createPrepareTransaction(connectionInfo, txid));
        }

        Response response = connection.request(new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER));
        assertNotNull(response);
        DataArrayResponse dar = (DataArrayResponse)response;
        assertEquals(4, dar.getData().length);

        // view prepared in kahadb view
        if (broker.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter) {
            PersistenceAdapterViewMBean kahadbView = getProxyToPersistenceAdapter(broker.getPersistenceAdapter().toString());
            String txFromView = kahadbView.getTransactions();
            LOG.info("Tx view fromm PA:" + txFromView);
            assertTrue("xid with our dud format in transaction string " + txFromView, txFromView.contains("XID:[55,"));
        }

        // restart the broker.
        restartBroker();

        connection = createConnection();
        connectionInfo = createConnectionInfo();
        connection.send(connectionInfo);


        response = connection.request(new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER));
        assertNotNull(response);
        dar = (DataArrayResponse)response;
        assertEquals(4, dar.getData().length);

        // verify XAResource scan loop
        XAResource transactionContextXAResource = new TransactionContext(ActiveMQConnection.makeConnection(broker.getVmConnectorURI().toString()));
        LinkedList<Xid> tracked = new LinkedList<Xid>();
        Xid[] recoveryXids = transactionContextXAResource.recover(XAResource.TMSTARTRSCAN);
        while (recoveryXids.length > 0) {
            tracked.addAll(Arrays.asList(recoveryXids));
            recoveryXids = transactionContextXAResource.recover(XAResource.TMNOFLAGS);
        }
        assertEquals("got 4 via scan loop", 4, tracked.size());

        // validate destination depth via jmx
        DestinationViewMBean destinationView = getProxyToDestination(destinationList(destination)[0]);
        assertEquals("enqueue count does not see prepared", 0, destinationView.getQueueSize());

        TransactionId first = (TransactionId)dar.getData()[0];
        int commitCount = 0;
        // via jmx, force outcome
        for (int i = 0; i < 4; i++) {
            RecoveredXATransactionViewMBean mbean =  getProxyToPreparedTransactionViewMBean((TransactionId)dar.getData()[i]);
            if (i%2==0) {
                mbean.heuristicCommit();
                commitCount++;
            } else {
                mbean.heuristicRollback();
            }
        }

        // verify all completed
        response = connection.request(new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER));
        assertNotNull(response);
        dar = (DataArrayResponse)response;
        assertEquals(0, dar.getData().length);

        // verify messages available
        assertEquals("enqueue count reflects outcome", commitCount, destinationView.getQueueSize());

        // verify mbeans gone
        try {
            RecoveredXATransactionViewMBean gone = getProxyToPreparedTransactionViewMBean(first);
            gone.heuristicRollback();
            fail("Excepted not found");
        } catch (InstanceNotFoundException expectedNotfound) {
        }
    }

    private PersistenceAdapterViewMBean getProxyToPersistenceAdapter(String name) throws MalformedObjectNameException, JMSException {
       return (PersistenceAdapterViewMBean)broker.getManagementContext().newProxyInstance(
               BrokerMBeanSupport.createPersistenceAdapterName(broker.getBrokerObjectName().toString(), name),
               PersistenceAdapterViewMBean.class, true);
    }

    private RecoveredXATransactionViewMBean getProxyToPreparedTransactionViewMBean(TransactionId xid) throws MalformedObjectNameException, JMSException {

        ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,transactionType=RecoveredXaTransaction,xid=" +
                JMXSupport.encodeObjectNamePart(xid.toString()));
        RecoveredXATransactionViewMBean proxy = (RecoveredXATransactionViewMBean) broker.getManagementContext().newProxyInstance(objectName,
                RecoveredXATransactionViewMBean.class, true);
        return proxy;
    }

    private DestinationViewMBean getProxyToDestination(ActiveMQDestination destination) throws MalformedObjectNameException, JMSException {

        final ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName="+broker.getBrokerName()+",destinationType="
                + JMXSupport.encodeObjectNamePart(destination.getDestinationTypeAsString())
                + ",destinationName=" + JMXSupport.encodeObjectNamePart(destination.getPhysicalName()));

        DestinationViewMBean proxy = (DestinationViewMBean) broker.getManagementContext().newProxyInstance(objectName,
                DestinationViewMBean.class, true);
        return proxy;

    }

    public void testPreparedTransactionRecoveredOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Prepare 4 message sends.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            XATransactionId txid = createXATransaction(sessionInfo);
            connection.send(createBeginTransaction(connectionInfo, txid));

            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);

            // Prepare
            connection.send(createPrepareTransaction(connectionInfo, txid));
        }

        // Since prepared but not committed.. they should not get delivered.
        assertNull(receiveMessage(connection));
        assertNoMessagesLeft(connection);
        connection.request(closeConnectionInfo(connectionInfo));

        // restart the broker.
        restartBroker();

        // Setup the consumer and try receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Since prepared but not committed.. they should not get delivered.
        assertNull(receiveMessage(connection));
        assertNoMessagesLeft(connection);

        Response response = connection.request(new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER));
        assertNotNull(response);
        DataArrayResponse dar = (DataArrayResponse)response;
        assertEquals(4, dar.getData().length);

        // ensure we can close a connection with prepared transactions
        connection.request(closeConnectionInfo(connectionInfo));

        // open again  to deliver outcome
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Commit the prepared transactions.
        for (int i = 0; i < dar.getData().length; i++) {
            TransactionId transactionId = (TransactionId) dar.getData()[i];
            LOG.info("commit: " + transactionId);
            connection.request(createCommitTransaction2Phase(connectionInfo, transactionId));
        }

        // We should get the committed transactions.
        final int countToReceive = expectedMessageCount(4, destination);
        for (int i = 0; i < countToReceive ; i++) {
            Message m = receiveMessage(connection, TimeUnit.SECONDS.toMillis(10));
            LOG.info("received: " + m);
            assertNotNull("Got non null message: " + i, m);
        }

        assertNoMessagesLeft(connection);
        assertEmptyDLQ();
    }

    private void assertEmptyDLQ() throws Exception {
        try {
            DestinationViewMBean destinationView = getProxyToDestination(new ActiveMQQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
            assertEquals("nothing on dlq", 0, destinationView.getQueueSize());
            assertEquals("nothing added to dlq", 0, destinationView.getEnqueueCount());
        } catch (java.lang.reflect.UndeclaredThrowableException maybeOk) {
            if (maybeOk.getUndeclaredThrowable() instanceof javax.management.InstanceNotFoundException) {
                // perfect no dlq
            } else {
                throw maybeOk;
            }
        }
    }

    public void testPreparedInterleavedTransactionRecoveredOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Prepare 4 message sends.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            XATransactionId txid = createXATransaction(sessionInfo);
            connection.send(createBeginTransaction(connectionInfo, txid));

            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);

            // Prepare
            connection.send(createPrepareTransaction(connectionInfo, txid));
        }

        // Since prepared but not committed.. they should not get delivered.
        assertNull(receiveMessage(connection));
        assertNoMessagesLeft(connection);

        // send non tx message
        Message message = createMessage(producerInfo, destination);
        message.setPersistent(true);
        connection.request(message);

        connection.request(closeConnectionInfo(connectionInfo));

        // restart the broker.
        restartBroker();

        // Setup the consumer and try receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // consume non transacted message, but don't ack
        int countToReceive = expectedMessageCount(1, destination);
        for (int i=0; i< countToReceive; i++) {
            Message m = receiveMessage(connection, TimeUnit.SECONDS.toMillis(10));
            LOG.info("received: " + m);
            assertNotNull("got non tx message after prepared", m);
        }

        // Since prepared but not committed.. they should not get delivered.
        assertNull(receiveMessage(connection));
        assertNoMessagesLeft(connection);

        Response response = connection.request(new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER));
        assertNotNull(response);
        DataArrayResponse dar = (DataArrayResponse)response;
        assertEquals(4, dar.getData().length);

        // ensure we can close a connection with prepared transactions
        connection.request(closeConnectionInfo(connectionInfo));

        // open again  to deliver outcome
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);

        // Commit the prepared transactions.
        for (int i = 0; i < dar.getData().length; i++) {
            TransactionId transactionId = (TransactionId) dar.getData()[i];
            LOG.info("commit: " + transactionId);
            connection.request(createCommitTransaction2Phase(connectionInfo, transactionId));
        }

        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // We should get the committed transactions and the non tx message
        countToReceive = expectedMessageCount(5, destination);
        for (int i = 0; i < countToReceive ; i++) {
            Message m = receiveMessage(connection, TimeUnit.SECONDS.toMillis(10));
            LOG.info("received: " + m);
            assertNotNull("Got non null message: " + i, m);
        }

        assertNoMessagesLeft(connection);
        assertEmptyDLQ();
    }

    public void testTopicPreparedTransactionRecoveredOnRestart() throws Exception {
        ActiveMQDestination destination = new ActiveMQTopic("TryTopic");

        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        // Prepare 4 message sends.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            XATransactionId txid = createXATransaction(sessionInfo);
            connection.send(createBeginTransaction(connectionInfo, txid));

            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);

            // Prepare
            connection.send(createPrepareTransaction(connectionInfo, txid));
        }

        // Since prepared but not committed.. they should not get delivered.
        assertNull(receiveMessage(connection));
        assertNoMessagesLeft(connection);
        connection.request(closeConnectionInfo(connectionInfo));

        // restart the broker.
        restartBroker();

        // Setup the consumer and try receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");

        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        // Since prepared but not committed.. they should not get delivered.
        assertNull(receiveMessage(connection));
        assertNoMessagesLeft(connection);

        Response response = connection.request(new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER));
        assertNotNull(response);
        DataArrayResponse dar = (DataArrayResponse) response;
        assertEquals(4, dar.getData().length);

        // ensure we can close a connection with prepared transactions
        connection.request(closeConnectionInfo(connectionInfo));

        // open again  to deliver outcome
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        // Commit the prepared transactions.
        for (int i = 0; i < dar.getData().length; i++) {
            connection.request(createCommitTransaction2Phase(connectionInfo, (TransactionId) dar.getData()[i]));
        }

        // We should get the committed transactions.
        for (int i = 0; i < expectedMessageCount(4, destination); i++) {
            Message m = receiveMessage(connection, TimeUnit.SECONDS.toMillis(10));
            assertNotNull(m);
        }

        assertNoMessagesLeft(connection);

    }

    public void testQueuePersistentCommitedMessagesNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);
        }

        // Commit
        connection.send(createCommitTransaction1Phase(connectionInfo, txid));
        connection.request(closeConnectionInfo(connectionInfo));
        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        for (int i = 0; i < expectedMessageCount(4, destination); i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
        }

        assertNoMessagesLeft(connection);
    }

    public void testQueuePersistentCommited2PhaseMessagesNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);
        }

        // Commit 2 phase
        connection.request(createPrepareTransaction(connectionInfo, txid));
        connection.send(createCommitTransaction2Phase(connectionInfo, txid));

        connection.request(closeConnectionInfo(connectionInfo));
        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        for (int i = 0; i < expectedMessageCount(4, destination); i++) {
            Message m = receiveMessage(connection);
            assertNotNull(m);
        }

        assertNoMessagesLeft(connection);
    }

    public void testQueuePersistentCommitedAcksNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        ConsumerInfo consumerInfo;
        Message m = null;
        for (ActiveMQDestination dest : destinationList(destination)) {
            // Setup the consumer and receive the message.
            consumerInfo = createConsumerInfo(sessionInfo, dest);
            connection.send(consumerInfo);

            for (int i = 0; i < 4; i++) {
                m = receiveMessage(connection);
                assertNotNull(m);
            }

            MessageAck ack = createAck(consumerInfo, m, 4, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }

        // Commit
        connection.request(createCommitTransaction1Phase(connectionInfo, txid));

        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // No messages should be delivered.
        assertNoMessagesLeft(connection);

        m = receiveMessage(connection);
        assertNull(m);
    }
    
    public void testQueuePersistentPreparedAcksNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        ConsumerInfo consumerInfo;
        Message m = null;
        for (ActiveMQDestination dest : destinationList(destination)) {
            // Setup the consumer and receive the message.
            consumerInfo = createConsumerInfo(sessionInfo, dest);
            connection.send(consumerInfo);

            for (int i = 0; i < 4; i++) {
                m = receiveMessage(connection);
                assertNotNull(m);
            }

            // one ack with last received, mimic a beforeEnd synchronization
            MessageAck ack = createAck(consumerInfo, m, 4, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }

        connection.request(createPrepareTransaction(connectionInfo, txid));

        // restart the broker.
        restartBroker();

        connection = createConnection();
        connectionInfo = createConnectionInfo();
        connection.send(connectionInfo);

        // validate recovery
        TransactionInfo recoverInfo = new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER);
        DataArrayResponse dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);

        assertEquals("there is a prepared tx", 1, dataArrayResponse.getData().length);
        assertEquals("it matches", txid, dataArrayResponse.getData()[0]);

        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);
        
        // no redelivery, exactly once semantics unless there is rollback
        m = receiveMessage(connection);
        assertNull(m);
        assertNoMessagesLeft(connection);

        // validate destination depth via jmx
        DestinationViewMBean destinationView = getProxyToDestination(destinationList(destination)[0]);
        assertEquals("enqueue count does not see prepared acks", 0, destinationView.getQueueSize());
        assertEquals("dequeue count does not see prepared acks", 0, destinationView.getDequeueCount());

        connection.request(createCommitTransaction2Phase(connectionInfo, txid));

        // validate recovery complete
        dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);
        assertEquals("there are no prepared tx", 0, dataArrayResponse.getData().length);

        assertEquals("enqueue count does not see commited acks", 0, destinationView.getQueueSize());
        assertEquals("dequeue count does not see commited acks", 4, destinationView.getDequeueCount());

    }

    public void initCombosForTestTopicPersistentPreparedAcksNotLostOnRestart() {
        addCombinationValues("prioritySupport", new Boolean[]{Boolean.FALSE, Boolean.TRUE});
    }

    public void testTopicPersistentPreparedAcksNotLostOnRestart() throws Exception {
        ActiveMQDestination destination = new ActiveMQTopic("TryTopic");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // setup durable subs
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        final int numMessages = 4;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        final int messageCount = expectedMessageCount(numMessages, destination);
        Message m = null;
        for (int i = 0; i < messageCount; i++) {
            m = receiveMessage(connection);
            assertNotNull("unexpected null on: " + i, m);
        }

        // one ack with last received, mimic a beforeEnd synchronization
        MessageAck ack = createAck(consumerInfo, m, messageCount, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        connection.request(createPrepareTransaction(connectionInfo, txid));

        // restart the broker.
        restartBroker();

        connection = createConnection();
        connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        connection.send(connectionInfo);

        // validate recovery
        TransactionInfo recoverInfo = new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER);
        DataArrayResponse dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);

        assertEquals("there is a prepared tx", 1, dataArrayResponse.getData().length);
        assertEquals("it matches", txid, dataArrayResponse.getData()[0]);

        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        // no redelivery, exactly once semantics unless there is rollback
        m = receiveMessage(connection);
        assertNull(m);
        assertNoMessagesLeft(connection);

        connection.request(createCommitTransaction2Phase(connectionInfo, txid));

        // validate recovery complete
        dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);
        assertEquals("there are no prepared tx", 0, dataArrayResponse.getData().length);
    }

    public void testTopicPersistentPreparedAcksNotLostOnRestartForNSubs() throws Exception {
        ActiveMQDestination destination = new ActiveMQTopic("TryTopic");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // setup durable subs
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("sub");
        connection.send(consumerInfo);

        ConsumerInfo consumerInfoX = createConsumerInfo(sessionInfo, destination);
        consumerInfoX.setSubscriptionName("subX");
        connection.send(consumerInfoX);
        connection.send(consumerInfoX.createRemoveCommand());

        final int numMessages = 4;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        final int messageCount = expectedMessageCount(numMessages, destination);
        Message m = null;
        for (int i = 0; i < messageCount; i++) {
            m = receiveMessage(connection);
            assertNotNull("unexpected null on: " + i, m);
        }

        // one ack with last received, mimic a beforeEnd synchronization
        MessageAck ack = createAck(consumerInfo, m, messageCount, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        connection.request(createPrepareTransaction(connectionInfo, txid));

        // restart the broker.
        restartBroker();

        connection = createConnection();
        connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        connection.send(connectionInfo);

        // validate recovery
        TransactionInfo recoverInfo = new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER);
        DataArrayResponse dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);

        assertEquals("there is a prepared tx", 1, dataArrayResponse.getData().length);
        assertEquals("it matches", txid, dataArrayResponse.getData()[0]);

        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("sub");
        connection.send(consumerInfo);

        // no redelivery, exactly once semantics unless there is rollback
        m = receiveMessage(connection);
        assertNull(m);
        assertNoMessagesLeft(connection);

        // ensure subX can get it's copy of the messages
        consumerInfoX = createConsumerInfo(sessionInfo, destination);
        consumerInfoX.setSubscriptionName("subX");
        connection.send(consumerInfoX);

        for (int i = 0; i < messageCount; i++) {
            m = receiveMessage(connection);
            assertNotNull("unexpected null for subX on: " + i, m);
        }

        connection.request(createCommitTransaction2Phase(connectionInfo, txid));

        // validate recovery complete
        dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);
        assertEquals("there are no prepared tx", 0, dataArrayResponse.getData().length);
    }

    public void testQueuePersistentPreparedAcksAvailableAfterRestartAndRollback() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        int numMessages = 4;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        ConsumerInfo consumerInfo;
        Message message = null;
        for (ActiveMQDestination dest : destinationList(destination)) {
            // Setup the consumer and receive the message.
            consumerInfo = createConsumerInfo(sessionInfo, dest);
            connection.send(consumerInfo);

            for (int i = 0; i < numMessages; i++) {
                message = receiveMessage(connection);
                assertNotNull(message);
            }

            // one ack with last received, mimic a beforeEnd synchronization
            MessageAck ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }

        connection.request(createPrepareTransaction(connectionInfo, txid));

        // restart the broker.
        restartBroker();

        connection = createConnection();
        connectionInfo = createConnectionInfo();
        connection.send(connectionInfo);

        // validate recovery
        TransactionInfo recoverInfo = new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER);
        DataArrayResponse dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);

        assertEquals("there is a prepared tx", 1, dataArrayResponse.getData().length);
        assertEquals("it matches", txid, dataArrayResponse.getData()[0]);

        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // no redelivery, exactly once semantics while prepared
        message = receiveMessage(connection);
        assertNull(message);
        assertNoMessagesLeft(connection);
        connection.request(consumerInfo.createRemoveCommand());

        LOG.info("Send some more before the rollback");
        // send some more messages
        producerInfo = createProducerInfo(sessionInfo);
        connection.send(producerInfo);

        for (int i = 0; i < numMessages*2; i++) {
            message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        LOG.info("Send some more before the rollback");

        // rollback so we get redelivery
        connection.request(createRollbackTransaction(connectionInfo, txid));

        LOG.info("new tx for redelivery");
        txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        Set<ConsumerInfo> consumerInfoSet = new HashSet<ConsumerInfo>();
        for (ActiveMQDestination dest : destinationList(destination)) {
            // Setup the consumer and receive the message.
            consumerInfo = createConsumerInfo(sessionInfo, dest);
            connection.send(consumerInfo);
            consumerInfoSet.add(consumerInfo);
            LOG.info("consume messages for: " + dest.getPhysicalName() + " " + consumerInfo.getConsumerId());

            for (int i = 0; i < numMessages; i++) {
                message = receiveMessage(connection);
                assertNotNull("unexpected null on:" + i, message);
                LOG.info(dest.getPhysicalName()  + " ID: " + message.getMessageId());
            }
            MessageAck ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.request(ack);

            // clear any pending messages on the stub connection via prefetch
            while ((message = receiveMessage(connection)) != null) {
                LOG.info("Pre fetched and unwanted: " + message.getMessageId() + " on " + message.getDestination().getPhysicalName());
            }
        }

        LOG.info("commit..");
        // Commit
        connection.request(createCommitTransaction1Phase(connectionInfo, txid));

        // remove consumers 'after' commit b/c of inflight tally issue
        for (ConsumerInfo info : consumerInfoSet) {
            connection.request(info.createRemoveCommand());
        }
        consumerInfoSet.clear();

        // validate recovery complete
        dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);
        assertEquals("there are no prepared tx", 0, dataArrayResponse.getData().length);

        LOG.info("consume additional messages");

        // clear any pending messages on the stub connection via prefetch
        while ((message = receiveMessage(connection)) != null) {
            LOG.info("Pre fetched and unwanted: " + message.getMessageId() + " on " + message.getDestination().getPhysicalName());
        }
        // consume the additional messages
        for (ActiveMQDestination dest : destinationList(destination)) {

            // Setup the consumer and receive the message.
            consumerInfo = createConsumerInfo(sessionInfo, dest);
            connection.request(consumerInfo);

            LOG.info("consume additional messages for: " + dest.getPhysicalName() + " " + consumerInfo.getConsumerId());

            for (int i = 0; i < numMessages*2; i++) {
                message = receiveMessage(connection);
                assertNotNull("unexpected null on:" + i, message);
                LOG.info(dest.getPhysicalName()  + " ID: " + message.getMessageId());
                MessageAck ack = createAck(consumerInfo, message, 1, MessageAck.STANDARD_ACK_TYPE);
                connection.request(ack);
            }
            connection.request(consumerInfo.createRemoveCommand());
        }

        assertNoMessagesLeft(connection);
    }

    public void testQueuePersistentPreparedAcksAvailableAfterRollbackPrefetchOne() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        int numMessages = 1;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        // use consumer per destination for the composite dest case
        // bc the same composite dest is used for sending so there
        // will be duplicate message ids in the mix which a single
        // consumer (PrefetchSubscription) cannot handle in a tx
        // atm. The matching is based on messageId rather than messageId
        // and destination
        Set<ConsumerInfo> consumerInfos = new HashSet<ConsumerInfo>();
        for (ActiveMQDestination dest : destinationList(destination)) {
            ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, dest);
            consumerInfo.setPrefetchSize(numMessages);
            consumerInfos.add(consumerInfo);
        }

        for (ConsumerInfo info : consumerInfos) {
            connection.send(info);
        }

        Message message = null;
        for (ConsumerInfo info : consumerInfos) {
            for (int i = 0; i < numMessages; i++) {
               message = receiveMessage(connection);
               assertNotNull(message);
               connection.send(createAck(info, message, 1, MessageAck.DELIVERED_ACK_TYPE));
            }
            MessageAck ack = createAck(info, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }
        connection.request(createPrepareTransaction(connectionInfo, txid));

        // reconnect
        connection.send(connectionInfo.createRemoveCommand());
        connection = createConnection();
        connection.send(connectionInfo);

        // validate recovery
        TransactionInfo recoverInfo = new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER);
        DataArrayResponse dataArrayResponse = (DataArrayResponse) connection.request(recoverInfo);

        assertEquals("there is a prepared tx", 1, dataArrayResponse.getData().length);
        assertEquals("it matches", txid, dataArrayResponse.getData()[0]);

        connection.send(sessionInfo);

        for (ConsumerInfo info : consumerInfos) {
            connection.send(info);
        }

        // no redelivery, exactly once semantics while prepared
        message = receiveMessage(connection);
        assertNull(message);
        assertNoMessagesLeft(connection);

        // rollback so we get redelivery
        connection.request(createRollbackTransaction(connectionInfo, txid));

        LOG.info("new tx for redelivery");
        txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        for (ConsumerInfo info : consumerInfos) {
            for (int i = 0; i < numMessages; i++) {
                message = receiveMessage(connection);
                assertNotNull("unexpected null on:" + i, message);
                MessageAck ack = createAck(info, message, 1, MessageAck.STANDARD_ACK_TYPE);
                ack.setTransactionId(txid);
                connection.send(ack);
            }
        }

        // Commit
        connection.request(createCommitTransaction1Phase(connectionInfo, txid));

        // validate recovery complete
        dataArrayResponse = (DataArrayResponse) connection.request(recoverInfo);
        assertEquals("there are no prepared tx", 0, dataArrayResponse.getData().length);
    }


    public void testQueuePersistentPreparedAcksAvailableAfterRollback() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        int numMessages = 4;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        // use consumer per destination for the composite dest case
        // bc the same composite dest is used for sending so there
        // will be duplicate message ids in the mix which a single
        // consumer (PrefetchSubscription) cannot handle in a tx
        // atm. The matching is based on messageId rather than messageId
        // and destination
        Set<ConsumerInfo> consumerInfos = new HashSet<ConsumerInfo>();
        for (ActiveMQDestination dest : destinationList(destination)) {
            ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, dest);
            consumerInfos.add(consumerInfo);
        }

        Message message = null;
        for (ConsumerInfo info : consumerInfos) {
            // one by one registration to avoid ordering issue with concurrent dispatch from composite dests broker side
            connection.request(info);
            for (int i = 0; i < numMessages; i++) {
                message = receiveMessage(connection);
                assertNotNull(message);
                LOG.info("ORIG " + message.getMessageId());
                connection.send(createAck(info, message, 1, MessageAck.DELIVERED_ACK_TYPE));
            }
            MessageAck ack = createAck(info, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.send(ack);
        }
        connection.request(createPrepareTransaction(connectionInfo, txid));

        // reconnect
        connection.send(connectionInfo.createRemoveCommand());
        connection = createConnection();
        connection.send(connectionInfo);

        // validate recovery
        TransactionInfo recoverInfo = new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER);
        DataArrayResponse dataArrayResponse = (DataArrayResponse) connection.request(recoverInfo);

        assertEquals("there is a prepared tx", 1, dataArrayResponse.getData().length);
        assertEquals("it matches", txid, dataArrayResponse.getData()[0]);

        connection.send(sessionInfo);

        LOG.info("add consumers..");
        for (ConsumerInfo info : consumerInfos) {
            connection.send(info);
        }

        // no redelivery, exactly once semantics while prepared
        message = receiveMessage(connection);
        assertNull(message);
        assertNoMessagesLeft(connection);

        // rollback so we get redelivery
        connection.request(createRollbackTransaction(connectionInfo, txid));

        LOG.info("new tx for redelivery");
        txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        for (ConsumerInfo info : consumerInfos) {
            for (int i = 0; i < numMessages; i++) {
                message = receiveMessage(connection);
                assertNotNull("unexpected null on:" + i, message);
                LOG.info("REC " + message.getMessageId());
                MessageAck ack = createAck(info, message, 1, MessageAck.STANDARD_ACK_TYPE);
                ack.setTransactionId(txid);
                connection.send(ack);
            }
        }

        // Commit
        connection.request(createCommitTransaction1Phase(connectionInfo, txid));

        // validate recovery complete
        dataArrayResponse = (DataArrayResponse) connection.request(recoverInfo);
        assertEquals("there are no prepared tx", 0, dataArrayResponse.getData().length);
    }

    public void initCombosForTestTopicPersistentPreparedAcksAvailableAfterRestartAndRollback() {
        addCombinationValues("prioritySupport", new Boolean[]{Boolean.FALSE, Boolean.TRUE});
    }

    public void testTopicPersistentPreparedAcksAvailableAfterRestartAndRollback() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TryTopic");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // setup durable subs
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        int numMessages = 4;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        Message message = null;
        for (int i = 0; i < numMessages; i++) {
            message = receiveMessage(connection);
            assertNotNull(message);
        }

        // one ack with last received, mimic a beforeEnd synchronization
        MessageAck ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        connection.request(createPrepareTransaction(connectionInfo, txid));

        // restart the broker.
        restartBroker();

        connection = createConnection();
        connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        connection.send(connectionInfo);

        // validate recovery
        TransactionInfo recoverInfo = new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER);
        DataArrayResponse dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);

        assertEquals("there is a prepared tx", 1, dataArrayResponse.getData().length);
        assertEquals("it matches", txid, dataArrayResponse.getData()[0]);

        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        // no redelivery, exactly once semantics while prepared
        message = receiveMessage(connection);
        assertNull(message);
        assertNoMessagesLeft(connection);

        // rollback so we get redelivery
        connection.request(createRollbackTransaction(connectionInfo, txid));

        LOG.info("new tx for redelivery");
        txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        for (int i = 0; i < numMessages; i++) {
            message = receiveMessage(connection);
            assertNotNull("unexpected null on:" + i, message);
        }
        ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        // Commit
        connection.request(createCommitTransaction1Phase(connectionInfo, txid));

        // validate recovery complete
        dataArrayResponse = (DataArrayResponse)connection.request(recoverInfo);
        assertEquals("there are no prepared tx", 0, dataArrayResponse.getData().length);
    }

    public void initCombosForTestTopicPersistentPreparedAcksAvailableAfterRollback() {
        addCombinationValues("prioritySupport", new Boolean[]{Boolean.FALSE, Boolean.TRUE});
    }

    public void testTopicPersistentPreparedAcksAvailableAfterRollback() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TryTopic");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // setup durable subs
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        int numMessages = 4;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        Message message = null;
        for (int i = 0; i < numMessages; i++) {
            message = receiveMessage(connection);
            assertNotNull(message);
        }

        // one ack with last received, mimic a beforeEnd synchronization
        MessageAck ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        connection.request(createPrepareTransaction(connectionInfo, txid));

        // rollback so we get redelivery
        connection.request(createRollbackTransaction(connectionInfo, txid));

        LOG.info("new consumer/tx for redelivery");
        connection.request(closeConnectionInfo(connectionInfo));

        connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);

        // setup durable subs
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        for (int i = 0; i < numMessages; i++) {
            message = receiveMessage(connection);
            assertNotNull("unexpected null on:" + i, message);
        }
        ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        // Commit
        connection.request(createCommitTransaction1Phase(connectionInfo, txid));
    }


    public void initCombosForTestTopicPersistentPreparedAcksUnavailableTillRollback() {
        addCombinationValues("keepDurableSubsActive", new Boolean[]{Boolean.FALSE, Boolean.TRUE});
    }

    public void testTopicPersistentPreparedAcksUnavailableTillRollback() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TryTopic");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // setup durable subs
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        int numMessages = 4;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        Message message = null;
        for (int i = 0; i < numMessages; i++) {
            message = receiveMessage(connection);
            assertNotNull(message);
        }

        // one ack with last received, mimic a beforeEnd synchronization
        MessageAck ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        connection.request(createEndTransaction(connectionInfo, txid));
        connection.request(createPrepareTransaction(connectionInfo, txid));

        // reconnect, verify perpared acks unavailable
        connection.request(closeConnectionInfo(connectionInfo));

        LOG.info("new consumer for *no* redelivery");

        connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);

        // setup durable subs
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        message = receiveMessage(connection, 2000);
        assertNull("unexpected non null", message);

        // rollback original tx
        connection.request(createRollbackTransaction(connectionInfo, txid));

        // verify receive after rollback
        for (int i = 0; i < numMessages; i++) {
            message = receiveMessage(connection);
            assertNotNull("unexpected null on:" + i, message);
        }

        // unsubscribe
        connection.request(consumerInfo.createRemoveCommand());
        RemoveSubscriptionInfo removeSubscriptionInfo = new RemoveSubscriptionInfo();
        removeSubscriptionInfo.setClientId(connectionInfo.getClientId());
        removeSubscriptionInfo.setSubscriptionName(consumerInfo.getSubscriptionName());
        connection.request(removeSubscriptionInfo);
    }

    public void initCombosForTestTopicPersistentPreparedAcksUnavailableTillComplete() {
        addCombinationValues("keepDurableSubsActive", new Boolean[]{Boolean.FALSE, Boolean.TRUE});
    }

    public void testTopicPersistentPreparedAcksUnavailableTillComplete() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TryTopic");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // setup durable subs
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        int numMessages = 4;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        Message message = null;
        for (int i = 0; i < numMessages; i++) {
            message = receiveMessage(connection);
            assertNotNull(message);
        }

        // one ack with last received, mimic a beforeEnd synchronization
        MessageAck ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        connection.request(createEndTransaction(connectionInfo, txid));
        connection.request(createPrepareTransaction(connectionInfo, txid));

        // reconnect, verify perpared acks unavailable
        connection.request(closeConnectionInfo(connectionInfo));

        LOG.info("new consumer for *no* redelivery");

        connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);

        // setup durable subs
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        message = receiveMessage(connection, 2000);
        assertNull("unexpected non null", message);

        // commit original tx
        connection.request(createCommitTransaction2Phase(connectionInfo, txid));

        // verify still unavailable
        message = receiveMessage(connection, 2000);
        assertNull("unexpected non null: " + message, message);

        // unsubscribe
        connection.request(consumerInfo.createRemoveCommand());
        RemoveSubscriptionInfo removeSubscriptionInfo = new RemoveSubscriptionInfo();
        removeSubscriptionInfo.setClientId(connectionInfo.getClientId());
        removeSubscriptionInfo.setSubscriptionName(consumerInfo.getSubscriptionName());
        connection.request(removeSubscriptionInfo);
    }

    public void initCombosForTestNoDupOnRollbackRedelivery() {
        addCombinationValues("keepDurableSubsActive", new Boolean[]{Boolean.FALSE, Boolean.TRUE});
    }

    public void testNoDupOnRollbackRedelivery() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TryTopic");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        // setup durable subs
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        int numMessages = 1;
        for (int i = 0; i < numMessages; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        Message message = null;
        for (int i = 0; i < numMessages; i++) {
            message = receiveMessage(connection);
            assertNotNull(message);
        }

        // one ack with last received, mimic a beforeEnd synchronization
        MessageAck ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        connection.request(createEndTransaction(connectionInfo, txid));
        connection.request(createRollbackTransaction(connectionInfo, txid));

        connection.send(consumerInfo.createRemoveCommand());
        connection.send(sessionInfo.createRemoveCommand());
        connection.send(connectionInfo.createRemoveCommand());


        LOG.info("new connection/consumer for redelivery");

        connection.request(closeConnectionInfo(connectionInfo));

        connectionInfo = createConnectionInfo();
        connectionInfo.setClientId("durable");
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);

        // setup durable subs
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSubscriptionName("durable");
        connection.send(consumerInfo);

        message = receiveMessage(connection);
        assertNotNull(message);

        Message dup = receiveMessage(connection);
        assertNull("no duplicate send: " + dup, dup);

        txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        ack = createAck(consumerInfo, message, numMessages, MessageAck.STANDARD_ACK_TYPE);
        ack.setTransactionId(txid);
        connection.send(ack);

        connection.request(createEndTransaction(connectionInfo, txid));
        connection.request(createCommitTransaction1Phase(connectionInfo, txid));

        // unsubscribe
        connection.request(consumerInfo.createRemoveCommand());
        RemoveSubscriptionInfo removeSubscriptionInfo = new RemoveSubscriptionInfo();
        removeSubscriptionInfo.setClientId(connectionInfo.getClientId());
        removeSubscriptionInfo.setSubscriptionName(consumerInfo.getSubscriptionName());
        connection.request(removeSubscriptionInfo);
    }

    private ActiveMQDestination[] destinationList(ActiveMQDestination dest) {
        return dest.isComposite() ? dest.getCompositeDestinations() : new ActiveMQDestination[]{dest};
    }

    private int expectedMessageCount(int i, ActiveMQDestination destination) {
        return i * (destination.isComposite() ? destination.getCompositeDestinations().length : 1);
    }

    public void testQueuePersistentUncommittedAcksLostOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
        }

        // Begin the transaction.
        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));

        Message message = null;
        for (ActiveMQDestination dest : destinationList(destination)) {
            // Setup the consumer and receive the message.
            ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, dest);
            connection.send(consumerInfo);

            for (int i = 0; i < 4; i++) {
                message = receiveMessage(connection);
                assertNotNull(message);
            }
            MessageAck ack = createAck(consumerInfo, message, 4, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection.request(ack);
        }

        // Don't commit

        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);

        for (ActiveMQDestination dest : destinationList(destination)) {
            // Setup the consumer and receive the message.
            ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, dest);
            connection.send(consumerInfo);

            for (int i = 0; i < 4; i++) {
                message = receiveMessage(connection);
                assertNotNull(message);
            }
        }

        assertNoMessagesLeft(connection);
    }

    @Override
    protected PolicyEntry getDefaultPolicy() {
        PolicyEntry policyEntry = super.getDefaultPolicy();
        policyEntry.setPrioritizedMessages(prioritySupport);
        return policyEntry;
    }

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);
        broker.setKeepDurableSubsActive(keepDurableSubsActive);
        maxWait = 2000;
    }

    public static Test suite() {
        return suite(XARecoveryBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected ActiveMQDestination createDestination() {
        return new ActiveMQQueue(getClass().getName() + "." + getName());
    }

}
