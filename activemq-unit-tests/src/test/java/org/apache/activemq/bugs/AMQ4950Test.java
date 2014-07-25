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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.XASession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQXAConnection;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerRestartTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransactionBroker;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.transport.failover.FailoverTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test for AMQ-4950.
 * Simulates an error during XA prepare call.
 */
public class AMQ4950Test extends BrokerRestartTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(AMQ4950Test.class);
    protected static final String simulatedExceptionMessage = "Simulating error inside tx prepare().";
    public boolean prioritySupport = false;
    protected String connectionUri = null;

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        broker.setDestinationPolicy(policyMap);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setUseJmx(false);
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();
        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {

                    @Override
                    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
                        getNext().prepareTransaction(context, xid);
                        LOG.debug("BrokerPlugin.prepareTransaction() will throw an exception.");
                        throw new XAException(simulatedExceptionMessage);
                    }

                    @Override
                    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
                        LOG.debug("BrokerPlugin.commitTransaction().");
                        super.commitTransaction(context, xid, onePhase);
                    }
                }
        });
   }

    /**
     * Creates XA transaction and invokes XA prepare().
     * Due to registered BrokerFilter prepare will be handled by broker
     * but then throw an exception.
     * Prior to fixing AMQ-4950, this resulted in a ClassCastException
     * in ConnectionStateTracker.PrepareReadonlyTransactionAction.onResponse()
     * causing the failover transport to reconnect and replay the XA prepare().
     */
    public void testXAPrepareFailure() throws Exception {

        assertNotNull(connectionUri);
        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory("failover:(" + connectionUri + ")");
        ActiveMQXAConnection xaConnection = (ActiveMQXAConnection)cf.createConnection();
        xaConnection.start();
        XASession session = xaConnection.createXASession();
        XAResource resource = session.getXAResource();
        Xid tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);

        MessageProducer producer = session.createProducer(session.createQueue(this.getClass().getName()));
        Message message = session.createTextMessage("Sample Message");
        producer.send(message);
        resource.end(tid, XAResource.TMSUCCESS);
        try {
            LOG.debug("Calling XA prepare(), expecting an exception");
            int ret = resource.prepare(tid);
            if (XAResource.XA_OK == ret) 
                resource.commit(tid, false);
        } catch (XAException xae) {
            LOG.info("Received excpected XAException: {}", xae.getMessage());
            LOG.info("Rolling back transaction {}", tid);
            
            // with bug AMQ-4950 the thrown error reads "Cannot call prepare now"
            // we check that we receive the original exception message as 
            // thrown by the BrokerPlugin
            assertEquals(simulatedExceptionMessage, xae.getMessage());
            resource.rollback(tid);
        }
        // couple of assertions
        assertTransactionGoneFromBroker(tid);
        assertTransactionGoneFromConnection(broker.getBrokerName(), xaConnection.getClientID(), xaConnection.getConnectionInfo().getConnectionId(), tid);
        assertTransactionGoneFromFailoverState(xaConnection, tid);

        //cleanup
        producer.close();
        session.close();
        xaConnection.close();
        LOG.debug("testXAPrepareFailure() finished.");
    }


    public Xid createXid() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {
            public int getFormatId() {
                return 86;
            }

            public byte[] getGlobalTransactionId() {
                return bs;
            }

            public byte[] getBranchQualifier() {
                return bs;
            }
        };
    }


    private void assertTransactionGoneFromFailoverState(
            ActiveMQXAConnection connection1, Xid tid) throws Exception {

        FailoverTransport transport = (FailoverTransport) connection1.getTransport().narrow(FailoverTransport.class);
        TransactionInfo info = new TransactionInfo(connection1.getConnectionInfo().getConnectionId(), new XATransactionId(tid), TransactionInfo.COMMIT_ONE_PHASE);
        assertNull("transaction should not exist in the state tracker",
                transport.getStateTracker().processCommitTransactionOnePhase(info));
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
}
