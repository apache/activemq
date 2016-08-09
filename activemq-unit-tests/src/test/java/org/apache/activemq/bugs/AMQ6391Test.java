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
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XASession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnection;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.ActiveMQXASession;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.state.ConnectionStateTracker;
import org.apache.activemq.state.TransactionState;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ6391Test {

    private static final Logger logger = LoggerFactory.getLogger(AMQ6391Test.class);

    private Xid currentXid;
    private XAResource recvResource;
    private XAResource otherResource;

    @SuppressWarnings("unchecked")
    @Test
    public void testCommitLeak() throws Exception {

        int messageCount = 1000;
        URI failoverUri = new URI("failover:(vm://localhost)?jms.redeliveryPolicy.maximumRedeliveries=0");

        Destination dest = new ActiveMQQueue("Failover.Leak");

        sendMessages(failoverUri, dest, messageCount);

        ActiveMQXAConnectionFactory xaCf = new ActiveMQXAConnectionFactory(failoverUri);
        ActiveMQXAConnection recvConnection = (ActiveMQXAConnection) xaCf.createXAConnection();
        recvConnection.start();

        final ActiveMQXASession recvSession = (ActiveMQXASession) recvConnection.createXASession();
        recvResource = recvSession.getXAResource();

        MessageConsumer consumer = recvSession.createConsumer(dest);

        final CountDownLatch latch = new CountDownLatch(messageCount);

        final ActiveMQXAConnection sendConnection = (ActiveMQXAConnection) xaCf.createXAConnection();
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message msg) {
                try {
                    startTx();

                    // if we would get the connection from the pool, we would just effectively just create a new session
                    XASession sendSession = sendConnection.createXASession();
                    XAResource res = sendSession.getXAResource();

                    joinTx(res);

                    // and send response within same transaction
                    Queue responseQueue = sendSession.createQueue("response");
                    MessageProducer prod = sendSession.createProducer(responseQueue);
                    TextMessage tmsg = (TextMessage) msg;
                    prod.send(sendSession.createTextMessage(tmsg.getText()));

                    // container would mark the end of transaction for us, but only the receiveConnection would get the
                    // commit
                    commit();
                    sendSession.close();
                } catch (JMSException | IOException e) {
                    logger.error("Error while processing ", e);
                } catch (XAException ex) {
                    logger.error("TX failure", ex);
                } finally {
                    latch.countDown();
                }
            }

        });

        latch.await();
        consumer.close();

        ResponseCorrelator respCorr = (ResponseCorrelator) sendConnection.getTransport();
        MutexTransport mutexTrans = (MutexTransport) respCorr.getNext();
        FailoverTransport failoverTrans = (FailoverTransport) mutexTrans.getNext();
        Field stateTrackerField = FailoverTransport.class.getDeclaredField("stateTracker");
        stateTrackerField.setAccessible(true);
        ConnectionStateTracker stateTracker = (ConnectionStateTracker) stateTrackerField.get(failoverTrans);
        Field statesField = ConnectionStateTracker.class.getDeclaredField("connectionStates");
        statesField.setAccessible(true);
        ConcurrentMap<ConnectionId, ConnectionState> states
                = (ConcurrentHashMap<ConnectionId, ConnectionState>) statesField.get(stateTracker);

        ConnectionState state = states.get(sendConnection.getConnectionInfo().getConnectionId());

        Collection<TransactionState> transactionStates = state.getTransactionStates();

        recvConnection.stop();
        recvConnection.close();
        sendConnection.stop();
        sendConnection.close();

        assertEquals("Transaction states not cleaned up", 0, transactionStates.size());
    }

    private ActiveMQConnection sendMessages(URI failoverUri, Destination dest, int messageCount) throws JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(failoverUri);
        ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(dest);
        for (int i = 0; i < messageCount; ++i) {
            producer.send(session.createTextMessage("Test message #" + i));
        }
        producer.close();
        session.commit();
        connection.close();
        return connection;
    }

    private void startTx() throws IOException, XAException {
        // before consumer is called a new session is started
        currentXid = createXid();
        otherResource = null;
        joinTx(recvResource);
    }

    private void joinTx(XAResource resource) throws XAException {
        resource.start(currentXid, XAResource.TMNOFLAGS);
        if (resource != recvResource) {
            otherResource = resource;
        }
    }

    private void end(XAResource resource) throws XAException {
        if (resource != null) {
            resource.end(currentXid, XAResource.TMSUCCESS);
        }
    }

    private void commit() throws XAException {
        end(recvResource);
        end(otherResource);
        // and after consumer is called the session is committed. Only one call per transaction and resource
        // manager is allowed. 
        recvResource.commit(currentXid, true);
    }

    private long txGenerator = 1;

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
}
