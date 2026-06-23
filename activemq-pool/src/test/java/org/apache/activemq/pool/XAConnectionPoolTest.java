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
package org.apache.activemq.pool;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.JMSContext;
import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.QueueSender;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;
import jakarta.jms.TopicPublisher;
import jakarta.jms.TopicSession;
import javax.naming.spi.ObjectFactory;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.InvalidTransactionException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.jms.pool.PooledSession;
import org.apache.activemq.test.TestSupport;

public class XAConnectionPoolTest extends TestSupport {

    // https://issues.apache.org/jira/browse/AMQ-3251
    public void testAfterCompletionCanClose() throws Exception {
        final Vector<Synchronization> syncs = new Vector<Synchronization>();
        ActiveMQTopic topic = new ActiveMQTopic("test");
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQXAConnectionFactory("vm://xaConnectionPoolTest?broker.persistent=false"));

        final Xid xid = createXid();
        // simple TM that is in a tx and will track syncs
        pcf.setTransactionManager(new TransactionManager() {
            @Override
            public void begin() throws NotSupportedException, SystemException {
            }

            @Override
            public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, RollbackException, SecurityException,
                SystemException {
            }

            @Override
            public int getStatus() throws SystemException {
                return Status.STATUS_ACTIVE;
            }

            @Override
            public Transaction getTransaction() throws SystemException {
                return new Transaction() {
                    @Override
                    public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException {
                    }

                    @Override
                    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
                        return false;
                    }

                    @Override
                    public boolean enlistResource(XAResource xaRes) throws IllegalStateException, RollbackException, SystemException {
                        try {
                            xaRes.start(xid, 0);
                        } catch (XAException e) {
                            throw new SystemException(e.getMessage());
                        }
                        return true;
                    }

                    @Override
                    public int getStatus() throws SystemException {
                        return 0;
                    }

                    @Override
                    public void registerSynchronization(Synchronization synch) throws IllegalStateException, RollbackException, SystemException {
                        syncs.add(synch);
                    }

                    @Override
                    public void rollback() throws IllegalStateException, SystemException {
                    }

                    @Override
                    public void setRollbackOnly() throws IllegalStateException, SystemException {
                    }
                };
            }

            @Override
            public void resume(Transaction tobj) throws IllegalStateException, InvalidTransactionException, SystemException {
            }

            @Override
            public void rollback() throws IllegalStateException, SecurityException, SystemException {
            }

            @Override
            public void setRollbackOnly() throws IllegalStateException, SystemException {
            }

            @Override
            public void setTransactionTimeout(int seconds) throws SystemException {
            }

            @Override
            public Transaction suspend() throws SystemException {
                return null;
            }
        });

        TopicConnection connection = (TopicConnection) pcf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        assertTrue(session instanceof PooledSession);

        TopicPublisher publisher = session.createPublisher(topic);
        publisher.publish(session.createMessage());

        // simulate a commit
        for (Synchronization sync : syncs) {
            sync.beforeCompletion();
        }
        for (Synchronization sync : syncs) {
            sync.afterCompletion(1);
        }
        connection.close();
        pcf.stop();
    }

    static long txGenerator = 22;
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

    public void testAckModeOfPoolNonXAWithTM() throws Exception {
        final Vector<Synchronization> syncs = new Vector<Synchronization>();
        ActiveMQTopic topic = new ActiveMQTopic("test");
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQXAConnectionFactory("vm://xaConnectionPoolTest?broker.persistent=false&jms.xaAckMode=" + Session.CLIENT_ACKNOWLEDGE));

        // simple TM that is in a tx and will track syncs
        pcf.setTransactionManager(new TransactionManager() {
            @Override
            public void begin() throws NotSupportedException, SystemException {
            }

            @Override
            public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, RollbackException, SecurityException,
                SystemException {
            }

            @Override
            public int getStatus() throws SystemException {
                return Status.STATUS_ACTIVE;
            }

            @Override
            public Transaction getTransaction() throws SystemException {
                return new Transaction() {
                    @Override
                    public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException {
                    }

                    @Override
                    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
                        return false;
                    }

                    @Override
                    public boolean enlistResource(XAResource xaRes) throws IllegalStateException, RollbackException, SystemException {
                        return false;
                    }

                    @Override
                    public int getStatus() throws SystemException {
                        return 0;
                    }

                    @Override
                    public void registerSynchronization(Synchronization synch) throws IllegalStateException, RollbackException, SystemException {
                        syncs.add(synch);
                    }

                    @Override
                    public void rollback() throws IllegalStateException, SystemException {
                    }

                    @Override
                    public void setRollbackOnly() throws IllegalStateException, SystemException {
                    }
                };
            }

            @Override
            public void resume(Transaction tobj) throws IllegalStateException, InvalidTransactionException, SystemException {
            }

            @Override
            public void rollback() throws IllegalStateException, SecurityException, SystemException {
            }

            @Override
            public void setRollbackOnly() throws IllegalStateException, SystemException {
            }

            @Override
            public void setTransactionTimeout(int seconds) throws SystemException {
            }

            @Override
            public Transaction suspend() throws SystemException {
                return null;
            }
        });

        TopicConnection connection = (TopicConnection) pcf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        assertEquals("client ack is enforce", Session.CLIENT_ACKNOWLEDGE, session.getAcknowledgeMode());
        TopicPublisher publisher = session.createPublisher(topic);
        publisher.publish(session.createMessage());

        // simulate a commit
        for (Synchronization sync : syncs) {
            sync.beforeCompletion();
        }
        for (Synchronization sync : syncs) {
            sync.afterCompletion(1);
        }
        connection.close();
        pcf.stop();
    }

    public void testInstanceOf() throws Exception {
        final XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        try {
            assertTrue(pcf instanceof QueueConnectionFactory);
            assertTrue(pcf instanceof TopicConnectionFactory);
        } finally {
            pcf.stop();
        }
    }

    public void testBindable() throws Exception {
        final XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        try {
            assertTrue(pcf instanceof ObjectFactory);
            assertTrue(((ObjectFactory) pcf).getObjectInstance(null, null, null, null) instanceof XaPooledConnectionFactory);
            assertTrue(pcf.isTmFromJndi());
        } finally {
            pcf.stop();
        }
    }

    public void testBindableEnvOverrides() throws Exception {
        final XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        try {
            assertTrue(pcf instanceof ObjectFactory);
            final Hashtable<String, String> environment = new Hashtable<>();
            environment.put("tmFromJndi", String.valueOf(Boolean.FALSE));
            assertTrue(((ObjectFactory) pcf).getObjectInstance(null, null, null, environment) instanceof XaPooledConnectionFactory);
            assertFalse(pcf.isTmFromJndi());
        } finally {
            pcf.stop();
        }
    }

    public void testSenderAndPublisherDest() throws Exception {
        final XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        try {
            pcf.setConnectionFactory(new ActiveMQXAConnectionFactory("vm://xaConnectionPoolTest?broker.persistent=false"));

            try (final QueueConnection connection = pcf.createQueueConnection()) {
                final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                final QueueSender sender = session.createSender(session.createQueue("AA"));
                assertNotNull(sender.getQueue().getQueueName());
            }

            try (final TopicConnection topicConnection = pcf.createTopicConnection()) {
                final TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
                final TopicPublisher topicPublisher = topicSession.createPublisher(topicSession.createTopic("AA"));
                assertNotNull(topicPublisher.getTopic().getTopicName());
            }
        } finally {
            pcf.stop();
        }
    }

    public void testSessionArgsIgnoredWithTm() throws Exception {
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQXAConnectionFactory("vm://xaConnectionPoolTest?broker.persistent=false"));
        // simple TM that with no tx
        pcf.setTransactionManager(new TransactionManager() {
            @Override
            public void begin() throws NotSupportedException, SystemException {
                throw new SystemException("NoTx");
            }

            @Override
            public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, RollbackException, SecurityException,
                SystemException {
                throw new IllegalStateException("NoTx");
            }

            @Override
            public int getStatus() throws SystemException {
                return Status.STATUS_NO_TRANSACTION;
            }

            @Override
            public Transaction getTransaction() throws SystemException {
                throw new SystemException("NoTx");
            }

            @Override
            public void resume(Transaction tobj) throws IllegalStateException, InvalidTransactionException, SystemException {
                throw new IllegalStateException("NoTx");
            }

            @Override
            public void rollback() throws IllegalStateException, SecurityException, SystemException {
                throw new IllegalStateException("NoTx");
            }

            @Override
            public void setRollbackOnly() throws IllegalStateException, SystemException {
                throw new IllegalStateException("NoTx");
            }

            @Override
            public void setTransactionTimeout(int seconds) throws SystemException {
            }

            @Override
            public Transaction suspend() throws SystemException {
                throw new SystemException("NoTx");
            }
        });

        final QueueConnection connection = pcf.createQueueConnection();
        // like ee tck
        assertNotNull("can create session(false, 0)", connection.createQueueSession(false, 0));

        connection.close();
        pcf.stop();
    }

    private TransactionManager trackingTransactionManager(final Vector<Synchronization> syncs, final AtomicReference<Xid> currentXid,
                                                          final AtomicReference<XAResource> enlisted) {
        return new TransactionManager() {
            @Override
            public void begin() throws NotSupportedException, SystemException {
            }

            @Override
            public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, RollbackException, SecurityException, SystemException {
            }

            @Override
            public int getStatus() throws SystemException {
                return Status.STATUS_ACTIVE;
            }

            @Override
            public Transaction getTransaction() throws SystemException {
                return new Transaction() {
                    @Override
                    public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException {
                    }

                    @Override
                    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
                        return false;
                    }

                    @Override
                    public boolean enlistResource(XAResource xaRes) throws IllegalStateException, RollbackException, SystemException {
                        try {
                            xaRes.start(currentXid.get(), 0);
                            enlisted.set(xaRes);
                        } catch (XAException e) {
                            SystemException se = new SystemException("XAException errorCode=" + e.errorCode);
                            se.initCause(e);
                            throw se;
                        }
                        return true;
                    }

                    @Override
                    public int getStatus() throws SystemException {
                        return 0;
                    }

                    @Override
                    public void registerSynchronization(Synchronization synch) throws IllegalStateException, RollbackException, SystemException {
                        syncs.add(synch);
                    }

                    @Override
                    public void rollback() throws IllegalStateException, SystemException {
                    }

                    @Override
                    public void setRollbackOnly() throws IllegalStateException, SystemException {
                    }
                };
            }

            @Override
            public void resume(Transaction tobj) throws IllegalStateException, InvalidTransactionException, SystemException {
            }

            @Override
            public void rollback() throws IllegalStateException, SecurityException, SystemException {
            }

            @Override
            public void setRollbackOnly() throws IllegalStateException, SystemException {
            }

            @Override
            public void setTransactionTimeout(int seconds) throws SystemException {
            }

            @Override
            public Transaction suspend() throws SystemException {
                return null;
            }
        };
    }

    public void testCreateContextSpansTransactions() throws Exception {
        final Vector<Synchronization> syncs = new Vector<Synchronization>();
        final AtomicReference<Xid> currentXid = new AtomicReference<Xid>(createXid());
        final AtomicReference<XAResource> enlisted = new AtomicReference<XAResource>();
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQXAConnectionFactory("vm://xaConnectionPoolTest?broker.persistent=false"));
        pcf.setTransactionManager(trackingTransactionManager(syncs, currentXid, enlisted));

        JMSContext context = pcf.createContext();
        try {
            Queue queue = context.createQueue("test.xa.pool.context");

            // first transaction
            Xid firstXid = currentXid.get();
            context.createProducer().send(queue, "one");
            assertEquals("first transaction should have enlisted one session", 1, syncs.size());
            enlisted.get().end(firstXid, XAResource.TMSUCCESS);
            enlisted.get().rollback(firstXid);
            syncs.get(0).beforeCompletion();
            syncs.get(0).afterCompletion(1);

            // second transaction - the context must renew its pooled session and enlist again
            Xid secondXid = createXid();
            currentXid.set(secondXid);
            context.createProducer().send(queue, "two");
            assertEquals("second transaction should have enlisted a renewed session", 2, syncs.size());
            enlisted.get().end(secondXid, XAResource.TMSUCCESS);
            enlisted.get().rollback(secondXid);
            syncs.get(1).beforeCompletion();
            syncs.get(1).afterCompletion(1);
        } finally {
            context.close();
            pcf.stop();
        }
    }

    public void testJcaCreateContextSpansTransactions() throws Exception {
        final Vector<Synchronization> syncs = new Vector<Synchronization>();
        final AtomicReference<Xid> currentXid = new AtomicReference<Xid>(createXid());
        final AtomicReference<XAResource> enlisted = new AtomicReference<XAResource>();
        JcaPooledConnectionFactory pcf = new JcaPooledConnectionFactory();
        pcf.setName("jca-pool-context-test");
        pcf.setConnectionFactory(new ActiveMQXAConnectionFactory("vm://xaConnectionPoolTest?broker.persistent=false"));
        pcf.setTransactionManager(trackingTransactionManager(syncs, currentXid, enlisted));

        JMSContext context = pcf.createContext();
        try {
            Queue queue = context.createQueue("test.jca.pool.context");

            // first transaction
            Xid firstXid = currentXid.get();
            context.createProducer().send(queue, "one");
            assertEquals("first transaction should have enlisted one session", 1, syncs.size());
            enlisted.get().end(firstXid, XAResource.TMSUCCESS);
            enlisted.get().rollback(firstXid);
            syncs.get(0).beforeCompletion();
            syncs.get(0).afterCompletion(1);

            // second transaction - the context must renew its pooled session and enlist again
            Xid secondXid = createXid();
            currentXid.set(secondXid);
            context.createProducer().send(queue, "two");
            assertEquals("second transaction should have enlisted a renewed session", 2, syncs.size());
            enlisted.get().end(secondXid, XAResource.TMSUCCESS);
            enlisted.get().rollback(secondXid);
            syncs.get(1).beforeCompletion();
            syncs.get(1).afterCompletion(1);
        } finally {
            context.close();
            pcf.stop();
        }
    }
}
