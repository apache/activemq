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
package org.apache.activemq.jms.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Vector;

import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.naming.spi.ObjectFactory;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.ActiveMQXASession;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Test;

public class XAConnectionPoolTest extends JmsPoolTestSupport {

    // https://issues.apache.org/jira/browse/AMQ-3251
    @Test(timeout = 60000)
    public void testAfterCompletionCanClose() throws Exception {
        final Vector<Synchronization> syncs = new Vector<Synchronization>();
        ActiveMQTopic topic = new ActiveMQTopic("test");
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        pcf.setConnectionFactory(new XAConnectionFactoryOnly(new ActiveMQXAConnectionFactory("vm://test?broker.persistent=false")));

        final Xid xid = createXid();

        // simple TM that is in a tx and will track syncs
        pcf.setTransactionManager(new TransactionManager(){
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
        PooledSession pooledSession = (PooledSession) session;
        assertTrue(pooledSession.getInternalSession() instanceof ActiveMQXASession);

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



    @Test(timeout = 60000)
    public void testAckModeOfPoolNonXAWithTM() throws Exception {
        final Vector<Synchronization> syncs = new Vector<Synchronization>();
        ActiveMQTopic topic = new ActiveMQTopic("test");
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        pcf.setConnectionFactory(new XAConnectionFactoryOnly(new ActiveMQXAConnectionFactory(
            "vm://test?broker.persistent=false&broker.useJmx=false&jms.xaAckMode=" + Session.CLIENT_ACKNOWLEDGE)));

        // simple TM that is in a tx and will track syncs
        pcf.setTransactionManager(new TransactionManager(){
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

    @Test(timeout = 60000)
    public void testInstanceOf() throws  Exception {
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        assertTrue(pcf instanceof QueueConnectionFactory);
        assertTrue(pcf instanceof TopicConnectionFactory);
        pcf.stop();
    }

    @Test(timeout = 60000)
    public void testBindable() throws Exception {
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        assertTrue(pcf instanceof ObjectFactory);
        assertTrue(((ObjectFactory)pcf).getObjectInstance(null, null, null, null) instanceof XaPooledConnectionFactory);
        assertTrue(pcf.isTmFromJndi());
        pcf.stop();
    }

    @Test(timeout = 60000)
    public void testBindableEnvOverrides() throws Exception {
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        assertTrue(pcf instanceof ObjectFactory);
        Hashtable<String, Object> environment = new Hashtable<>();
        environment.put("tmFromJndi", Boolean.FALSE);
        assertTrue(((ObjectFactory) pcf).getObjectInstance(null, null, null, environment) instanceof XaPooledConnectionFactory);
        assertFalse(pcf.isTmFromJndi());
        pcf.stop();
    }

    @Test(timeout = 60000)
    public void testSenderAndPublisherDest() throws Exception {
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQXAConnectionFactory(
            "vm://test?broker.persistent=false&broker.useJmx=false"));

        QueueConnection connection = pcf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueSender sender = session.createSender(session.createQueue("AA"));
        assertNotNull(sender.getQueue().getQueueName());

        connection.close();

        TopicConnection topicConnection = pcf.createTopicConnection();
        TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicPublisher topicPublisher = topicSession.createPublisher(topicSession.createTopic("AA"));
        assertNotNull(topicPublisher.getTopic().getTopicName());

        topicConnection.close();
        pcf.stop();
    }

    @Test(timeout = 60000)
    public void testSessionArgsIgnoredWithTm() throws Exception {
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQXAConnectionFactory(
            "vm://test?broker.persistent=false&broker.useJmx=false"));

        // simple TM that with no tx
        pcf.setTransactionManager(new TransactionManager() {
            @Override
            public void begin() throws NotSupportedException, SystemException {
                throw new SystemException("NoTx");
            }

            @Override
            public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, RollbackException, SecurityException, SystemException {
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

        QueueConnection connection = pcf.createQueueConnection();
        // like ee tck
        assertNotNull("can create session(false, 0)", connection.createQueueSession(false, 0));

        connection.close();
        pcf.stop();
    }

    static class XAConnectionFactoryOnly implements XAConnectionFactory {
        private final XAConnectionFactory connectionFactory;

        XAConnectionFactoryOnly(XAConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        @Override
        public XAConnection createXAConnection() throws JMSException {
            return connectionFactory.createXAConnection();
        }

        @Override
        public XAConnection createXAConnection(String userName, String password) throws JMSException {
            return connectionFactory.createXAConnection(userName, password);
        }
    }
}
