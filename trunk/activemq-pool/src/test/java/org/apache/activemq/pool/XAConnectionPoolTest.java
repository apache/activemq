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

import java.util.Vector;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
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
import javax.transaction.xa.XAResource;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.test.TestSupport;

public class XAConnectionPoolTest extends TestSupport {

    // https://issues.apache.org/jira/browse/AMQ-3251
    public void testAfterCompletionCanClose() throws Exception {
        final Vector<Synchronization> syncs = new Vector<Synchronization>();
        ActiveMQTopic topic = new ActiveMQTopic("test");
        XaPooledConnectionFactory pcf = new XaPooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQXAConnectionFactory("vm://test?broker.persistent=false"));

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
    }
}
