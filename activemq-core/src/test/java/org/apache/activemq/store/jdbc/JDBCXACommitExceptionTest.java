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

package org.apache.activemq.store.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

// https://issues.apache.org/activemq/browse/AMQ-2880
public class JDBCXACommitExceptionTest extends JDBCCommitExceptionTest {
    private static final Log LOG = LogFactory.getLog(JDBCXACommitExceptionTest.class);

    private long txGenerator = System.currentTimeMillis();

    protected ActiveMQXAConnectionFactory factory = new ActiveMQXAConnectionFactory(
            "tcp://localhost:61616?jms.prefetchPolicy.all=0&jms.redeliveryPolicy.maximumRedeliveries="+messagesExpected); 

    boolean onePhase = true;

    public void testTwoPhaseSqlException() throws Exception {
        onePhase = false;
        doTestSqlException();
    }

    @Override
    protected int receiveMessages(int messagesExpected) throws Exception {
        XAConnection connection = factory.createXAConnection();
        connection.start();
        XASession session = connection.createXASession();

        jdbc.setShouldBreak(true);

        // first try and receive these messages, they'll continually fail
        receiveMessages(messagesExpected, session, onePhase);

        jdbc.setShouldBreak(false);

        // now that the store is sane, try and get all the messages sent
        return receiveMessages(messagesExpected, session, onePhase);
    }

    protected int receiveMessages(int messagesExpected, XASession session, boolean onePhase) throws Exception {
        int messagesReceived = 0;

        for (int i=0; i<messagesExpected; i++) {
            Destination destination = session.createQueue("TEST");
            MessageConsumer consumer = session.createConsumer(destination);

            XAResource resource = session.getXAResource();
            resource.recover(XAResource.TMSTARTRSCAN);
            resource.recover(XAResource.TMNOFLAGS);

            Xid tid = createXid();

            Message message = null;
            try {
                LOG.debug("Receiving message " + (messagesReceived+1) + " of " + messagesExpected);
                resource.start(tid, XAResource.TMNOFLAGS);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                resource.end(tid, XAResource.TMSUCCESS);
                if (message != null) {
                    if (onePhase) {
                        resource.commit(tid, true);
                    } else {
                        resource.prepare(tid);
                        resource.commit(tid, false);
                    }
                    messagesReceived++;
                }
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);

                try {
                    LOG.debug("Rolling back transaction (just in case, no need to do this as it is implicit in a 1pc commit failure) " + tid);
                    resource.rollback(tid);
                }
                catch (XAException ex) {
                    try {
                        LOG.debug("Caught exception during rollback: " + ex + " forgetting transaction " + tid);
                        resource.forget(tid);
                    }
                    catch (XAException ex1) {
                        LOG.debug("rollback/forget failed: " + ex1.errorCode);
                    }
                }
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        return messagesReceived;
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


}
