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
package org.apache.activemq.broker.ft;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TransactionRolledBackException;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbRestartJDBCQueueMasterSlaveTest extends JDBCQueueMasterSlaveTest {
    private static final transient Logger LOG = LoggerFactory.getLogger(DbRestartJDBCQueueMasterSlaveTest.class);
    
    protected void messageSent() throws Exception {
        verifyExpectedBroker(inflightMessageCount);
        if (++inflightMessageCount == failureCount) {
            LOG.info("STOPPING DB!@!!!!");
            final EmbeddedDataSource ds = ((SyncCreateDataSource)getExistingDataSource()).getDelegate();
            ds.setShutdownDatabase("shutdown");
            ds.setCreateDatabase("not_any_more");
            LOG.info("DB STOPPED!@!!!!");
            
            Thread dbRestartThread = new Thread("db-re-start-thread") {
                public void run() {
                    delayTillRestartRequired();
                    ds.setShutdownDatabase("false");
                    LOG.info("DB RESTARTED!@!!!!");
                }
            };
            dbRestartThread.start();
        }
        verifyExpectedBroker(inflightMessageCount);
    }

    protected void verifyExpectedBroker(int inflightMessageCount) {
        if (inflightMessageCount == 0) {
            assertEquals("connected to master", master.getBrokerName(), ((ActiveMQConnection)sendConnection).getBrokerName());
        } else if (inflightMessageCount == failureCount + 10) {
            assertEquals("connected to slave, count:" + inflightMessageCount, slave.get().getBrokerName(), ((ActiveMQConnection)sendConnection).getBrokerName());
        }
    }

    protected void delayTillRestartRequired() {
        LOG.info("Waiting for master broker to Stop");
        master.waitUntilStopped();
    }

    protected void sendToProducer(MessageProducer producer,
            Destination producerDestination, Message message) throws JMSException {
        producer.send(producerDestination, message);
    }

    @Override
    protected Session createReceiveSession(Connection receiveConnection) throws Exception {
        return receiveConnection.createSession(true, Session.SESSION_TRANSACTED);
    }

    @Override
    protected void consumeMessage(Message message, List<Message> messageList) {
        try {
            receiveSession.commit();
            super.consumeMessage(message, messageList);
        } catch (JMSException e) {
            LOG.info("Failed to commit message receipt: " + message, e);
            try {
                receiveSession.rollback();
            } catch (JMSException ignored) {
            }

            if (e instanceof TransactionRolledBackException) {
                TransactionRolledBackException transactionRolledBackException = (TransactionRolledBackException) e;
                if (transactionRolledBackException.getMessage().indexOf("in doubt") != -1) {
                    // failover chucked bc there is a missing reply to a commit.
                    // failover is involved b/c the store exception is handled broker side and the client just
                    // sees a disconnect (socket.close()).
                    // If the client needs to be aware of the failure then it should not use IOExceptionHandler
                    // so that the exception will propagate back

                    // for this test case:
                    // the commit may have got there and the reply is lost "or" the commit may be lost.
                    // so we may or may not get a resend.
                    //
                    // At the application level we need to determine if the message is there or not which is not trivial
                    // for this test we assert received == sent
                    // so we need to know whether the message will be replayed.
                    // we can ask the store b/c we know it is jdbc - guess we could go through a destination
                    // message store interface also or use jmx
                    java.sql.Connection dbConnection = null;
                    try {
                        ActiveMQMessage mqMessage = (ActiveMQMessage) message;
                        MessageId id = mqMessage.getMessageId();
                        dbConnection = sharedDs.getConnection();
                        PreparedStatement s = dbConnection.prepareStatement(findStatement);
                        s.setString(1, id.getProducerId().toString());
                        s.setLong(2, id.getProducerSequenceId());
                        ResultSet rs = s.executeQuery();

                        if (!rs.next()) {
                            // message is gone, so lets count it as consumed
                            LOG.info("On TransactionRolledBackException we know that the ack/commit got there b/c message is gone so we count it: " + mqMessage);
                            super.consumeMessage(message, messageList);
                        } else {
                            LOG.info("On TransactionRolledBackException we know that the ack/commit was lost so we expect a replay of: " + mqMessage);
                        }
                    } catch (Exception dbe) {
                        dbe.printStackTrace();
                    } finally {
                        try {
                            dbConnection.close();
                        } catch (SQLException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }
        }
    }

}
