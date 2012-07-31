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

import java.util.List;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TransactionRolledBackException;
import org.apache.activemq.ActiveMQConnection;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbRestartJDBCQueueMasterSlaveTest extends JDBCQueueMasterSlaveTest {
    private static final transient Logger LOG = LoggerFactory.getLogger(DbRestartJDBCQueueMasterSlaveTest.class);
    
    protected void messageSent() throws Exception {
        verifyExpectedBroker(inflightMessageCount);
        if (++inflightMessageCount == failureCount) {
            LOG.info("STOPPING DB!@!!!!");
            final EmbeddedDataSource ds = ((SyncDataSource)getExistingDataSource()).getDelegate();
            ds.setShutdownDatabase("shutdown");
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
            assertEquals("connected to slave", slave.get().getBrokerName(), ((ActiveMQConnection)sendConnection).getBrokerName());
        }
    }

    protected void delayTillRestartRequired() {
        LOG.info("Waiting for master broker to Stop");
        master.waitUntilStopped();
    }

    protected void sendToProducer(MessageProducer producer,
            Destination producerDestination, Message message) throws JMSException {
        {   
            // do some retries as db failures filter back to the client until broker sees
            // db lock failure and shuts down
            boolean sent = false;
            do {
                try { 
                    producer.send(producerDestination, message);
                    sent = true;
                } catch (JMSException e) {
                    LOG.info("Exception on producer send for: " + message, e);
                    try { 
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {
                    }
                }
            } while(!sent);
        }
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
            } catch (JMSException ignored) {}

            if (e.getCause() instanceof TransactionRolledBackException) {
                TransactionRolledBackException transactionRolledBackException = (TransactionRolledBackException)e.getCause();
                if (transactionRolledBackException.getMessage().indexOf("in doubt") != -1) {
                    // failover chucked bc there is a missing reply to a commit. the ack may have got there and the reply
                    // was lost or the ack may be lost.
                    // so we may not get a resend.
                    //
                    // REVISIT: A JDBC store IO exception should not cause the connection to drop, so it needs to be wrapped
                    // possibly by the IOExceptionHandler
                    // The commit/close wrappers in jdbc TransactionContext need to delegate to the IOExceptionHandler

                    // this would leave the application aware of the store failure, and possible aware of whether the commit
                    // was a success, rather than going into failover-retries as it does now.

                }

            }
        }
    }
}
