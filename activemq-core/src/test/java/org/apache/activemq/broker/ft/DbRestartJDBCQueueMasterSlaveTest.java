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

import java.sql.SQLException;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.activemq.ActiveMQConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class DbRestartJDBCQueueMasterSlaveTest extends JDBCQueueMasterSlaveTest {
    private static final transient Logger LOG = LoggerFactory.getLogger(DbRestartJDBCQueueMasterSlaveTest.class);
    
    protected void messageSent() throws Exception {
        verifyExpectedBroker(inflightMessageCount);
        if (++inflightMessageCount == failureCount) {
            LOG.info("STOPPING DB!@!!!!");
            final EmbeddedDataSource ds = getExistingDataSource();
            ds.setShutdownDatabase("shutdown");
            LOG.info("DB STOPPED!@!!!!");
            
            Thread dbRestartThread = new Thread("db-re-start-thread") {
                public void run() {
                    delayTillRestartRequired();
                    ds.setShutdownDatabase("false");
                    try {
                        ds.getConnection().close();
                    } catch (SQLException ignored) {}
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
                    LOG.info("Exception on producer send:", e);
                    try { 
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {
                    }
                }
            } while(!sent);
        }
    }
}
