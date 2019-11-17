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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTopicSendReceiveWithTwoConnectionsTest;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.jdbc.DataSourceServiceSupport;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.util.DefaultIOExceptionHandler;
import org.apache.activemq.util.IOHelper;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbRestartJDBCQueueTest extends JmsTopicSendReceiveWithTwoConnectionsTest implements ExceptionListener {
    private static final transient Logger LOG = LoggerFactory.getLogger(DbRestartJDBCQueueTest.class);

    public boolean transactedSends = false;
    public int failureCount = 25;  // or 20 for even tx batch boundary

    int inflightMessageCount = 0;
    EmbeddedDataSource sharedDs;
    BrokerService broker;
    final CountDownLatch restartDBLatch = new CountDownLatch(1);

    protected void setUp() throws Exception {
        setAutoFail(true);
        topic = false;
        verbose = true;
        // startup db
        sharedDs = (EmbeddedDataSource) DataSourceServiceSupport.createDataSource(IOHelper.getDefaultDataDirectory());

        broker = new BrokerService();

        DefaultIOExceptionHandler handler = new DefaultIOExceptionHandler();
        handler.setIgnoreSQLExceptions(false);
        handler.setStopStartConnectors(true);
        broker.setIoExceptionHandler(handler);
        broker.addConnector("tcp://localhost:0");
        broker.setUseJmx(false);
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(true);
        JDBCPersistenceAdapter persistenceAdapter = new JDBCPersistenceAdapter();
        persistenceAdapter.setDataSource(sharedDs);
        persistenceAdapter.setUseLock(false);
        persistenceAdapter.setLockKeepAlivePeriod(500);
        persistenceAdapter.getLocker().setLockAcquireSleepInterval(500);
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.start();
        super.setUp();
    }

    protected void tearDown() throws  Exception {
       super.tearDown();
       broker.stop();
    }

    @After
    public void shutDownDerby() {
        DataSourceServiceSupport.shutdownDefaultDataSource(sharedDs);
    }

    protected Session createSendSession(Connection sendConnection) throws Exception {
        if (transactedSends) {
            return sendConnection.createSession(true, Session.SESSION_TRANSACTED);
        } else {
            return sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory f =
                new ActiveMQConnectionFactory("failover://" + broker.getTransportConnectors().get(0).getPublishableConnectString());
        f.setExceptionListener(this);
        return f;
    }

    @Override
    protected void messageSent() throws Exception {    
        if (++inflightMessageCount == failureCount) {
            LOG.info("STOPPING DB!@!!!!");
            final EmbeddedDataSource ds = sharedDs;
            ds.setShutdownDatabase("shutdown");
            ds.setCreateDatabase("not_any_more");
            try {
                ds.getConnection();
            } catch (Exception ignored) {
            }
            LOG.info("DB STOPPED!@!!!!");
            
            Thread dbRestartThread = new Thread("db-re-start-thread") {
                public void run() {
                    LOG.info("Sleeping for 10 seconds before allowing db restart");
                    try {
                        restartDBLatch.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    ds.setShutdownDatabase("false");
                    LOG.info("DB RESTARTED!@!!!!");
                }
            };
            dbRestartThread.start();
        }
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

                    if (transactedSends && ((inflightMessageCount+1) %10 == 0 || (inflightMessageCount+1) >= messageCount)) {
                        LOG.info("committing on send: " + inflightMessageCount + " message: " + message);
                        session.commit();
                    }

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

    @Override
    public void onException(JMSException exception) {
        LOG.error("exception on connection: ", exception);
    }
}
