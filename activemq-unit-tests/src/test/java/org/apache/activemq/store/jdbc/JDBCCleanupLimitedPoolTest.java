/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.jdbc;

import org.apache.activemq.ActiveMQXAConnection;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IOHelper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.MessageProducer;
import javax.jms.XASession;
import javax.sql.DataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static org.apache.activemq.util.TestUtils.createXid;


public class JDBCCleanupLimitedPoolTest {

    BrokerService broker;
    JDBCPersistenceAdapter jdbcPersistenceAdapter;
    BasicDataSource pool;
    EmbeddedDataSource derby;

    @Before
    public void setUp() throws Exception {
        System.setProperty("derby.system.home", new File(IOHelper.getDefaultDataDirectory()).getCanonicalPath());
        derby = new EmbeddedDataSource();
        derby.setDatabaseName("derbyDb");
        derby.setCreateDatabase("create");
        derby.getConnection().close();

        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        pool.close();
        DataSourceServiceSupport.shutdownDefaultDataSource(derby);
    }

    protected BrokerService createBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        jdbcPersistenceAdapter = new JDBCPersistenceAdapter();
        jdbcPersistenceAdapter.deleteAllMessages();
        jdbcPersistenceAdapter.setCleanupPeriod(0);
        jdbcPersistenceAdapter.setUseLock(false);
        pool = new BasicDataSource();
        pool.setDriverClassName(EmbeddedDriver.class.getCanonicalName());
        pool.setUrl("jdbc:derby:derbyDb;create=false");
        pool.setUsername("uid");
        pool.setPassword("pwd");
        pool.setMaxTotal(2);
        jdbcPersistenceAdapter.setDataSource(pool);
        broker.setPersistenceAdapter(jdbcPersistenceAdapter);
        broker.addConnector("tcp://0.0.0.0:0");
        return broker;
    }


    @Test
    public void testNoDeadlockOnXaPoolExhaustion() throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        final CountDownLatch doneCommit = new CountDownLatch(1000);

        final ActiveMQXAConnectionFactory factory = new ActiveMQXAConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());

        ExecutorService executorService = Executors.newCachedThreadPool();
        // some contention over pool of 2
        for (int i = 0; i < 3; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        ActiveMQXAConnection conn = (ActiveMQXAConnection) factory.createXAConnection();
                        conn.start();
                        XASession sess = conn.createXASession();
                        while (done.getCount() > 0 && doneCommit.getCount() > 0) {
                            Xid xid = createXid();
                            sess.getXAResource().start(xid, XAResource.TMNOFLAGS);
                            MessageProducer producer = sess.createProducer(sess.createQueue("test"));
                            producer.send(sess.createTextMessage("test"));
                            sess.getXAResource().end(xid, XAResource.TMSUCCESS);
                            sess.getXAResource().prepare(xid);
                            sess.getXAResource().commit(xid, false);
                            doneCommit.countDown();
                        }

                        conn.close();

                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                    }
                }
            });
        }


        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!done.await(10, TimeUnit.MILLISECONDS) && doneCommit.getCount() > 0) {
                        jdbcPersistenceAdapter.cleanup();
                    }
                } catch (Exception ignored) {
                }

            }
        });

        executorService.shutdown();
        boolean allComplete = executorService.awaitTermination(40, TimeUnit.SECONDS);
        if (!allComplete) {
            TestSupport.dumpAllThreads("Why-at-count-" + doneCommit.getCount() +"-");
        }
        done.countDown();
        assertTrue("all complete", allComplete);
        executorService.shutdownNow();

        assertTrue("xa tx done", doneCommit.await(10, TimeUnit.SECONDS));
    }

}
