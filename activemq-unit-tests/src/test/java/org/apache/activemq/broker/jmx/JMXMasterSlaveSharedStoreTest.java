/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.broker.jmx;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import org.apache.activemq.TestSupport;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.xbean.XBeanBrokerService;
import org.junit.Test;

public class JMXMasterSlaveSharedStoreTest extends TestSupport {
    protected XBeanBrokerService master;
    protected XBeanBrokerService slave;
    protected AtomicReference<XBeanBrokerService> slaveAtomicReference = new AtomicReference<XBeanBrokerService>();
    protected CountDownLatch slaveStarted = new CountDownLatch(1);
    protected PersistenceAdapter persistenceAdapter;
    protected File messageStore;
    protected File schedulerStoreFile;

    @Override
    protected void setUp() throws Exception {
        setMaxTestTime(TimeUnit.MINUTES.toMillis(10));
        setAutoFail(true);

        messageStore = new File("target/activemq-data/kahadb/JMXMasterSlaveSharedStoreTest");
        schedulerStoreFile = new File("target/activemq-data/scheduler/JMXMasterSlaveSharedStoreTest/");

        if (System.getProperty("basedir") == null) {
            File file = new File(".");
            System.setProperty("basedir", file.getAbsolutePath());
        }

        createMaster();

        // Give master a chance to aquire lock.
        Thread.sleep(1000);
        createSlave();

        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        if (slave != null) {
            slave.stop();
        }

        if (master != null) {
            master.stop();
        }
    }

    protected void createMaster() throws Exception {
        master = createXBeanBrokerService("master");
        master.afterPropertiesSet();
    }

    protected void createSlave() throws Exception {
        // Start the Brokers async since starting them up could be a blocking operation..
        new Thread(new Runnable() {
            public void run() {
                try {
                    slave = createXBeanBrokerService("slave");
                    slave.afterPropertiesSet();
                    slaveAtomicReference.set(slave);
                    slaveStarted.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }).start();

        // Wait for slave to be set as new broker.
        Thread.sleep(100);
    }

    private XBeanBrokerService createXBeanBrokerService(String name) throws Exception {
        String[] connectors = {"tcp://localhost:" + 0};

        // Setup messaging store
        PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(messageStore);

        XBeanBrokerService broker = new XBeanBrokerService();
        broker.setUseJmx(true);
        broker.setBrokerName(name);
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.setTransportConnectorURIs(connectors);
        broker.setSchedulerSupport(true);
        broker.setSchedulerDirectoryFile(schedulerStoreFile);
        broker.getManagementContext().setCreateConnector(false);
        return broker;
    }

    private String getXBeanBrokerServiceMBeanName(String brokerName) {
        return "org.apache.activemq:type=Broker,brokerName=" + brokerName;
    }


    @Test
    public void testJMXMBeanIsRegisteredForSlave() throws Exception {
        assertFalse(master.isSlave());
        assertTrue(slave.isSlave());

        // Expected MBeans:
        ObjectName masterMBeanName = new ObjectName(getXBeanBrokerServiceMBeanName("master"));
        ObjectName slaveMBeanName = new ObjectName(getXBeanBrokerServiceMBeanName("slave"));

        MBeanServerConnection connection = master.getManagementContext().getMBeanServer();
        assertFalse(connection.queryMBeans(masterMBeanName, null).isEmpty());
        assertFalse(connection.queryMBeans(slaveMBeanName, null).isEmpty());
    }
}