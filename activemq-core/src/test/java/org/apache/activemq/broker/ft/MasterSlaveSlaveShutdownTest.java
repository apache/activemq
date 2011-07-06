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

import java.io.File;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterSlaveSlaveShutdownTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(MasterSlaveSlaveShutdownTest.class);

    BrokerService master;
    BrokerService slave;

    private void createMasterBroker() throws Exception {
        final BrokerService master = new BrokerService();
        master.setBrokerName("master");
        master.setPersistent(false);
        master.addConnector("tcp://localhost:0");

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        kaha.deleteAllMessages();
        master.setPersistenceAdapter(kaha);

        this.master = master;
    }

    private void createSlaveBroker() throws Exception {

        final BrokerService slave = new BrokerService();
        slave.setBrokerName("slave");
        slave.setPersistent(false);
        URI masterUri = master.getTransportConnectors().get(0).getConnectUri();
        slave.setMasterConnectorURI(masterUri.toString());
        slave.setUseJmx(false);
        slave.getManagementContext().setCreateConnector(false);

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        slave.setPersistenceAdapter(kaha);

        this.slave = slave;
    }

    public void tearDown() {
        try {
            this.master.stop();
        } catch (Exception e) {
        }
        this.master.waitUntilStopped();
        this.master = null;
        this.slave = null;
    }

    public void testSlaveShutsdownWhenWaitingForLock() throws Exception {

        createMasterBroker();
        createSlaveBroker();

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                try {
                    master.start();
                } catch (Exception e) {
                    LOG.warn("Exception starting master: " + e);
                    e.printStackTrace();
                }
            }
        });
        master.waitUntilStarted();

        Thread.sleep(2000);

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                try {
                    slave.start();
                } catch (Exception e) {
                    LOG.warn("Exception starting master: " + e);
                    e.printStackTrace();
                }
            }
        });
        slave.waitUntilStarted();
        Thread.sleep(TimeUnit.SECONDS.toMillis(15));

        LOG.info("killing slave..");
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                try {
                    slave.stop();
                } catch (Exception e) {
                    LOG.warn("Exception starting master: " + e);
                    e.printStackTrace();
                }
            }
        });

        Thread.sleep(TimeUnit.SECONDS.toMillis(15));
        assertFalse(slave.isStarted());
        slave.waitUntilStopped();
    }
}
