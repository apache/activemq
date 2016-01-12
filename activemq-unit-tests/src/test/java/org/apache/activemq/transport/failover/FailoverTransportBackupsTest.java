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
package org.apache.activemq.transport.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverTransportBackupsTest {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverTransportBackupsTest.class);

    protected Transport transport;
    protected FailoverTransport failoverTransport;
    private int transportInterruptions;
    private int transportResumptions;

    BrokerService broker1;
    BrokerService broker2;
    BrokerService broker3;

    @Before
    public void setUp() throws Exception {
        broker1 = createBroker("1");
        broker2 = createBroker("2");
        broker3 = createBroker("3");

        broker1.start();
        broker2.start();
        broker3.start();

        broker1.waitUntilStarted();
        broker2.waitUntilStarted();
        broker3.waitUntilStarted();

        // Reset stats
        transportInterruptions = 0;
        transportResumptions = 0;
    }

    @After
    public void tearDown() throws Exception {
        if (transport != null) {
            transport.stop();
        }

        broker1.stop();
        broker1.waitUntilStopped();
        broker2.stop();
        broker2.waitUntilStopped();
        broker3.stop();
        broker3.waitUntilStopped();
    }

    @Test
    public void testBackupsAreCreated() throws Exception {
        this.transport = createTransport(2);
        assertNotNull(failoverTransport);
        assertTrue(failoverTransport.isBackup());
        assertEquals(2, failoverTransport.getBackupPoolSize());

        assertTrue("Timed out waiting for Backups to connect.", Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                LOG.debug("Current Backup Count = " + failoverTransport.getCurrentBackups());
                return failoverTransport.getCurrentBackups() == 2;
            }
        }));
    }

    @Test
    public void testFailoverToBackups() throws Exception {
        this.transport = createTransport(2);
        assertNotNull(failoverTransport);
        assertTrue(failoverTransport.isBackup());
        assertEquals(2, failoverTransport.getBackupPoolSize());

        assertTrue("Timed out waiting for Backups to connect.", Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                LOG.debug("Current Backup Count = " + failoverTransport.getCurrentBackups());
                return failoverTransport.getCurrentBackups() == 2;
            }
        }));

        assertEquals("conected to..", "1", currentBrokerInfo.getBrokerName());
        broker1.stop();

        assertTrue("Timed out waiting for Backups to connect.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.debug("Current Backup Count = " + failoverTransport.getCurrentBackups());
                return failoverTransport.getCurrentBackups() == 1;
            }
        }));

        assertTrue("Incorrect number of Transport interruptions", transportInterruptions >= 1);
        assertTrue("Incorrect number of Transport resumptions", transportResumptions >= 1);

        assertEquals("conected to..", "2", currentBrokerInfo.getBrokerName());
        broker2.stop();

        assertTrue("Timed out waiting for Backups to connect.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.debug("Current Backup Count = " + failoverTransport.getCurrentBackups());
                return failoverTransport.getCurrentBackups() == 0;
            }
        }));

        assertTrue("Incorrect number of Transport interruptions", transportInterruptions >= 2);
        assertTrue("Incorrect number of Transport resumptions", transportResumptions >= 2);

        assertEquals("conected to..", "3", currentBrokerInfo.getBrokerName());
    }

    @Test
    public void testBackupsRefilled() throws Exception {
        this.transport = createTransport(1);
        assertNotNull(failoverTransport);
        assertTrue(failoverTransport.isBackup());
        assertEquals(1, failoverTransport.getBackupPoolSize());

        assertTrue("Timed out waiting for Backups to connect.", Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                LOG.debug("Current Backup Count = " + failoverTransport.getCurrentBackups());
                return failoverTransport.getCurrentBackups() == 1;
            }
        }));

        broker1.stop();

        assertTrue("Timed out waiting for Backups to connect.", Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                LOG.debug("Current Backup Count = " + failoverTransport.getCurrentBackups());
                return failoverTransport.getCurrentBackups() == 1;
            }
        }));

        broker2.stop();

        assertTrue("Timed out waiting for Backups to connect.", Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                LOG.debug("Current Backup Count = " + failoverTransport.getCurrentBackups());
                return failoverTransport.getCurrentBackups() == 0;
            }
        }));
    }

    private BrokerService createBroker(String name) throws Exception {
        BrokerService bs = new BrokerService();
        bs.setBrokerName(name);
        bs.setUseJmx(false);
        bs.setPersistent(false);
        bs.addConnector("tcp://localhost:0");
        return bs;
    }

    BrokerInfo currentBrokerInfo;
    protected Transport createTransport(int backups) throws Exception {
        String connectionUri = "failover://("+
                               broker1.getTransportConnectors().get(0).getPublishableConnectString() + "," +
                               broker2.getTransportConnectors().get(0).getPublishableConnectString() + "," +
                               broker3.getTransportConnectors().get(0).getPublishableConnectString() + ")";

        if (backups > 0) {
            connectionUri += "?randomize=false&backup=true&backupPoolSize=" + backups;
        }

        Transport transport = TransportFactory.connect(new URI(connectionUri));
        transport.setTransportListener(new TransportListener() {

            @Override
            public void onCommand(Object command) {
                LOG.debug("Test Transport Listener received Command: " + command);
                if (command instanceof BrokerInfo) {
                    currentBrokerInfo = (BrokerInfo) command;
                    LOG.info("BrokerInfo: " + currentBrokerInfo);
                }
            }

            @Override
            public void onException(IOException error) {
                LOG.debug("Test Transport Listener received Exception: " + error);
            }

            @Override
            public void transportInterupted() {
                transportInterruptions++;
                LOG.debug("Test Transport Listener records transport Interrupted: " + transportInterruptions);
            }

            @Override
            public void transportResumed() {
                transportResumptions++;
                LOG.debug("Test Transport Listener records transport Resumed: " + transportResumptions);
            }
        });
        transport.start();

        this.failoverTransport = transport.narrow(FailoverTransport.class);

        return transport;
    }
}
