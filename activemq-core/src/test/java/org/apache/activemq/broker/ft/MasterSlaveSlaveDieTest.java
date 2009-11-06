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

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MasterSlaveSlaveDieTest extends TestCase {
    
    private static final Log LOG = LogFactory.getLog(MasterSlaveSlaveDieTest.class);

    private final AtomicBoolean pluginStopped = new AtomicBoolean(false);
    class Plugin extends BrokerPluginSupport {

        @Override
        public void start() throws Exception {
            LOG.info("plugin start");
            super.start();
        }

        @Override
        public void stop() throws Exception {
            LOG.info("plugin stop");
            pluginStopped.set(true);
            super.stop();
        }
        
    }
    public void testSlaveDieMasterStays() throws Exception {
        final BrokerService master = new BrokerService();
        master.setBrokerName("master");
        master.setPersistent(false);
        master.addConnector("tcp://localhost:0");
        master.setWaitForSlave(true);
        master.setPlugins(new BrokerPlugin[] { new Plugin() });
        
        final BrokerService slave = new BrokerService();
        slave.setBrokerName("slave");
        slave.setPersistent(false);
        URI masterUri = master.getTransportConnectors().get(0).getConnectUri();
        //SocketProxy masterProxy = new SocketProxy(masterUri);
        slave.setMasterConnectorURI(masterUri.toString());
        
        slave.setUseJmx(false);
        slave.getManagementContext().setCreateConnector(false);

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                try {
                    master.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        slave.start();
        slave.waitUntilStarted();
        
        master.waitUntilStarted();
        
        LOG.info("killing slave..");
        slave.stop();
        slave.waitUntilStopped();

        LOG.info("checking master still alive");
        assertTrue("master is still alive", master.isStarted());
        assertFalse("plugin was not yet stopped", pluginStopped.get());
        master.stop();
        master.waitUntilStopped();
    }
}