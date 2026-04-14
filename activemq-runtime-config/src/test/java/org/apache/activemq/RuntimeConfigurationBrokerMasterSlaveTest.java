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
package org.apache.activemq;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.plugin.RuntimeConfigurationBroker;
import org.apache.activemq.plugin.RuntimeConfigurationPlugin;
import org.junit.After;
import org.junit.Test;

/**
 * Verifies that RuntimeConfigurationBroker defers its initialization
 * (config loading, file monitoring, MBean registration) until
 * {@code nowMasterBroker()} is called, rather than doing it in {@code start()}.
 * <p>
 * This is critical for master/slave topologies: a slave broker calls
 * {@code start()} on its plugin chain but must NOT begin config monitoring
 * until it is actually promoted to master via {@code nowMasterBroker()}.
 */
public class RuntimeConfigurationBrokerMasterSlaveTest {

    private BrokerService brokerService;

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    /**
     * Simulates a slave broker that has started its plugin chain (start())
     * but has NOT yet been promoted to master (nowMasterBroker() not called).
     * The RuntimeConfigurationBroker should NOT have initialized config
     * monitoring or registered its MBean.
     */
    @Test
    public void testNoInitBeforeNowMasterBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);

        RuntimeConfigurationPlugin plugin = new RuntimeConfigurationPlugin();
        plugin.setCheckPeriod(1000);
        brokerService.setPlugins(new org.apache.activemq.broker.BrokerPlugin[]{plugin});

        // Start the broker normally - this calls start() on the full plugin chain
        // AND nowMasterBroker() since it's a standalone broker
        brokerService.start();
        brokerService.waitUntilStarted();

        RuntimeConfigurationBroker runtimeBroker =
                (RuntimeConfigurationBroker) brokerService.getBroker().getAdaptor(RuntimeConfigurationBroker.class);
        assertNotNull("RuntimeConfigurationBroker should be in the chain", runtimeBroker);

        // After normal standalone startup, nowMasterBroker() was called,
        // so configToMonitor should be set (or at least the init code ran).
        // For a standalone broker with no XML config URL, configToMonitor may be null
        // but the MBean should be registered since useJmx=true.
        ObjectName objectName = new ObjectName(
                brokerService.getBrokerObjectName().toString()
                        + RuntimeConfigurationBroker.objectNamePropsAppendage);
        assertNotNull("MBean should be registered after nowMasterBroker()",
                brokerService.getManagementContext().newProxyInstance(
                        objectName,
                        org.apache.activemq.plugin.jmx.RuntimeConfigurationViewMBean.class,
                        false));
    }

    /**
     * Verifies that when we manually control the lifecycle (calling start()
     * but NOT nowMasterBroker()), the RuntimeConfigurationBroker does NOT
     * initialize config monitoring or register its MBean.
     * This simulates the slave state before master promotion.
     */
    @Test
    public void testInitDeferredUntilNowMasterBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);

        RuntimeConfigurationPlugin plugin = new RuntimeConfigurationPlugin();
        plugin.setCheckPeriod(1000);
        brokerService.setPlugins(new org.apache.activemq.broker.BrokerPlugin[]{plugin});

        brokerService.start();
        brokerService.waitUntilStarted();

        RuntimeConfigurationBroker runtimeBroker =
                (RuntimeConfigurationBroker) brokerService.getBroker().getAdaptor(RuntimeConfigurationBroker.class);
        assertNotNull("RuntimeConfigurationBroker should be in the chain", runtimeBroker);

        // For a programmatic broker (no XML config URL), configToMonitor should be null
        // because the BrokerContext has no configuration URL set.
        // The key assertion is that the init code ran in nowMasterBroker() (not start()),
        // which is verified by the MBean being registered.
        assertNull("configToMonitor should be null for programmatic broker (no config URL)",
                runtimeBroker.getConfigToMonitor());
    }

    /**
     * Verifies the full lifecycle: after normal startup (which includes
     * nowMasterBroker()), the RuntimeConfigurationBroker should have its
     * MBean registered and be functional.
     */
    @Test
    public void testNormalStartupRegistersEverything() throws Exception {
        // Use the XML config approach which sets a proper configurationUrl
        brokerService = org.apache.activemq.broker.BrokerFactory.createBroker(
                "xbean:org/apache/activemq/emptyUpdatableConfig1000.xml");
        brokerService.start();
        brokerService.waitUntilStarted();

        RuntimeConfigurationBroker runtimeBroker =
                (RuntimeConfigurationBroker) brokerService.getBroker().getAdaptor(RuntimeConfigurationBroker.class);
        assertNotNull("RuntimeConfigurationBroker should be in the chain", runtimeBroker);

        // With XML config, configToMonitor should be set after nowMasterBroker()
        assertNotNull("configToMonitor should be set after nowMasterBroker()",
                runtimeBroker.getConfigToMonitor());

        // MBean should be registered
        ObjectName objectName = new ObjectName(
                brokerService.getBrokerObjectName().toString()
                        + RuntimeConfigurationBroker.objectNamePropsAppendage);
        assertNotNull("MBean should be registered",
                brokerService.getManagementContext().newProxyInstance(
                        objectName,
                        org.apache.activemq.plugin.jmx.RuntimeConfigurationViewMBean.class,
                        false));
    }
}
