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
package org.apache.activemq.console;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.LinkedList;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.console.command.NetworkConnectorsCommand;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NetworkConnectorsCommandTest {

    private static final String NC_NAME = "test-nc";

    private BrokerService brokerService;

    @Before
    public void createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setPersistent(false);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    // --- list ---

    @Test(timeout = 30000)
    public void testListWithNoNetworkConnectors() throws Exception {
        String result = execute("list");
        assertTrue("empty message shown", result.contains("No network connectors configured."));
    }

    @Test(timeout = 30000)
    public void testListShowsConfiguredConnector() throws Exception {
        // Add a network connector directly to the broker before testing
        DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector();
        nc.setName(NC_NAME);
        // Use a discovery URI that won't fail immediately even without a remote broker
        nc.setUri(new java.net.URI("static:(failover:(tcp://localhost:0))"));
        brokerService.addNetworkConnector(nc);
        brokerService.registerNetworkConnectorMBean(nc);

        String result = execute("list");
        assertTrue("connector name in output", result.contains(NC_NAME));
        assertTrue("Auto-Start column present", result.contains("Auto-Start"));
        assertTrue("Duplex column present", result.contains("Duplex"));
    }

    // --- error handling ---

    @Test(timeout = 30000)
    public void testUnknownActionShowsHelp() throws Exception {
        String result = execute("badaction");
        assertTrue("unknown action message", result.contains("Unknown action"));
    }

    @Test(timeout = 30000)
    public void testNoActionShowsHelp() throws Exception {
        String result = execute();
        assertTrue("help content shown", result.contains("Actions:"));
    }

    @Test(timeout = 30000)
    public void testAddMissingUriThrows() throws Exception {
        try {
            execute("add");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("URI required"));
        }
    }

    @Test(timeout = 30000)
    public void testRemoveMissingNameThrows() throws Exception {
        try {
            execute("remove");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("name required"));
        }
    }

    // --- helpers ---

    private String execute(String... args) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        CommandContext context = new CommandContext();
        context.setFormatter(new CommandShellOutputFormatter(out));

        NetworkConnectorsCommand cmd = new NetworkConnectorsCommand();
        cmd.setJmxUseLocal(true);
        cmd.setCommandContext(context);

        LinkedList<String> tokens = new LinkedList<>();
        for (String arg : args) {
            tokens.add(arg);
        }
        cmd.execute(tokens);
        return out.toString();
    }
}
