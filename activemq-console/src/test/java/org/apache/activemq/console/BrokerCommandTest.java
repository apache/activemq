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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.LinkedList;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.console.command.BrokerCommand;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BrokerCommandTest {

    private static final String CONNECTOR_NAME = "test-tcp";

    private BrokerService brokerService;

    @Before
    public void createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setPersistent(false);
        brokerService.addConnector("tcp://0.0.0.0:0").setName(CONNECTOR_NAME);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    // --- info ---

    @Test(timeout = 30000)
    public void testBrokerInfoShowsName() throws Exception {
        String result = execute("info");
        assertTrue("broker name present", result.contains("localhost"));
        assertTrue("version field present", result.contains("Version"));
        assertTrue("uptime field present", result.contains("Uptime"));
        assertTrue("memory usage field present", result.contains("Memory usage"));
        assertTrue("store usage field present", result.contains("Store usage"));
        assertTrue("queues count field present", result.contains("Queues"));
        assertTrue("topics count field present", result.contains("Topics"));
    }

    @Test(timeout = 30000)
    public void testBrokerInfoShowsConnectionCounts() throws Exception {
        String result = execute("info");
        assertTrue("connections field present", result.contains("Connections"));
        assertTrue("producers field present", result.contains("Producers"));
        assertTrue("consumers field present", result.contains("Consumers"));
    }

    // --- connectors ---

    @Test(timeout = 30000)
    public void testListConnectorsShowsConfiguredConnector() throws Exception {
        String result = execute("connectors");
        assertTrue("connector name present", result.contains(CONNECTOR_NAME));
        assertTrue("URI column present", result.contains("URI"));
    }

    @Test(timeout = 30000)
    public void testListConnectorsExplicitSubAction() throws Exception {
        String result = execute("connectors", "list");
        assertTrue("connector name present", result.contains(CONNECTOR_NAME));
    }

    @Test(timeout = 30000)
    public void testConnectorsUnknownSubActionShowsHelp() throws Exception {
        String result = execute("connectors", "badsubaction");
        assertTrue("unknown sub-action message", result.contains("Unknown connectors sub-action"));
    }

    // --- scheduler ---

    @Test(timeout = 30000)
    public void testSchedulerDisabledMessage() throws Exception {
        // Default broker has no scheduler — verify the "not enabled" message
        String result = execute("scheduler");
        assertTrue("scheduler not enabled message", result.contains("not enabled"));
    }

    @Test(timeout = 30000)
    public void testSchedulerEnabledShowsCounts() throws Exception {
        // Restart broker with scheduler support enabled
        brokerService.stop();
        brokerService = new BrokerService();
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setPersistent(false);
        brokerService.setSchedulerSupport(true);
        brokerService.start();
        brokerService.waitUntilStarted();

        String result = execute("scheduler");
        assertTrue("scheduled messages count present", result.contains("Scheduled messages"));
        assertTrue("delayed messages count present", result.contains("Delayed messages"));
        assertTrue("next scheduled at present", result.contains("Next scheduled at"));
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

    // --- helpers ---

    private String execute(String... args) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        CommandContext context = new CommandContext();
        context.setFormatter(new CommandShellOutputFormatter(out));

        BrokerCommand cmd = new BrokerCommand();
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
