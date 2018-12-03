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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.console.command.DstatCommand;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DstatCommandTest {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DstatCommandTest.class);

    final String CONNECTOR_NAME="tcp-openWire";

    BrokerService brokerService;


    @Before
    public void createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setPersistent(false);
        brokerService.setDestinations(new ActiveMQDestination[]{new ActiveMQQueue("Q1"), new ActiveMQQueue("Q2"), new ActiveMQTopic("T1")});
        brokerService.addConnector("tcp://0.0.0.0:0").setName(CONNECTOR_NAME);
        brokerService.start();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void verifyInflightAttribute() throws Exception {
        String result = executeQuery("");
        LOG.info("Result:" + result);
        assertFalse("Output valid", result.contains("Inflight"));

        result = executeQuery("queues");
        LOG.info("Result:" + result);
        assertTrue("Output valid", result.contains("Inflight"));
    }

    private String executeQuery(String query) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
        CommandContext context = new CommandContext();
        context.setFormatter(new CommandShellOutputFormatter(byteArrayOutputStream));

        DstatCommand queryCommand = new DstatCommand();
        queryCommand.setJmxUseLocal(true);
        queryCommand.setCommandContext(context);

        LinkedList<String> args = new LinkedList<>();
        args.addAll(Arrays.asList(query.split(" ")));
        queryCommand.execute(args);

        return byteArrayOutputStream.toString();
    }
}
