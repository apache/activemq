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

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import javax.jms.Connection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.console.command.QueryCommand;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryCommandTest {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(QueryCommandTest.class);

    final String CONNECTOR_NAME="tcp-openWire";
    final String CLIENT_ID="some-id";

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
    public void tryQuery() throws Exception {

        String result = executeQuery("-QQueue=* --view destinationName,EnqueueCount,DequeueCount");
        assertTrue("Output valid", result.contains("Q1"));
        assertTrue("Output valid", result.contains("Q2"));
        assertFalse("Output valid", result.contains("T1"));

        result = executeQuery("-QQueue=Q2 --view destinationName,QueueSize");
        assertTrue("size present", result.contains("QueueSize"));
        assertTrue("Output valid", result.contains("Q2"));
        assertFalse("Output valid", result.contains("Q1"));
        assertFalse("Output valid", result.contains("T1"));

        result = executeQuery("-QQueue=* -xQQueue=Q1 --view destinationName,QueueSize");
        assertTrue("size present", result.contains("QueueSize"));
        assertTrue("q2", result.contains("Q2"));
        assertFalse("!q1: " + result, result.contains("Q1"));
        assertFalse("!t1", result.contains("T1"));

        result = executeQuery("-QTopic=* -QQueue=* --view destinationName");
        assertTrue("got Q1", result.contains("Q1"));
        assertTrue("got Q2", result.contains("Q2"));
        assertTrue("got T1", result.contains("T1"));

        result = executeQuery("-QQueue=*");
        assertTrue("got Q1", result.contains("Q1"));
        assertTrue("got Q2", result.contains("Q2"));
        assertFalse("!T1", result.contains("T1"));

        result = executeQuery("-QBroker=*");
        assertTrue("got localhost", result.contains("localhost"));

        result = executeQuery("--view destinationName");
        // all mbeans with a destinationName attribute
        assertTrue("got Q1", result.contains("Q1"));
        assertTrue("got Q2", result.contains("Q2"));
        assertTrue("got T1", result.contains("T1"));

        result = executeQuery("--objname type=Broker,brokerName=*,destinationType=Queue,destinationName=*");
        assertTrue("got Q1", result.contains("Q1"));
        assertTrue("got Q2", result.contains("Q2"));
        assertFalse("!T1", result.contains("T1"));

        result = executeQuery("--objname type=Broker,brokerName=*,destinationType=*,destinationName=* --xobjname type=Broker,brokerName=*,destinationType=Queue,destinationName=Q1");
        assertFalse("!Q1", result.contains("Q1"));
        assertTrue("got Q2", result.contains("Q2"));
        assertTrue("T1", result.contains("T1"));

    }

    @Test
    public void testConnection() throws Exception {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getPublishableConnectURI());
        Connection connection = connectionFactory.createConnection();
        connection.setClientID(CLIENT_ID);
        connection.start();

        String result = executeQuery("-QConnection=* --view ClientId");
        assertTrue("got client id", result.contains(CLIENT_ID));

        result = executeQuery("--objname type=Broker,brokerName=*,connector=clientConnectors,connectorName=* -xQNetworkConnector=*");
        assertTrue("got named", result.contains(CONNECTOR_NAME));

        result = executeQuery("-QConnector=*");
        assertTrue("got named", result.contains(CONNECTOR_NAME));
    }


    @Test
    public void testInvoke() throws Exception {

        String result = executeQuery("-QQueue=Q* --view Paused");
        assertTrue("got pause status", result.contains("Paused = false"));

        result = executeQuery("-QQueue=* --invoke pause");
        LOG.info("result of invoke: " + result);
        assertTrue("invoked", result.contains("Q1"));
        assertTrue("invoked", result.contains("Q2"));

        result = executeQuery("-QQueue=Q2 --view Paused");
        assertTrue("got pause status", result.contains("Paused = true"));

        result = executeQuery("-QQueue=Q2 --invoke resume");
        LOG.info("result of invoke: " + result);
        assertTrue("invoked", result.contains("Q2"));

        result = executeQuery("-QQueue=Q2 --view Paused");
        assertTrue("pause status", result.contains("Paused = false"));

        result = executeQuery("-QQueue=Q1 --view Paused");
        assertTrue("pause status", result.contains("Paused = true"));

        // op with string param
        result = executeQuery("-QQueue=Q2 --invoke sendTextMessage,hi");
        LOG.info("result of invoke: " + result);
        assertTrue("invoked", result.contains("Q2"));

        result = executeQuery("-QQueue=Q2 --view EnqueueCount");
        assertTrue("enqueueCount", result.contains("EnqueueCount = 1"));
    }

    private String executeQuery(String query) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
        CommandContext context = new CommandContext();
        context.setFormatter(new CommandShellOutputFormatter(byteArrayOutputStream));

        QueryCommand queryCommand = new QueryCommand();
        queryCommand.setJmxUseLocal(true);
        queryCommand.setCommandContext(context);

        LinkedList<String> args = new LinkedList<>();
        args.addAll(Arrays.asList(query.split(" ")));
        queryCommand.execute(args);

        return byteArrayOutputStream.toString();
    }
}
