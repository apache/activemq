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

import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.console.command.TopicsCommand;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TopicsCommandTest {

    private BrokerService brokerService;

    @Before
    public void createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setPersistent(false);
        brokerService.setDestinations(new ActiveMQDestination[]{
                new ActiveMQTopic("T1"),
                new ActiveMQTopic("T2")
        });
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
    public void testListShowsAllTopics() throws Exception {
        String result = execute("list");
        assertTrue("T1 in output", result.contains("T1"));
        assertTrue("T2 in output", result.contains("T2"));
        assertTrue("column header present", result.contains("Messages"));
        assertTrue("column header present", result.contains("Consumers"));
    }

    @Test(timeout = 30000)
    public void testListDoesNotShowQueues() throws Exception {
        String result = execute("list");
        // Queues should never appear in topic list output
        assertFalse("queue header not shown", result.contains("Queue Size"));
    }

    // --- create ---

    @Test(timeout = 30000)
    public void testCreateTopic() throws Exception {
        String result = execute("create", "NEW.TOPIC");
        assertTrue("confirmation message", result.contains("Topic created: NEW.TOPIC"));

        // Verify it now appears in list
        String listResult = execute("list");
        assertTrue("new topic in list", listResult.contains("NEW.TOPIC"));
    }

    // --- delete ---

    @Test(timeout = 30000)
    public void testDeleteTopic() throws Exception {
        String result = execute("delete", "T1");
        assertTrue("confirmation message", result.contains("Topic deleted: T1"));

        // T1 should no longer appear in list
        String listResult = execute("list");
        assertFalse("T1 gone from list", listResult.contains("T1"));
        assertTrue("T2 still present", listResult.contains("T2"));
    }

    // --- info ---

    @Test(timeout = 30000)
    public void testInfoTopic() throws Exception {
        String result = execute("info", "T1");
        assertTrue("name field", result.contains("T1"));
        assertTrue("Messages field", result.contains("Messages"));
        assertTrue("Consumers field", result.contains("Consumers"));
        assertTrue("Producers field", result.contains("Producers"));
        assertTrue("Memory usage field", result.contains("Memory usage"));
    }

    @Test(timeout = 30000)
    public void testInfoTopicNotFound() throws Exception {
        String result = execute("info", "DOES.NOT.EXIST");
        assertTrue("not found message", result.contains("not found"));
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

        TopicsCommand cmd = new TopicsCommand();
        cmd.setJmxUseLocal(true);
        cmd.setCommandContext(context);

        LinkedList<String> tokens = new LinkedList<>();
        for (String arg : args) {
            tokens.add(arg);
        }
        cmd.execute(tokens);
        return out.toString();
    }

    private TopicViewMBean getTopicProxy(String topicName) throws Exception {
        ObjectName objectName = new ObjectName(
                "org.apache.activemq:type=Broker,brokerName=localhost" +
                ",destinationType=Topic,destinationName=" + topicName);
        return (TopicViewMBean) brokerService.getManagementContext()
                .newProxyInstance(objectName, TopicViewMBean.class, true);
    }
}
