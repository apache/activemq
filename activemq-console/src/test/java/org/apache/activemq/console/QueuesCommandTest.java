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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.LinkedList;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.console.command.QueuesCommand;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class QueuesCommandTest {

    @Rule
    public TestName name = new TestName();

    private BrokerService brokerService;
    private ActiveMQConnectionFactory connectionFactory;

    @Before
    public void createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setPersistent(false);
        brokerService.setDestinations(new ActiveMQDestination[]{
                new ActiveMQQueue("Q1"),
                new ActiveMQQueue("Q2")
        });
        TransportConnector connector = brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.start();
        brokerService.waitUntilStarted();
        connectionFactory = new ActiveMQConnectionFactory(connector.getPublishableConnectString());
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    // --- list ---

    @Test(timeout = 30000)
    public void testListShowsAllQueues() throws Exception {
        String result = execute("list");
        assertTrue("Q1 in output", result.contains("Q1"));
        assertTrue("Q2 in output", result.contains("Q2"));
        assertTrue("column header present", result.contains("Messages"));
        assertTrue("column header present", result.contains("Consumers"));
    }

    // --- create ---

    @Test(timeout = 30000)
    public void testCreateQueue() throws Exception {
        String result = execute("create", "NEW.QUEUE");
        assertTrue("confirmation message", result.contains("Queue created: NEW.QUEUE"));

        // Verify via JMX proxy that the queue now exists
        QueueViewMBean proxy = getQueueProxy("NEW.QUEUE");
        assertEquals(0, proxy.getQueueSize());
    }

    // --- delete ---

    @Test(timeout = 30000)
    public void testDeleteQueue() throws Exception {
        // Q1 was created at startup; delete it
        String result = execute("delete", "Q1");
        assertTrue("confirmation message", result.contains("Queue deleted: Q1"));

        // Verify Q1 no longer appears in list
        String listResult = execute("list");
        assertFalse("Q1 gone from list", listResult.contains("Q1"));
        assertTrue("Q2 still present", listResult.contains("Q2"));
    }

    // --- purge ---

    @Test(timeout = 30000)
    public void testPurgeQueue() throws Exception {
        sendMessages("Q1", 5);

        QueueViewMBean proxy = getQueueProxy("Q1");
        assertEquals("5 messages before purge", 5, proxy.getQueueSize());

        String result = execute("purge", "Q1");
        assertTrue("confirmation message", result.contains("Queue purged: Q1"));
        assertEquals("0 messages after purge", 0, proxy.getQueueSize());
    }

    // --- info ---

    @Test(timeout = 30000)
    public void testInfoQueue() throws Exception {
        sendMessages("Q1", 3);
        String result = execute("info", "Q1");

        assertTrue("name field", result.contains("Q1"));
        assertTrue("Messages field", result.contains("Messages"));
        assertTrue("Consumers field", result.contains("Consumers"));
        assertTrue("Producers field", result.contains("Producers"));
        assertTrue("Memory usage field", result.contains("Memory usage"));
        assertTrue("Paused field", result.contains("Paused"));
    }

    @Test(timeout = 30000)
    public void testInfoQueueNotFound() throws Exception {
        String result = execute("info", "DOES.NOT.EXIST");
        assertTrue("not found message", result.contains("not found"));
    }

    // --- browse ---

    @Test(timeout = 30000)
    public void testBrowseQueue() throws Exception {
        sendTextMessages("Q1", "hello-browse-msg", 2);

        String result = execute("browse", "Q1");
        assertTrue("browse header present", result.contains("Browsing queue"));
        assertTrue("message count in header", result.contains("2 message(s)"));
        assertTrue("JMSMessageID field present", result.contains("JMSMessageID"));
        assertTrue("message body present", result.contains("hello-browse-msg"));
    }

    @Test(timeout = 30000)
    public void testBrowseEmptyQueue() throws Exception {
        String result = execute("browse", "Q1");
        assertTrue("empty queue message", result.contains("No messages in queue"));
    }

    // --- produce ---

    @Test(timeout = 30000)
    public void testProduceMessage() throws Exception {
        QueueViewMBean proxy = getQueueProxy("Q1");
        assertEquals("queue empty before produce", 0, proxy.getQueueSize());

        String result = execute("produce", "Q1", "test-message-body");
        assertTrue("confirmation with message ID", result.contains("Message sent to Q1"));
        assertTrue("message ID in output", result.contains("ID:"));

        assertEquals("queue has 1 message after produce", 1, proxy.getQueueSize());
    }

    @Test(timeout = 30000)
    public void testProduceMissingBody() throws Exception {
        try {
            execute("produce", "Q1");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("body required"));
        }
    }

    // --- pause / resume ---

    @Test(timeout = 30000)
    public void testPauseQueue() throws Exception {
        QueueViewMBean proxy = getQueueProxy("Q1");
        assertFalse("queue not paused initially", proxy.isPaused());

        String result = execute("pause", "Q1");
        assertTrue("confirmation message", result.contains("Queue paused: Q1"));
        assertTrue("queue is now paused", proxy.isPaused());
    }

    @Test(timeout = 30000)
    public void testResumeQueue() throws Exception {
        // Pause first
        execute("pause", "Q1");
        QueueViewMBean proxy = getQueueProxy("Q1");
        assertTrue("queue is paused", proxy.isPaused());

        String result = execute("resume", "Q1");
        assertTrue("confirmation message", result.contains("Queue resumed: Q1"));
        assertFalse("queue is no longer paused", proxy.isPaused());
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

        QueuesCommand cmd = new QueuesCommand();
        cmd.setJmxUseLocal(true);
        cmd.setCommandContext(context);

        LinkedList<String> tokens = new LinkedList<>();
        for (String arg : args) {
            tokens.add(arg);
        }
        cmd.execute(tokens);
        return out.toString();
    }

    private QueueViewMBean getQueueProxy(String queueName) throws Exception {
        ObjectName objectName = new ObjectName(
                "org.apache.activemq:type=Broker,brokerName=localhost" +
                ",destinationType=Queue,destinationName=" + queueName);
        return (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(objectName, QueueViewMBean.class, true);
    }

    private void sendMessages(String queueName, int count) throws Exception {
        Connection conn = connectionFactory.createConnection();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        jakarta.jms.Queue dest = session.createQueue(queueName);
        MessageProducer producer = session.createProducer(dest);
        for (int i = 0; i < count; i++) {
            producer.send(session.createMessage());
        }
        conn.close();
    }

    private void sendTextMessages(String queueName, String body, int count) throws Exception {
        Connection conn = connectionFactory.createConnection();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        jakarta.jms.Queue dest = session.createQueue(queueName);
        MessageProducer producer = session.createProducer(dest);
        for (int i = 0; i < count; i++) {
            TextMessage msg = session.createTextMessage(body);
            producer.send(msg);
        }
        conn.close();
    }
}
