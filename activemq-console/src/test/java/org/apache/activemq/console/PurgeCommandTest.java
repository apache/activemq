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
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.console.command.PurgeCommand;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests for the purge command.
 */
public class PurgeCommandTest {

    private BrokerService brokerService;
    private ConnectionFactory factory;

    @Rule public TestName name = new TestName();

    @Before
    public void createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setPersistent(false);
        TransportConnector connector = brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.start();
        brokerService.waitUntilStarted();

        factory = new ActiveMQConnectionFactory(connector.getPublishableConnectString());
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test(timeout = 30000)
    public void testPurge() throws Exception {
        produce(10);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        assertEquals(10, queueView.getQueueSize());

        executePurge(getDestinationName());

        assertEquals(0, queueView.getQueueSize());
    }

    @Test(timeout = 30000)
    public void testPurgeWithReset() throws Exception {
        produce(20);
        consume(10);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        assertEquals(10, queueView.getQueueSize());
        assertEquals(20, queueView.getEnqueueCount());
        assertEquals(10, queueView.getDequeueCount());

        // Normal purge doesn't change stats.
        executePurge(getDestinationName());

        assertEquals(0, queueView.getQueueSize());
        assertEquals(20, queueView.getEnqueueCount());
        assertEquals(20, queueView.getDequeueCount());

        // Purge on empty leaves stats alone.
        executePurge(getDestinationName());

        assertEquals(0, queueView.getQueueSize());
        assertEquals(20, queueView.getEnqueueCount());
        assertEquals(20, queueView.getDequeueCount());

        executePurge("--reset " + getDestinationName());

        // Purge on empty with reset clears stats.
        assertEquals(0, queueView.getQueueSize());
        assertEquals(0, queueView.getEnqueueCount());
        assertEquals(0, queueView.getDequeueCount());

        produce(20);
        consume(10);

        assertEquals(10, queueView.getQueueSize());
        assertEquals(20, queueView.getEnqueueCount());
        assertEquals(10, queueView.getDequeueCount());

        executePurge("--reset " + getDestinationName());

        // Purge on non-empty with reset clears stats.
        assertEquals(0, queueView.getQueueSize());
        assertEquals(0, queueView.getEnqueueCount());
        assertEquals(0, queueView.getDequeueCount());
    }

    private String getDestinationName() {
        return name.getMethodName();
    }

    private String executePurge(String options) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
        CommandContext context = new CommandContext();
        context.setFormatter(new CommandShellOutputFormatter(byteArrayOutputStream));

        PurgeCommand purgeCommand = new PurgeCommand();
        purgeCommand.setJmxUseLocal(true);
        purgeCommand.setCommandContext(context);

        LinkedList<String> args = new LinkedList<>();
        args.addAll(Arrays.asList(options.split(" ")));
        purgeCommand.execute(args);

        return byteArrayOutputStream.toString();
    }

    private QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    private void produce(int count) throws Exception {
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < count; ++i) {
            producer.send(session.createMessage());
        }

        connection.close();
    }

    private void consume(int count) throws Exception {
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();

        for (int i = 0; i < count; ++i) {
            assertNotNull(consumer.receive(1000));
        }

        connection.close();
    }
}
