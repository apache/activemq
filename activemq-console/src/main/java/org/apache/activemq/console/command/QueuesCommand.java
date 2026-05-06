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
package org.apache.activemq.console.command;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.console.util.JmxMBeansUtil;

public class QueuesCommand extends AbstractJmxCommand {

    protected String[] helpFile = new String[] {
        "Task Usage: activemq queues <action> [options] [queue-name] [args]",
        "Description: List, create, delete, purge, browse, produce, pause, resume, or inspect queues.",
        "",
        "Actions:",
        "    list                           List all queues with their key statistics.",
        "    create <queue-name>            Create a new queue.",
        "    delete <queue-name>            Delete an existing queue.",
        "    purge  <queue-name>            Purge all messages from a queue.",
        "    info   <queue-name>            Show detailed statistics for a queue.",
        "    browse <queue-name>            Browse messages in a queue (non-destructive).",
        "    produce <queue-name> <body>    Send a text message to a queue.",
        "    pause  <queue-name>            Pause dispatch on a queue.",
        "    resume <queue-name>            Resume dispatch on a paused queue.",
        "",
        "Options:",
        "    --jmxurl <url>               Set the JMX URL to connect to.",
        "    --pid <pid>                  Set the pid to connect to (only on Sun JVM).",
        "    --jmxuser <user>             Set the JMX user used for authenticating.",
        "    --jmxpassword <password>     Set the JMX password used for authenticating.",
        "    --jmxlocal                   Use the local JMX server instead of a remote one.",
        "    --version                    Display the version information.",
        "    -h,-?,--help                 Display this help information.",
        "",
        "Examples:",
        "    activemq queues list",
        "        - List all queues with their statistics.",
        "    activemq queues create FOO.BAR",
        "        - Create a new queue named FOO.BAR.",
        "    activemq queues delete FOO.BAR",
        "        - Delete the queue named FOO.BAR.",
        "    activemq queues purge FOO.BAR",
        "        - Purge all messages from queue FOO.BAR.",
        "    activemq queues info FOO.BAR",
        "        - Show detailed statistics for queue FOO.BAR.",
        "    activemq queues browse FOO.BAR",
        "        - Browse all messages in queue FOO.BAR without consuming them.",
        "    activemq queues produce FOO.BAR \"Hello World\"",
        "        - Send a text message with body 'Hello World' to queue FOO.BAR.",
        "    activemq queues pause FOO.BAR",
        "        - Pause message dispatch on queue FOO.BAR.",
        "    activemq queues resume FOO.BAR",
        "        - Resume message dispatch on queue FOO.BAR.",
        ""
    };

    @Override
    public String getName() {
        return "queues";
    }

    @Override
    public String getOneLineDescription() {
        return "List, create, delete, purge, browse, produce, pause, or resume queues";
    }

    @Override
    protected void runTask(List<String> tokens) throws Exception {
        if (tokens.isEmpty()) {
            printHelp();
            return;
        }

        String action = tokens.remove(0);
        if (action.equals("list")) {
            listQueues();
        } else if (action.equals("create")) {
            requireQueueName(tokens, "create");
            createQueue(tokens.get(0));
        } else if (action.equals("delete")) {
            requireQueueName(tokens, "delete");
            deleteQueue(tokens.get(0));
        } else if (action.equals("purge")) {
            requireQueueName(tokens, "purge");
            purgeQueue(tokens.get(0));
        } else if (action.equals("info")) {
            requireQueueName(tokens, "info");
            infoQueue(tokens.get(0));
        } else if (action.equals("browse")) {
            requireQueueName(tokens, "browse");
            browseQueue(tokens.get(0));
        } else if (action.equals("produce")) {
            requireQueueName(tokens, "produce");
            if (tokens.size() < 2) {
                throw new IllegalArgumentException("Message body required: activemq queues produce <queue-name> <body>");
            }
            produceMessage(tokens.get(0), tokens.get(1));
        } else if (action.equals("pause")) {
            requireQueueName(tokens, "pause");
            pauseQueue(tokens.get(0));
        } else if (action.equals("resume")) {
            requireQueueName(tokens, "resume");
            resumeQueue(tokens.get(0));
        } else {
            context.printInfo("Unknown action '" + action + "'. See 'activemq queues --help'.");
            printHelp();
        }
    }

    @SuppressWarnings("unchecked")
    private void listQueues() throws Exception {
        List<ObjectInstance> queueList = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,destinationType=Queue,destinationName=*");

        Collections.sort(queueList, new Comparator<ObjectInstance>() {
            @Override
            public int compare(ObjectInstance o1, ObjectInstance o2) {
                return o1.getObjectName().compareTo(o2.getObjectName());
            }
        });

        final String fmt = "%-50s  %10s  %10s  %10s  %10s  %10s";
        context.print(String.format(Locale.US, fmt, "Name", "Messages", "Consumers", "Producers", "Enqueued", "Dequeued"));
        context.print(String.format(Locale.US, fmt,
                dashes(50), dashes(10), dashes(10), dashes(10), dashes(10), dashes(10)));

        for (ObjectInstance obj : queueList) {
            ObjectName name = obj.getObjectName();
            QueueViewMBean q = MBeanServerInvocationHandler.newProxyInstance(
                    createJmxConnection(), name, QueueViewMBean.class, true);
            context.print(String.format(Locale.US, "%-50s  %10d  %10d  %10d  %10d  %10d",
                    q.getName(),
                    q.getQueueSize(),
                    q.getConsumerCount(),
                    q.getProducerCount(),
                    q.getEnqueueCount(),
                    q.getDequeueCount()));
        }

        if (queueList.isEmpty()) {
            context.print("No queues found.");
        }
    }

    private void createQueue(String queueName) throws Exception {
        getBrokerMBean().addQueue(queueName);
        context.print("Queue created: " + queueName);
    }

    private void deleteQueue(String queueName) throws Exception {
        getBrokerMBean().removeQueue(queueName);
        context.print("Queue deleted: " + queueName);
    }

    @SuppressWarnings("unchecked")
    private void purgeQueue(String queueName) throws Exception {
        List<ObjectInstance> results = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,destinationType=Queue,destinationName=" + queueName);
        if (results.isEmpty()) {
            context.printInfo("Queue not found: " + queueName);
            return;
        }
        ObjectName name = results.get(0).getObjectName();
        QueueViewMBean q = MBeanServerInvocationHandler.newProxyInstance(
                createJmxConnection(), name, QueueViewMBean.class, true);
        q.purge();
        context.print("Queue purged: " + queueName);
    }

    @SuppressWarnings("unchecked")
    private void infoQueue(String queueName) throws Exception {
        List<ObjectInstance> results = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,destinationType=Queue,destinationName=" + queueName);
        if (results.isEmpty()) {
            context.printInfo("Queue not found: " + queueName);
            return;
        }
        ObjectName name = results.get(0).getObjectName();
        QueueViewMBean q = MBeanServerInvocationHandler.newProxyInstance(
                createJmxConnection(), name, QueueViewMBean.class, true);

        context.print("Name          : " + q.getName());
        context.print("Messages      : " + q.getQueueSize());
        context.print("Consumers     : " + q.getConsumerCount());
        context.print("Producers     : " + q.getProducerCount());
        context.print("Enqueued      : " + q.getEnqueueCount());
        context.print("Dequeued      : " + q.getDequeueCount());
        context.print("In-flight     : " + q.getInFlightCount());
        context.print("Memory usage  : " + q.getMemoryPercentUsage() + "%");
        context.print("Paused        : " + q.isPaused());
    }

    @SuppressWarnings("unchecked")
    private void browseQueue(String queueName) throws Exception {
        List<ObjectInstance> results = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,destinationType=Queue,destinationName=" + queueName);
        if (results.isEmpty()) {
            context.printInfo("Queue not found: " + queueName);
            return;
        }
        ObjectName name = results.get(0).getObjectName();
        QueueViewMBean q = MBeanServerInvocationHandler.newProxyInstance(
                createJmxConnection(), name, QueueViewMBean.class, true);
        CompositeData[] messages = q.browse();
        if (messages == null || messages.length == 0) {
            context.print("No messages in queue: " + queueName);
            return;
        }
        context.print("Browsing queue: " + queueName + " (" + messages.length + " message(s))");
        for (int i = 0; i < messages.length; i++) {
            CompositeData msg = messages[i];
            context.print("");
            context.print("--- Message " + (i + 1) + " ---");
            for (String key : msg.getCompositeType().keySet()) {
                Object value = msg.get(key);
                if (value != null && !value.toString().isEmpty()) {
                    context.print(String.format("  %-24s: %s", key, value));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void produceMessage(String queueName, String body) throws Exception {
        List<ObjectInstance> results = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,destinationType=Queue,destinationName=" + queueName);
        if (results.isEmpty()) {
            context.printInfo("Queue not found: " + queueName);
            return;
        }
        ObjectName name = results.get(0).getObjectName();
        QueueViewMBean q = MBeanServerInvocationHandler.newProxyInstance(
                createJmxConnection(), name, QueueViewMBean.class, true);
        String messageId = q.sendTextMessage(body);
        context.print("Message sent to " + queueName + ". ID: " + messageId);
    }

    @SuppressWarnings("unchecked")
    private void pauseQueue(String queueName) throws Exception {
        List<ObjectInstance> results = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,destinationType=Queue,destinationName=" + queueName);
        if (results.isEmpty()) {
            context.printInfo("Queue not found: " + queueName);
            return;
        }
        ObjectName name = results.get(0).getObjectName();
        QueueViewMBean q = MBeanServerInvocationHandler.newProxyInstance(
                createJmxConnection(), name, QueueViewMBean.class, true);
        q.pause();
        context.print("Queue paused: " + queueName);
    }

    @SuppressWarnings("unchecked")
    private void resumeQueue(String queueName) throws Exception {
        List<ObjectInstance> results = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,destinationType=Queue,destinationName=" + queueName);
        if (results.isEmpty()) {
            context.printInfo("Queue not found: " + queueName);
            return;
        }
        ObjectName name = results.get(0).getObjectName();
        QueueViewMBean q = MBeanServerInvocationHandler.newProxyInstance(
                createJmxConnection(), name, QueueViewMBean.class, true);
        q.resume();
        context.print("Queue resumed: " + queueName);
    }

    @SuppressWarnings("unchecked")
    private BrokerViewMBean getBrokerMBean() throws Exception {
        List<ObjectInstance> brokers = JmxMBeansUtil.getAllBrokers(createJmxConnection());
        if (brokers.isEmpty()) {
            throw new Exception("No broker found in JMX context.");
        }
        ObjectName brokerName = brokers.get(0).getObjectName();
        return MBeanServerInvocationHandler.newProxyInstance(
                createJmxConnection(), brokerName, BrokerViewMBean.class, true);
    }

    private void requireQueueName(List<String> tokens, String action) throws Exception {
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Queue name required for '" + action + "'.");
        }
    }

    private static String dashes(int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append('-');
        }
        return sb.toString();
    }

    @Override
    protected void printHelp() {
        context.printHelp(helpFile);
    }
}
