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

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.console.util.JmxMBeansUtil;

public class TopicsCommand extends AbstractJmxCommand {

    protected String[] helpFile = new String[] {
        "Task Usage: activemq topics <action> [options] [topic-name]",
        "Description: List, create, delete, or inspect topics on the broker.",
        "",
        "Actions:",
        "    list                         List all topics with their key statistics.",
        "    create <topic-name>          Create a new topic.",
        "    delete <topic-name>          Delete an existing topic.",
        "    info   <topic-name>          Show detailed statistics for a topic.",
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
        "    activemq topics list",
        "        - List all topics with their statistics.",
        "    activemq topics create MY.TOPIC",
        "        - Create a new topic named MY.TOPIC.",
        "    activemq topics delete MY.TOPIC",
        "        - Delete the topic named MY.TOPIC.",
        "    activemq topics info MY.TOPIC",
        "        - Show detailed statistics for topic MY.TOPIC.",
        ""
    };

    @Override
    public String getName() {
        return "topics";
    }

    @Override
    public String getOneLineDescription() {
        return "List, create, delete, or inspect topics";
    }

    @Override
    protected void runTask(List<String> tokens) throws Exception {
        if (tokens.isEmpty()) {
            printHelp();
            return;
        }

        String action = tokens.remove(0);
        if (action.equals("list")) {
            listTopics();
        } else if (action.equals("create")) {
            requireTopicName(tokens, "create");
            createTopic(tokens.get(0));
        } else if (action.equals("delete")) {
            requireTopicName(tokens, "delete");
            deleteTopic(tokens.get(0));
        } else if (action.equals("info")) {
            requireTopicName(tokens, "info");
            infoTopic(tokens.get(0));
        } else {
            context.printInfo("Unknown action '" + action + "'. See 'activemq topics --help'.");
            printHelp();
        }
    }

    @SuppressWarnings("unchecked")
    private void listTopics() throws Exception {
        List<ObjectInstance> topicList = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,destinationType=Topic,destinationName=*");

        Collections.sort(topicList, new Comparator<ObjectInstance>() {
            @Override
            public int compare(ObjectInstance o1, ObjectInstance o2) {
                return o1.getObjectName().compareTo(o2.getObjectName());
            }
        });

        final String fmt = "%-50s  %10s  %10s  %10s  %10s  %10s";
        context.print(String.format(Locale.US, fmt, "Name", "Messages", "Consumers", "Producers", "Enqueued", "Dequeued"));
        context.print(String.format(Locale.US, fmt,
                dashes(50), dashes(10), dashes(10), dashes(10), dashes(10), dashes(10)));

        for (ObjectInstance obj : topicList) {
            ObjectName name = obj.getObjectName();
            TopicViewMBean t = MBeanServerInvocationHandler.newProxyInstance(
                    createJmxConnection(), name, TopicViewMBean.class, true);
            context.print(String.format(Locale.US, "%-50s  %10d  %10d  %10d  %10d  %10d",
                    t.getName(),
                    t.getQueueSize(),
                    t.getConsumerCount(),
                    t.getProducerCount(),
                    t.getEnqueueCount(),
                    t.getDequeueCount()));
        }

        if (topicList.isEmpty()) {
            context.print("No topics found.");
        }
    }

    private void createTopic(String topicName) throws Exception {
        getBrokerMBean().addTopic(topicName);
        context.print("Topic created: " + topicName);
    }

    private void deleteTopic(String topicName) throws Exception {
        getBrokerMBean().removeTopic(topicName);
        context.print("Topic deleted: " + topicName);
    }

    @SuppressWarnings("unchecked")
    private void infoTopic(String topicName) throws Exception {
        List<ObjectInstance> results = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,destinationType=Topic,destinationName=" + topicName);
        if (results.isEmpty()) {
            context.printInfo("Topic not found: " + topicName);
            return;
        }
        ObjectName name = results.get(0).getObjectName();
        TopicViewMBean t = MBeanServerInvocationHandler.newProxyInstance(
                createJmxConnection(), name, TopicViewMBean.class, true);

        context.print("Name          : " + t.getName());
        context.print("Messages      : " + t.getQueueSize());
        context.print("Consumers     : " + t.getConsumerCount());
        context.print("Producers     : " + t.getProducerCount());
        context.print("Enqueued      : " + t.getEnqueueCount());
        context.print("Dequeued      : " + t.getDequeueCount());
        context.print("In-flight     : " + t.getInFlightCount());
        context.print("Memory usage  : " + t.getMemoryPercentUsage() + "%");
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

    private void requireTopicName(List<String> tokens, String action) throws Exception {
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Topic name required for '" + action + "'.");
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
