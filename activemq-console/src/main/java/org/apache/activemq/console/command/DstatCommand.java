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

import org.apache.activemq.broker.jmx.QueueView;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicView;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.console.util.JmxMBeansUtil;

public class DstatCommand extends AbstractJmxCommand {

    private static final String queryString =
        "type=Broker,brokerName=*,destinationType=%1,destinationName=*,*";

    protected String[] helpFile = new String[] {
        "Task Usage: activemq-admin dstat [dstat-options] [destination-type]",
        "Description: Performs a predefined query that displays useful statistics regarding the specified .",
        "             destination type (Queues or Topics) and displays those results in a tabular format.",
        "             If no broker name is specified, it will try and select from all registered brokers.",
        "",
        "dstat Options:",
        "    --jmxurl <url>                Set the JMX URL to connect to.",
        "    --pid <pid>                   Set the pid to connect to (only on Sun JVM).",
        "    --jmxuser <user>              Set the JMX user used for authenticating.",
        "    --jmxpassword <password>      Set the JMX password used for authenticating.",
        "    --jmxlocal                    Use the local JMX server instead of a remote one.",
        "    --version                     Display the version information.",
        "    -h,-?,--help                  Display the query broker help information.",
        "",
        "Examples:",
        "    activemq-admin dstat queues",
        "        - Display a tabular summary of statistics for the queues on the broker.",
        "    activemq-admin dstat topics",
        "        - Display a tabular summary of statistics for the queues on the broker."
    };

    /**
     * Execute the dstat command, which allows you to display information for topics or queue in
     * a tabular format.
     *
     * @param tokens - command arguments
     * @throws Exception
     */
    @Override
    protected void runTask(List<String> tokens) throws Exception {
        if (tokens.contains("topics")) {
            displayTopicStats();
        } else if (tokens.contains("queues")) {
            displayQueueStats();
        } else {
            displayAllDestinations();
        }
    }

    @SuppressWarnings("unchecked")
    private void displayAllDestinations() throws Exception {

        String query = JmxMBeansUtil.createQueryString(queryString, "*");
        List queueList = JmxMBeansUtil.queryMBeans(createJmxConnection(), query);

        final String header = "%-50s  %10s  %10s  %10s  %10s  %10s  %10s  %10s";
        final String tableRow = "%-50s  %10d  %10d  %10d  %10d  %10d  %10d  %10d";

        // sort list so the names is A..Z
        Collections.sort(queueList, new ObjectInstanceComparator());

        context.print(String.format(Locale.US, header, "Name", "Queue Size", "Producer #", "Consumer #", "Enqueue #", "Dequeue #", "Forward #", "Memory %"));

        // Iterate through the queue result
        for (Object view : queueList) {
            ObjectInstance obj = (ObjectInstance) view;
            if (!filterMBeans(obj)) {
                continue;
            }
            ObjectName queueName = obj.getObjectName();

            QueueViewMBean queueView = MBeanServerInvocationHandler.
                newProxyInstance(createJmxConnection(), queueName, QueueViewMBean.class, true);

            context.print(String.format(Locale.US, tableRow,
                    queueView.getName(),
                    queueView.getQueueSize(),
                    queueView.getProducerCount(),
                    queueView.getConsumerCount(),
                    queueView.getEnqueueCount(),
                    queueView.getDequeueCount(),
                    queueView.getForwardCount(),
                    queueView.getMemoryPercentUsage()));
        }
    }

    @SuppressWarnings("unchecked")
    private void displayQueueStats() throws Exception {

        String query = JmxMBeansUtil.createQueryString(queryString, "Queue");
        List queueList = JmxMBeansUtil.queryMBeans(createJmxConnection(), query);

        final String header = "%-50s  %10s  %10s  %10s  %10s  %10s  %10s  %10s  %10s";
        final String tableRow = "%-50s  %10d  %10d  %10d  %10d  %10d  %10d  %10d  %10d";

        context.print(String.format(Locale.US, header, "Name", "Queue Size", "Producer #", "Consumer #", "Enqueue #", "Dequeue #", "Forward #", "Memory %", "Inflight #"));

        Collections.sort(queueList, new ObjectInstanceComparator());

        // Iterate through the queue result
        for (Object view : queueList) {
            ObjectInstance obj = (ObjectInstance) view;
            if (!filterMBeans(obj)) {
                continue;
            }
            ObjectName queueName = obj.getObjectName();

            QueueViewMBean queueView = MBeanServerInvocationHandler.
                newProxyInstance(createJmxConnection(), queueName, QueueViewMBean.class, true);

            context.print(String.format(Locale.US, tableRow,
                    queueView.getName(),
                    queueView.getQueueSize(),
                    queueView.getProducerCount(),
                    queueView.getConsumerCount(),
                    queueView.getEnqueueCount(),
                    queueView.getDequeueCount(),
                    queueView.getForwardCount(),
                    queueView.getMemoryPercentUsage(),
                    queueView.getInFlightCount()));
        }
    }

    @SuppressWarnings("unchecked")
    private void displayTopicStats() throws Exception {

        String query = JmxMBeansUtil.createQueryString(queryString, "Topic");
        List topicsList = JmxMBeansUtil.queryMBeans(createJmxConnection(), query);

        final String header = "%-50s  %10s  %10s  %10s  %10s  %10s  %10s  %10s";
        final String tableRow = "%-50s  %10d  %10d  %10d  %10d  %10d  %10d  %10d";

        // sort list so the names is A..Z
        Collections.sort(topicsList, new ObjectInstanceComparator());

        context.print(String.format(Locale.US, header, "Name", "Queue Size", "Producer #", "Consumer #", "Enqueue #", "Dequeue #", "Forward #", "Memory %"));

        // Iterate through the topics result
        for (Object view : topicsList) {
            ObjectInstance obj = (ObjectInstance) view;
            if (!filterMBeans(obj)) {
                continue;
            }
            ObjectName topicName = obj.getObjectName();

            TopicViewMBean topicView = MBeanServerInvocationHandler.
                newProxyInstance(createJmxConnection(), topicName, TopicViewMBean.class, true);

            context.print(String.format(Locale.US, tableRow,
                    topicView.getName(),
                    topicView.getQueueSize(),
                    topicView.getProducerCount(),
                    topicView.getConsumerCount(),
                    topicView.getEnqueueCount(),
                    topicView.getDequeueCount(),
                    topicView.getForwardCount(),
                    topicView.getMemoryPercentUsage()));
        }
    }

    @Override
    public String getName() {
        return "dstat";
    }

    @Override
    public String getOneLineDescription() {
        return "Performs a predefined query that displays useful tabular statistics regarding the specified destination type";
    }

    /**
     * Print the help messages for this command
     */
    @Override
    protected void printHelp() {
        context.printHelp(helpFile);
    }

    protected boolean filterMBeans(ObjectInstance obj) {
        String className = obj.getClassName();
        return className.equals(QueueView.class.getName()) || className.equals(TopicView.class.getName());
    }

    private static class ObjectInstanceComparator implements Comparator<ObjectInstance> {

        @Override
        public int compare(ObjectInstance o1, ObjectInstance o2) {
            return o1.getObjectName().compareTo(o2.getObjectName());
        }
    }

}