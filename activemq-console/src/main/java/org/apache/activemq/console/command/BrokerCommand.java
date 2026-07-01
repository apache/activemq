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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.JobSchedulerViewMBean;
import org.apache.activemq.console.util.JmxMBeansUtil;

public class BrokerCommand extends AbstractJmxCommand {

    protected String[] helpFile = new String[] {
        "Task Usage: activemq broker <action> [options] [args]",
        "Description: Inspect broker attributes, manage transport connectors, and view the scheduler.",
        "",
        "Actions:",
        "    info                              Show broker attributes (name, version, uptime, memory, etc).",
        "    connectors                        List all transport connectors and their URIs.",
        "    connectors add <uri>              Add a new transport connector (e.g. tcp://0.0.0.0:61617).",
        "    connectors remove <name>          Remove a transport connector by name.",
        "    scheduler                         Show scheduler statistics and upcoming jobs.",
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
        "    activemq broker info",
        "        - Show broker name, version, uptime, memory/store usage, and connection counts.",
        "    activemq broker connectors",
        "        - List all transport connectors (tcp, ssl, amqp, stomp, mqtt, ws, etc).",
        "    activemq broker connectors add tcp://0.0.0.0:61617",
        "        - Add a new TCP transport connector on port 61617.",
        "    activemq broker connectors remove tcp",
        "        - Remove the transport connector named 'tcp'.",
        "    activemq broker scheduler",
        "        - Show scheduled message counts and upcoming jobs.",
        ""
    };

    @Override
    public String getName() {
        return "broker";
    }

    @Override
    public String getOneLineDescription() {
        return "Show broker info, manage transport connectors, or inspect the scheduler";
    }

    @Override
    protected void runTask(List<String> tokens) throws Exception {
        if (tokens.isEmpty()) {
            printHelp();
            return;
        }

        String action = tokens.remove(0);
        if (action.equals("info")) {
            showBrokerInfo();
        } else if (action.equals("connectors")) {
            handleConnectors(tokens);
        } else if (action.equals("scheduler")) {
            showSchedulerInfo();
        } else {
            context.printInfo("Unknown action '" + action + "'. See 'activemq broker --help'.");
            printHelp();
        }
    }

    private void handleConnectors(List<String> tokens) throws Exception {
        if (tokens.isEmpty() || tokens.get(0).equals("list")) {
            listConnectors();
            return;
        }
        String subAction = tokens.remove(0);
        if (subAction.equals("add")) {
            if (tokens.isEmpty()) {
                throw new IllegalArgumentException("Connector URI required: activemq broker connectors add <uri>");
            }
            addConnector(tokens.get(0));
        } else if (subAction.equals("remove")) {
            if (tokens.isEmpty()) {
                throw new IllegalArgumentException("Connector name required: activemq broker connectors remove <name>");
            }
            removeConnector(tokens.get(0));
        } else {
            context.printInfo("Unknown connectors sub-action '" + subAction + "'. Valid: list, add, remove.");
            printHelp();
        }
    }

    private void showBrokerInfo() throws Exception {
        BrokerViewMBean broker = getBrokerMBean();

        context.print("Broker Name      : " + broker.getBrokerName());
        context.print("Broker ID        : " + broker.getBrokerId());
        context.print("Version          : " + broker.getBrokerVersion());
        context.print("Uptime           : " + broker.getUptime());
        context.print("Persistent       : " + broker.isPersistent());
        context.print("Data Directory   : " + broker.getDataDirectory());
        context.print("");
        context.print("Connections      : " + broker.getCurrentConnectionsCount() + " current / " + broker.getTotalConnectionsCount() + " total");
        context.print("Producers        : " + broker.getTotalProducerCount());
        context.print("Consumers        : " + broker.getTotalConsumerCount());
        context.print("Messages         : " + broker.getTotalMessageCount());
        context.print("");
        context.print("Memory usage     : " + broker.getMemoryPercentUsage() + "%  (limit " + broker.getMemoryLimit() / (1024 * 1024) + " MB)");
        context.print("Store usage      : " + broker.getStorePercentUsage() + "%  (limit " + broker.getStoreLimit() / (1024 * 1024) + " MB)");
        context.print("Temp usage       : " + broker.getTempPercentUsage() + "%  (limit " + broker.getTempLimit() / (1024 * 1024) + " MB)");
        context.print("");
        context.print("Total enqueued   : " + broker.getTotalEnqueueCount());
        context.print("Total dequeued   : " + broker.getTotalDequeueCount());
        context.print("Queues           : " + broker.getTotalQueuesCount());
        context.print("Topics           : " + broker.getTotalTopicsCount());
    }

    private void listConnectors() throws Exception {
        BrokerViewMBean broker = getBrokerMBean();
        Map<String, String> connectors = broker.getTransportConnectors();

        if (connectors == null || connectors.isEmpty()) {
            context.print("No transport connectors configured.");
            return;
        }

        final String fmt = "%-12s  %s";
        context.print(String.format(fmt, "Name", "URI"));
        context.print(String.format(fmt, dashes(12), dashes(60)));

        for (Map.Entry<String, String> entry : connectors.entrySet()) {
            context.print(String.format(fmt, entry.getKey(), entry.getValue()));
        }
    }

    private void addConnector(String uri) throws Exception {
        String connectorName = getBrokerMBean().addConnector(uri);
        context.print("Transport connector added: " + connectorName + " -> " + uri);
    }

    private void removeConnector(String name) throws Exception {
        boolean removed = getBrokerMBean().removeConnector(name);
        if (removed) {
            context.print("Transport connector removed: " + name);
        } else {
            context.printInfo("Transport connector not found: " + name);
        }
    }

    private void showSchedulerInfo() throws Exception {
        BrokerViewMBean broker = getBrokerMBean();
        ObjectName schedulerName = broker.getJMSJobScheduler();

        if (schedulerName == null) {
            context.print("Job scheduler is not enabled on this broker.");
            return;
        }

        JobSchedulerViewMBean scheduler = MBeanServerInvocationHandler.newProxyInstance(
                createJmxConnection(), schedulerName, JobSchedulerViewMBean.class, true);

        context.print("Scheduled messages : " + scheduler.getScheduledMessageCount());
        context.print("Delayed messages   : " + scheduler.getDelayedMessageCount());
        context.print("Next scheduled at  : " + scheduler.getNextScheduleTime());

        TabularData jobs = scheduler.getNextScheduleJobs();
        if (jobs == null || jobs.isEmpty()) {
            context.print("No upcoming jobs.");
            return;
        }

        context.print("");
        context.print("Upcoming jobs:");
        context.print(dashes(60));

        Collection<?> rows = jobs.values();
        int index = 1;
        for (Object row : rows) {
            CompositeData job = (CompositeData) row;
            context.print("Job " + index++ + ":");
            for (String key : job.getCompositeType().keySet()) {
                Object value = job.get(key);
                if (value != null && !value.toString().isEmpty()) {
                    context.print(String.format("  %-20s: %s", key, value));
                }
            }
            context.print("");
        }
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
