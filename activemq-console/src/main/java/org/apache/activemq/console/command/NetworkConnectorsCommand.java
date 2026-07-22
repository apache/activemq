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

import java.util.List;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.NetworkConnectorViewMBean;
import org.apache.activemq.console.util.JmxMBeansUtil;

public class NetworkConnectorsCommand extends AbstractJmxCommand {

    protected String[] helpFile = new String[] {
        "Task Usage: activemq network-connectors <action> [options] [args]",
        "Description: List, add, or remove network connectors on the broker.",
        "",
        "Actions:",
        "    list                         List all network connectors.",
        "    add <uri>                    Add a new network connector (e.g. static:(tcp://remote:61616)).",
        "    remove <name>                Remove a network connector by name.",
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
        "    activemq network-connectors list",
        "        - List all configured network connectors.",
        "    activemq network-connectors add static:(tcp://remote-broker:61616)",
        "        - Add a static network connector to a remote broker.",
        "    activemq network-connectors remove NC1",
        "        - Remove the network connector named NC1.",
        ""
    };

    @Override
    public String getName() {
        return "network-connectors";
    }

    @Override
    public String getOneLineDescription() {
        return "List, add, or remove network connectors";
    }

    @Override
    protected void runTask(List<String> tokens) throws Exception {
        if (tokens.isEmpty()) {
            printHelp();
            return;
        }

        String action = tokens.remove(0);
        if (action.equals("list")) {
            listNetworkConnectors();
        } else if (action.equals("add")) {
            if (tokens.isEmpty()) {
                throw new IllegalArgumentException("Connector URI required: activemq network-connectors add <uri>");
            }
            addNetworkConnector(tokens.get(0));
        } else if (action.equals("remove")) {
            if (tokens.isEmpty()) {
                throw new IllegalArgumentException("Connector name required: activemq network-connectors remove <name>");
            }
            removeNetworkConnector(tokens.get(0));
        } else {
            context.printInfo("Unknown action '" + action + "'. See 'activemq network-connectors --help'.");
            printHelp();
        }
    }

    @SuppressWarnings("unchecked")
    private void listNetworkConnectors() throws Exception {
        List<ObjectInstance> connectors = JmxMBeansUtil.queryMBeans(createJmxConnection(),
                "type=Broker,brokerName=*,connector=networkConnectors,networkConnectorName=*");

        if (connectors.isEmpty()) {
            context.print("No network connectors configured.");
            return;
        }

        final String fmt = "%-30s  %-10s  %-10s  %s";
        context.print(String.format(fmt, "Name", "Auto-Start", "Duplex", "User"));
        context.print(String.format(fmt, dashes(30), dashes(10), dashes(10), dashes(20)));

        for (ObjectInstance obj : connectors) {
            ObjectName name = obj.getObjectName();
            NetworkConnectorViewMBean nc = MBeanServerInvocationHandler.newProxyInstance(
                    createJmxConnection(), name, NetworkConnectorViewMBean.class, true);
            context.print(String.format(fmt,
                    name.getKeyProperty("networkConnectorName"),
                    nc.isAutoStart(),
                    nc.isDuplex(),
                    nc.getUserName() != null ? nc.getUserName() : ""));
        }
    }

    private void addNetworkConnector(String uri) throws Exception {
        String connectorName = getBrokerMBean().addNetworkConnector(uri);
        context.print("Network connector added: " + connectorName + " -> " + uri);
    }

    private void removeNetworkConnector(String name) throws Exception {
        boolean removed = getBrokerMBean().removeNetworkConnector(name);
        if (removed) {
            context.print("Network connector removed: " + name);
        } else {
            context.printInfo("Network connector not found: " + name);
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
