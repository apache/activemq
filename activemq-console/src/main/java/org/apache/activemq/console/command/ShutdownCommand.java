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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.console.formatter.GlobalWriter;
import org.apache.activemq.console.util.JmxMBeansUtil;

public class ShutdownCommand extends AbstractJmxCommand {

    protected String[] helpFile = new String[] {
        "Task Usage: Main stop [stop-options] [broker-name1] [broker-name2] ...",
        "Description: Stops a running broker.",
        "", 
        "Stop Options:",
        "    --jmxurl <url>      Set the JMX URL to connect to.",
        "    --all               Stop all brokers.",
        "    --version           Display the version information.",
        "    -h,-?,--help        Display the stop broker help information.",
        "",
        "Broker Names:",
        "    Name of the brokers that will be stopped.",
        "    If omitted, it is assumed that there is only one broker running, and it will be stopped.",
        "    Use -all to stop all running brokers.",
        ""
    };

    private boolean isStopAllBrokers;

    /**
     * Shuts down the specified broker or brokers
     * 
     * @param brokerNames - names of brokers to shutdown
     * @throws Exception
     */
    protected void runTask(List brokerNames) throws Exception {
        try {
            Collection mbeans;

            // Stop all brokers
            if (isStopAllBrokers) {
                mbeans = JmxMBeansUtil.getAllBrokers(useJmxServiceUrl());
                brokerNames.clear();
            } else if (brokerNames.isEmpty()) {
                // Stop the default broker
                mbeans = JmxMBeansUtil.getAllBrokers(useJmxServiceUrl());

                // If there is no broker to stop
                if (mbeans.isEmpty()) {
                    GlobalWriter.printInfo("There are no brokers to stop.");
                    return;

                    // There should only be one broker to stop
                } else if (mbeans.size() > 1) {
                    GlobalWriter.printInfo("There are multiple brokers to stop. Please select the broker(s) to stop or use --all to stop all brokers.");
                    return;

                    // Get the first broker only
                } else {
                    Object firstBroker = mbeans.iterator().next();
                    mbeans.clear();
                    mbeans.add(firstBroker);
                }
            } else {
                // Stop each specified broker
                String brokerName;
                mbeans = new HashSet();
                while (!brokerNames.isEmpty()) {
                    brokerName = (String)brokerNames.remove(0);
                    Collection matchedBrokers = JmxMBeansUtil.getBrokersByName(useJmxServiceUrl(), brokerName);
                    if (matchedBrokers.isEmpty()) {
                        GlobalWriter.printInfo(brokerName + " did not match any running brokers.");
                    } else {
                        mbeans.addAll(matchedBrokers);
                    }
                }
            }

            // Stop all brokers in set
            stopBrokers(useJmxServiceUrl(), mbeans);
        } catch (Exception e) {
            GlobalWriter.printException(new RuntimeException("Failed to execute stop task. Reason: " + e));
            throw new Exception(e);
        }
    }

    /**
     * Stops the list of brokers.
     * 
     * @param jmxServiceUrl - JMX service url to connect to
     * @param brokerBeans - broker mbeans to stop
     * @throws Exception
     */
    protected void stopBrokers(JMXServiceURL jmxServiceUrl, Collection brokerBeans) throws Exception {
        MBeanServerConnection server = createJmxConnector().getMBeanServerConnection();

        ObjectName brokerObjName;
        for (Iterator i = brokerBeans.iterator(); i.hasNext();) {
            brokerObjName = ((ObjectInstance)i.next()).getObjectName();

            String brokerName = brokerObjName.getKeyProperty("BrokerName");
            GlobalWriter.print("Stopping broker: " + brokerName);

            try {
                server.invoke(brokerObjName, "terminateJVM", new Object[] {
                    Integer.valueOf(0)
                }, new String[] {
                    "int"
                });
                GlobalWriter.print("Succesfully stopped broker: " + brokerName);
            } catch (Exception e) {
                // TODO: Check exceptions throwned
                // System.out.println("Failed to stop broker: [ " + brokerName +
                // " ]. Reason: " + e.getMessage());
            }
        }

        closeJmxConnector();
    }

    /**
     * Handle the --all option.
     * 
     * @param token - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    protected void handleOption(String token, List<String> tokens) throws Exception {
        // Try to handle the options first
        if (token.equals("--all")) {
            isStopAllBrokers = true;
        } else {
            // Let the super class handle the option
            super.handleOption(token, tokens);
        }
    }

    /**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        GlobalWriter.printHelp(helpFile);
    }

}
