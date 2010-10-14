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

import org.apache.activemq.console.util.JmxMBeansUtil;

public class ShutdownCommand extends AbstractJmxCommand {

    protected String[] helpFile = new String[] {
        "Task Usage: Main stop [stop-options] [broker-name1] [broker-name2] ...",
        "Description: Stops a running broker.",
        "", 
        "Stop Options:",
        "    --jmxurl <url>             Set the JMX URL to connect to.",
        "    --pid <pid>                   Set the pid to connect to (only on Sun JVM).",            
        "    --jmxuser <user>           Set the JMX user used for authenticating.",
        "    --jmxpassword <password>   Set the JMX password used for authenticating.",
        "    --jmxlocal                 Use the local JMX server instead of a remote one.",
        "    --all                      Stop all brokers.",
        "    --version                  Display the version information.",
        "    -h,-?,--help               Display the stop broker help information.",
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
                mbeans = JmxMBeansUtil.getAllBrokers(createJmxConnection());
                brokerNames.clear();
            } else if (brokerNames.isEmpty()) {
                // Stop the default broker
                mbeans = JmxMBeansUtil.getAllBrokers(createJmxConnection());

                // If there is no broker to stop
                if (mbeans.isEmpty()) {
                    context.printInfo("There are no brokers to stop.");
                    return;

                    // There should only be one broker to stop
                } else if (mbeans.size() > 1) {
                    context.printInfo("There are multiple brokers to stop. Please select the broker(s) to stop or use --all to stop all brokers.");
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
                    Collection matchedBrokers = JmxMBeansUtil.getBrokersByName(createJmxConnection(), brokerName);
                    if (matchedBrokers.isEmpty()) {
                        context.printInfo(brokerName + " did not match any running brokers.");
                    } else {
                        mbeans.addAll(matchedBrokers);
                    }
                }
            }

            // Stop all brokers in set
            stopBrokers(createJmxConnection(), mbeans);
        } catch (Exception e) {
            context.printException(new RuntimeException("Failed to execute stop task. Reason: " + e));
            throw new Exception(e);
        }
    }

    /**
     * Stops the list of brokers.
     * 
     * @param jmxConnection - connection to the mbean server
     * @param brokerBeans - broker mbeans to stop @throws Exception
     */
    protected void stopBrokers(MBeanServerConnection jmxConnection, Collection brokerBeans) throws Exception {
        ObjectName brokerObjName;
        for (Iterator i = brokerBeans.iterator(); i.hasNext();) {
            brokerObjName = ((ObjectInstance)i.next()).getObjectName();

            String brokerName = brokerObjName.getKeyProperty("BrokerName");
            context.print("Stopping broker: " + brokerName);

            try {
                jmxConnection.invoke(brokerObjName, "terminateJVM", new Object[] {
                    Integer.valueOf(0)
                }, new String[] {
                    "int"
                });
                context.print("Succesfully stopped broker: " + brokerName);
            } catch (Exception e) {
                // TODO: Check exceptions throwned
                // System.out.println("Failed to stop broker: [ " + brokerName +
                // " ]. Reason: " + e.getMessage());
            }
        }

        closeJmxConnection();
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
        context.printHelp(helpFile);
    }

}
