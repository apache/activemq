/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.console;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ObjectInstance;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;

public class ShutdownTask extends AbstractJmxTask {
    private boolean isStopAllBrokers = false;

    protected void startTask(List brokerNames) {
        try {
            Set mbeans = new HashSet();
            MBeanServerConnection server = createJmxConnector().getMBeanServerConnection();

            // Stop all brokers
            if (isStopAllBrokers) {
                mbeans = AmqJmxSupport.getAllBrokers(server);
                brokerNames.clear();
            }

            // Stop the default broker
            else if (brokerNames.isEmpty()) {
                mbeans = AmqJmxSupport.getAllBrokers(server);

                // If there is no broker to stop
                if (mbeans.isEmpty()) {
                    System.out.println("There are no brokers to stop.");
                    return;

                // There should only be one broker to stop
                } else if (mbeans.size() > 1) {
                    System.out.println("There are multiple brokers to stop. Please select the broker(s) to stop or use --all to stop all brokers.");
                    System.out.println();
                    AmqJmxSupport.printBrokerList(mbeans);
                    return;

                // Get the first broker only
                } else {
                    Object firstBroker = mbeans.iterator().next();
                    mbeans.clear();
                    mbeans.add(firstBroker);
                }
            }

            // Stop each specified broker
            else {
                String brokerName;
                while (!brokerNames.isEmpty()) {
                    brokerName = (String)brokerNames.remove(0);
                    Set matchedBrokers = AmqJmxSupport.getBrokers(server, brokerName);
                    if (matchedBrokers.isEmpty()) {
                        System.out.println(brokerName + " did not match any running brokers.");
                    } else {
                        mbeans.addAll(matchedBrokers);
                    }
                }
            }

            // Stop all brokers in set
            stopBrokers(server, mbeans);
            
            closeJmxConnector();
        } catch (Throwable e) {
            System.out.println("Failed to execute stop task. Reason: " + e);
        }
    }

    protected void stopBrokers(MBeanServerConnection server, Set brokerBeans) throws Exception {
        ObjectName brokerObjName;
        for (Iterator i=brokerBeans.iterator(); i.hasNext();) {
            brokerObjName = ((ObjectInstance)i.next()).getObjectName();

            String brokerName = brokerObjName.getKeyProperty("BrokerName");
            System.out.println("Stopping broker: " + brokerName);

            try {
                server.invoke(brokerObjName, "terminateJVM", new Object[] {new Integer(0)}, new String[] {"int"});
                System.out.println("Succesfully stopped broker: " + brokerName);
            } catch (Exception e) {
                // TODO: Check exceptions throwned
                //System.out.println("Failed to stop broker: [ " + brokerName + " ]. Reason: " + e.getMessage());
            }
        }
    }

    protected void printHelp() {
        System.out.println("Task Usage: Main stop [stop-options] [broker-name1] [broker-name2] ...");
        System.out.println("Description: Stops a running broker.");
        System.out.println("");
        System.out.println("Stop Options:");
        System.out.println("    --jmxurl <url>      Set the JMX URL to connect to.");
        System.out.println("    --all               Stop all brokers.");
        System.out.println("    --version           Display the version information.");
        System.out.println("    -h,-?,--help        Display the stop broker help information.");
        System.out.println("");
        System.out.println("Broker Names:");
        System.out.println("    Name of the brokers that will be stopped.");
        System.out.println("    If omitted, it is assumed that there is only one broker running, and it will be stopped.");
        System.out.println("    Use -all to stop all running brokers.");
        System.out.println("");
    }

    protected void handleOption(String token, List tokens) throws Exception {
        // Try to handle the options first
        if (token.equals("--all")) {
            isStopAllBrokers = true;
        } else {
            // Let the super class handle the option
            super.handleOption(token, tokens);
        }
    }
}
