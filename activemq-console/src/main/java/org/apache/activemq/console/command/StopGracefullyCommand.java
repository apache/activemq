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
import java.util.Iterator;
import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;


/**
 * A StopGracefullyCommand
 *
 */
public class StopGracefullyCommand extends ShutdownCommand {
    
    @Override
    public String getName() {
        return "stop-gracefully";
    }

    @Override
    public String getOneLineDescription() {
        return "Stops a running broker gracefully.";
    }

        protected String connectorName, queueName;
        protected long timeout;
        protected long pollInterval;
        /**
         * Constructor
         */
        public StopGracefullyCommand(){
            super();
            this.helpFile = new String[] {
                "Task Usage: Main stopGracefully [stop-options] [broker-name1] [broker-name2] ...",
                "Description: Stops a running broker if there is no pending messages in the queues. It first stops the connector for client connection, then check queuesize until it becomes 0 before stop the broker.",
                "", 
                "Stop Options:",
                "    --connectorName <connectorName> connectorName to stop",
                "    --queueName <queueName>         check the queuesize of the queueName for pending message",
                "    --timeout <timeout>             periodically check the queuesize before the timeout expires",
                "    --pollInterval <pollInterval>   the time interval it checks the queuesize",
                "    --jmxurl <url>             Set the JMX URL to connect to.",
                "    --jmxuser <user>           Set the JMX user used for authenticating.",
                "    --jmxpassword <password>   Set the JMX password used for authenticating.",
                "    --jmxlocal                 Use the local JMX server instead of a remote one.",
                "    --localProcessId           Use the local process id to connect( ignore jmxurl, jmxuser, jmxpassword), need to be root to use this option",
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
                    jmxConnection.invoke(brokerObjName, "stopGracefully", new Object[] {
                       connectorName, queueName, timeout, pollInterval
                    }, new String[] {
                        "java.lang.String", "java.lang.String", "long", "long"
                    });
                    context.print("Succesfully stopped broker: " + brokerName);
                } catch (Exception e) {
                    if(!(e.getMessage().startsWith("Error unmarshaling return header"))){
                        context.print("Exception:"+e.getMessage());
                    }
                }
            }

            closeJmxConnection();
        }
    /**
     * @param token - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    protected void handleOption(String token, List<String> tokens) throws Exception {
        // Try to handle the options first
        if (token.equals("--connectorName")) {
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("connectorName not specified"));
                return;
            }

            connectorName=(String)tokens.remove(0);
        } else if (token.equals("--timeout")) {
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("timeout not specified"));
                return;
            }
            timeout=Long.parseLong(tokens.remove(0));
        } else if (token.equals("--pollInterval")) {
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("pollInterval not specified"));
                return;
            }
            pollInterval=Long.parseLong(tokens.remove(0));
        }else if(token.equals("--queueName")) {
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("queueName not specified"));
                return;
            }
            queueName=(String)tokens.remove(0);
        }else {
            // Let the super class handle the option
            super.handleOption(token, tokens);
        }
    }

}