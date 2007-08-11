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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BstatCommand extends QueryCommand {

    protected String[] helpFile = new String[] {
        "Task Usage: activemq-admin bstat [bstat-options] [broker-name]",
        "Description: Performs a predefined query that displays useful statistics regarding the specified broker.",
        "             If no broker name is specified, it will try and select from all registered brokers.",
        "",
        "Bstat Options:",
        "    --jmxurl <url>                Set the JMX URL to connect to.",
        "    --version                     Display the version information.",
        "    -h,-?,--help                  Display the query broker help information.",
        "",
        "Examples:",
        "    activemq-admin bstat localhost",
        "        - Display a summary of statistics for the broker 'localhost'"
    };
    
    /**
     * Performs a predefiend query option
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void runTask(List<String> tokens) throws Exception {
        List<String> queryTokens = new ArrayList<String>();
        // Find the first non-option token
        String brokerName = "*";
        for (Iterator i = tokens.iterator(); i.hasNext();) {
            String token = (String)i.next();
            if (!token.startsWith("-")) {
                brokerName = token;
                break;
            } else {
                // Re-insert options
                queryTokens.add(token);
            }
        }

        // Build the predefined option
        queryTokens.add("--objname");
        queryTokens.add("Type=*,BrokerName=" + brokerName);
        queryTokens.add("-xQTopic=ActiveMQ.Advisory.*");
        queryTokens.add("--vuew");
        queryTokens.add("Type,BrokerName,Destination,ConnectorName,EnqueueCount,"
                        + "DequeueCount,TotalEnqueueCount,TotalDequeueCount,Messages,"
                        + "TotalMessages,ConsumerCount,TotalConsumerCount,DispatchQueueSize");

        // Call the query command
        super.runTask(queryTokens);
    }

}
