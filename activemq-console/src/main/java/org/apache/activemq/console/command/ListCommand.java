/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.console.command;

import org.apache.activemq.console.formatter.GlobalWriter;
import org.apache.activemq.console.util.JmxMBeansUtil;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class ListCommand extends AbstractJmxCommand {

    /**
     * List all running brokers registered in the specified JMX context
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void runTask(List tokens) throws Exception {
        try {
            Set propsView = new HashSet();
            propsView.add("BrokerName");
            GlobalWriter.printMBean(JmxMBeansUtil.filterMBeansView(JmxMBeansUtil.getAllBrokers(useJmxServiceUrl()), propsView));
        } catch (Exception e) {
            GlobalWriter.printException(new RuntimeException("Failed to execute list task. Reason: " + e));
            throw new Exception(e);
        }
    }
    
    /**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        GlobalWriter.printHelp(helpFile);
    }

    protected String[] helpFile = new String[] {
        "Task Usage: Main list [list-options]",
        "Description:  Lists all available broker in the specified JMX context.",
        "",
        "List Options:",
        "    --jmxurl <url>      Set the JMX URL to connect to.",
        "    --version           Display the version information.",
        "    -h,-?,--help        Display the stop broker help information.",
        ""
    };
}
