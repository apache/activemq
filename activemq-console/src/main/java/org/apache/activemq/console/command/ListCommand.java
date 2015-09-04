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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.activemq.console.util.JmxMBeansUtil;

public class ListCommand extends AbstractJmxCommand {

    protected String[] helpFile = new String[] {
        "Task Usage: Main list [list-options]",
        "Description:  Lists all available broker in the specified JMX context.",
        "",
        "List Options:",
        "    --jmxurl <url>             Set the JMX URL to connect to.",
        "    --pid <pid>                Set the pid to connect to (only on Sun JVM).",            
        "    --jmxuser <user>           Set the JMX user used for authenticating.",
        "    --jmxpassword <password>   Set the JMX password used for authenticating.",
        "    --jmxlocal                 Use the local JMX server instead of a remote one.",
        "    --version                  Display the version information.",
        "    -h,-?,--help               Display the stop broker help information.",
        ""
    };

    @Override
    public String getName() {
        return "list";
    }

    @Override
    public String getOneLineDescription() {
        return "Lists all available brokers in the specified JMX context";
    }

    /**
     * List all running brokers registered in the specified JMX context
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void runTask(List tokens) throws Exception {
        Set<String> propsView = new HashSet<String>();
        propsView.add("brokerName");
        context.printMBean(JmxMBeansUtil.filterMBeansView(JmxMBeansUtil.getAllBrokers(createJmxConnection()), propsView));
    }
    
    /**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        context.printHelp(helpFile);
    }

}
