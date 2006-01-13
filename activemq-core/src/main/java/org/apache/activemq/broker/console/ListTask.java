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

import java.util.List;

public class ListTask extends AbstractJmxTask {

    protected void startTask(List tokens) {
        try {
            AmqJmxSupport.printBrokerList(AmqJmxSupport.getAllBrokers(createJmxConnector().getMBeanServerConnection()));
            closeJmxConnector();
        } catch (Throwable e) {
            System.out.println("Failed to execute list task. Reason: " + e);
        }

    }

    protected void printHelp() {
        System.out.println("Task Usage: Main list [list-options]");
        System.out.println("Description:  Lists all available broker in the specified JMX context.");
        System.out.println("");
        System.out.println("List Options:");
        System.out.println("    --jmxurl <url>      Set the JMX URL to connect to.");
        System.out.println("    --version           Display the version information.");
        System.out.println("    -h,-?,--help        Display the stop broker help information.");
        System.out.println("");
    }
}
