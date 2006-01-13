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

public class DefaultTask extends StartTask {

    protected void printHelp() {
        System.out.println("Usage: Main [task] [--extdir <dir>] [task-options] [task data]");
        System.out.println("");
        System.out.println("Tasks (default task is start):");
        System.out.println("    start           - Creates and starts a broker using a configuration file, or a broker URI.");
        System.out.println("    stop            - Stops a running broker specified by the broker name.");
        System.out.println("    list            - Lists all available brokers in the specified JMX context.");
        System.out.println("    query           - Display selected broker component's attributes and statistics.");
        System.out.println("    --extdir <dir>  - Add the jar files in the directory to the classpath.");
        System.out.println("    --version       - Display the version information.");
        System.out.println("    -h,-?,--help    - Display this help information. To display task specific help, use Main [task] -h,-?,--help");
        System.out.println("");
        System.out.println("Task Options:");
        System.out.println("    - Properties specific to each task.");
        System.out.println("");
        System.out.println("Task Data:");
        System.out.println("    - Information needed by each specific task.");
        System.out.println("");
    }
}
