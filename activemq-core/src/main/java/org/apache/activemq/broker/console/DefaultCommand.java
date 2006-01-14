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

public class DefaultCommand extends AbstractCommand {

    protected void execute(List tokens) {
        
        // Process task token
        if( tokens.size() > 0 ) {
            String taskToken = (String)tokens.remove(0);
            if (taskToken.equals("start")) {
                new StartCommand().execute(tokens);
            } else if (taskToken.equals("stop")) {
                new ShutdownCommand().execute(tokens);
            } else if (taskToken.equals("list")) {
                new ListCommand().execute(tokens);
            } else if (taskToken.equals("query")) {
                new QueryCommand().execute(tokens);
            } else {
                // If not valid task, push back to list
                tokens.add(0, taskToken);
                new StartCommand().execute(tokens);
            }
        } else {
            new StartCommand().execute(tokens);
        }
        
    }

    protected void printHelp() {
        out.println("Usage: Main [task] [--extdir <dir>] [task-options] [task data]");
        out.println("");
        out.println("Tasks (default task is start):");
        out.println("    start           - Creates and starts a broker using a configuration file, or a broker URI.");
        out.println("    stop            - Stops a running broker specified by the broker name.");
        out.println("    list            - Lists all available brokers in the specified JMX context.");
        out.println("    query           - Display selected broker component's attributes and statistics.");
        out.println("    --extdir <dir>  - Add the jar files in the directory to the classpath.");
        out.println("    --version       - Display the version information.");
        out.println("    -h,-?,--help    - Display this help information. To display task specific help, use Main [task] -h,-?,--help");
        out.println("");
        out.println("Task Options:");
        out.println("    - Properties specific to each task.");
        out.println("");
        out.println("Task Data:");
        out.println("    - Information needed by each specific task.");
        out.println("");
    }
}
