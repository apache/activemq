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
package org.apache.activemq.console.command;

import org.apache.activemq.console.formatter.GlobalWriter;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.InputStream;
import java.io.PrintStream;

public class ShellCommand extends AbstractCommand {

    /**
     * Main method to run a command shell client.
     * @param args - command line arguments
     * @param in - input stream to use
     * @param out - output stream to use
     * @return 0 for a successful run, -1 if there are any exception
     */
    public static int main(String[] args, InputStream in, PrintStream out) {
        GlobalWriter.instantiate(new CommandShellOutputFormatter(out));

        // Convert arguments to list for easier management
        List tokens = new ArrayList(Arrays.asList(args));

        ShellCommand main = new ShellCommand();
        try {
            main.execute(tokens);
            return 0;
        } catch (Exception e) {
            GlobalWriter.printException(e);
            return -1;
        }
    }

    /**
     * Parses for specific command task, default task is a start task.
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void runTask(List tokens) throws Exception {
        
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
            } else if (taskToken.equals("browse")) {
                new AmqBrowseCommand().execute(tokens);
            } else if (taskToken.equals("purge")) {
                new PurgeCommand().execute(tokens);
            } else {
                // If not valid task, push back to list
                tokens.add(0, taskToken);
                new StartCommand().execute(tokens);
            }
        } else {
            new StartCommand().execute(tokens);
        }
        
    }

    /**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        GlobalWriter.printHelp(helpFile);
    }

    protected String[] helpFile = new String[] {
        "Usage: Main [--extdir <dir>] [task] [task-options] [task data]",
        "",
        "Tasks (default task is start):",
        "    start           - Creates and starts a broker using a configuration file, or a broker URI.",
        "    stop            - Stops a running broker specified by the broker name.",
        "    list            - Lists all available brokers in the specified JMX context.",
        "    query           - Display selected broker component's attributes and statistics.",
        "    browse          - Display selected messages in a specified destination.",
        "",
        "Task Options (Options specific to each task):",
        "    --extdir <dir>  - Add the jar files in the directory to the classpath.",
        "    --version       - Display the version information.",
        "    -h,-?,--help    - Display this help information. To display task specific help, use Main [task] -h,-?,--help",
        "",
        "Task Data:",
        "    - Information needed by each specific task.",
        ""
    };
}
