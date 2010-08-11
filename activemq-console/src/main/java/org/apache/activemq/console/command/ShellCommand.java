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

import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.activemq.console.CommandContext;
import org.apache.activemq.console.command.store.amq.AMQJournalToolCommand;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;

public class ShellCommand extends AbstractCommand {

    private boolean interactive;
    private String[] helpFile;

    public ShellCommand() {
        this(false);
    }

    public ShellCommand(boolean interactive) {
        this.interactive = interactive;
        this.helpFile = new String[] {
            interactive ? "Usage: [task] [task-options] [task data]" : "Usage: Main [--extdir <dir>] [task] [task-options] [task data]", 
            "",
            "Tasks (default task is start):",
            "    start           - Creates and starts a broker using a configuration file, or a broker URI.",
            "    create          - Creates a runnable broker instance in the specified path",
            "    stop            - Stops a running broker specified by the broker name.",
            "    list            - Lists all available brokers in the specified JMX context.",
            "    query           - Display selected broker component's attributes and statistics.",
            "    browse          - Display selected messages in a specified destination.",
            "    journal-audit   - Allows you to view records stored in the persistent journal.",
            "    purge           - Delete selected destination's messages that matches the message selector",
            "",
            "Task Options (Options specific to each task):",
            "    --extdir <dir>  - Add the jar files in the directory to the classpath.",
            "    --version       - Display the version information.",
            "    -h,-?,--help    - Display this help information. To display task specific help, use " + (interactive ? "" : "Main ") + "[task] -h,-?,--help", 
            "",
            "Task Data:",
            "    - Information needed by each specific task.",
            ""
        };
    }

    /**
     * Main method to run a command shell client.
     * 
     * @param args - command line arguments
     * @param in - input stream to use
     * @param out - output stream to use
     * @return 0 for a successful run, -1 if there are any exception
     */
    public static int main(String[] args, InputStream in, PrintStream out) {
        
        CommandContext context = new CommandContext();
        context.setFormatter(new CommandShellOutputFormatter(out));

        // Convert arguments to list for easier management
        List<String> tokens = new ArrayList<String>(Arrays.asList(args));

        ShellCommand main = new ShellCommand();
        try {
            main.setCommandContext(context);
            main.execute(tokens);
            return 0;
        } catch (Exception e) {
            context.printException(e);
            return -1;
        }
    }

    public boolean isInteractive() {
        return interactive;
    }

    public void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }

    /**
     * Parses for specific command task.
     * 
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void runTask(List<String> tokens) throws Exception {

        // Process task token
        if (tokens.size() > 0) {
            Command command=null;
            String taskToken = (String)tokens.remove(0);
            if (taskToken.equals("start")) {
                command = new StartCommand();
            } else if (taskToken.equals("create")) {
                command = new CreateCommand();
            } else if (taskToken.equals("stop")) {
                command = new ShutdownCommand();
            } else if (taskToken.equals("list")) {
                command = new ListCommand();
            } else if (taskToken.equals("query")) {
                command = new QueryCommand();
            } else if (taskToken.equals("bstat")) {
                command = new BstatCommand();
            } else if (taskToken.equals("browse")) {
                command = new AmqBrowseCommand();
            } else if (taskToken.equals("purge")) {
                command = new PurgeCommand();
            } else if (taskToken.equals("journal-audit")) {
                command = new AMQJournalToolCommand();
            } else if (taskToken.equals("help")) {
                printHelp();
            } else {
                printHelp();
            }
            
            if( command!=null ) {
                command.setCommandContext(context);
                command.execute(tokens);
            }
        } else {
            printHelp();
        }

    }

	/**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        context.printHelp(helpFile);
    }
}
