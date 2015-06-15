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

import org.apache.activemq.console.CommandContext;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.*;

public class ShellCommand extends AbstractCommand {

    private boolean interactive;
    private String[] helpFile;

    public ShellCommand() {
        this(false);
    }

    public ShellCommand(boolean interactive) {
        this.interactive = interactive;
        ArrayList<String> help = new ArrayList<String>();
        help.addAll(Arrays.asList(new String[] {
                    interactive ? "Usage: [task] [task-options] [task data]" : "Usage: Main [--extdir <dir>] [task] [task-options] [task data]",
                    "",
                    "Tasks:"}));

        ArrayList<Command> commands = getCommands();
        Collections.sort(commands, new Comparator<Command>() {
            @Override
            public int compare(Command command, Command command1) {
                return command.getName().compareTo(command1.getName());
            }
        });

        for( Command command: commands) {
            help.add(String.format("    %-24s - %s", command.getName(), command.getOneLineDescription()));
        }

        help.addAll(Arrays.asList(new String[] {
                    "",
                    "Task Options (Options specific to each task):",
                    "    --extdir <dir>  - Add the jar files in the directory to the classpath.",
                    "    --version       - Display the version information.",
                    "    -h,-?,--help    - Display this help information. To display task specific help, use " + (interactive ? "" : "Main ") + "[task] -h,-?,--help",
                    "",
                    "Task Data:",
                    "    - Information needed by each specific task.",
                    "",
                    "JMX system property options:",
                    "    -Dactivemq.jmx.url=<jmx service uri> (default is: 'service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi')",
                    "    -Dactivemq.jmx.user=<user name>",
                    "    -Dactivemq.jmx.password=<password>",
                    ""
                }));

        this.helpFile = help.toArray(new String[help.size()]);
    }

    @Override
    public String getName() {
        return "shell";
    }

    @Override
    public String getOneLineDescription() {
        return "Runs the activemq sub shell";
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
            return 1;
        }
    }

    public static void main(String[] args) {
        main(args, System.in, System.out);
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


            for( Command c: getCommands() ) {
                if( taskToken.equals(c.getName()) ) {
                    command = c;
                    break;
                }
            }
            if( command == null ) {
                if (taskToken.equals("help")) {
                    printHelp();
                } else {
                    printHelp();
                }
            }

            if( command!=null ) {
                command.setCommandContext(context);
                command.execute(tokens);
            }
        } else {
            printHelp();
        }

    }

    ArrayList<Command> getCommands() {
        ServiceLoader<Command> loader = ServiceLoader.load(Command.class);
        Iterator<Command> iterator = loader.iterator();
        ArrayList<Command> rc = new ArrayList<Command>();
        boolean done = false;
        while (!done) {
            try {
                if( iterator.hasNext() ) {
                    rc.add(iterator.next());
                } else {
                    done = true;
                }
            } catch (ServiceConfigurationError e) {
                // it's ok, some commands may not load if their dependencies
                // are not available.
            }
        }
        return rc;
    }

	/**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        context.printHelp(helpFile);
    }
}
