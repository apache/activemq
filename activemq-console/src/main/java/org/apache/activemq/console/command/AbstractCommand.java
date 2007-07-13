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

import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.console.formatter.GlobalWriter;

import java.util.List;

public abstract class AbstractCommand implements Command {
    public static final String COMMAND_OPTION_DELIMETER = ",";

    private boolean isPrintHelp    = false;
    private boolean isPrintVersion = false;

    /**
     * Exceute a generic command, which includes parsing the options for the command and running the specific task.
     * @param tokens - command arguments
     * @throws Exception
     */
    public void execute(List tokens) throws Exception {
        // Parse the options specified by "-"
        parseOptions(tokens);

        // Print the help file of the task
        if (isPrintHelp) {
            printHelp();

        // Print the AMQ version
        } else if (isPrintVersion) {
            GlobalWriter.printVersion(ActiveMQConnectionMetaData.PROVIDER_VERSION);

        // Run the specified task
        } else {
            runTask(tokens);
        }
    }

    /**
     * Parse any option parameters in the command arguments specified by a '-' as the first character of the token.
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void parseOptions(List tokens) throws Exception {
        while (!tokens.isEmpty()) {
            String token = (String)tokens.remove(0);
            if (token.startsWith("-")) {
                // Token is an option
                handleOption(token, tokens);
            } else {
                // Push back to list of tokens
                tokens.add(0, token);
                return;
            }
        }
    }

    /**
     * Handle the general options for each command, which includes -h, -?, --help, -D, --version.
     * @param token - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    protected void handleOption(String token, List tokens) throws Exception {
        // If token is a help option
        if (token.equals("-h") || token.equals("-?") || token.equals("--help")) {
            isPrintHelp = true;
            tokens.clear();

        // If token is a version option
        } else if (token.equals("--version")) {
            isPrintVersion = true;
            tokens.clear();
        }

        // If token is a system property define option
        else if (token.startsWith("-D")) {
            String key = token.substring(2);
            String value = "";
            int pos = key.indexOf("=");
            if (pos >= 0) {
                value = key.substring(pos + 1);
                key = key.substring(0, pos);
            }
            System.setProperty(key, value);

        }

        // Token is unrecognized
        else {
            GlobalWriter.printInfo("Unrecognized option: " + token);
            isPrintHelp = true;
        }
    }

    /**
     * Run the specific task.
     * @param tokens - command arguments
     * @throws Exception
     */
    abstract protected void runTask(List tokens) throws Exception;

    /**
     * Print the help messages for the specific task
     */
    abstract protected void printHelp();
}
