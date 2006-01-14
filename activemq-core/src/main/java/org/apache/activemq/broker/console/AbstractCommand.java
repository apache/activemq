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

import org.apache.activemq.ActiveMQConnectionMetaData;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractCommand implements Command {
    private boolean isPrintHelp    = false;
    private boolean isPrintVersion = false;
    protected PrintStream out;

    public int main(String[] args, InputStream in, PrintStream out) {
        this.out = out;
        try {
            List tokens = new ArrayList(Arrays.asList(args));
            parseOptions(tokens);

            if (isPrintHelp) {
                printHelp();
            } else if (isPrintVersion) {
                printVersion();
            } else {
                execute(tokens);
            }
            return 0;
        } catch (Exception e) {
            out.println("Failed to execute main task. Reason: " + e);
            e.printStackTrace(out);
            return -1;
        }
    }

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
            out.println("Ignoring unrecognized option: " + token);
        }
    }

    protected void printVersion() {
        out.println();
        out.println("ActiveMQ " + ActiveMQConnectionMetaData.PROVIDER_VERSION);
        out.println("For help or more information please see: http://www.logicblaze.com");
        out.println();
    }

    protected void printError(String message) {
        isPrintHelp = true;
        out.println(message);
        out.println();
    }

    abstract protected void execute(List tokens) throws Exception;
    abstract protected void printHelp();
}
