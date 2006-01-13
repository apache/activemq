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

import java.util.List;

public abstract class AbstractTask implements Task {
    private boolean isPrintHelp    = false;
    private boolean isPrintVersion = false;

    public void runTask(List tokens) throws Exception {
        parseOptions(tokens);

        if (isPrintHelp) {
            printHelp();
        } else if (isPrintVersion) {
            printVersion();
        } else {
            startTask(tokens);
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
            System.out.println("Ignoring unrecognized option: " + token);
        }
    }

    protected void printVersion() {
        System.out.println();
        System.out.println("ActiveMQ " + ActiveMQConnectionMetaData.PROVIDER_VERSION);
        System.out.println("For help or more information please see: http://www.logicblaze.com");
        System.out.println();
    }

    protected void printError(String message) {
        isPrintHelp = true;
        System.out.println(message);
        System.out.println();
    }

    abstract protected void startTask(List tokens) throws Exception;
    abstract protected void printHelp();
}
