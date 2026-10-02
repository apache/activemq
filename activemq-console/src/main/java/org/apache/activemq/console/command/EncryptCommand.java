/*
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

import java.util.List;

import org.apache.activemq.util.ActiveMQEncryptor;

public class EncryptCommand extends AbstractCommand {

    protected String[] helpFile = new String[] {
            "Task Usage: Main encrypt --password <password> --input <input>",
            "Description: Encrypts given text using AES-256-GCM with a PBKDF2 derived key.",
            "",
            "Encrypt Options:",
            "    --password <password>      Password to be used by the encryptor.  Defaults to",
            "                               the value in the ACTIVEMQ_ENCRYPTION_PASSWORD env variable.",
            "    --input <input>            Text to be encrypted.",
            "    --version                  Display the version information.",
            "    -h,-?,--help               Display the stop broker help information.",
            ""
        };

    ActiveMQEncryptor encryptor = new ActiveMQEncryptor();
    String input;
    String password;
    String algorithm;

    @Override
    public String getName() {
        return "encrypt";
    }

    @Override
    public String getOneLineDescription() {
        return "Encrypts given text";
    }

    @Override
    protected void printHelp() {
        context.printHelp(helpFile);
    }

    @Override
    protected void runTask(List<String> tokens) throws Exception {
        if( password == null ) {
            password = System.getenv("ACTIVEMQ_ENCRYPTION_PASSWORD");
        }
        if (password == null || input == null) {
            context.printException(new IllegalArgumentException("input and password parameters are mandatory"));
            return;
        }
        if (algorithm != null) {
            context.printException(new IllegalArgumentException(
                    "--algorithm is only supported by the decrypt command for reading legacy values;"
                    + " encryption always uses AES-256-GCM with a PBKDF2 derived key"));
            return;
        }
        encryptor.setPassword(password);
        var encrypted = encryptor.encrypt(input);
        context.print("Encrypted text: " + encrypted);
        context.print("Property value: " + ActiveMQEncryptor.wrapEncryptedValue(encrypted));
    }

    @Override
    protected void handleOption(String token, List<String> tokens) throws Exception {
        if (token.startsWith("--input")) {
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("input not specified"));
                return;
            }

            input=(String)tokens.remove(0);
        } else if (token.startsWith("--password")) {
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("password not specified"));
                return;
            }

            password=(String)tokens.remove(0);
        } else if (token.startsWith("--algorithm")) {
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("algorithm not specified"));
                return;
            }

            algorithm=(String)tokens.remove(0);
        } else {
            super.handleOption(token, tokens);
        }
    }



}
