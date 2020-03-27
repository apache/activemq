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

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.iv.RandomIvGenerator;

public class EncryptCommand extends AbstractCommand {

    protected String[] helpFile = new String[] {
            "Task Usage: Main encrypt --password <password> --input <input>",
            "Description: Encrypts given text.",
            "", 
            "Encrypt Options:",
            "    --password <password>      Password to be used by the encryptor.  Defaults to",
            "                               the value in the ACTIVEMQ_ENCRYPTION_PASSWORD env variable.",
            "    --input <input>            Text to be encrypted.",
            "    --algorithm <algorithm>    Algorithm to use.",
            "    --version                  Display the version information.",
            "    -h,-?,--help               Display the stop broker help information.",
            ""
        };
    
    StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
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
        encryptor.setPassword(password);
        if (algorithm != null) {
             encryptor.setAlgorithm(algorithm);
             // From Jasypt: for PBE-AES-based algorithms, the IV generator is MANDATORY"
             if (algorithm.startsWith("PBE") && algorithm.contains("AES")) {
                 encryptor.setIvGenerator(new RandomIvGenerator());
             }
        }
        context.print("Encrypted text: " + encryptor.encrypt(input));
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
