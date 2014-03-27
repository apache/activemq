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

import org.jasypt.exceptions.EncryptionOperationNotPossibleException;

public class DecryptCommand extends EncryptCommand {

    protected String[] helpFile = new String[] {
            "Task Usage: Main decrypt --password <password> --input <input>",
            "Description: Decrypts given text.",
            "", 
            "Encrypt Options:",
            "    --password <password>      Password to be used by the encryptor.  Defaults to",
            "                               the value in the ACTIVEMQ_ENCRYPTION_PASSWORD env variable.",
            "    --input <input>            Text to be encrypted.",
            "    --version                  Display the version information.",
            "    -h,-?,--help               Display the stop broker help information.",
            ""
        };    
    
    @Override
    public String getName() {
        return "decrypt";
    }

    @Override
    public String getOneLineDescription() {
        return "Decrypts given text";
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
        try {
            context.print("Decrypted text: " + encryptor.decrypt(input));
        } catch (EncryptionOperationNotPossibleException e) {
            context.print("ERROR: Text cannot be decrypted, check your input and password and try again!");
        }
    }

    
    
}
