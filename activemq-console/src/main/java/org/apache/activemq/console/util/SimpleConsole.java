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
package org.apache.activemq.console.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.activemq.broker.util.CommandMessageListener;

/**
 * A simple interactive console which can be used to communicate with a running
 * broker assuming that the classpath is fully setup
 * 
 * @version $Revision$
 */
public final class SimpleConsole {
    
    private SimpleConsole() {
    }

    public static void main(String[] args) {
        CommandMessageListener listener = new CommandMessageListener(null);

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String line = reader.readLine();
                if (line == null || "quit".equalsIgnoreCase(line)) {
                    break;
                }
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }

                System.out.println(listener.processCommandText(line));
            }
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }
}
