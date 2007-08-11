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
package org.apache.activemq.systest.impl;

import org.apache.activemq.systest.AgentStopper;
import org.apache.activemq.systest.AgentSupport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Starts a separate process on this machine until its asked to be killed.
 * 
 * @version $Revision: 1.1 $
 */
public class SeparateProcessAgent extends AgentSupport {

    private String[] commands;
    private Process process;
    private long sleepTime = 10000;

    public SeparateProcessAgent() {
    }

    public String[] getCommands() {
        if (commands == null) {
            commands = createCommand();
        }
        return commands;
    }

    public void setCommands(String[] command) {
        this.commands = command;
    }

    public Process getProcess() {
        return process;
    }

    public void start() throws Exception {
        process = createProcess();
        Thread thread = new Thread(new Runnable() {
            public void run() {
                readInputStream(process.getInputStream());
            }
        });
        thread.start();

        Thread thread2 = new Thread(new Runnable() {
            public void run() {
                waitForProcessExit();
            }
        });
        thread2.start();
        
        // lets wait for the process to startup
        
        System.out.println("Waiting a little while to give the broker process to start");
        Thread.sleep(sleepTime);
    }

    public void stop(AgentStopper stopper) {
        if (process != null) {
            try {
                process.destroy();
            }
            catch (Exception e) {
                stopper.onException(this, e);
            }
            finally {
                process = null;
            }
        }
    }

    protected Process createProcess() throws Exception {
        return Runtime.getRuntime().exec(commands);
    }

    protected String[] createCommand() {
        throw new IllegalArgumentException("You must configure the 'commands' property");
    }

    protected void readInputStream(InputStream inputStream) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                System.out.println(line);
            }
        }
        catch (IOException e) {
            // ignore exceptions
            // probably end of file
        }
    }

    protected void waitForProcessExit() {
        Process p = process;
        try {
            p.waitFor();
        }
        catch (InterruptedException e) {
            System.out.println("Interrupted while waiting for process to complete: " + e);
            e.printStackTrace();
        }
        int value = p.exitValue();
        System.out.println("Process completed with exit value: " + value);

    }
}
