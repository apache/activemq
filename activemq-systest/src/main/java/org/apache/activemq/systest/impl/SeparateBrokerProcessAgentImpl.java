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

import java.io.File;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.systest.BrokerAgent;

import javax.jms.ConnectionFactory;


/**
 * Runs a broker in a separate process
 * 
 * @version $Revision: 1.1 $
 */
public class SeparateBrokerProcessAgentImpl extends SeparateProcessAgent implements BrokerAgent {

    private static final String ENV_HOME = "ACTIVEMQ_HOME";

    private static int portCounter = 61616;

    private int port;
    private String connectionURI;
    private String brokerScript;
    private File workingDirectory = new File("target/test-brokers");
    private String defaultPrefix = "~/activemq";
    private String coreURI;

    public SeparateBrokerProcessAgentImpl(String host) throws Exception {
        port = portCounter++;
        coreURI = "tcp://" + host + ":" + port;
        connectionURI = "failover:(" + coreURI + ")?useExponentialBackOff=false&initialReconnectDelay=500&&maxReconnectAttempts=20";
    }

    public void kill() throws Exception {
        stop();
    }

    public ConnectionFactory getConnectionFactory() {
        return new ActiveMQConnectionFactory(getConnectionURI());
    }

    public String getConnectionURI() {
        return connectionURI;
    }

    public void connectTo(BrokerAgent remoteBroker) throws Exception {
        // lets assume discovery works! :)
    }

    public String getBrokerScript() {
        if (brokerScript == null) {
            brokerScript = createBrokerScript();
        }
        return brokerScript;
    }

    public void setBrokerScript(String activemqScript) {
        this.brokerScript = activemqScript;
    }

    public String getDefaultPrefix() {
        return defaultPrefix;
    }

    public void setDefaultPrefix(String defaultPrefix) {
        this.defaultPrefix = defaultPrefix;
    }

    public File getWorkingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(File workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Process createProcess() throws Exception {
        String commands[] = getCommands();

        System.out.print("About to execute command:");
        for (int i=0; i<commands.length; i++) {
            System.out.print(" ");
            System.out.print(commands[i]);
        }
        System.out.println();

        File workingDir = createBrokerWorkingDirectory();

        System.out.println("In directory: " + workingDir);

        Process answer = Runtime.getRuntime().exec(commands, null, workingDir);
        return answer;
    }

    protected File createBrokerWorkingDirectory() {
        workingDirectory.mkdirs();

        // now lets create a new temporary directory
        File brokerDir = new File(workingDirectory, "broker_" + port);
        brokerDir.mkdirs();

        File varDir = new File(brokerDir, "data");
        varDir.mkdirs();

        File workDir = new File(brokerDir, "work");
        workDir.mkdirs();
        return workDir;
    }

    protected String createBrokerScript() {
        String version = null;
        Package p = Package.getPackage("org.apache.activemq");
        if (p != null) {
            version = p.getImplementationVersion();
        }
        if (version == null) {
            version = "activemq-4.0-SNAPSHOT";
        }
        return "../../../../../assembly/target/" + version + "/bin/" + version + "/bin/activemq";
    }

    protected String[] createCommand() {
        // lets try load the broker script from a system property
        String script = System.getProperty("brokerScript");
        if (script == null) {
            String home = System.getenv(ENV_HOME);
            if (home == null) {
                script = getBrokerScript();
            }
            else {
                script = home + "/bin/" + brokerScript;
            }
        }

        String[] answer = { "/bin/bash", script, "broker:" + coreURI };
        return answer;
    }
}
