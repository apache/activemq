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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

public class StartCommand extends AbstractCommand {

    public static final String DEFAULT_CONFIG_URI = "xbean:activemq.xml";

    protected String[] helpFile = new String[] {
        "Task Usage: Main start [start-options] [uri]",
        "Description: Creates and starts a broker using a configuration file, or a broker URI.",
        "",
        "Start Options:",
        "    -D<name>=<value>      Define a system property.",
        "    --version             Display the version information.", 
        "    -h,-?,--help          Display the start broker help information.",
        "",
        "URI:",
        "",
        "    XBean based broker configuration:",
        "",
        "        Example: Main xbean:file:activemq.xml",
        "            Loads the xbean configuration file from the current working directory",
        "        Example: Main xbean:activemq.xml",
        "            Loads the xbean configuration file from the classpath",
        "",
        "    URI Parameter based broker configuration:",
        "",
        "        Example: Main broker:(tcp://localhost:61616, tcp://localhost:5000)?useJmx=true",
        "            Configures the broker with 2 transport connectors and jmx enabled",
        "        Example: Main broker:(tcp://localhost:61616, network:tcp://localhost:5000)?persistent=false",
        "            Configures the broker with 1 transport connector, and 1 network connector and persistence disabled",
        ""
    };

    private URI configURI;
    private List<BrokerService> brokers = new ArrayList<BrokerService>(5);

    /**
     * The default task to start a broker or a group of brokers
     * 
     * @param brokerURIs
     */
    protected void runTask(List<String> brokerURIs) throws Exception {
        try {
            // If no config uri, use default setting
            if (brokerURIs.isEmpty()) {
                setConfigUri(new URI(DEFAULT_CONFIG_URI));
                startBroker(getConfigUri());

                // Set configuration data, if available, which in this case
                // would be the config URI
            } else {
                String strConfigURI;

                while (!brokerURIs.isEmpty()) {
                    strConfigURI = (String)brokerURIs.remove(0);

                    try {
                        setConfigUri(new URI(strConfigURI));
                    } catch (URISyntaxException e) {
                        context.printException(e);
                        return;
                    }

                    startBroker(getConfigUri());
                }
            }

            // Prevent the main thread from exiting unless it is terminated
            // elsewhere
            waitForShutdown();
        } catch (Exception e) {
            context.printException(new RuntimeException("Failed to execute start task. Reason: " + e, e));
            throw new Exception(e);
        }
    }

    /**
     * Create and run a broker specified by the given configuration URI
     * 
     * @param configURI
     * @throws Exception
     */
    public void startBroker(URI configURI) throws Exception {
        System.out.println("Loading message broker from: " + configURI);
        BrokerService broker = BrokerFactory.createBroker(configURI);
        brokers.add(broker);
        broker.start();
    }

    /**
     * Wait for a shutdown invocation elsewhere
     * 
     * @throws Exception
     */
    protected void waitForShutdown() throws Exception {
        final boolean[] shutdown = new boolean[] {
            false
        };
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                synchronized (shutdown) {
                    shutdown[0] = true;
                    shutdown.notify();
                }
            }
        });

        // Wait for any shutdown event
        synchronized (shutdown) {
            while (!shutdown[0]) {
                try {
                    shutdown.wait();
                } catch (InterruptedException e) {
                }
            }
        }

        // Stop each broker
        for (Iterator<BrokerService> i = brokers.iterator(); i.hasNext();) {
            BrokerService broker = i.next();
            broker.stop();
        }
    }

    /**
     * Sets the current configuration URI used by the start task
     * 
     * @param uri
     */
    public void setConfigUri(URI uri) {
        configURI = uri;
    }

    /**
     * Gets the current configuration URI used by the start task
     * 
     * @return current configuration URI
     */
    public URI getConfigUri() {
        return configURI;
    }

    /**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        context.printHelp(helpFile);
    }

}
