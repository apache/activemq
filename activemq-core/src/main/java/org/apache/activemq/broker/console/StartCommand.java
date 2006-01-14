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

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.net.URI;
import java.net.URISyntaxException;

public class StartCommand extends AbstractCommand {

    public static final String DEFAULT_CONFIG_URI   = "xbean:activemq.xml";

    private URI  configURI;
    private List brokers = new ArrayList(5);

    /**
     * The default task to start a broker or a group of brokers
     * @param brokerURIs
     */
    protected void execute(List brokerURIs) {
        try {
            // If no config uri, use default setting
            if (brokerURIs.isEmpty()) {
                setConfigUri(new URI(DEFAULT_CONFIG_URI));
                startBroker(getConfigUri());

            // Set configuration data, if available, which in this case would be the config URI
            } else {
                String strConfigURI;

                while (!brokerURIs.isEmpty()) {
                    strConfigURI = (String)brokerURIs.remove(0);

                    try {
                        setConfigUri(new URI(strConfigURI));
                    } catch (URISyntaxException e) {
                        printError("Invalid broker configuration URI: " + strConfigURI + ", reason: " + e.getMessage());
                        return;
                    }

                    startBroker(getConfigUri());
                }
            }

            // Prevent the main thread from exiting unless it is terminated elsewhere
            waitForShutdown();
        } catch (Throwable e) {
            System.out.println("Failed to execute start task. Reason: " + e);
        }
    }

    /**
     * Create and run a broker specified by the given configuration URI
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
     * @throws Exception
     */
    protected void waitForShutdown() throws Exception {
        final boolean[] shutdown = new boolean[] {false};
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                synchronized(shutdown) {
                    shutdown[0]=true;
                    shutdown.notify();
                }
            }
        });

        // Wait for any shutdown event
        synchronized(shutdown) {
            while( !shutdown[0] ) {
                try {
                    shutdown.wait();
                } catch (InterruptedException e) {
                }
            }
        }

        // Stop each broker
        for (Iterator i=brokers.iterator(); i.hasNext();) {
            BrokerService broker = (BrokerService)i.next();
            broker.stop();
        }
    }

    /**
     * Prints the help for the start broker task
     */
    protected void printHelp() {
        System.out.println("Task Usage: Main start [start-options] [uri]");
        System.out.println("Description: Creates and starts a broker using a configuration file, or a broker URI.");
        System.out.println("");
        System.out.println("Start Options:");
        System.out.println("    --extdir <dir>        Add the jar files in the directory to the classpath.");
        System.out.println("    -D<name>=<value>      Define a system property.");
        System.out.println("    --version             Display the version information.");
        System.out.println("    -h,-?,--help          Display the start broker help information.");
        System.out.println("");
        System.out.println("URI:");
        System.out.println("");
        System.out.println("    XBean based broker configuration:");
        System.out.println("");
        System.out.println("        Example: Main xbean:file:activemq.xml");
        System.out.println("            Loads the xbean configuration file from the current working directory");
        System.out.println("        Example: Main xbean:activemq.xml");
        System.out.println("            Loads the xbean configuration file from the classpath");
        System.out.println("");
        System.out.println("    URI Parameter based broker configuration:");
        System.out.println("");
        System.out.println("        Example: Main broker:(tcp://localhost:61616, tcp://localhost:5000)?useJmx=true");
        System.out.println("            Configures the broker with 2 transport connectors and jmx enabled");
        System.out.println("        Example: Main broker:(tcp://localhost:61616, network:tcp://localhost:5000)?persistent=false");
        System.out.println("            Configures the broker with 1 transport connector, and 1 network connector and persistence disabled");
        System.out.println("");
    }

    /**
     * Sets the current configuration URI used by the start task
     * @param uri
     */
    public void setConfigUri(URI uri) {
        configURI = uri;
    }

    /**
     * Gets the current configuration URI used by the start task
     * @return current configuration URI
     */
    public URI getConfigUri() {
        return configURI;
    }
}
