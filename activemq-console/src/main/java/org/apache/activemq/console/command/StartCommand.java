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
import java.util.List;
import java.util.concurrent.CountDownLatch;

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

    @Override
    public String getName() {
        return "start";
    }

    @Override
    public String getOneLineDescription() {
        return "Creates and starts a broker using a configuration file, or a broker URI.";
    }

    /**
     * The default task to start a broker or a group of brokers
     * 
     * @param brokerURIs
     */
    protected void runTask(List<String> brokerURIs) throws Exception {
        URI configURI;

        while( true ) {
            final BrokerService broker;
            try {
                // If no config uri, use default setting
                if (brokerURIs.isEmpty()) {
                    configURI = new URI(DEFAULT_CONFIG_URI);
                } else {
                    configURI = new URI(brokerURIs.get(0));
                }

                System.out.println("Loading message broker from: " + configURI);
                broker = BrokerFactory.createBroker(configURI);
                broker.start();

            } catch (Exception e) {
                context.printException(new RuntimeException("Failed to execute start task. Reason: " + e, e));
                throw e;
            }

            if (!broker.waitUntilStarted()) {
                throw new Exception(broker.getStartException());
            }

            // The broker started up fine.  Now lets wait for it to stop...
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            final Thread jvmShutdownHook = new Thread() {
                public void run() {
                    try {
                        broker.stop();
                    } catch (Exception e) {
                    }
                }
            };

            Runtime.getRuntime().addShutdownHook(jvmShutdownHook);
            broker.addShutdownHook(new Runnable() {
                public void run() {
                    shutdownLatch.countDown();
                }
            });

            // The broker has stopped..
            shutdownLatch.await();
            try {
                Runtime.getRuntime().removeShutdownHook(jvmShutdownHook);
            } catch (Throwable e) {
                // may already be shutdown in progress so ignore
            }

            if( !broker.isRestartRequested() ) {
                break;
            }
            System.out.println("Restarting broker");
        }
    }

    /**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        context.printHelp(helpFile);
    }

}
