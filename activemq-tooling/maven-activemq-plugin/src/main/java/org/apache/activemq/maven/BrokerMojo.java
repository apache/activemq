package org.apache.activemq.maven;

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

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

import java.util.Properties;

/**
 * Goal which starts an activemq broker.
 *
 * @goal run
 * @phase process-sources
 */
public class BrokerMojo extends AbstractMojo {
    /**
	 * The maven project.
	 *
	 * @parameter expression="${project}"
	 * @required
	 * @readonly
	 */
	protected MavenProject project;

    /**
     * The broker configuration uri
     *
     * The list of currently supported URI syntaxes is described
     * <a href="http://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection.html">here</a>
     *
     * @parameter expression="${configUri}" default-value="broker:(tcp://localhost:61616)?useJmx=false&persistent=false"
     * @required
     */
    private String configUri;

    /**
     * Indicates whether to fork the broker, useful for integration tests.
     *
     * @parameter expression="${fork}" default-value="false"
     */
    private boolean fork;

    /**
     * System properties to add
     * @parameter expression="${systemProperties}"
     */
    private Properties systemProperties;

    public void execute() throws MojoExecutionException {
        try {
            setSystemProperties();
            getLog().info("Loading broker configUri: " + configUri);

            final BrokerService broker = BrokerFactory.createBroker(configUri);
            if (fork) {
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            broker.start();
                            waitForShutdown(broker);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            } else {
                broker.start();
                waitForShutdown(broker);
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to start ActiveMQ Broker", e);
        }
    }

    /**
     * Wait for a shutdown invocation elsewhere
     * @throws Exception
     */
    protected void waitForShutdown(BrokerService broker) throws Exception {
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

        // Stop broker
        broker.stop();
    }

    /**
     * Set system properties
     */
    protected void setSystemProperties() {
        // Set the default properties
        System.setProperty("activemq.base", project.getBuild().getDirectory() + "/");
        System.setProperty("activemq.home", project.getBuild().getDirectory() + "/");
        System.setProperty("org.apache.activemq.UseDedicatedTaskRunner", "true");
        System.setProperty("org.apache.activemq.default.directory.prefix", project.getBuild().getDirectory() + "/");
        System.setProperty("derby.system.home", project.getBuild().getDirectory() + "/");
        System.setProperty("derby.storage.fileSyncTransactionLog", "true");

        // Overwrite any custom properties
        System.getProperties().putAll(systemProperties);
    }
}
