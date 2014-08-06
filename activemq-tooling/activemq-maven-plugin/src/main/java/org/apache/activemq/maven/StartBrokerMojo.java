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
package org.apache.activemq.maven;

import java.util.Properties;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

/**
 * Goal which starts an activemq broker.
 *
 * @goal run
 * @phase process-sources
 */
public class StartBrokerMojo extends AbstractMojo {

    /**
     * The maven project.
     *
     * @parameter property="project" default-value="${project}"
     * @required
     * @readonly
     */
    protected MavenProject project;

    /**
     * The broker configuration uri The list of currently supported URI syntaxes
     * is described <a
     * href="http://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection.html">here</a>
     *
     * @parameter property="configUri"
     *            default-value="broker:(tcp://localhost:61616)?useJmx=false&persistent=false"
     * @required
     */
    private String configUri;

    /**
     * Indicates whether to fork the broker, useful for integration tests.
     *
     * @parameter property="fork" default-value="false"
     */
    private boolean fork;

    /**
     * System properties to add
     *
     * @parameter property="systemProperties"
     */
    private Properties systemProperties;

    /**
     * Skip execution of the ActiveMQ Broker plugin if set to true
     *
     * @parameter property="skip"
     */
    private boolean skip;

    @Override
    public void execute() throws MojoExecutionException {
        if (skip) {
            getLog().info("Skipped execution of ActiveMQ Broker");
            return;
        }

        setSystemProperties();

        getLog().info("Loading broker configUri: " + configUri);
        if (XBeanFileResolver.isXBeanFile(configUri)) {
            getLog().debug("configUri before transformation: " + configUri);
            configUri = XBeanFileResolver.toUrlCompliantAbsolutePath(configUri);
            getLog().debug("configUri after transformation: " + configUri);
        }

        Broker.start(fork, configUri);

        getLog().info("Started the ActiveMQ Broker");
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
