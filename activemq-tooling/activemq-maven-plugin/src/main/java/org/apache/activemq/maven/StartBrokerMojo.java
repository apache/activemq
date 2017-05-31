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

import org.apache.activemq.broker.TransportConnector;
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
     * Default connector property name format.
     */
    public static final String  DEFAULT_CONNECTOR_PROPERTY_NAME_FORMAT = "org.apache.activemq.connector.%s.uri";

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

    /**
     * Format of the connector URI property names.  The first argument for the format is the connector name.
     *
     * @parameter property="connectorPropertyNameFormat"
     */
    private String connectorPropertyNameFormat = DEFAULT_CONNECTOR_PROPERTY_NAME_FORMAT;

    /**
     * Broker manager used to start and stop the broker.
     */
    private MavenBrokerManager  brokerManager;

    /**
     * XBean File Resolver used to detect and transform xbean file URIs.
     */
    private XBeanFileResolver   xBeanFileResolver = new XBeanFileResolver();

    /**
     * Retrieve the Maven project for this mojo.
     *
     * @return the Maven project.
     */
    public MavenProject getProject() {
        return project;
    }

    /**
     * Set the Maven project for this mojo.
     *
     * @param project the Maven project.
     */
    public void setProject(MavenProject project) {
        this.project = project;
    }

    /**
     * Retrieve the URI used to configure the broker on startup.
     *
     * @return the configuration URI.
     */
    public String getConfigUri() {
        return configUri;
    }

    /**
     * Set the URI used to configure the broker on startup.
     *
     * @param configUri the URI used to configure the broker.
     */
    public void setConfigUri(String configUri) {
        this.configUri = configUri;
    }

    /**
     * Determine if the mojo is configured to fork a broker.
     *
     * @return true => the mojo will fork a broker (i.e. start it in the background); false => start the broker and
     * wait synchronously for it to terminate.
     */
    public boolean isFork() {
        return fork;
    }

    /**
     * Configure the mojo to run the broker asynchronously (i.e. fork) or synchronously.
     *
     * @param fork true => start the broker asynchronously; true => start the broker synchronously.
     */
    public void setFork(boolean fork) {
        this.fork = fork;
    }

    /**
     * Determine if the mojo is configured to skip the broker startup.
     *
     * @return true => the mojo will skip the broker startup; false => the mojo will start the broker normally.
     */
    public boolean isSkip() {
        return skip;
    }

    /**
     * Configure the mojo to skip or normally execute the broker startup.
     *
     * @param skip true => the mojo will skip the broker startup; false => the mojo will start the broker normally.
     */
    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    /**
     * Retrieve properties to add to the System properties on broker startup.
     *
     * @return properties to add to the System properties.
     */
    public Properties getSystemProperties() {
        return systemProperties;
    }

    /**
     * Set properties to add to the System properties on broker startup.
     *
     * @param systemProperties properties to add to the System properties.
     */
    public void setSystemProperties(Properties systemProperties) {
        this.systemProperties = systemProperties;
    }

    /**
     * Retrieve the format used to generate property names when registering connector URIs.
     *
     * @return the format used to generate property names.
     */
    public String getConnectorPropertyNameFormat() {
        return connectorPropertyNameFormat;
    }

    /**
     * Set the format used to generate property names when registering connector URIs.
     *
     * @param connectorPropertyNameFormat the new format to use when generating property names.
     */
    public void setConnectorPropertyNameFormat(String connectorPropertyNameFormat) {
        this.connectorPropertyNameFormat = connectorPropertyNameFormat;
    }

    /**
     * Retrieve the manager used to create and retain the started broker.
     *
     * @return the manager.
     */
    public MavenBrokerManager getBrokerManager() {
        return brokerManager;
    }

    /**
     * Set the manager used to create and retain the started broker.
     *
     * @param brokerManager the new manager to use.
     */
    public void setBrokerManager(MavenBrokerManager brokerManager) {
        this.brokerManager = brokerManager;
    }

    /**
     * Retrieve the XBeanFileResolver used to detect and transform XBean URIs.
     *
     * @return the XBeanFileResolver used.
     */
    public XBeanFileResolver getxBeanFileResolver() {
        return xBeanFileResolver;
    }

    /**
     * Set the XBeanFileResolver to use when detecting and transforming XBean URIs.
     *
     * @param xBeanFileResolver the XBeanFileResolver to use.
     */
    public void setxBeanFileResolver(XBeanFileResolver xBeanFileResolver) {
        this.xBeanFileResolver = xBeanFileResolver;
    }

    /**
     * Perform the mojo operation, which starts the ActiveMQ broker unless configured to skip it.  Also registers the
     * connector URIs in the maven project properties on startup, which enables the use of variable substitution in
     * the pom.xml file to determine the address of the connector using the standard ${...} syntax.
     *
     * @throws MojoExecutionException
     */
    @Override
    public void execute() throws MojoExecutionException {
        if (skip) {
            getLog().info("Skipped execution of ActiveMQ Broker");
            return;
        }

        addActiveMQSystemProperties();

        getLog().info("Loading broker configUri: " + configUri);
        if (this.xBeanFileResolver.isXBeanFile(configUri)) {
            getLog().debug("configUri before transformation: " + configUri);
            configUri = this.xBeanFileResolver.toUrlCompliantAbsolutePath(configUri);
            getLog().debug("configUri after transformation: " + configUri);
        }

        this.useBrokerManager().start(fork, configUri);

        //
        // Register the transport connector URIs in the Maven project.
        //
        this.registerTransportConnectorUris();

        getLog().info("Started the ActiveMQ Broker");
    }

    /**
     * Set system properties
     */
    protected void addActiveMQSystemProperties() {
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

    /**
     * Register all of the broker's transport connector URIs in the Maven project as properties.  Property names are
     * formed from the connectorPropertyNameFormat using String.format(), with the connector name as the one and only
     * argument.  The value of each property is that returned by getPublishableConnectString().
     */
    protected void  registerTransportConnectorUris () {
        Properties props = project.getProperties();

        //
        // Loop through all of the connectors, adding each.
        //
        for ( TransportConnector oneConnector : this.useBrokerManager().getBroker().getTransportConnectors() ) {
            try {
                //
                // Format the name of the property and grab the value.
                //
                String propName = String.format(this.connectorPropertyNameFormat, oneConnector.getName());
                String value    = oneConnector.getPublishableConnectString();

                getLog().debug("setting transport connector URI property: propertyName=\"" + propName +
                               "\"; value=\"" + value + "\"");

                //
                // Set the property.
                //
                props.setProperty(propName, value);
            } catch (Exception exc) {
                //
                // Warn of the issue and continue.
                //
                getLog().warn("error on obtaining broker connector uri; connector=" + oneConnector, exc);
            }
        }
    }

    /**
     * Use the configured broker manager, if defined; otherwise, use the default broker manager.
     *
     * @return the broker manager to use.
     */
    protected MavenBrokerManager    useBrokerManager () {
        if ( this.brokerManager == null ) {
            this.brokerManager = new MavenBrokerSingletonManager();
        }

        return  this.brokerManager;
    }
}
