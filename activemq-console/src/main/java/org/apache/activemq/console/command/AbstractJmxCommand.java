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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public abstract class AbstractJmxCommand extends AbstractCommand {
    public static final String DEFAULT_JMX_URL    = "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi";

    private JMXServiceURL jmxServiceUrl;
    private JMXConnector  jmxConnector;

    /**
     * Get the current specified JMX service url.
     * @return JMX service url
     */
    protected JMXServiceURL getJmxServiceUrl() {
        return jmxServiceUrl;
    }

    /**
     * Get the current JMX service url being used, or create a default one if no JMX service url has been specified.
     * @return JMX service url
     * @throws MalformedURLException
     */
    protected JMXServiceURL useJmxServiceUrl() throws MalformedURLException {
        if (getJmxServiceUrl() == null) {
            setJmxServiceUrl(DEFAULT_JMX_URL);
        }

        return getJmxServiceUrl();
    }

    /**
     * Sets the JMX service url to use.
     * @param jmxServiceUrl - new JMX service url to use
     */
    protected void setJmxServiceUrl(JMXServiceURL jmxServiceUrl) {
        this.jmxServiceUrl = jmxServiceUrl;
    }

    /**
     * Sets the JMX service url to use.
     * @param jmxServiceUrl - new JMX service url to use
     * @throws MalformedURLException
     */
    protected void setJmxServiceUrl(String jmxServiceUrl) throws MalformedURLException {
        setJmxServiceUrl(new JMXServiceURL(jmxServiceUrl));
    }

    /**
     * Create a JMX connector using the current specified JMX service url. If there is an existing connection,
     * it tries to reuse this connection.
     * @return created JMX connector
     * @throws IOException
     */
    protected JMXConnector createJmxConnector() throws IOException {
        // Reuse the previous connection
        if (jmxConnector != null) {
            jmxConnector.connect();
            return jmxConnector;
        }

        // Create a new JMX connector
        jmxConnector = JMXConnectorFactory.connect(useJmxServiceUrl());
        return jmxConnector;
    }

    /**
     * Close the current JMX connector
     */
    protected void closeJmxConnector() {
        try {
            if (jmxConnector != null) {
                jmxConnector.close();
                jmxConnector = null;
            }
        } catch (IOException e) {
        }
    }

    /**
     * Handle the --jmxurl option.
     * @param token - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    protected void handleOption(String token, List<String> tokens) throws Exception {
        // Try to handle the options first
        if (token.equals("--jmxurl")) {
            // If no jmx url specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("JMX URL not specified."));
            }

            // If jmx url already specified
            if (getJmxServiceUrl() != null) {
                context.printException(new IllegalArgumentException("Multiple JMX URL cannot be specified."));
                tokens.clear();
            }

            String strJmxUrl = (String)tokens.remove(0);
            try {
                this.setJmxServiceUrl(new JMXServiceURL(strJmxUrl));
            } catch (MalformedURLException e) {
                context.printException(e);
                tokens.clear();
            }
        } else {
            // Let the super class handle the option
            super.handleOption(token, tokens);
        }
    }
}
