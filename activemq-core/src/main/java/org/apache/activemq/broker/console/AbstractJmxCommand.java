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

import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import java.util.List;
import java.net.MalformedURLException;
import java.io.IOException;

public abstract class AbstractJmxCommand extends AbstractCommand {
    public static final String DEFAULT_JMX_URL    = "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi";

    private JMXServiceURL jmxServiceUrl;
    private JMXConnector  jmxConnector;

    protected JMXServiceURL getJmxServiceUrl() throws Exception {
        return jmxServiceUrl;
    }

    protected void setJmxServiceUrl(JMXServiceURL jmxServiceUrl) {
        this.jmxServiceUrl = jmxServiceUrl;
    }

    protected void setJmxServiceUrl(String jmxServiceUrl) throws Exception {
        setJmxServiceUrl(new JMXServiceURL(jmxServiceUrl));
    }

    protected JMXConnector createJmxConnector() throws Exception {
        // Reuse the previous connection
        if (jmxConnector != null) {
            jmxConnector.connect();
            return jmxConnector;
        }

        // Create a new JMX connector
        if (getJmxServiceUrl() == null) {
            setJmxServiceUrl(DEFAULT_JMX_URL);
        }
        jmxConnector = JMXConnectorFactory.connect(getJmxServiceUrl());
        return jmxConnector;
    }

    protected void closeJmxConnector() {
        try {
            if (jmxConnector != null) {
                jmxConnector.close();
                jmxConnector = null;
            }
        } catch (IOException e) {
        }
    }

    protected void handleOption(String token, List tokens) throws Exception {
        // Try to handle the options first
        if (token.equals("--jmxurl")) {
            // If no jmx url specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                printError("JMX URL not specified.");
            }

            // If jmx url already specified
            if (getJmxServiceUrl() != null) {
                printError("Multiple JMX URL cannot be specified.");
                tokens.clear();
            }

            String strJmxUrl = (String)tokens.remove(0);
            try {
                this.setJmxServiceUrl(new JMXServiceURL(strJmxUrl));
            } catch (MalformedURLException e) {
                printError("Invalid JMX URL format: " + strJmxUrl);
                tokens.clear();
            }
        } else {
            // Let the super class handle the option
            super.handleOption(token, tokens);
        }
    }
}
