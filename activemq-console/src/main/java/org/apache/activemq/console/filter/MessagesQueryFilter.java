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
package org.apache.activemq.console.filter;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class MessagesQueryFilter extends AbstractQueryFilter {

    private JMXServiceURL jmxServiceUrl;
    private ObjectName destName;

    /**
     * Create a JMS message query filter
     * 
     * @param jmxServiceUrl - JMX service URL to connect to
     * @param destName - object name query to retrieve the destination
     */
    public MessagesQueryFilter(JMXServiceURL jmxServiceUrl, ObjectName destName) {
        super(null);
        this.jmxServiceUrl = jmxServiceUrl;
        this.destName = destName;
    }

    /**
     * Queries the specified destination using the message selector format query
     * 
     * @param queries - message selector queries
     * @return list messages that matches the selector
     * @throws Exception
     */
    public List query(List queries) throws Exception {
        String selector = "";

        // Convert to message selector
        for (Iterator i = queries.iterator(); i.hasNext();) {
            selector = selector + "(" + i.next().toString() + ") AND ";
        }

        // Remove last AND
        if (!selector.equals("")) {
            selector = selector.substring(0, selector.length() - 5);
        }

        return queryMessages(selector);
    }

    /**
     * Query the messages of a queue destination using JMX
     * 
     * @param selector - message selector
     * @return list of messages that matches the selector
     * @throws Exception
     */
    protected List queryMessages(String selector) throws Exception {
        JMXConnector connector = createJmxConnector();
        MBeanServerConnection server = connector.getMBeanServerConnection();
        CompositeData[] messages = (CompositeData[])server.invoke(destName, "browse", new Object[] {}, new String[] {});
        connector.close();

        return Arrays.asList(messages);
    }

    /**
     * Get the JMX service URL the query is connecting to.
     * 
     * @return JMX service URL
     */
    public JMXServiceURL getJmxServiceUrl() {
        return jmxServiceUrl;
    }

    /**
     * Sets the JMX service URL the query is going to connect to.
     * 
     * @param jmxServiceUrl - new JMX service URL
     */
    public void setJmxServiceUrl(JMXServiceURL jmxServiceUrl) {
        this.jmxServiceUrl = jmxServiceUrl;
    }

    /**
     * Sets the JMX service URL the query is going to connect to.
     * 
     * @param jmxServiceUrl - new JMX service URL
     */
    public void setJmxServiceUrl(String jmxServiceUrl) throws MalformedURLException {
        setJmxServiceUrl(new JMXServiceURL(jmxServiceUrl));
    }

    /**
     * Creates a JMX connector
     * 
     * @return JMX connector
     * @throws java.io.IOException
     */
    protected JMXConnector createJmxConnector() throws IOException {
        return JMXConnectorFactory.connect(getJmxServiceUrl());
    }
}
