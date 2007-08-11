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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class MBeansObjectNameQueryFilter extends AbstractQueryFilter {

    public static final String DEFAULT_JMX_DOMAIN = "org.apache.activemq";
    public static final String QUERY_EXP_PREFIX = "MBeans.QueryExp.";

    private JMXServiceURL jmxServiceUrl;

    /**
     * Creates an mbeans object name query filter that will query on the given
     * JMX Service URL
     * 
     * @param jmxUrl - JMX service URL to connect to
     * @throws MalformedURLException
     */
    public MBeansObjectNameQueryFilter(String jmxUrl) throws MalformedURLException {
        this(new JMXServiceURL(jmxUrl));
    }

    /**
     * Creates an mbeans objecet name query filter that will query on the given
     * JMX Service URL
     * 
     * @param jmxUrl - JMX service URL to connect to
     */
    public MBeansObjectNameQueryFilter(JMXServiceURL jmxUrl) {
        super(null);
        this.jmxServiceUrl = jmxUrl;
    }

    /**
     * Queries the JMX service using a mapping of keys and values to construct
     * the object name
     * 
     * @param queries - mapping of keys and values
     * @return collection of ObjectInstance that matches the query
     * @throws MalformedObjectNameException - if the given string is an invalid
     *                 object name
     * @throws IOException - if there is a problem querying the JMX context
     */
    public List query(List queries) throws MalformedObjectNameException, IOException {

        // Query all mbeans
        if (queries == null || queries.isEmpty()) {
            return queryMBeans(new ObjectName(DEFAULT_JMX_DOMAIN + ":*"), null);
        }

        // Constructs object name query
        String objNameQuery = "";
        String queryExp = "";
        for (Iterator i = queries.iterator(); i.hasNext();) {
            String key = (String)i.next();
            String val = "";
            int pos = key.indexOf("=");
            if (pos >= 0) {
                val = key.substring(pos + 1);
                key = key.substring(0, pos);
            }

            if (val.startsWith(QUERY_EXP_PREFIX)) {
                // do nothing as of the moment
            } else if (!key.equals("") && !val.equals("")) {
                objNameQuery = objNameQuery + key + "=" + val + ",";
            }
        }

        // Append * to object name
        objNameQuery = objNameQuery + "*";

        return queryMBeans(new ObjectName(DEFAULT_JMX_DOMAIN + ":" + objNameQuery), queryExp);
    }

    /**
     * Advance query that enables you to specify both the object name and the
     * query expression to use. Note: Query expression is currently unsupported.
     * 
     * @param objName - object name to use for query
     * @param queryExpStr - query expression string
     * @return set of mbeans that matches the query
     * @throws IOException - if there is a problem querying the JMX context
     */
    protected List queryMBeans(ObjectName objName, String queryExpStr) throws IOException {
        JMXConnector jmxConn = createJmxConnector();
        MBeanServerConnection server = jmxConn.getMBeanServerConnection();

        QueryExp queryExp = createQueryExp(queryExpStr);

        // Convert mbeans set to list to make it standard throughout the query
        // filter
        List mbeans = new ArrayList(server.queryMBeans(objName, queryExp));

        jmxConn.close();

        return mbeans;
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
     * @throws IOException
     */
    protected JMXConnector createJmxConnector() throws IOException {
        return JMXConnectorFactory.connect(getJmxServiceUrl());
    }

    /**
     * Creates a query expression based on the query expression string Note:
     * currently unsupported
     * 
     * @param queryExpStr - query expression string
     * @return the created query expression
     */
    protected QueryExp createQueryExp(String queryExpStr) {
        // Currently unsupported
        return null;
    }
}
