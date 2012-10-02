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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;

public class MBeansObjectNameQueryFilter extends AbstractQueryFilter {

    public static final String DEFAULT_JMX_DOMAIN = "org.apache.activemq";
    public static final String QUERY_EXP_PREFIX = "MBeans.QueryExp.";

    private MBeanServerConnection jmxConnection;

    /**
     * Creates an mbeans object name query filter that will query on the given
     * JMX connection
     * 
     * @param jmxConnection - JMX connection to use
     */
    public MBeansObjectNameQueryFilter(MBeanServerConnection jmxConnection) {
        super(null);
        this.jmxConnection = jmxConnection;
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
        QueryExp queryExp = createQueryExp(queryExpStr);

        // Convert mbeans set to list to make it standard throughout the query
        // filter
        List mbeans = new ArrayList(jmxConnection.queryMBeans(objName, queryExp));

        return mbeans;
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
