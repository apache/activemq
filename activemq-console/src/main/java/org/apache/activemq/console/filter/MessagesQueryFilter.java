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

    private MBeanServerConnection jmxConnection;
    private ObjectName destName;

    /**
     * Create a JMS message query filter
     * 
     * @param jmxConnection - JMX connection to use
     * @param destName - object name query to retrieve the destination
     */
    public MessagesQueryFilter(MBeanServerConnection jmxConnection, ObjectName destName) {
        super(null);
        this.jmxConnection = jmxConnection;
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
        CompositeData[] messages = (CompositeData[]) jmxConnection.invoke(destName, "browse", new Object[] {}, new String[] {});
        return Arrays.asList(messages);
    }

}
