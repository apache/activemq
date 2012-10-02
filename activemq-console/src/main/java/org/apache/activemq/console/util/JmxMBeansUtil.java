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
package org.apache.activemq.console.util;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.console.filter.GroupPropertiesViewFilter;
import org.apache.activemq.console.filter.MBeansAttributeQueryFilter;
import org.apache.activemq.console.filter.MBeansObjectNameQueryFilter;
import org.apache.activemq.console.filter.MBeansRegExQueryFilter;
import org.apache.activemq.console.filter.MapTransformFilter;
import org.apache.activemq.console.filter.MessagesQueryFilter;
import org.apache.activemq.console.filter.PropertiesViewFilter;
import org.apache.activemq.console.filter.QueryFilter;
import org.apache.activemq.console.filter.StubQueryFilter;
import org.apache.activemq.console.filter.WildcardToMsgSelectorTransformFilter;
import org.apache.activemq.console.filter.WildcardToRegExTransformFilter;

public final class JmxMBeansUtil {

    private JmxMBeansUtil() {
    }

    public static List getAllBrokers(MBeanServerConnection jmxConnection) throws Exception {
        return (new MBeansObjectNameQueryFilter(jmxConnection)).query("Type=Broker");
    }

    public static List getBrokersByName(MBeanServerConnection jmxConnection, String brokerName) throws Exception {
        return (new MBeansObjectNameQueryFilter(jmxConnection)).query("Type=Broker,BrokerName=" + brokerName);
    }

    public static List getAllBrokers(MBeanServerConnection jmxConnection, Set attributes) throws Exception {
        return (new MBeansAttributeQueryFilter(jmxConnection, attributes, new MBeansObjectNameQueryFilter(jmxConnection))).query("Type=Broker");
    }

    public static List getBrokersByName(MBeanServerConnection jmxConnection, String brokerName, Set attributes) throws Exception {
        return (new MBeansAttributeQueryFilter(jmxConnection, attributes, new MBeansObjectNameQueryFilter(jmxConnection))).query("Type=Broker,BrokerName=" + brokerName);
    }

    public static List queryMBeans(MBeanServerConnection jmxConnection, List queryList) throws Exception {
        // If there is no query defined get all mbeans
        if (queryList == null || queryList.size() == 0) {
            return createMBeansObjectNameQuery(jmxConnection).query("");

            // Parse through all the query strings
        } else {
            return createMBeansObjectNameQuery(jmxConnection).query(queryList);
        }
    }

    public static List queryMBeans(MBeanServerConnection jmxConnection, List queryList, Set attributes) throws Exception {
        // If there is no query defined get all mbeans
        if (queryList == null || queryList.size() == 0) {
            return createMBeansAttributeQuery(jmxConnection, attributes).query("");

            // Parse through all the query strings
        } else {
            return createMBeansAttributeQuery(jmxConnection, attributes).query(queryList);
        }
    }

    public static List queryMBeans(MBeanServerConnection jmxConnection, String queryString) throws Exception {
        return createMBeansObjectNameQuery(jmxConnection).query(queryString);
    }

    public static List queryMBeans(MBeanServerConnection jmxConnection, String queryString, Set attributes) throws Exception {
        return createMBeansAttributeQuery(jmxConnection, attributes).query(queryString);
    }

    public static List filterMBeansView(List mbeans, Set viewFilter) throws Exception {
        return new PropertiesViewFilter(viewFilter, new MapTransformFilter(new StubQueryFilter(mbeans))).query("");
    }

    public static String createQueryString(String query, String param) {
        return query.replaceAll("%1", param);
    }

    public static String createQueryString(String query, List params) {
        String output = query;
        int count = 1;
        for (Iterator i = params.iterator(); i.hasNext();) {
            output = output.replaceAll("%" + count++, i.next().toString());
        }

        return output;
    }

    public static QueryFilter createMBeansObjectNameQuery(MBeanServerConnection jmxConnection) {
        // Let us be able to accept wildcard queries
        // Use regular expressions to filter the query results
        // Let us retrieve the mbeans object name specified by the query
        return new WildcardToRegExTransformFilter(new MBeansRegExQueryFilter(new MBeansObjectNameQueryFilter(jmxConnection)));
    }

    public static QueryFilter createMBeansAttributeQuery(MBeanServerConnection jmxConnection, Set attributes) {
        // Let use be able to accept wildcard queries
        // Use regular expressions to filter the query result
        // Retrieve the attributes needed
        // Retrieve the mbeans object name specified by the query
        return new WildcardToRegExTransformFilter(new MBeansRegExQueryFilter(new MBeansAttributeQueryFilter(jmxConnection, attributes, new MBeansObjectNameQueryFilter(jmxConnection))));
    }

    public static QueryFilter createMessageQueryFilter(MBeanServerConnection jmxConnection, ObjectName destName) {
        return new WildcardToMsgSelectorTransformFilter(new MessagesQueryFilter(jmxConnection, destName));
    }

    public static List filterMessagesView(List messages, Set groupViews, Set attributeViews) throws Exception {
        return (new PropertiesViewFilter(attributeViews, new GroupPropertiesViewFilter(groupViews, new MapTransformFilter(new StubQueryFilter(messages))))).query("");
    }
}
