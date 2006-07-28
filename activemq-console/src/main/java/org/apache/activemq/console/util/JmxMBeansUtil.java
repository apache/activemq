/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.console.util;

import org.apache.activemq.console.filter.*;

import javax.management.remote.JMXServiceURL;
import javax.management.ObjectName;
import java.util.Set;
import java.util.List;
import java.util.Iterator;

public class JmxMBeansUtil {

    public static List getAllBrokers(JMXServiceURL jmxUrl) throws Exception {
        return (new MBeansObjectNameQueryFilter(jmxUrl)).query("Type=Broker");
    }

    public static List getBrokersByName(JMXServiceURL jmxUrl, String brokerName) throws Exception {
        return (new MBeansObjectNameQueryFilter(jmxUrl)).query("Type=Broker,BrokerName=" + brokerName);
    }

    public static List getAllBrokers(JMXServiceURL jmxUrl, Set attributes) throws Exception {
        return (new MBeansAttributeQueryFilter(jmxUrl, attributes, new MBeansObjectNameQueryFilter(jmxUrl))).query("Type=Broker");
    }

    public static List getBrokersByName(JMXServiceURL jmxUrl, String brokerName, Set attributes) throws Exception {
        return (new MBeansAttributeQueryFilter(jmxUrl, attributes, new MBeansObjectNameQueryFilter(jmxUrl))).query("Type=Broker,BrokerName=" + brokerName);
    }

    public static List queryMBeans(JMXServiceURL jmxUrl, List queryList) throws Exception {
        // If there is no query defined get all mbeans
        if (queryList==null || queryList.size()==0) {
            return createMBeansObjectNameQuery(jmxUrl).query("");

        // Parse through all the query strings
        } else {
            return createMBeansObjectNameQuery(jmxUrl).query(queryList);
        }
    }

    public static List queryMBeans(JMXServiceURL jmxUrl, List queryList, Set attributes) throws Exception {
        // If there is no query defined get all mbeans
        if (queryList==null || queryList.size()==0) {
            return createMBeansAttributeQuery(jmxUrl, attributes).query("");

        // Parse through all the query strings
        } else {
            return createMBeansAttributeQuery(jmxUrl, attributes).query(queryList);
        }
    }

    public static List queryMBeans(JMXServiceURL jmxUrl, String queryString) throws Exception {
        return createMBeansObjectNameQuery(jmxUrl).query(queryString);
    }

    public static List queryMBeans(JMXServiceURL jmxUrl, String queryString, Set attributes) throws Exception {
        return createMBeansAttributeQuery(jmxUrl, attributes).query(queryString);
    }

    public static List filterMBeansView(List mbeans, Set viewFilter) throws Exception {
        return (new PropertiesViewFilter(viewFilter, new MapTransformFilter(new StubQueryFilter(mbeans))).query(""));
    }

    public static String createQueryString(String query, String param) {
        return query.replaceAll("%1", param);
    }

    public static String createQueryString(String query, List params) {

        int count = 1;
        for (Iterator i=params.iterator();i.hasNext();) {
            query.replaceAll("%" + count++, i.next().toString());
        }

        return query;
    }

    public static QueryFilter createMBeansObjectNameQuery(JMXServiceURL jmxUrl) {
        return new WildcardToRegExTransformFilter(       // Let us be able to accept wildcard queries
            new MBeansRegExQueryFilter(                  // Use regular expressions to filter the query results
                new MBeansObjectNameQueryFilter(jmxUrl)  // Let us retrieve the mbeans object name specified by the query
            )
        );
    }

    public static QueryFilter createMBeansAttributeQuery(JMXServiceURL jmxUrl, Set attributes) {
        return new WildcardToRegExTransformFilter(                  // Let use be able to accept wildcard queries
            new MBeansRegExQueryFilter(                             // Use regular expressions to filter the query result
                new MBeansAttributeQueryFilter(jmxUrl, attributes,  // Retrieve the attributes needed
                    new MBeansObjectNameQueryFilter(jmxUrl)         // Retrieve the mbeans object name specified by the query
                )
            )
        );
    }

    public static QueryFilter createMessageQueryFilter(JMXServiceURL jmxUrl, ObjectName destName) {
        return new WildcardToMsgSelectorTransformFilter(
            new MessagesQueryFilter(jmxUrl, destName)
        );
    }

    public static List filterMessagesView(List messages, Set groupViews, Set attributeViews) throws Exception {
        return (new PropertiesViewFilter(attributeViews,
            new GroupPropertiesViewFilter(groupViews,
                new MapTransformFilter(
                    new StubQueryFilter(messages)
                )
            )
        )).query("");
    }
}
