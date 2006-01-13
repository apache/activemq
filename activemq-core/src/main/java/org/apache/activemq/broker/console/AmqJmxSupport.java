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

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.MBeanAttributeInfo;
import javax.management.ObjectInstance;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class AmqJmxSupport {
    public static final String DEFAULT_JMX_DOMAIN = "org.apache.activemq";

    public static Set getAllBrokers(MBeanServerConnection server) throws Exception {
        return queryMBeans(server, new ObjectName(DEFAULT_JMX_DOMAIN + ":Type=Broker,*"));
    }

    public static Set getBrokers(MBeanServerConnection server, String brokerName) throws Exception {
        return queryMBeans(server, "Type=Broker,BrokerName=" + brokerName + ",*");
    }

    public static Set queryMBeans(MBeanServerConnection server, List queryList) throws Exception {
        Set mbeans;

        // If there is no query defined get all mbeans
        if (queryList==null || queryList.size()==0) {
            ObjectName queryName = new ObjectName(DEFAULT_JMX_DOMAIN + ":*");

            mbeans = queryMBeans(server, queryName);

        // Parse through all the query strings
        } else {
            mbeans = new HashSet();

            for (Iterator i=queryList.iterator(); i.hasNext();) {
                String queryStr = (String)i.next();
                mbeans.addAll(queryMBeans(server, queryStr));
            }
        }

        return mbeans;
    }

    public static Set queryMBeans(MBeanServerConnection server, String queryString) throws Exception {
        // Transform string to support regex filtering
        List regexProp = new ArrayList();
        queryString = transformWildcardQueryToObjectName(queryString, regexProp);

        ObjectName queryName = new ObjectName(DEFAULT_JMX_DOMAIN + ":" + queryString);

        return filterUsingRegEx(queryMBeans(server, queryName), regexProp);
    }

    public static Set queryMBeans(MBeanServerConnection server, ObjectName objName) throws Exception {
        return server.queryMBeans(objName, null);
    }

    public static Map queryMBeanAttrs(MBeanServerConnection server, ObjectName mbeanObjName, List attrView) throws Exception {
        Map attr = new HashMap();
        MBeanAttributeInfo[] attrs = server.getMBeanInfo(mbeanObjName).getAttributes();

        // If the mbean has no attribute, print a no attribute message
        if (attrs.length == 0) {
            return null;
        }

        // If there is no view specified, get all attributes
        if (attrView == null || attrView.isEmpty()) {
            for (int i=0; i<attrs.length; i++) {
                Object attrVal = server.getAttribute(mbeanObjName, attrs[i].getName());
                attr.put(attrs[i].getName(), attrVal);
            }
            return attr;
        }

        // Get attributes specified by view
        for (int i=0; i<attrs.length; i++) {
            if (attrView.contains(attrs[i].getName())) {
                Object attrVal = server.getAttribute(mbeanObjName, attrs[i].getName());
                attr.put(attrs[i].getName(), attrVal);
            }
        }

        return attr;
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

    public static void printBrokerList(Set brokerList) {
        Object[] brokerArray = brokerList.toArray();

        System.out.println("List of available brokers:");
        for (int i=0; i<brokerArray.length; i++) {
            String brokerName = ((ObjectInstance)brokerArray[i]).getObjectName().getKeyProperty("BrokerName");
            System.out.println("    " + (i+1) + ".) " + brokerName);
        }
    }

    public static void printMBeanProp(ObjectInstance mbean, List propView) {
        // Filter properties to print
        if (propView != null && !propView.isEmpty()) {
            Map mbeanProps = mbean.getObjectName().getKeyPropertyList();
            for (Iterator i=propView.iterator(); i.hasNext();) {
                Object key = i.next();
                Object val = mbeanProps.get(key);

                if (val != null) {
                    System.out.println("MBean " + key + ": " + val);
                }
            }

        // Print all properties
        } else {
            Map mbeanProps = mbean.getObjectName().getKeyPropertyList();
            for (Iterator i=mbeanProps.keySet().iterator(); i.hasNext();) {
                Object key = i.next();
                Object val = mbeanProps.get(key);

                System.out.println("MBean " + key + ": " + val);
            }
        }
    }

    public static void printMBeanAttr(MBeanServerConnection server, ObjectInstance mbean, List attrView) {
        try {
            Map attrList = queryMBeanAttrs(server, mbean.getObjectName(), attrView);

            // If the mbean has no attribute, print a no attribute message
            if (attrList == null) {
                System.out.println("    MBean has no attributes.");
                System.out.println();
                return;
            }

            // If the mbean's attributes did not match any of the view, display a message
            if (attrList.isEmpty()) {
                System.out.println("    View did not match any of the mbean's attributes.");
                System.out.println("");
                return;
            }

            // Display mbean attributes

            // If attrView is available, use it. This allows control over the display order
            if (attrView != null && !attrView.isEmpty()) {
                for (Iterator i=attrView.iterator(); i.hasNext();) {
                    Object key = i.next();
                    Object val = attrList.get(key);

                    if (val != null) {
                        System.out.println("    " + key + " = " + attrList.get(key));
                    }
                }

            // If attrView is not available, print all attributes
            } else {
                for (Iterator i=attrList.keySet().iterator(); i.hasNext();) {
                    Object key = i.next();
                    System.out.println("    " + key + " = " + attrList.get(key));
                }
            }
            System.out.println("");

        } catch (Exception e) {
            System.out.println("Failed to print mbean attributes. Reason: " + e.getMessage());
        }
    }

    private static String transformWildcardQueryToObjectName(String query, List regExMap) throws Exception {
        if (regExMap==null) {
            regExMap = new ArrayList();
        }

        StringBuffer newQueryStr = new StringBuffer();

        for (StringTokenizer tokenizer = new StringTokenizer(query, ","); tokenizer.hasMoreTokens();) {
            String token = tokenizer.nextToken();

            // Get key value pair
            String key = token;
            String value = "";
            int pos = key.indexOf("=");
            if (pos >= 0) {
                value = key.substring(pos + 1);
                key = key.substring(0, pos);
            }

            // Check if value is a wildcard query
            if ((value.indexOf("*") >= 0) || (value.indexOf("?") >= 0)) {
                // If value is a wildcard query, convert to regex
                // and remove the object name query to ensure it selects all
                regExMap.add(Pattern.compile("(.*)(" + key + "=)(" + transformWildcardQueryToRegEx(value) + ")(,)(.*)"));

            // Re-add valid key value pair. Remove all * property and just add one at the end.
            } else if ((key != "") && (value != "")) {
                newQueryStr.append(key + "=" + value + ",");
            }
        }

        newQueryStr.append("*");
        return newQueryStr.toString();
    }

    private static String transformWildcardQueryToRegEx(String query) {
        query = query.replaceAll("[.]", "\\\\."); // Escape all dot characters. From (.) to (\.)
        query = query.replaceAll("[?]", ".");
        query = query.replaceAll("[*]", ".*?"); // Use reluctant quantifier

        return query;
    }

    private static Set filterUsingRegEx(Set mbeans, List regexProp) {
        // No regular expressions filtering needed
        if (regexProp==null || regexProp.isEmpty()) {
            return mbeans;
        }

        Set filteredMbeans = new HashSet();

        // Get each bean to filter
        for (Iterator i=mbeans.iterator(); i.hasNext();) {
            ObjectInstance mbeanInstance = (ObjectInstance)i.next();
            String mbeanName = mbeanInstance.getObjectName().getKeyPropertyListString();

            // Ensure name ends with ,* to guarantee correct parsing behavior
            if (!mbeanName.endsWith(",*")) {
                mbeanName = mbeanName + ",*";
            }
            boolean match = true;

            // Match the object name to each regex
            for (Iterator j=regexProp.iterator(); j.hasNext();) {
                Pattern p = (Pattern)j.next();

                if (!p.matcher(mbeanName).matches()) {
                    match = false;
                    break;
                }
            }

            // If name of mbean matches all regex pattern, add it
            if (match) {
                filteredMbeans.add(mbeanInstance);
            }
        }

        return filteredMbeans;
    }
}
