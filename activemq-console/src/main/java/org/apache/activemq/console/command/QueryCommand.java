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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.activemq.console.util.JmxMBeansUtil;

public class QueryCommand extends AbstractJmxCommand {
    // Predefined type=identifier query
    private static final Properties PREDEFINED_OBJNAME_QUERY = new Properties();

    static {
        PREDEFINED_OBJNAME_QUERY.setProperty("Broker", "Type=Broker,BrokerName=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Connection", "Type=Connection,Connection=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Connector", "Type=Connector,ConnectorName=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("NetworkConnector", "Type=NetworkConnector,BrokerName=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Queue", "Type=Queue,Destination=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Topic", "Type=Topic,Destination=%1,*");
    };

    protected String[] helpFile = new String[] {
        "Task Usage: Main query [query-options]",
        "Description: Display selected broker component's attributes and statistics.",
        "",
        "Query Options:",
        "    -Q<type>=<name>               Add to the search list the specific object type matched",
        "                                  by the defined object identifier.",
        "    -xQ<type>=<name>              Remove from the search list the specific object type",
        "                                  matched by the object identifier.",
        "    --objname <query>             Add to the search list objects matched by the query similar",
        "                                  to the JMX object name format.",
        "    --xobjname <query>            Remove from the search list objects matched by the query",
        "                                  similar to the JMX object name format.",
        "    --view <attr1>,<attr2>,...    Select the specific attribute of the object to view.",
        "                                  By default all attributes will be displayed.",
        "    --jmxurl <url>                Set the JMX URL to connect to.",
        "    --pid <pid>                   Set the pid to connect to (only on Sun JVM).",            
        "    --jmxuser <user>              Set the JMX user used for authenticating.",
        "    --jmxpassword <password>      Set the JMX password used for authenticating.",
        "    --jmxlocal                    Use the local JMX server instead of a remote one.",
        "    --version                     Display the version information.",
        "    -h,-?,--help                  Display the query broker help information.",
        "", "Examples:",
        "    query",
        "        - Print all the attributes of all registered objects queues, topics, connections, etc).",
        "",
        "    query -QQueue=TEST.FOO",
        "        - Print all the attributes of the queue with destination name TEST.FOO.",
        "",
        "    query -QTopic=*",
        "        - Print all the attributes of all registered topics.",
        "",
        "    query --view EnqueueCount,DequeueCount", 
        "        - Print the attributes EnqueueCount and DequeueCount of all registered objects.",
        "",
        "    query -QTopic=* --view EnqueueCount,DequeueCount",
        "        - Print the attributes EnqueueCount and DequeueCount of all registered topics.",
        "",
        "    query -QTopic=* -QQueue=* --view EnqueueCount,DequeueCount",
        "        - Print the attributes EnqueueCount and DequeueCount of all registered topics and",
        "          queues.",
        "",
        "    query -QTopic=* -xQTopic=ActiveMQ.Advisory.*", 
        "        - Print all attributes of all topics except those that has a name that begins",
        "          with \"ActiveMQ.Advisory\".",
        "",
        "    query --objname Type=*Connect*,BrokerName=local* -xQNetworkConnector=*",
        "        - Print all attributes of all connectors, connections excluding network connectors",
        "          that belongs to the broker that begins with local.", 
        "", 
        "    query -QQueue=* -xQQueue=????", 
        "        - Print all attributes of all queues except those that are 4 letters long.",
        "",
    };

    private final List<String> queryAddObjects = new ArrayList<String>(10);
    private final List<String> querySubObjects = new ArrayList<String>(10);
    private final Set queryViews = new HashSet(10);

    /**
     * Queries the mbeans registered in the specified JMX context
     * 
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void runTask(List<String> tokens) throws Exception {
        try {
            // Query for the mbeans to add
            List addMBeans = JmxMBeansUtil.queryMBeans(createJmxConnection(), queryAddObjects, queryViews);

            // Query for the mbeans to sub
            if (querySubObjects.size() > 0) {
                List subMBeans = JmxMBeansUtil.queryMBeans(createJmxConnection(), querySubObjects, queryViews);
                addMBeans.removeAll(subMBeans);
            }

            context.printMBean(JmxMBeansUtil.filterMBeansView(addMBeans, queryViews));

        } catch (Exception e) {
            context.printException(new RuntimeException("Failed to execute query task. Reason: " + e));
            throw new Exception(e);
        }
    }

    /**
     * Handle the -Q, -xQ, --objname, --xobjname, --view options.
     * 
     * @param token - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    protected void handleOption(String token, List<String> tokens) throws Exception {
        // If token is a additive predefined query define option
        if (token.startsWith("-Q")) {
            String key = token.substring(2);
            String value = "";
            int pos = key.indexOf("=");
            if (pos >= 0) {
                value = key.substring(pos + 1);
                key = key.substring(0, pos);
            }

            // If additive query
            String predefQuery = PREDEFINED_OBJNAME_QUERY.getProperty(key);
            if (predefQuery == null) {
                context.printException(new IllegalArgumentException("Unknown query object type: " + key));
                return;
            }
            String queryStr = JmxMBeansUtil.createQueryString(predefQuery, value);
            StringTokenizer queryTokens = new StringTokenizer(queryStr, COMMAND_OPTION_DELIMETER);
            while (queryTokens.hasMoreTokens()) {
                queryAddObjects.add(queryTokens.nextToken());
            }
        } else if (token.startsWith("-xQ")) {
            // If token is a substractive predefined query define option
            String key = token.substring(3);
            String value = "";
            int pos = key.indexOf("=");
            if (pos >= 0) {
                value = key.substring(pos + 1);
                key = key.substring(0, pos);
            }

            // If subtractive query
            String predefQuery = PREDEFINED_OBJNAME_QUERY.getProperty(key);
            if (predefQuery == null) {
                context.printException(new IllegalArgumentException("Unknown query object type: " + key));
                return;
            }
            String queryStr = JmxMBeansUtil.createQueryString(predefQuery, value);
            StringTokenizer queryTokens = new StringTokenizer(queryStr, COMMAND_OPTION_DELIMETER);
            while (queryTokens.hasMoreTokens()) {
                querySubObjects.add(queryTokens.nextToken());
            }
        } else if (token.startsWith("--objname")) {
            // If token is an additive object name query option

            // If no object name query is specified, or next token is a new
            // option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("Object name query not specified"));
                return;
            }

            StringTokenizer queryTokens = new StringTokenizer((String)tokens.remove(0), COMMAND_OPTION_DELIMETER);
            while (queryTokens.hasMoreTokens()) {
                queryAddObjects.add(queryTokens.nextToken());
            }
        } else if (token.startsWith("--xobjname")) {
            // If token is a substractive object name query option

            // If no object name query is specified, or next token is a new
            // option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("Object name query not specified"));
                return;
            }

            StringTokenizer queryTokens = new StringTokenizer((String)tokens.remove(0), COMMAND_OPTION_DELIMETER);
            while (queryTokens.hasMoreTokens()) {
                querySubObjects.add(queryTokens.nextToken());
            }
        } else if (token.startsWith("--view")) {
            // If token is a view option

            // If no view specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("Attributes to view not specified"));
                return;
            }

            // Add the attributes to view
            Enumeration viewTokens = new StringTokenizer((String)tokens.remove(0), COMMAND_OPTION_DELIMETER);
            while (viewTokens.hasMoreElements()) {
                queryViews.add(viewTokens.nextElement());
            }
        } else {
            // Let super class handle unknown option
            super.handleOption(token, tokens);
        }
    }

    /**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        context.printHelp(helpFile);
    }

}
