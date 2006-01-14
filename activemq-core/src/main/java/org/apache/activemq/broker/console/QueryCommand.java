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

import javax.management.remote.JMXConnector;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import java.util.List;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.Iterator;

public class QueryCommand extends AbstractJmxCommand {
    // Predefined type=identifier query
    private static final Properties PREDEFINED_OBJNAME_QUERY = new Properties();

    static {
        PREDEFINED_OBJNAME_QUERY.setProperty("Broker",           "Type=Broker,BrokerName=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Connection",       "Type=Connection,Connection=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Connector",        "Type=Connector,ConnectorName=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("NetworkConnector", "Type=NetworkConnector,BrokerName=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Queue",            "Type=Queue,Destination=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Topic",            "Type=Topic,Destination=%1,*");
    };

    private final List queryAddObjects = new ArrayList(10);
    private final List querySubObjects = new ArrayList(10);
    private final List queryViews      = new ArrayList(10);

    protected void execute(List tokens) {
        try {
            // Connect to jmx server
            JMXConnector jmxConnector = createJmxConnector();
            MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

            // Query for the mbeans to add
            Set addMBeans = AmqJmxSupport.queryMBeans(server, queryAddObjects);

            // Query for the mbeans to sub
            if (querySubObjects.size() > 0) {
                Set subMBeans = AmqJmxSupport.queryMBeans(server, querySubObjects);
                addMBeans.removeAll(subMBeans);
            }

            for (Iterator i=addMBeans.iterator(); i.hasNext();) {
                ObjectInstance mbean = (ObjectInstance)i.next();
                AmqJmxSupport.printMBeanProp(mbean, null);
                AmqJmxSupport.printMBeanAttr(server, mbean, queryViews);
            }

            closeJmxConnector();
        } catch (Throwable e) {
            System.out.println("Failed to execute query task. Reason: " + e);
        }
    }

    protected void handleOption(String token, List tokens) throws Exception {
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
                printError("Unknown query object type: " + key);
                return;
            }
            String queryStr = AmqJmxSupport.createQueryString(predefQuery, value);
            queryAddObjects.add(queryStr);
        }

        // If token is a substractive predefined query define option
        else if (token.startsWith("-xQ")) {
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
                printError("Unknown query object type: " + key);
                return;
            }
            String queryStr = AmqJmxSupport.createQueryString(predefQuery, value);
            querySubObjects.add(queryStr);
        }

        // If token is an additive object name query option
        else if (token.startsWith("--objname")) {

            // If no object name query is specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                printError("Object name query not specified");
                return;
            }

            String queryString = (String)tokens.remove(0);
            queryAddObjects.add(queryString);
        }

        // If token is a substractive object name query option
        else if (token.startsWith("--xobjname")) {

            // If no object name query is specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                printError("Object name query not specified");
                return;
            }

            String queryString = (String)tokens.remove(0);
            querySubObjects.add(queryString);
        }

        // If token is a view option
        else if (token.startsWith("--view")) {

            // If no view specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                printError("Attributes to view not specified");
                return;
            }

            // Add the attributes to view
            Enumeration viewTokens = new StringTokenizer((String)tokens.remove(0), ",", false);
            while (viewTokens.hasMoreElements()) {
                queryViews.add(viewTokens.nextElement());
            }
        }

        // Let super class handle unknown option
        else {
            super.handleOption(token, tokens);
        }
    }

    protected void printHelp() {
        System.out.println("Task Usage: Main query [query-options]");
        System.out.println("Description: Display selected broker component's attributes and statistics.");
        System.out.println("");
        System.out.println("Query Options:");
        System.out.println("    -Q<type>=<name>               Add to the search list the specific object type matched by the defined object identifier.");
        System.out.println("    -xQ<type>=<name>              Remove from the search list the specific object type matched by the object identifier.");
        System.out.println("    --objname <query>             Add to the search list objects matched by the query similar to the JMX object name format.");
        System.out.println("    --xobjname <query>            Remove from the search list objects matched by the query similar to the JMX object name format.");
        System.out.println("    --view <attr1>,<attr2>,...    Select the specific attribute of the object to view. By default all attributes will be displayed.");
        System.out.println("    --jmxurl <url>                Set the JMX URL to connect to.");
        System.out.println("    --version                     Display the version information.");
        System.out.println("    -h,-?,--help                  Display the query broker help information.");
        System.out.println("");
        System.out.println("Examples:");
        System.out.println("    Main query");
        System.out.println("        - Print all the attributes of all registered objects (queues, topics, connections, etc).");
        System.out.println("");
        System.out.println("    Main query -QQueue=TEST.FOO");
        System.out.println("        - Print all the attributes of the queue with destination name TEST.FOO.");
        System.out.println("");
        System.out.println("    Main query -QTopic=*");
        System.out.println("        - Print all the attributes of all registered topics.");
        System.out.println("");
        System.out.println("    Main query --view EnqueueCount,DequeueCount");
        System.out.println("        - Print the attributes EnqueueCount and DequeueCount of all registered objects.");
        System.out.println("");
        System.out.println("    Main -QTopic=* --view EnqueueCount,DequeueCount");
        System.out.println("        - Print the attributes EnqueueCount and DequeueCount of all registered topics.");
        System.out.println("");
        System.out.println("    Main -QTopic=* -QQueue=* --view EnqueueCount,DequeueCount");
        System.out.println("        - Print the attributes EnqueueCount and DequeueCount of all registered topics and queues.");
        System.out.println("");
        System.out.println("    Main -QTopic=* -xQTopic=ActiveMQ.Advisory.*");
        System.out.println("        - Print all attributes of all topics except those that has a name that begins with \"ActiveMQ.Advisory\".");
        System.out.println("");
        System.out.println("    Main --objname Type=*Connect*,BrokerName=local* -xQNetworkConnector=*");
        System.out.println("        - Print all attributes of all connectors, connections excluding network connectors that belongs to the broker that begins with local.");
        System.out.println("");
        System.out.println("    Main -QQueue=* -xQQueue=????");
        System.out.println("        - Print all attributes of all queues except those that are 4 letters long.");
        System.out.println("");
    }
}
